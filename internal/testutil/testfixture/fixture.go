package testfixture

import (
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/config"
	"github.com/go-git/go-git/v6/plumbing"

	sourcecache "github.com/home-operations/flate/internal/engine/source/cache"
)

const (
	inputsDirectory          = "inputs"
	defaultConcurrencyPerCPU = 4
)

// Fixture isolates materialized state from the immutable disk scenario.
type Fixture struct {
	Plan               Case
	Repo               string
	Inputs             string
	Home               string
	Responses          []Response
	root               string
	dir                string
	schema             *scenario
	commits            map[string]plumbing.Hash
	repositories       map[string]*git.Repository
	remoteRepositories map[string]string
	origins            map[string]string
	commitOrigins      map[string]map[string]string
	registries         map[string]string
	prepared           *Fixture
	firstReady         bool
	runs               int
}

// Scenario owns prepared input for the lifetime of its enclosing test.
type Scenario struct {
	dir      string
	schema   *scenario
	prepared *Fixture
}

// Prepare builds reusable scenario input before the enclosing test opens its cases.
func Prepare(tb testing.TB, scenarioName string) *Scenario {
	tb.Helper()
	return prepare(tb, scenarioPath(tb, scenarioName))
}

// Materialize locates scenarios independently of the current working directory.
func Materialize(tb testing.TB, scenarioName, variant string) *Fixture {
	tb.Helper()
	return materialize(tb, scenarioPath(tb, scenarioName), variant)
}

func scenarioPath(tb testing.TB, scenarioName string) string {
	tb.Helper()
	if !local(scenarioName) {
		tb.Fatal("scenario path escapes testdata")
	}
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		tb.Fatal("cannot locate fixture source")
	}
	return filepath.Join(filepath.Dir(file), "..", "..", "..", "testdata", scenarioName)
}

func materialize(tb testing.TB, dir, variant string) *Fixture {
	tb.Helper()
	return loadScenario(tb, dir).Materialize(tb, variant)
}

func prepare(tb testing.TB, dir string) *Scenario {
	tb.Helper()
	sc := loadScenario(tb, dir)
	s := sc.schema
	if len(s.Commits) > 0 && s.canShareHistory() {
		sc.prepared = sc.newFixture(tb, s)
		sc.prepared.build(tb)
	}
	return sc
}

func loadScenario(tb testing.TB, dir string) *Scenario {
	tb.Helper()
	s, err := load(dir)
	must(tb, err)
	return &Scenario{dir: dir, schema: s}
}

func (s *scenario) canShareHistory() bool {
	pathDependent := func(value string) bool {
		return strings.Contains(value, "@@repo@@") || strings.Contains(value, "@@baseline@@") ||
			strings.Contains(value, "@@inputs@@")
	}
	for _, r := range s.Repositories {
		if slices.ContainsFunc(r.Remotes, func(r remote) bool { return pathDependent(r.URL) }) {
			return false
		}
	}
	if slices.ContainsFunc(s.Tokens, func(t token) bool { return pathDependent(t.Value) }) {
		return false
	}
	for _, o := range s.Overlays {
		// Cache seeds need the case cache, and chmod can make a tree impossible to copy.
		if slices.ContainsFunc(o.Operations, func(op operation) bool {
			return op.CacheFailure != nil || op.Chmod != nil
		}) && slices.ContainsFunc(s.Commits, func(c commit) bool { return c.Overlay == o.Name }) {
			return false
		}
	}
	return true
}

// Materialize lends the immutable scenario data and gives the case its own writable files.
func (s *Scenario) Materialize(tb testing.TB, variant string) *Fixture {
	tb.Helper()
	for _, plan := range s.schema.Cases {
		if plan.Name != variant {
			continue
		}
		f := s.newFixture(tb, s.schema)
		f.Plan = plan
		f.prepared = s.prepared
		f.firstReady = true
		f.restore(tb)
		return f
	}
	tb.Fatalf("unknown case %q", variant)
	return nil
}

func (s *Scenario) newFixture(tb testing.TB, schema *scenario) *Fixture {
	tb.Helper()
	root := tb.TempDir()
	return &Fixture{
		root:      root,
		dir:       s.dir,
		schema:    schema,
		Repo:      filepath.Join(root, "repo"),
		Inputs:    filepath.Join(root, inputsDirectory),
		Home:      filepath.Join(root, "home"),
		Responses: schema.Transport,
	}
}

// StartReplay uses the initial preparation if no edit or replay consumed it.
func (f *Fixture) StartReplay(tb testing.TB) {
	tb.Helper()
	if !f.firstReady {
		f.Restore(tb)
		return
	}
	f.beginReplay(tb)
}

// Restore preserves caches only when the case explicitly requests reuse.
func (f *Fixture) Restore(tb testing.TB) {
	tb.Helper()
	f.beginReplay(tb)
	f.restore(tb)
}

func (f *Fixture) beginReplay(tb testing.TB) {
	tb.Helper()
	f.firstReady = false
	if f.runs == 0 || f.Plan.Cache.Repeat == "reset" {
		for _, p := range []string{"caches", "home"} {
			must(tb, os.RemoveAll(filepath.Join(f.root, p)))
		}
	}
	must(tb, os.MkdirAll(f.Home, 0o700))
	f.runs++
}

func (f *Fixture) restore(tb testing.TB) {
	tb.Helper()
	if f.prepared == nil {
		f.build(tb)
	} else {
		f.copyPrepared(tb)
	}
	for i := range f.Plan.Setup {
		f.apply(tb, &f.Plan.Setup[i])
	}
}

func (f *Fixture) copyPrepared(tb testing.TB) {
	tb.Helper()
	template := f.prepared
	f.origins = rebaseOrigins(template.origins, template.root, f.root)
	f.commitOrigins = make(map[string]map[string]string, len(template.commitOrigins))
	for name, origins := range template.commitOrigins {
		f.commitOrigins[name] = rebaseOrigins(origins, template.root, f.root)
	}
	f.commits = maps.Clone(template.commits)
	f.remoteRepositories = template.remoteRepositories
	f.repositories = make(map[string]*git.Repository, len(f.schema.Repositories))
	for _, r := range f.schema.Repositories {
		target := filepath.Join(f.root, r.Name)
		must(tb, os.RemoveAll(target))
		must(tb, copyTree(filepath.Join(template.root, r.Name), target, nil))
		repo, err := git.PlainOpen(target)
		must(tb, err)
		f.repositories[r.Name] = repo
	}
	must(tb, os.RemoveAll(f.Inputs))
	must(tb, copyTree(template.Inputs, f.Inputs, nil))
}

func rebaseOrigins(origins map[string]string, from, to string) map[string]string {
	result := make(map[string]string, len(origins))
	for path, source := range origins {
		result[to+strings.TrimPrefix(path, from)] = source
	}
	return result
}

func (f *Fixture) build(tb testing.TB) {
	tb.Helper()
	f.origins = make(map[string]string)
	f.commitOrigins = make(map[string]map[string]string)
	f.commits = make(map[string]plumbing.Hash)
	f.remoteRepositories = make(map[string]string)
	f.repositories = make(map[string]*git.Repository)
	for _, r := range f.schema.Repositories {
		target := filepath.Join(f.root, r.Name)
		must(tb, os.RemoveAll(target))
		f.copyRepository(tb, r, target)
		repo, err := git.PlainInit(target, false)
		must(tb, err)
		cfg, err := repo.Config()
		must(tb, err)
		cfg.Commit.GpgSign = config.OptBoolFalse
		must(tb, repo.SetConfig(cfg))
		f.repositories[r.Name] = repo
		for _, remote := range r.Remotes {
			url := f.Resolve(tb, remote.URL, "")
			_, err := repo.CreateRemote(&config.RemoteConfig{Name: remote.Name, URLs: []string{url}})
			must(tb, err)
			if _, exists := f.remoteRepositories[url]; !exists {
				f.remoteRepositories[url] = r.Name
			}
		}
	}
	must(tb, os.RemoveAll(f.Inputs))
	must(tb, os.MkdirAll(f.Inputs, 0o700))
	if f.schema.Inputs != "" {
		must(tb, copyTree(filepath.Join(f.dir, f.schema.Inputs), f.Inputs, f.origins))
	}
	for i := range f.schema.Commits {
		f.createCommit(tb, &f.schema.Commits[i])
	}
}

// Edit permits only the operation declared by an edit step.
func (f *Fixture) Edit(tb testing.TB, step Step) {
	tb.Helper()
	if step.Edit == nil {
		tb.Fatal("step is not an edit")
	}
	f.firstReady = false
	f.apply(tb, step.Edit)
}

// CachePath must not reset or create caches while resolving expectations.
func (f *Fixture) CachePath(name string) string {
	return filepath.Join(f.root, "caches", name)
}

// Cache resets only the cache selected by the invocation.
func (f *Fixture) Cache(tb testing.TB, inv *Invocation) string {
	tb.Helper()
	name := inv.Cache.Name
	if name == "" {
		name = "default"
	}
	target := f.CachePath(name)
	if inv.Cache.Reset {
		must(tb, os.RemoveAll(target))
	}
	must(tb, os.MkdirAll(target, 0o700))
	return target
}

// Resolve rejects commit tokens that the scenario does not declare.
func (f *Fixture) Resolve(tb testing.TB, value, cache string) string {
	tb.Helper()
	reason := sourcecache.RenderPersistenceDisabledReason()
	value = strings.NewReplacer("@@repo@@", f.Repo, "@@baseline@@", filepath.Join(f.root, "baseline"), "@@inputs@@", f.Inputs, "@@cache@@", cache,
		"<CONCURRENCY_DEFAULT>", strconv.Itoa(runtime.NumCPU()*defaultConcurrencyPerCPU),
		"<PERSISTENCE_REASON>", reason).
		Replace(value)
	branch := "${1}"
	if reason != "" {
		branch = "${2}"
	}
	value = regexp.MustCompile(`(?s)<PERSISTENCE:([^|<>]*)\|([^|<>]*)>`).ReplaceAllString(value, branch)
	for strings.Contains(value, "@@commit:") {
		start := strings.Index(value, "@@commit:")
		end := strings.Index(value[start+len("@@commit:"):], "@@")
		if end < 0 {
			tb.Fatal("unterminated commit token")
		}
		end += start + len("@@commit:")
		name := value[start+len("@@commit:") : end]
		hash, ok := f.commits[name]
		if !ok {
			tb.Fatalf("unknown commit token %q", name)
		}
		value = value[:start] + hash.String() + value[end+2:]
	}
	return value
}

// Read rejects paths outside the disk scenario.
func (f *Fixture) Read(tb testing.TB, path, cache string) string {
	tb.Helper()
	if !local(path) {
		tb.Fatal("fixture read escapes scenario")
	}
	body, err := os.ReadFile(filepath.Join(f.dir, path))
	must(tb, err)
	return f.Resolve(tb, string(body), cache)
}

func (f *Fixture) copyRepository(tb testing.TB, r repository, target string) {
	tb.Helper()
	f.forgetOrigins(target)
	if r.Directory == "" {
		must(tb, os.MkdirAll(target, 0o700))
		return
	}
	must(tb, copyTree(filepath.Join(f.dir, r.Directory), target, f.origins))
}

func copyTree(source, target string, origins map[string]string) error {
	err := filepath.WalkDir(source, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("walk fixture: %w", err)
		}
		rel, err := filepath.Rel(source, path)
		if err != nil {
			return fmt.Errorf("fixture path: %w", err)
		}
		dest := filepath.Join(target, rel)
		if d.Type()&os.ModeSymlink != 0 {
			return copyFixtureLink(path, dest, origins == nil)
		}
		info, err := d.Info()
		if err != nil {
			return fmt.Errorf("fixture mode: %w", err)
		}
		if d.IsDir() {
			if origins != nil {
				return os.MkdirAll(dest, 0o700)
			}
			return os.MkdirAll(dest, info.Mode().Perm())
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read fixture: %w", err)
		}
		if err := os.WriteFile(dest, data, info.Mode().Perm()); err != nil {
			return fmt.Errorf("write fixture: %w", err)
		}
		if origins != nil {
			origins[dest] = path
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("copy fixture: %w", err)
	}
	return nil
}

func copyFixtureLink(source, target string, prepared bool) error {
	// Copy only links that the fixture created from validated operations.
	if !prepared {
		return fmt.Errorf("fixture symlink %s is forbidden", source)
	}
	link, err := os.Readlink(source)
	if err != nil {
		return fmt.Errorf("read fixture symlink: %w", err)
	}
	if err := os.Symlink(link, target); err != nil {
		return fmt.Errorf("write fixture symlink: %w", err)
	}
	return nil
}

func must(tb testing.TB, err error) {
	tb.Helper()
	if err != nil {
		tb.Fatal(err)
	}
}

func (f *Fixture) forgetOrigins(path string) {
	for current := range f.origins {
		if current == path || strings.HasPrefix(current, path+string(filepath.Separator)) {
			delete(f.origins, current)
		}
	}
}

func (f *Fixture) repositoryOrigins(name string) map[string]string {
	origins := make(map[string]string)
	prefix := filepath.Join(f.root, name) + string(filepath.Separator)
	for path, source := range f.origins {
		if strings.HasPrefix(path, prefix) {
			origins[path] = source
		}
	}
	return origins
}

func (f *Fixture) useCommitOrigins(repository, name string) {
	f.forgetOrigins(filepath.Join(f.root, repository))
	maps.Copy(f.origins, f.commitOrigins[name])
}
