package testfixture

import (
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/config"
	"github.com/go-git/go-git/v6/plumbing"

	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

const (
	inputsDirectory          = "inputs"
	defaultConcurrencyPerCPU = 4
)

// Fixture isolates materialized state from the immutable disk scenario.
type Fixture struct {
	Plan          Case
	Repo          string
	Inputs        string
	Home          string
	Responses     []Response
	root          string
	dir           string
	schema        *scenario
	commits       map[string]plumbing.Hash
	repositories  map[string]*git.Repository
	origins       map[string]string
	commitOrigins map[string]map[string]string
	registries    map[string]string
	runs          int
}

// Materialize locates scenarios independently of the current working directory.
func Materialize(tb testing.TB, scenarioName, variant string) *Fixture {
	tb.Helper()
	if !local(scenarioName) {
		tb.Fatal("scenario path escapes testdata")
	}
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		tb.Fatal("cannot locate fixture source")
	}
	return materialize(tb, filepath.Join(filepath.Dir(file), "..", "..", "testdata", scenarioName), variant)
}

func materialize(tb testing.TB, dir, variant string) *Fixture {
	tb.Helper()
	s, err := load(dir)
	if err != nil {
		tb.Fatal(err)
	}
	var plan *Case
	for i := range s.Cases {
		if s.Cases[i].Name == variant {
			plan = &s.Cases[i]
			break
		}
	}
	if plan == nil {
		tb.Fatalf("unknown case %q", variant)
	}
	root := tb.TempDir()
	f := &Fixture{
		Plan:      *plan,
		root:      root,
		dir:       dir,
		schema:    s,
		Repo:      filepath.Join(root, "repo"),
		Inputs:    filepath.Join(root, inputsDirectory),
		Home:      filepath.Join(root, "home"),
		Responses: s.Transport,
	}
	f.restore(tb)
	return f
}

// Restore preserves caches only when the case explicitly requests reuse.
func (f *Fixture) Restore(tb testing.TB) {
	tb.Helper()
	f.restore(tb)
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
	f.origins = make(map[string]string)
	f.commitOrigins = make(map[string]map[string]string)
	f.commits = make(map[string]plumbing.Hash)
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
			_, err := repo.CreateRemote(
				&config.RemoteConfig{Name: remote.Name, URLs: []string{f.Resolve(tb, remote.URL, "")}},
			)
			must(tb, err)
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
	for i := range f.Plan.Setup {
		f.apply(tb, &f.Plan.Setup[i])
	}
}

// Edit permits only the operation declared by an edit step.
func (f *Fixture) Edit(tb testing.TB, step Step) {
	tb.Helper()
	if step.Edit == nil {
		tb.Fatal("step is not an edit")
	}
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
		if d.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("fixture symlink %s is forbidden", path)
		}
		dest := filepath.Join(target, rel)
		if d.IsDir() {
			return os.MkdirAll(dest, 0o700)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read fixture: %w", err)
		}
		info, err := d.Info()
		if err != nil {
			return fmt.Errorf("fixture mode: %w", err)
		}
		if err := os.WriteFile(dest, data, info.Mode().Perm()); err != nil {
			return fmt.Errorf("write fixture: %w", err)
		}
		origins[dest] = path
		return nil
	})
	if err != nil {
		return fmt.Errorf("copy fixture: %w", err)
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
