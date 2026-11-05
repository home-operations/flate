// Package testfixture owns disk scenarios and their isolated repositories and caches.
package testfixture

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/go-git/go-git/v5/plumbing"
)

type scenario struct {
	Source       provenance   `json:"source"`
	Repositories []repository `json:"repositories"`
	Commits      []commit     `json:"commits"`
	Overlays     []overlay    `json:"overlays"`
	Inputs       string       `json:"inputs,omitempty"`
	Tokens       []token      `json:"tokens,omitempty"`
	Transport    []Response   `json:"transport,omitempty"`
	Cases        []Case       `json:"cases"`
	Files        []fileUse    `json:"files"`
}

type provenance struct {
	Repository string   `json:"repository"`
	Revision   string   `json:"revision"`
	Paths      []string `json:"paths"`
	Reductions string   `json:"reductions"`
}

type repository struct {
	Name      string   `json:"name"`
	Directory string   `json:"directory"`
	Remotes   []remote `json:"remotes,omitempty"`
}

type remote struct {
	Name string `json:"name"`
	URL  string `json:"url"`
}

type commit struct {
	Repository string   `json:"repository"`
	Name       string   `json:"name"`
	Overlay    string   `json:"overlay,omitempty"`
	Author     string   `json:"author"`
	Email      string   `json:"email"`
	Time       string   `json:"time"`
	Parents    []string `json:"parents"`
	Refs       []string `json:"refs,omitempty"`
}

type overlay struct {
	Name       string      `json:"name"`
	Directory  string      `json:"directory,omitempty"`
	Operations []operation `json:"operations,omitempty"`
}

type token struct {
	File     string `json:"file"`
	Sentinel string `json:"sentinel"`
	Value    string `json:"value"`
}

type operation struct {
	Overlay      string        `json:"overlay,omitempty"`
	Commit       string        `json:"commit,omitempty"`
	Delete       string        `json:"delete,omitempty"`
	Rename       *rename       `json:"rename,omitempty"`
	Chmod        *mode         `json:"chmod,omitempty"`
	Symlink      *symlink      `json:"symlink,omitempty"`
	CacheFailure *cacheFailure `json:"cacheFailure,omitempty"`
	Shallow      string        `json:"shallow,omitempty"`
	MissingBlob  *blob         `json:"missingBlob,omitempty"`
	Ref          *reference    `json:"ref,omitempty"`
	NoGit        string        `json:"noGit,omitempty"`
}

type rename struct {
	From string `json:"from"`
	To   string `json:"to"`
}

type cacheFailure struct {
	Cache      string `json:"cache"`
	Repository string `json:"repository"`
	Chart      string `json:"chart"`
	Version    string `json:"version"`
	Attempts   int    `json:"attempts"`
}

type symlink struct {
	Path   string `json:"path"`
	Target string `json:"target"`
}

type mode struct {
	Path string `json:"path"`
	Mode uint32 `json:"mode"`
}

type blob struct {
	Commit string `json:"commit"`
	Path   string `json:"path"`
}

type reference struct {
	Repository string `json:"repository"`
	Name       string `json:"name"`
	Commit     string `json:"commit"`
}

type fileUse struct {
	Path     string   `json:"path"`
	Cases    []string `json:"cases"`
	Purpose  string   `json:"purpose"`
	Package  string   `json:"package,omitempty"`
	Consumer string   `json:"consumer,omitempty"`
}

// Case requires an explicit cache policy and ordered edits or invocations.
type Case struct {
	Name        string            `json:"name"`
	Description string            `json:"description"`
	Setup       []operation       `json:"setup"`
	Directory   string            `json:"directory,omitempty"`
	Env         map[string]string `json:"env"`
	Cache       replayCache       `json:"cache"`
	Steps       []Step            `json:"steps"`
}

type replayCache struct {
	Repeat string `json:"repeat"`
}

// Step permits exactly one edit or invocation.
type Step struct {
	ID     string      `json:"id"`
	Edit   *operation  `json:"edit,omitempty"`
	Invoke *Invocation `json:"invoke,omitempty"`
}

// Invocation requires complete output and exit-code expectations.
type Invocation struct {
	Argv      []string          `json:"argv"`
	Transport []Response        `json:"transport,omitempty"`
	Env       map[string]string `json:"env,omitempty"`
	Cache     cacheSelection    `json:"cache"`
	Expect    expectation       `json:"expect"`
}

type cacheSelection struct {
	Name  string `json:"name,omitempty"`
	Reset bool   `json:"reset,omitempty"`
}

type expectation struct {
	Stdout string `json:"stdout"`
	Stderr string `json:"stderr"`
	Code   *int   `json:"code"`
}

// Response limits fixture transport to a declared host, method, and path.
type Response struct {
	Host    string            `json:"host"`
	Method  string            `json:"method"`
	Path    string            `json:"path"`
	Status  int               `json:"status,omitempty"`
	Error   string            `json:"error,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
	Body    string            `json:"body,omitempty"`
}

func load(dir string) (*scenario, error) {
	data, err := os.ReadFile(filepath.Join(dir, "scenario.json"))
	if err != nil {
		return nil, fmt.Errorf("read scenario: %w", err)
	}
	var s scenario
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&s); err != nil {
		return nil, fmt.Errorf("parse scenario: %w", err)
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("scenario requires one JSON value")
	}
	if err := s.validate(dir); err != nil {
		return nil, err
	}
	return &s, nil
}

func local(path string) bool {
	return filepath.IsLocal(path) && !strings.Contains(path, "\\")
}

func (s *scenario) validate(dir string) error {
	if s.Source.Repository == "" || len(s.Source.Revision) != 40 || len(s.Source.Paths) == 0 ||
		s.Source.Reductions == "" {
		return fmt.Errorf("scenario requires pinned source provenance")
	}
	if _, err := hex.DecodeString(s.Source.Revision); err != nil {
		return fmt.Errorf("invalid source revision: %w", err)
	}
	if s.Inputs != "" && !local(s.Inputs) {
		return fmt.Errorf("inputs path escapes scenario")
	}
	repos, err := s.validateRepositories()
	if err != nil {
		return err
	}
	overlays, err := s.validateOverlays()
	if err != nil {
		return err
	}
	commitRepos, err := s.validateCommits(repos, overlays)
	if err != nil {
		return err
	}
	if overlayErr := s.validateOverlayOperations(overlays, commitRepos, repos); overlayErr != nil {
		return overlayErr
	}
	names, err := s.validateCases(dir, overlays, commitRepos, repos)
	if err != nil {
		return err
	}
	return s.validateInputs(names, repos)
}

func (s *scenario) validateInputs(names, repos map[string]bool) error {
	if err := s.validateFileUses(names, repos); err != nil {
		return err
	}
	if err := validateResponses(s.Transport); err != nil {
		return err
	}
	for _, token := range s.Tokens {
		if !treePath(token.File, repos) || token.Sentinel == "" || token.Value == "" {
			return fmt.Errorf("invalid source token %q", token.File)
		}
	}
	return nil
}

func (s *scenario) validateFileUses(names, repos map[string]bool) error {
	for _, f := range s.Files {
		if !local(f.Path) || len(f.Cases) == 0 || f.Purpose == "" {
			return fmt.Errorf("invalid file use %q", f.Path)
		}
		switch f.Package {
		case "", "failure", "excluded":
			if f.Consumer != "" {
				return fmt.Errorf("only a component declares a consumer")
			}
		case "component":
			if !treePath(f.Consumer, repos) {
				return fmt.Errorf("component consumer escapes fixture")
			}
		default:
			return fmt.Errorf("invalid package role %q", f.Package)
		}
		for _, name := range f.Cases {
			if !names[name] {
				return fmt.Errorf("unknown file-use case %q", name)
			}
		}
	}
	return nil
}

func (s *scenario) validateRepositories() (map[string]bool, error) {
	repos := map[string]bool{}
	for _, r := range s.Repositories {
		reserved := r.Name == inputsDirectory || r.Name == "home" || r.Name == "caches" || r.Name == ".git"
		if !local(r.Name) || r.Name == "." || strings.ContainsAny(r.Name, "/\\") || reserved {
			return nil, fmt.Errorf("invalid repository name %q", r.Name)
		}
		if (r.Directory != "" && !local(r.Directory)) || repos[r.Name] {
			return nil, fmt.Errorf("invalid repository %q", r.Name)
		}
		repos[r.Name] = true
	}
	if !repos["repo"] {
		return nil, fmt.Errorf("scenario requires repo")
	}
	return repos, nil
}

func (s *scenario) validateOverlays() (map[string]bool, error) {
	overlays := map[string]bool{"base": true}
	for _, o := range s.Overlays {
		if o.Name == "" || overlays[o.Name] || (o.Directory != "" && !local(o.Directory)) {
			return nil, fmt.Errorf("invalid overlay %q", o.Name)
		}
		overlays[o.Name] = true
	}
	return overlays, nil
}

func (s *scenario) validateCommits(repos, overlays map[string]bool) (map[string]string, error) {
	commitRepos := map[string]string{}
	for i := range s.Commits {
		c := &s.Commits[i]
		if err := validateCommit(c, repos, overlays, commitRepos); err != nil {
			return nil, err
		}
		commitRepos[c.Name] = c.Repository
	}
	return commitRepos, nil
}

func validateCommit(c *commit, repos, overlays map[string]bool, commitRepos map[string]string) error {
	if c.Name == "" || commitRepos[c.Name] != "" || !repos[c.Repository] || c.Author == "" || c.Email == "" ||
		c.Time == "" {
		return fmt.Errorf("invalid commit %q", c.Name)
	}
	if c.Overlay != "" && !overlays[c.Overlay] {
		return fmt.Errorf("unknown commit overlay %q", c.Overlay)
	}
	if _, err := time.Parse(time.RFC3339, c.Time); err != nil {
		return fmt.Errorf("commit time: %w", err)
	}
	for _, p := range c.Parents {
		if commitRepos[p] == "" || commitRepos[p] != c.Repository {
			return fmt.Errorf("unknown parent %q", p)
		}
	}
	for _, ref := range c.Refs {
		if err := plumbing.ReferenceName(ref).Validate(); err != nil {
			return fmt.Errorf("commit reference: %w", err)
		}
	}
	return nil
}

func (s *scenario) validateOverlayOperations(
	overlays map[string]bool,
	commitRepos map[string]string,
	repos map[string]bool,
) error {
	for _, o := range s.Overlays {
		for i := range o.Operations {
			op := &o.Operations[i]
			if op.Overlay != "" || op.Commit != "" || op.Shallow != "" || op.MissingBlob != nil || op.Ref != nil ||
				op.NoGit != "" {
				return fmt.Errorf("overlay operations require delete, rename, chmod, symlink or cacheFailure")
			}
			if err := validateOperation(op, overlays, commitRepos, repos); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *scenario) validateCases(
	dir string,
	overlays map[string]bool,
	commitRepos map[string]string,
	repos map[string]bool,
) (map[string]bool, error) {
	names := map[string]bool{}
	for _, c := range s.Cases {
		if c.Name == "" || names[c.Name] || c.Description == "" {
			return nil, fmt.Errorf("invalid case %q", c.Name)
		}
		names[c.Name] = true
		if c.Cache.Repeat != "reset" && c.Cache.Repeat != "reuse" {
			return nil, fmt.Errorf("case %s requires cache repeat reset or reuse", c.Name)
		}
		for i := range c.Setup {
			if err := validateOperation(&c.Setup[i], overlays, commitRepos, repos); err != nil {
				return nil, err
			}
		}
		if err := c.validateSteps(dir, overlays); err != nil {
			return nil, err
		}
	}
	return names, nil
}

func (c *Case) validateSteps(dir string, overlays map[string]bool) error {
	ids := map[string]bool{}
	if len(c.Steps) == 0 {
		return fmt.Errorf("case %s has no steps", c.Name)
	}
	for _, step := range c.Steps {
		if step.ID == "" || ids[step.ID] || (step.Edit == nil) == (step.Invoke == nil) {
			return fmt.Errorf("invalid or duplicate step %q", step.ID)
		}
		ids[step.ID] = true
		if step.Edit != nil {
			if step.Edit.Overlay == "" {
				return fmt.Errorf("edit %s requires overlay", step.ID)
			}
			if !overlays[step.Edit.Overlay] {
				return fmt.Errorf("unknown overlay %q", step.Edit.Overlay)
			}
			if *step.Edit != (operation{Overlay: step.Edit.Overlay}) {
				return fmt.Errorf("edit %s permits only overlay", step.ID)
			}
		} else if err := validateInvocation(dir, step.Invoke); err != nil {
			return err
		}
	}
	return nil
}

func validateInvocation(dir string, inv *Invocation) error {
	if err := validateResponses(inv.Transport); err != nil {
		return err
	}
	if len(inv.Argv) == 0 || inv.Expect.Code == nil || inv.Expect.Stdout == "" || inv.Expect.Stderr == "" {
		return fmt.Errorf("invocation requires argv and complete expectations")
	}
	if inv.Cache.Name != "" && !local(inv.Cache.Name) {
		return fmt.Errorf("invalid cache name %q", inv.Cache.Name)
	}
	for _, path := range []string{inv.Expect.Stdout, inv.Expect.Stderr} {
		if !local(path) || !strings.HasPrefix(path, "expect/") {
			return fmt.Errorf("invalid expectation path %q", path)
		}
		if _, err := os.Stat(filepath.Join(dir, path)); err != nil {
			return fmt.Errorf("read expectation: %w", err)
		}
	}
	return nil
}

func validateOperation(
	op *operation,
	overlays map[string]bool,
	commits map[string]string,
	repos map[string]bool,
) error {
	if err := validateOperationAction(op); err != nil {
		return err
	}
	if op.NoGit != "" && !repos[op.NoGit] {
		return fmt.Errorf("unknown repository %q", op.NoGit)
	}
	if op.Overlay != "" && !overlays[op.Overlay] {
		return fmt.Errorf("unknown overlay %q", op.Overlay)
	}
	for _, name := range []string{op.Commit, op.Shallow} {
		if name != "" && commits[name] == "" {
			return fmt.Errorf("unknown commit %q", name)
		}
	}
	if err := validateOperationPaths(op, commits, repos); err != nil {
		return err
	}
	if op.Ref != nil {
		if !repos[op.Ref.Repository] || commits[op.Ref.Commit] != op.Ref.Repository {
			return fmt.Errorf("reference requires a commit in its repository")
		}
		if err := plumbing.ReferenceName(op.Ref.Name).Validate(); err != nil {
			return fmt.Errorf("setup reference: %w", err)
		}
	}
	return nil
}

func validateOperationAction(op *operation) error {
	count := 0
	for _, set := range []bool{op.Overlay != "", op.Commit != "", op.Delete != "", op.Rename != nil, op.Chmod != nil, op.Symlink != nil, op.CacheFailure != nil, op.Shallow != "", op.MissingBlob != nil, op.Ref != nil, op.NoGit != ""} {
		if set {
			count++
		}
	}
	if count != 1 {
		return fmt.Errorf("setup operation requires exactly one action")
	}
	if failure := op.CacheFailure; failure != nil {
		if !local(failure.Cache) || failure.Repository == "" || failure.Chart == "" || failure.Version == "" ||
			failure.Attempts < 1 ||
			failure.Attempts > 12 {
			return fmt.Errorf("invalid cache failure operation")
		}
	}
	return nil
}

func validateOperationPaths(op *operation, commits map[string]string, repos map[string]bool) error {
	paths := []string{op.Delete}
	if op.Symlink != nil {
		link := op.Symlink
		if link.Target == "" || filepath.IsAbs(link.Target) || strings.Contains(link.Target, "\\") ||
			!treePath(filepath.Join(filepath.Dir(link.Path), link.Target), repos) {
			return fmt.Errorf("symlink target escapes fixture")
		}
		paths = append(paths, link.Path)
	}
	if op.Rename != nil {
		paths = append(paths, op.Rename.From, op.Rename.To)
	}
	if op.Chmod != nil {
		paths = append(paths, op.Chmod.Path)
	}
	if op.MissingBlob != nil {
		if !local(op.MissingBlob.Path) || commits[op.MissingBlob.Commit] == "" {
			return fmt.Errorf("unknown blob commit")
		}
	}
	if slices.ContainsFunc(paths, func(p string) bool { return p != "" && !treePath(p, repos) }) {
		return fmt.Errorf("operation path escapes fixture")
	}
	return nil
}

func treePath(path string, repos map[string]bool) bool {
	if !local(path) {
		return false
	}
	root, _, ok := strings.Cut(filepath.ToSlash(filepath.Clean(path)), "/")
	return ok && (repos[root] || root == inputsDirectory) && !strings.Contains(path, ".git")
}

func validateResponses(responses []Response) error {
	for _, r := range responses {
		if r.Host == "" || r.Method == "" || !strings.HasPrefix(r.Path, "/") {
			return fmt.Errorf("invalid transport response")
		}
		if r.Error != "" {
			if r.Error != "connection-reset" || r.Status != 0 || r.Body != "" || len(r.Headers) != 0 {
				return fmt.Errorf("invalid transport error %q", r.Error)
			}
		} else if r.Status < 100 || r.Status > 599 || !local(r.Body) {
			return fmt.Errorf("invalid transport response status or body")
		}
	}
	return nil
}
