package testfixture

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/transport"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
)

func fixtureSource(t *testing.T, repeat string) string {
	t.Helper()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "repo/value", "before")
	testutil.WriteFile(t, dir, "baseline/value", "independent")
	testutil.WriteFile(t, dir, "inputs/supplied", "input")
	testutil.WriteFile(t, dir, "overlays/after/repo/value", "after")
	testutil.WriteFile(t, dir, "expect/empty.stdout", "")
	testutil.WriteFile(t, dir, "expect/empty.stderr", "")
	zero := 0
	s := scenario{
		Source: provenance{
			Repository: "authored",
			Revision:   strings.Repeat("0", 40),
			Paths:      []string{"value"},
			Reductions: "Fixture engine witness.",
		},
		Repositories: []repository{
			{Name: "repo", Directory: "repo", Remotes: []remote{{Name: "origin", URL: "https://fixture.invalid/repo"}}},
			{Name: "baseline", Directory: "baseline"},
		},
		Inputs: "inputs",
		Commits: []commit{
			{
				Repository: "repo",
				Name:       "base",
				Author:     "fixture",
				Email:      "fixture@example.test",
				Time:       "2000-01-01T00:00:00Z",
				Refs:       []string{"refs/heads/base"},
			},
			{
				Repository: "repo",
				Name:       "head",
				Overlay:    "after",
				Parents:    []string{"base"},
				Author:     "fixture",
				Email:      "fixture@example.test",
				Time:       "2000-01-02T00:00:00Z",
			},
			{
				Repository: "baseline",
				Name:       "baseline",
				Author:     "fixture",
				Email:      "fixture@example.test",
				Time:       "2000-01-01T00:00:00Z",
			},
		},
		Overlays: []overlay{{Name: "after", Directory: "overlays/after"}},
		Cases: []Case{
			{
				Name:        "sequence",
				Description: "Engine sequence",
				Setup:       []operation{{Commit: "base"}},
				Cache:       replayCache{Repeat: repeat},
				Steps: []Step{
					{ID: "edit", Edit: &operation{Overlay: "after"}},
					{
						ID: "invoke",
						Invoke: &Invocation{
							Argv: []string{"build"},
							Expect: expectation{
								Stdout: "expect/empty.stdout",
								Stderr: "expect/empty.stderr",
								Code:   &zero,
							},
						},
					},
				},
			},
		},
	}
	data, err := json.Marshal(s)
	assert.NoError(t, err)
	testutil.WriteFile(t, dir, "scenario.json", string(data))
	return dir
}

func TestFixture_SequenceReplay(t *testing.T) {
	t.Parallel()
	f := materialize(t, fixtureSource(t, "reset"), "sequence")
	root, inputs := f.Repo, f.Inputs
	hashes := f.Resolve(t, "@@commit:base@@ @@commit:head@@", "")
	for range 2 {
		f.Restore(t)
		data, err := os.ReadFile(filepath.Join(f.Repo, "value"))
		assert.NoError(t, err)
		assert.Equal(t, string(data), "before")
		f.Edit(t, f.Plan.Steps[0])
		for range 2 {
			data, err = os.ReadFile(filepath.Join(f.Repo, "value"))
			assert.NoError(t, err)
			assert.Equal(t, string(data), "after")
		}
		assert.Equal(t, f.Repo, root)
		assert.Equal(t, f.Inputs, inputs)
		assert.Equal(t, f.Resolve(t, "@@commit:base@@ @@commit:head@@", ""), hashes)
		data, err = os.ReadFile(filepath.Join(f.root, "baseline", "value"))
		assert.NoError(t, err)
		assert.Equal(t, string(data), "independent")
		repo, err := git.PlainOpen(f.Repo)
		assert.NoError(t, err)
		head, err := repo.Head()
		assert.NoError(t, err)
		assert.Equal(t, head.Hash().String(), f.Resolve(t, "@@commit:base@@", ""))
		remote, err := repo.Remote("origin")
		assert.NoError(t, err)
		assert.Equal(t, remote.Config().URLs[0], "https://fixture.invalid/repo")
	}
	other := materialize(t, f.dir, "sequence")
	assert.Equal(t, other.Resolve(t, "@@commit:base@@ @@commit:head@@", ""), hashes)
	assert.Equal(t, f.Resolve(t, "${VAR} {{ .Values.value }}", ""), "${VAR} {{ .Values.value }}")
}

func TestFixture_CommitTokens(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		replace bool
	}{
		{"inherited", false},
		{"replaced", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := fixtureSource(t, "reset")
			s, err := load(dir)
			assert.NoError(t, err)
			testutil.WriteFile(t, dir, "repo/token", "parent: SENTINEL")
			testutil.WriteFile(t, dir, "baseline/token", "BASELINE")
			if tc.replace {
				testutil.WriteFile(t, dir, "overlays/after/repo/token", "child: SENTINEL")
			}
			s.Tokens = []token{
				{File: "repo/token", Sentinel: "SENTINEL", Value: "resolved"},
				{File: "baseline/token", Sentinel: "BASELINE", Value: "independent"},
			}
			data, err := json.Marshal(s)
			assert.NoError(t, err)
			testutil.WriteFile(t, dir, "scenario.json", string(data))
			f := materialize(t, dir, "sequence")
			for range 2 {
				for _, name := range []string{"base", "head", "baseline"} {
					c := f.namedCommit(name)
					obj, commitErr := f.repositories[c.Repository].CommitObject(f.commits[name])
					assert.NoError(t, commitErr)
					file, fileErr := obj.File("token")
					assert.NoError(t, fileErr)
					got, readErr := file.Contents()
					assert.NoError(t, readErr)
					want := "parent: resolved"
					if name == "baseline" {
						want = "independent"
					} else if name == "head" && tc.replace {
						want = "child: resolved"
					}
					assert.Equal(t, got, want)
				}
				f.Restore(t)
			}
		})
	}
}

func TestFixture_CacheOwnership(t *testing.T) {
	t.Parallel()
	for _, repeat := range []string{"reset", "reuse"} {
		t.Run(repeat, func(t *testing.T) {
			t.Parallel()
			f := materialize(t, fixtureSource(t, repeat), "sequence")
			inv := f.Plan.Steps[1].Invoke
			f.Restore(t)
			cache := f.Cache(t, inv)
			testutil.WriteFile(t, cache, "marker", "warm")
			testutil.WriteFile(t, f.Home, "marker", "os cache")
			isolated := *inv
			isolated.Cache = cacheSelection{Name: "cold", Reset: true}
			cold := f.Cache(t, &isolated)
			testutil.WriteFile(t, cold, "marker", "cold")
			assert.Equal(t, f.Cache(t, inv), cache)
			f.Cache(t, &isolated)
			_, err := os.Stat(filepath.Join(cold, "marker"))
			assert.Equal(t, os.IsNotExist(err), true)
			_, err = os.Stat(filepath.Join(cache, "marker"))
			assert.NoError(t, err)
			f.Restore(t)
			for _, dir := range []string{cache, f.Home} {
				_, err := os.Stat(filepath.Join(dir, "marker"))
				assert.Equal(t, os.IsNotExist(err), repeat == "reset")
			}
		})
	}
}

func TestFixture_GitFaultsAndFileOperations(t *testing.T) {
	t.Parallel()
	f := materialize(t, fixtureSource(t, "reset"), "sequence")
	f.apply(t, &operation{Rename: &rename{From: "repo/value", To: "repo/renamed"}})
	f.apply(t, &operation{Chmod: &mode{Path: "repo/renamed", Mode: 0o755}})
	info, err := os.Stat(filepath.Join(f.Repo, "renamed"))
	assert.NoError(t, err)
	assert.Equal(t, info.Mode().Perm(), os.FileMode(0o755))
	f.apply(t, &operation{Delete: "repo/renamed"})
	_, err = os.Stat(filepath.Join(f.Repo, "renamed"))
	assert.Equal(t, os.IsNotExist(err), true)
	f.apply(t, &operation{Shallow: "head"})
	shallow, err := f.repositories["repo"].Storer.Shallow()
	assert.NoError(t, err)
	assert.Diff(t, shallow, []plumbing.Hash{f.commits["head"]})
	f.apply(t, &operation{Ref: &reference{Repository: "repo", Name: "refs/heads/custom", Commit: "head"}})
	ref, err := f.repositories["repo"].Reference("refs/heads/custom", true)
	assert.NoError(t, err)
	assert.Equal(t, ref.Hash(), f.commits["head"])
	f.apply(t, &operation{MissingBlob: &blob{Commit: "head", Path: "value"}})
	commit, err := f.repositories["repo"].CommitObject(f.commits["head"])
	assert.NoError(t, err)
	tree, err := commit.Tree()
	assert.NoError(t, err)
	entry, err := tree.FindEntry("value")
	assert.NoError(t, err)
	_, err = f.repositories["repo"].BlobObject(entry.Hash)
	assert.Equal(t, errors.Is(err, plumbing.ErrObjectNotFound), true)
	f.Restore(t)
	shallow, err = f.repositories["repo"].Storer.Shallow()
	assert.NoError(t, err)
	assert.Equal(t, len(shallow), 0)
	_, err = f.repositories["baseline"].CommitObject(f.commits["head"])
	assert.Equal(t, err != nil, true)
}

func TestScenario_RejectsInvalidDeclarations(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"unknown field", "unknown overlay", "escape", "duplicate id", "both operations", "missing expectation", "cache policy"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dir := fixtureSource(t, "reset")
			path := filepath.Join(dir, "scenario.json")
			data, err := os.ReadFile(path)
			assert.NoError(t, err)
			var s scenario
			assert.NoError(t, json.Unmarshal(data, &s))
			switch name {
			case "unknown field":
				data = []byte(strings.Replace(string(data), `"source":`, `"unknown":true,"source":`, 1))
			case "unknown overlay":
				s.Cases[0].Steps[0].Edit.Overlay = "missing"
			case "escape":
				s.Cases[0].Setup = []operation{{Delete: "../outside"}}
			case "duplicate id":
				s.Cases[0].Steps[1].ID = "edit"
			case "both operations":
				s.Cases[0].Steps[1].Edit = &operation{Overlay: "after"}
			case "missing expectation":
				s.Cases[0].Steps[1].Invoke.Expect.Code = nil
			case "cache policy":
				s.Cases[0].Cache.Repeat = ""
			}
			if name != "unknown field" {
				data, err = json.Marshal(s)
				assert.NoError(t, err)
			}
			assert.NoError(t, os.WriteFile(path, data, 0o600))
			_, err = load(dir)
			if err == nil {
				t.Fatal("invalid scenario accepted")
			}
		})
	}
}

func TestCopyTree_RejectsSymlink(t *testing.T) {
	t.Parallel()
	source, target := t.TempDir(), t.TempDir()
	assert.NoError(t, os.Symlink(t.TempDir(), filepath.Join(source, "escape")))
	if err := copyTree(source, target, make(map[string]string)); err == nil {
		t.Fatal("symlink accepted")
	}
}

func TestFixture_DeclaredGitStores(t *testing.T) {
	t.Parallel()
	f := materialize(t, fixtureSource(t, "reset"), "sequence")
	endpoint, err := transport.NewEndpoint("https://fixture.invalid/repo")
	assert.NoError(t, err)
	for range 2 {
		storage, loadErr := f.Load(endpoint)
		assert.NoError(t, loadErr)
		assert.Equal(t, storage == f.repositories["repo"].Storer, true)
		assert.Equal(t, storage == f.repositories["baseline"].Storer, false)
		f.Restore(t)
	}
	endpoint, err = transport.NewEndpoint("https://undeclared.invalid/repo")
	assert.NoError(t, err)
	_, err = f.Load(endpoint)
	assert.Equal(t, errors.Is(err, transport.ErrRepositoryNotFound), true)
}

func TestFixture_SymlinkReplay(t *testing.T) {
	t.Parallel()
	for _, target := range []string{"../value", "missing.yaml"} {
		t.Run(target, func(t *testing.T) {
			t.Parallel()
			dir := fixtureSource(t, "reset")
			s, err := load(dir)
			assert.NoError(t, err)
			s.Overlays[0].Operations = []operation{{Symlink: &symlink{Path: "repo/nested/link", Target: target}}}
			data, err := json.Marshal(s)
			assert.NoError(t, err)
			testutil.WriteFile(t, dir, "scenario.json", string(data))
			f := materialize(t, dir, "sequence")
			for range 2 {
				f.Restore(t)
				_, err := os.Lstat(filepath.Join(f.Repo, "nested", "link"))
				assert.Equal(t, os.IsNotExist(err), true)
				f.Edit(t, f.Plan.Steps[0])
				got, err := os.Readlink(filepath.Join(f.Repo, "nested", "link"))
				assert.NoError(t, err)
				assert.Equal(t, got, target)
				body, err := os.ReadFile(filepath.Join(f.Repo, "nested", "link"))
				if target == "missing.yaml" {
					assert.Equal(t, os.IsNotExist(err), true)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, string(body), "after")
				}
			}
		})
	}
}

func TestScenario_SymlinkBoundary(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		link  symlink
		valid bool
	}{
		{"dangling", symlink{Path: "repo/link", Target: "missing"}, true},
		{"parent", symlink{Path: "repo/nested/link", Target: "../value"}, true},
		{"absolute", symlink{Path: "repo/link", Target: "/tmp/outside"}, false},
		{"escape", symlink{Path: "repo/link", Target: "../../outside"}, false},
		{"git", symlink{Path: "repo/link", Target: ".git/HEAD"}, false},
		{"link escape", symlink{Path: "../link", Target: "repo/value"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := validateOperation(&operation{Symlink: &tc.link}, nil, nil, map[string]bool{"repo": true})
			assert.Equal(t, err == nil, tc.valid)
		})
	}
}

func TestFixture_EmptyRepositoryReplay(t *testing.T) {
	t.Parallel()
	dir := fixtureSource(t, "reset")
	s, err := load(dir)
	assert.NoError(t, err)
	s.Repositories[0].Directory = ""
	data, err := json.Marshal(s)
	assert.NoError(t, err)
	testutil.WriteFile(t, dir, "scenario.json", string(data))
	f := materialize(t, dir, "sequence")
	for range 2 {
		f.Restore(t)
		_, err := os.Stat(filepath.Join(f.Repo, "value"))
		assert.Equal(t, os.IsNotExist(err), true)
		f.Edit(t, f.Plan.Steps[0])
		body, err := os.ReadFile(filepath.Join(f.Repo, "value"))
		assert.NoError(t, err)
		assert.Equal(t, string(body), "after")
	}
}

func TestFixture_SourceOriginsReplay(t *testing.T) {
	t.Parallel()
	dir := fixtureSource(t, "reset")
	f := materialize(t, dir, "sequence")
	value := filepath.Join(f.Repo, "value")
	for range 2 {
		f.Restore(t)
		assert.Equal(t, f.origins[value], filepath.Join(dir, "repo", "value"))
		f.Edit(t, f.Plan.Steps[0])
		after := filepath.Join(dir, "overlays", "after", "repo", "value")
		assert.Equal(t, f.origins[value], after)
		f.apply(t, &operation{Rename: &rename{From: "repo/value", To: "repo/moved"}})
		assert.Equal(t, f.origins[filepath.Join(f.Repo, "moved")], after)
		_, exists := f.origins[value]
		assert.Equal(t, exists, false)
		f.apply(t, &operation{Delete: "repo/moved"})
		_, exists = f.origins[filepath.Join(f.Repo, "moved")]
		assert.Equal(t, exists, false)
		f.checkout(t, "head")
		assert.Equal(t, f.origins[value], after)
		f.checkout(t, "base")
		assert.Equal(t, f.origins[value], filepath.Join(dir, "repo", "value"))
		f.apply(t, &operation{Symlink: &symlink{Path: "repo/link", Target: "missing"}})
		assert.Equal(t, f.origins[filepath.Join(f.Repo, "link")], filepath.Join(dir, "repo", "link"))
	}
}

func TestFixture_NoGitReplay(t *testing.T) {
	t.Parallel()
	dir := fixtureSource(t, "reset")
	s, err := load(dir)
	assert.NoError(t, err)
	s.Cases[0].Setup = append(s.Cases[0].Setup, operation{NoGit: "repo"})
	data, err := json.Marshal(s)
	assert.NoError(t, err)
	testutil.WriteFile(t, dir, "scenario.json", string(data))
	f := materialize(t, dir, "sequence")
	for range 2 {
		f.Restore(t)
		_, err := git.PlainOpenWithOptions(f.Repo, &git.PlainOpenOptions{DetectDotGit: true})
		assert.Equal(t, errors.Is(err, git.ErrRepositoryNotExists), true)
		body, err := os.ReadFile(filepath.Join(f.Repo, "value"))
		assert.NoError(t, err)
		assert.Equal(t, string(body), "before")
	}
}

func TestScenario_TransportOverrides(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		response Response
		valid    bool
	}{
		{"success", Response{Host: "fixture.invalid", Method: "GET", Path: "/index.yaml", Status: 200, Body: "inputs/index.yaml"}, true},
		{"reset", Response{Host: "fixture.invalid", Method: "GET", Path: "/index.yaml", Error: "connection-reset"}, true},
		{"unknown error", Response{Host: "fixture.invalid", Method: "GET", Path: "/index.yaml", Error: "unknown"}, false},
		{"mixed result", Response{Host: "fixture.invalid", Method: "GET", Path: "/index.yaml", Error: "connection-reset", Status: 200, Body: "inputs/index.yaml"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := fixtureSource(t, "reset")
			s, err := load(dir)
			assert.NoError(t, err)
			s.Cases[0].Steps[1].Invoke.Transport = []Response{tc.response}
			assert.Equal(t, s.validate(dir) == nil, tc.valid)
		})
	}
}
