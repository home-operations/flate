package baseline

import (
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/testutil"
)

// TestAutoResolve_ExplicitBaseFetchedFromOrigin pins the fetch fallback:
// a checkout that carries an origin remote but none of the baseline's
// history (an agent's file dump over `git init`, a fetch-depth=1 clone
// of another branch) fetches the rev from origin instead of failing,
// whatever form the rev takes.
func TestAutoResolve_ExplicitBaseFetchedFromOrigin(t *testing.T) {
	remote := t.TempDir()
	mainCommit := initRepoWithFile(t, remote, "a.yaml", "base")
	remoteRepo := openHelper(t, remote)
	setRef(t, remoteRepo, plumbing.NewBranchReferenceName("main"), mainCommit)
	if _, err := remoteRepo.CreateTag("v1", mainCommit, &git.CreateTagOptions{
		Tagger:  &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)},
		Message: "v1",
	}); err != nil {
		t.Fatal(err)
	}
	url := filepath.Join(remote, ".git")

	cases := []struct{ name, base string }{
		{"branch", "main"},
		{"remote-qualified branch", "origin/main"},
		{"tag", "v1"},
		{"full ref", "refs/heads/main"},
		{"sha", mainCommit.String()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := historylessCheckout(t, url, "a.yaml", "pr-tip")
			res, err := AutoResolve(t.Context(), dir, tc.base, testOwner(t, ""))
			if err != nil {
				t.Fatalf("AutoResolve(--base=%s): %v", tc.base, err)
			}

			got, err := fs.ReadFile(res.BaseInput.FS, "a.yaml")
			if err != nil {
				t.Fatalf("read materialized: %v", err)
			}
			if string(got) != "base" {
				t.Errorf("materialized a.yaml = %q, want %q (origin's main, not the checkout)", got, "base")
			}
			if res.Rev != shortRev(mainCommit) {
				t.Errorf("Rev = %q, want %q", res.Rev, shortRev(mainCommit))
			}
			if !strings.Contains(res.Source.Metadata["baseline-selection"], "fetched from origin") {
				t.Errorf("Source = %q, want it to say the rev was fetched from origin", res.Source.Metadata["baseline-selection"])
			}
			// The checkout is read-only: the fetch lands nowhere near it.
			if _, err := openHelper(t, dir).CommitObject(mainCommit); err == nil {
				t.Error("baseline commit landed in the checkout; the fetch must not write into it")
			}
		})
	}
}

// TestAutoResolve_ExplicitBaseFetchedDespiteLocalHistory: local history
// that simply lacks the rev takes the same fallback, so `--base main`
// works in a shallow clone of a feature branch.
func TestAutoResolve_ExplicitBaseFetchedDespiteLocalHistory(t *testing.T) {
	remote := t.TempDir()
	mainCommit := initRepoWithFile(t, remote, "a.yaml", "base")
	setRef(t, openHelper(t, remote), plumbing.NewBranchReferenceName("main"), mainCommit)

	dir := t.TempDir()
	initRepoWithFile(t, dir, "a.yaml", "pr-tip")
	addOrigin(t, dir, filepath.Join(remote, ".git"))

	res, err := AutoResolve(t.Context(), dir, "main", testOwner(t, ""))
	if err != nil {
		t.Fatalf("AutoResolve(--base=main): %v", err)
	}

	if res.Rev != shortRev(mainCommit) {
		t.Errorf("Rev = %q, want %q", res.Rev, shortRev(mainCommit))
	}
}

// TestAutoResolve_ExplicitBaseNotOnOrigin: the fallback ran and origin
// has no such ref either; the error says where it looked.
func TestAutoResolve_ExplicitBaseNotOnOrigin(t *testing.T) {
	remote := t.TempDir()
	initRepoWithFile(t, remote, "a.yaml", "base")
	dir := historylessCheckout(t, filepath.Join(remote, ".git"), "a.yaml", "pr-tip")

	_, err := AutoResolve(t.Context(), dir, "nope", testOwner(t, ""))
	if err == nil {
		t.Fatal("expected error for a rev origin does not have")
	}
	for _, want := range []string{"nope", "origin"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q should mention %q", err, want)
		}
	}
}

// TestAutoResolve_ExplicitBaseNoOriginRemote: nothing local and no
// origin to fetch from; the error names the missing remote.
func TestAutoResolve_ExplicitBaseNoOriginRemote(t *testing.T) {
	dir := historylessCheckout(t, "", "a.yaml", "pr-tip")

	_, err := AutoResolve(t.Context(), dir, "main", testOwner(t, ""))
	if err == nil {
		t.Fatal("expected error without an origin remote")
	}
	if !strings.Contains(err.Error(), "origin remote") {
		t.Errorf("error %q should say there is no origin remote to fetch from", err)
	}
}

// TestAutoResolve_UnbornHEADWithoutBase: auto-detection needs a HEAD
// commit to merge-base from, so a history-less checkout must be told
// to pass --base rather than failing on a bare ref lookup.
func TestAutoResolve_UnbornHEADWithoutBase(t *testing.T) {
	remote := t.TempDir()
	initRepoWithFile(t, remote, "a.yaml", "base")
	dir := historylessCheckout(t, filepath.Join(remote, ".git"), "a.yaml", "pr-tip")

	_, err := AutoResolve(t.Context(), dir, "", testOwner(t, ""))
	if err == nil {
		t.Fatal("expected error for an unborn HEAD without --base")
	}
	for _, want := range []string{"no commits", "--base"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q should mention %q", err, want)
		}
	}
}

// historylessCheckout builds the layout an agent runner produces: the
// files written straight to disk, `git init` over them, and an origin
// remote when url is non-empty. No commits, no objects.
func historylessCheckout(t *testing.T, url, path, content string) string {
	t.Helper()
	dir := t.TempDir()
	testutil.WriteFileAt(t, filepath.Join(dir, path), content)
	if _, err := git.PlainInit(dir, false); err != nil {
		t.Fatal(err)
	}
	if url != "" {
		addOrigin(t, dir, url)
	}
	return dir
}

func addOrigin(t *testing.T, dir, url string) {
	t.Helper()
	if _, err := openHelper(t, dir).CreateRemote(&config.RemoteConfig{Name: "origin", URLs: []string{url}}); err != nil {
		t.Fatal(err)
	}
}
