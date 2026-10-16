package git

import (
	"testing"

	gitlib "github.com/go-git/go-git/v5"

	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func TestFetcher_ContentIsolation(t *testing.T) {
	root := t.TempDir()
	mustInitRepo(t, root)
	commit := mustSetRefToHEAD(t, root, "refs/heads/other")
	c := newGitCache(t, "")
	f := &Fetcher{Cache: c}
	base := &manifest.GitRepository{URL: "file://" + root, Reference: &manifest.GitRepositoryRef{Commit: commit}}
	first, err := f.fetch(t.Context(), gitAcquisition{spec: base, options: gitlib.CloneOptions{URL: base.URL, NoCheckout: true}}, sourcecache.RetryConfig{})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name     string
		change   func(*manifest.GitRepository, *Fetcher)
		wantSame bool
	}{
		{"unchanged", func(*manifest.GitRepository, *Fetcher) {}, true},
		{"name and namespace without auth", func(r *manifest.GitRepository, _ *Fetcher) { r.Name, r.Namespace = "other", "other" }, true},
		{"empty ignore", func(r *manifest.GitRepository, _ *Fetcher) { r.Ignore = new("") }, true},
		{"empty sparse", func(r *manifest.GitRepository, _ *Fetcher) { r.SparseCheckout = []string{} }, true},
		{"ignore", func(r *manifest.GitRepository, _ *Fetcher) { r.Ignore = new("*.tmp\n") }, false},
		{"sparse", func(r *manifest.GitRepository, _ *Fetcher) { r.SparseCheckout = []string{"hello.txt"} }, false},
		{"submodules", func(r *manifest.GitRepository, _ *Fetcher) { r.RecurseSubmodules = true }, false},
		{"ref", func(r *manifest.GitRepository, _ *Fetcher) {
			r.Reference = &manifest.GitRepositoryRef{Commit: commit, Branch: "other"}
		}, false},
		{"URL", func(r *manifest.GitRepository, _ *Fetcher) { r.URL += "/." }, false},
		{"depth", func(_ *manifest.GitRepository, f *Fetcher) { f.Depth = 1 }, false},
		{"secret", func(r *manifest.GitRepository, _ *Fetcher) {
			r.SecretRef = &manifest.LocalObjectReference{Name: "auth"}
		}, false},
		{"proxy", func(r *manifest.GitRepository, _ *Fetcher) {
			r.ProxySecretRef = &manifest.LocalObjectReference{Name: "proxy"}
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := *base
			fetcher := &Fetcher{Cache: c}
			tc.change(&r, fetcher)
			art, err := fetcher.fetch(t.Context(), gitAcquisition{spec: &r, options: gitlib.CloneOptions{URL: r.URL, NoCheckout: true}}, sourcecache.RetryConfig{})
			if err != nil {
				t.Fatal(err)
			}
			if (art.Key == first.Key) != tc.wantSame {
				t.Fatalf("identity=%q, base=%q, want same=%t", art.Key, first.Key, tc.wantSame)
			}
		})
	}
}
