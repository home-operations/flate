package git

import (
	"io/fs"
	"os"
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	gitlib "github.com/go-git/go-git/v5"

	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func TestFetcher_ContentIsolation(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	mustInitRepoWithFiles(t, root, map[string]string{"hello.txt": "hello", "docs.tmp": "docs"})
	commit := mustSetRefToHEAD(t, root, "refs/heads/other")
	c := cachetest.New(t, "", sourcecache.New)
	f := &Fetcher{Cache: c}
	base := &manifest.GitRepository{URL: "file://" + root, Reference: &sourcev1.GitRepositoryRef{Commit: commit}}
	first, err := f.fetch(t.Context(), gitAcquisition{spec: base, options: gitlib.CloneOptions{URL: base.URL, NoCheckout: true}}, sourcecache.Policy{Retry: sourcecache.RetryConfig{}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fs.Stat(first.FS, "docs.tmp"); err != nil {
		t.Fatalf("plain checkout lost docs.tmp: %v", err)
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
			r.Reference = &sourcev1.GitRepositoryRef{Commit: commit, Branch: "other"}
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
			t.Parallel()
			r := *base
			fetcher := &Fetcher{Cache: c}
			tc.change(&r, fetcher)
			art, err := fetcher.fetch(t.Context(), gitAcquisition{spec: &r, options: gitlib.CloneOptions{URL: r.URL, NoCheckout: true}}, sourcecache.Policy{Retry: sourcecache.RetryConfig{}})
			if err != nil {
				t.Fatal(err)
			}
			if (art.Key == first.Key) != tc.wantSame {
				t.Fatalf("identity=%q, base=%q, want same=%t", art.Key, first.Key, tc.wantSame)
			}
			if tc.name == "ignore" {
				if _, err := fs.Stat(art.FS, "docs.tmp"); !os.IsNotExist(err) {
					t.Fatalf("ignored checkout retained docs.tmp: %v", err)
				}
				plain, err := f.fetch(t.Context(), gitAcquisition{spec: base, options: gitlib.CloneOptions{URL: base.URL, NoCheckout: true}}, sourcecache.Policy{Retry: sourcecache.RetryConfig{}})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := fs.Stat(plain.FS, "docs.tmp"); err != nil {
					t.Fatalf("plain checkout lost docs.tmp after ignored acquisition: %v", err)
				}
			}
		})
	}
}
