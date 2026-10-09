package discovery

import (
	"bytes"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cacheroot"
	"github.com/home-operations/flate/pkg/store"
)

func TestAliasBootstrapSources_DeclaredRefs(t *testing.T) {
	for _, tc := range []struct {
		name                                              string
		ref                                               *manifest.GitRepositoryRef
		head, fallback, fail, noStore, sparse, submodules bool
	}{
		{name: "no-ref", head: true},
		{name: "empty-ref", ref: &manifest.GitRepositoryRef{}, head: true},
		{name: "head", ref: &manifest.GitRepositoryRef{Tag: "v2.0.0"}, head: true},
		{name: "non-head", ref: &manifest.GitRepositoryRef{Tag: "v1.0.0"}},
		{name: "conflicting", ref: &manifest.GitRepositoryRef{Tag: "v1.0.0", Branch: "bad branch"}},
		{name: "missing-tag", ref: &manifest.GitRepositoryRef{Tag: "missing"}, fallback: true},
		{name: "missing-name", ref: &manifest.GitRepositoryRef{Name: "refs/pull/9/head"}, fallback: true},
		{name: "missing-branch", ref: &manifest.GitRepositoryRef{Branch: "missing"}, fallback: true},
		{name: "missing-semver", ref: &manifest.GitRepositoryRef{SemVer: ">3.0.0"}, fallback: true},
		{name: "no-store", ref: &manifest.GitRepositoryRef{Tag: "v1.0.0"}, noStore: true, fallback: true},
		{name: "no-store-invalid", ref: &manifest.GitRepositoryRef{Commit: "abc"}, noStore: true, fail: true},
		{name: "no-store-no-ref", noStore: true, head: true},
		{name: "sparse", ref: &manifest.GitRepositoryRef{Tag: "v1.0.0"}, sparse: true, fallback: true},
		{name: "submodules", ref: &manifest.GitRepositoryRef{Tag: "v1.0.0"}, submodules: true, fallback: true},
		{name: "head-sparse", ref: &manifest.GitRepositoryRef{Tag: "v2.0.0"}, sparse: true, head: true},
		{name: "missing-commit", ref: &manifest.GitRepositoryRef{Commit: strings.Repeat("f", 40)}, sparse: true, fallback: true},
		{name: "missing-commit-branch", ref: &manifest.GitRepositoryRef{Commit: strings.Repeat("f", 40), Branch: "master"}, fallback: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := discoveryRefFixture(t)
			if tc.noStore {
				if err := os.RemoveAll(filepath.Join(root, ".git")); err != nil {
					t.Fatal(err)
				}
			}
			st := store.New()
			repository := &manifest.GitRepository{Name: "cluster", Namespace: "flux-system"}
			repository.URL, repository.Reference = "git://fixture.invalid/cluster", tc.ref
			repository.RecurseSubmodules = tc.submodules
			if tc.sparse {
				repository.SparseCheckout = []string{"apps"}
			}
			st.AddObject(repository)
			cacheRoot := t.TempDir()
			d := discoverer{cfg: Config{Store: st, SelfURLs: []string{repository.URL}}}
			if !tc.head && !tc.fallback && !tc.fail {
				d.cfg.SourceCache = source.NewCache(cacheroot.New(cacheRoot))
			}
			var logs bytes.Buffer
			previous := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
			t.Cleanup(func() { slog.SetDefault(previous) })
			err := d.aliasBootstrapSources(t.Context(), root)
			if tc.fail {
				if !errors.Is(err, manifest.ErrFlux) || st.GetArtifact(repository.Named()) != nil {
					t.Fatalf("invalid source = %v, %v", err, st.GetArtifact(repository.Named()))
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if tc.fallback {
				if st.GetArtifact(repository.Named()) != nil {
					t.Fatal("fallback installed a HEAD artifact")
				}
				if status, ok := st.GetStatus(repository.Named()); ok && status.Status == store.StatusReady {
					t.Fatal("fallback marked source ready")
				}
				if strings.Count(logs.String(), "level=WARN") != 1 || !strings.Contains(logs.String(), "cluster") || !strings.Contains(logs.String(), manifest.GitRefString(*tc.ref)) {
					t.Fatalf("fallback warning = %s", logs.String())
				}
				return
			}
			artifact := st.GetArtifact(repository.Named()).(*store.SourceArtifact)
			want := "A"
			if tc.head {
				want = "dirty"
				if artifact.LocalPath != root {
					t.Fatalf("head alias path = %s", artifact.LocalPath)
				}
			} else if !strings.HasPrefix(artifact.LocalPath, cacheRoot+string(filepath.Separator)) || artifact.URL != repository.URL {
				t.Fatalf("committed artifact did not use supplied cache: %+v", artifact)
			}
			content, err := os.ReadFile(filepath.Join(artifact.LocalPath, "value.txt"))
			if err != nil || string(content) != want {
				t.Fatalf("source content = %q, %v; want %s", content, err, want)
			}
			if logs.Len() != 0 {
				t.Fatalf("materialization/HEAD unexpectedly warns: %s", logs.String())
			}
		})
	}
}

func TestAliasBootstrapSources_LocalPathOwnership(t *testing.T) {
	for _, tc := range []struct {
		name, kind, url string
		local           bool
	}{
		{name: "pinned-local", kind: manifest.KindGitRepository, url: "git://fixture.invalid/cluster", local: true},
		{name: "external-git", kind: manifest.KindGitRepository, url: "git://fixture.invalid/other"},
		{name: "external-oci", kind: manifest.KindOCIRepository, url: "oci://fixture.invalid/cluster"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := discoveryRefFixture(t)
			st := store.New()
			id := manifest.NamedResource{Kind: tc.kind, Namespace: "flux-system", Name: "cluster"}
			if tc.kind == manifest.KindGitRepository {
				st.AddObject(&manifest.GitRepository{
					Name: id.Name, Namespace: id.Namespace, URL: tc.url,
					Reference: &manifest.GitRepositoryRef{Tag: "v1.0.0"},
				})
			} else {
				st.AddObject(&manifest.OCIRepository{Name: id.Name, Namespace: id.Namespace, URL: tc.url})
			}
			ks := &manifest.Kustomization{
				Name: "apps", Namespace: id.Namespace, Path: "./apps",
				SourceKind: id.Kind, SourceName: id.Name, SourceNamespace: id.Namespace,
			}
			st.AddObject(ks)
			d := discoverer{cfg: Config{
				Store: st, SelfURLs: []string{"git://fixture.invalid/cluster"},
				SourceCache: source.NewCache(cacheroot.New(t.TempDir())),
			}}
			if err := d.aliasBootstrapSources(t.Context(), root); err != nil {
				t.Fatal(err)
			}
			if tc.local {
				artifact := st.GetArtifact(id).(*store.SourceArtifact)
				if artifact.LocalPath == root {
					t.Fatal("pinned source must render its committed tree")
				}
			} else {
				st.SetArtifact(id, &store.SourceArtifact{Kind: id.Kind, URL: tc.url, LocalPath: t.TempDir()})
			}
			prefixes := loader.KSPathPrefixesLocalOnly(st, root, nil)
			child := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "apps", Name: "release"}
			parent, owned := loader.LongestParent(prefixes, "apps/release.yaml", child)
			if owned != tc.local || owned && parent != ks.Named() {
				t.Fatalf("local path ownership = %v, %v; want local=%v", parent, owned, tc.local)
			}
		})
	}
}

func TestAliasBootstrapSources_PreservesCanonicalSource(t *testing.T) {
	root := t.TempDir()
	st := store.New()
	repository := &manifest.GitRepository{Name: "flux-system", Namespace: "flux-system"}
	repository.URL = "git://fixture.invalid/cluster"
	repository.Reference = &manifest.GitRepositoryRef{Commit: "invalid"}
	st.AddObject(repository)
	d := discoverer{cfg: Config{Path: root, RepoRoot: root, Store: st, SelfURLs: []string{repository.URL}}}
	if _, err := d.seedBootstrapSource(); err != nil {
		t.Fatal(err)
	}
	if err := d.aliasBootstrapSources(t.Context(), root); err != nil {
		t.Fatal(err)
	}
	artifact := st.GetArtifact(repository.Named()).(*store.SourceArtifact)
	if artifact.LocalPath != root || st.GetObject(repository.Named()) != repository {
		t.Fatalf("canonical bootstrap or authored object changed: %+v", artifact)
	}
}

func discoveryRefFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	repo, err := git.PlainInit(root, false)
	if err != nil {
		t.Fatal(err)
	}
	wt, err := repo.Worktree()
	if err != nil {
		t.Fatal(err)
	}
	for i, value := range []string{"A", "B"} {
		testutil.WriteFile(t, root, "value.txt", value)
		if _, err := wt.Add("."); err != nil {
			t.Fatal(err)
		}
		hash, err := wt.Commit(value, &git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(0, 0)}})
		if err != nil {
			t.Fatal(err)
		}
		if err := repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewTagReferenceName([]string{"v1.0.0", "v2.0.0"}[i]), hash)); err != nil {
			t.Fatal(err)
		}
	}
	testutil.WriteFile(t, root, "value.txt", "dirty")
	return root
}

func TestAliasBootstrapSources_NilCacheIsLazy(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("isolated default cache uses XDG_CACHE_HOME on Linux")
	}
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	root := discoveryRefFixture(t)
	defaultRoot := cacheroot.Default()
	for _, tag := range []string{"", "v2.0.0", "missing", "v1.0.0"} {
		repository := &manifest.GitRepository{Name: "cluster", Namespace: "flux-system"}
		repository.URL = "git://fixture.invalid/cluster"
		if tag != "" {
			repository.Reference = &manifest.GitRepositoryRef{Tag: tag}
		}
		st := store.New()
		st.AddObject(repository)
		d := discoverer{cfg: Config{Store: st, SelfURLs: []string{repository.URL}}}
		if err := d.aliasBootstrapSources(t.Context(), root); err != nil {
			t.Fatal(err)
		}
		_, err := os.Stat(defaultRoot)
		if tag != "v1.0.0" && !os.IsNotExist(err) {
			t.Fatalf("default cache created for %q: %v", tag, err)
		}
		if tag == "v1.0.0" {
			artifact := st.GetArtifact(repository.Named()).(*store.SourceArtifact)
			if err != nil || !strings.HasPrefix(artifact.LocalPath, defaultRoot+string(filepath.Separator)) {
				t.Fatalf("nil cache materialization = %+v, %v", artifact, err)
			}
		}
	}
}
