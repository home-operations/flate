package discovery

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cacheroot"
	"github.com/home-operations/flate/pkg/store"
)

func TestRun_PinnedArtifactDiscovery(t *testing.T) {
	for _, tc := range []struct {
		revealed, broad bool
	}{{}, {revealed: true}, {broad: true}, {revealed: true, broad: true}} {
		t.Run(fmt.Sprintf("followed_source_%t_broad_%t", tc.revealed, tc.broad), func(t *testing.T) {
			root := discoveryRefFixture(t)
			repo, err := git.PlainOpen(root)
			if err != nil {
				t.Fatal(err)
			}
			entry := "flux"
			if tc.revealed {
				entry = "sources"
				testutil.WriteFile(t, root, "flux/entry.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: entry, namespace: flux-system}
spec: {path: ./sources, sourceRef: {kind: GitRepository, name: flux-system}}
`)
			}
			testutil.WriteFile(t, root, entry+"/pinned.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: cluster, namespace: flux-system}
spec: {url: 'git://fixture.invalid/cluster', ref: {tag: pinned}}
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: parent, namespace: flux-system}
spec: {path: ./apps, sourceRef: {kind: GitRepository, name: cluster}}
`)
			child := `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: child}
spec: {path: ./leaf, sourceRef: {kind: GitRepository, name: cluster, namespace: flux-system}}
`
			testutil.WriteFile(t, root, "apps/child.yaml", child)
			testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [child.yaml, cm.yaml]\nconfigMapGenerator:\n- name: generated\n  literals: [value=pinned]\n")
			testutil.WriteFile(t, root, "apps/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: flux-system}\ndata: {value: pinned}\n")
			wt, err := repo.Worktree()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := wt.Add("."); err != nil {
				t.Fatal(err)
			}
			hash, err := wt.Commit("pinned", &git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(1, 0)}})
			if err != nil {
				t.Fatal(err)
			}
			if err := repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewTagReferenceName("pinned"), hash)); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(filepath.Join(root, "apps/child.yaml")); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(filepath.Join(root, "apps/cm.yaml")); err != nil {
				t.Fatal(err)
			}
			testutil.WriteFile(t, root, "apps/phantom.yaml", strings.Replace(child, "name: child", "name: phantom", 1))
			testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [phantom.yaml]\nconfigMapGenerator:\n- name: generated\n  literals: [value=dirty]\n- name: phantom-generated\n  literals: [value=dirty]\n")
			if _, err := wt.Add("."); err != nil {
				t.Fatal(err)
			}
			if _, err := wt.Commit("head", &git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(2, 0)}}); err != nil {
				t.Fatal(err)
			}
			st := store.New()
			scan := filepath.Join(root, "flux")
			if tc.broad {
				scan = root
			}
			result, err := Run(t.Context(), Config{
				Path: scan, Store: st,
				SelfURLs: []string{"git://fixture.invalid/cluster"}, SourceCache: source.NewCache(cacheroot.New(t.TempDir())),
			})
			if err != nil {
				t.Fatal(err)
			}
			parent := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "parent"}
			childID := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "child"}
			phantom := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "phantom"}
			cm := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "settings"}
			artifact := st.GetArtifact(manifest.NamedResource{Kind: manifest.KindGitRepository, Namespace: "flux-system", Name: "cluster"}).(*store.SourceArtifact)
			if st.GetObject(phantom) != nil || st.GetObject(childID) == nil || result.ParentOf[childID] != parent {
				t.Fatalf("pinned children: phantom=%v child=%v parent=%v", st.GetObject(phantom), st.GetObject(childID), result.ParentOf[childID])
			}
			generated := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "generated"}
			generatedCM, ok := st.GetObject(generated).(*manifest.ConfigMap)
			if !ok || generatedCM.Data["value"] != "pinned" || st.GetObject(manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "phantom-generated"}) != nil {
				t.Fatalf("pinned generators = %v", generatedCM)
			}
			for _, id := range []manifest.NamedResource{childID, cm, generated} {
				file := filepath.Join(root, filepath.FromSlash(result.SourceFiles[id]))
				if !pathUnderRoot(file, artifact.LocalPath) {
					t.Fatalf("source file for %s = %s, want under %s", id, file, artifact.LocalPath)
				}
			}
			if got := result.SelfProduce.ProducedBy(cm); !slices.Equal(got, []manifest.NamedResource{parent}) {
				t.Fatalf("self-produce = %v, want [%s]", got, parent)
			}
			f := change.NewFilterWithOptions(change.NewSet([]string{"flux/unrelated.yaml"}), result.SourceFiles, st, change.FilterOptions{
				RepoRoot: root, FileOwners: result.SelfProduce.OwnersOfFile,
			})
			if got := f.ProducersFor(cm); !slices.Equal(got, []manifest.NamedResource{parent}) {
				t.Fatalf("producers = %v, want [%s]", got, parent)
			}
		})
	}
}

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
			_, err := d.overrideSelfReferentialGitRepositories(t.Context(), root)
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
			if _, err := d.overrideSelfReferentialGitRepositories(t.Context(), root); err != nil {
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
	if _, err := d.overrideSelfReferentialGitRepositories(t.Context(), root); err != nil {
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
		if _, err := d.overrideSelfReferentialGitRepositories(t.Context(), root); err != nil {
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
