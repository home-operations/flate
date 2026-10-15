package discovery

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/baseline"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/safepath"
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
			testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [child.yaml, cm.yaml]\nconfigMapGenerator:\n- name: generated\n  literals: [value=pinned]\n- name: file-generated\n  files: [value=input.txt]\n")
			testutil.WriteFile(t, root, "apps/input.txt", "pinned")
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
			testutil.WriteFile(t, root, "apps/input.txt", "dirty")
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
				Path: scan, RepoRoot: root, Store: st, SelfURLs: []string{"git://fixture.invalid/cluster"},
			})
			if err != nil {
				t.Fatal(err)
			}
			parent := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "parent"}
			childID := manifest.NamedResource{Kind: manifest.KindKustomization, Name: "child"}
			phantom := manifest.NamedResource{Kind: manifest.KindKustomization, Name: "phantom"}
			cm := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "settings"}
			artifact := st.GetArtifact(manifest.NamedResource{Kind: manifest.KindGitRepository, Namespace: "flux-system", Name: "cluster"}).(*store.SourceArtifact)
			if st.GetObject(phantom) != nil || st.GetObject(childID) == nil || parentsForTest(result.Index)[childID] != parent {
				t.Fatalf("pinned children: phantom=%v child=%v parent=%v", st.GetObject(phantom), st.GetObject(childID), parentsForTest(result.Index)[childID])
			}
			generated := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "generated"}
			generatedCM, ok := st.GetObject(generated).(*manifest.ConfigMap)
			if !ok || generatedCM.Data["value"] != "pinned" || st.GetObject(manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "phantom-generated"}) != nil {
				t.Fatalf("pinned generators = %v", generatedCM)
			}
			fileGenerated, ok := st.GetObject(manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "file-generated"}).(*manifest.ConfigMap)
			if !ok || fileGenerated.Data["value"] != "pinned" {
				t.Fatalf("committed generator file = %v", fileGenerated)
			}
			for _, id := range []manifest.NamedResource{childID, cm, generated} {
				file := filepath.Join(root, filepath.FromSlash(filesForTest(result.Index)[id]))
				if !safepath.Contains(artifact.LocalPath, file) {
					t.Fatalf("source file for %s = %s, want under %s", id, file, artifact.LocalPath)
				}
			}
			if got := renderersForTest(result.Index, cm); !slices.Equal(got, []manifest.NamedResource{parent}) {
				t.Fatalf("self-produce = %v, want [%s]", got, parent)
			}
			f := change.NewFilter(change.NewSet([]string{"flux/unrelated.yaml"}), result.Index, st)
			if got := f.ProducersFor(cm); !slices.Equal(got, []manifest.NamedResource{parent}) {
				t.Fatalf("producers = %v, want [%s]", got, parent)
			}
		})
	}
}

func TestAliasBootstrapSources_DeclaredRefs(t *testing.T) {
	for _, tc := range []struct {
		name, base                                        string
		commit                                            bool
		ref                                               *manifest.GitRepositoryRef
		head, fallback, fail, noStore, sparse, submodules bool
	}{
		{name: "no-ref", head: true},
		{name: "empty-ref", ref: &manifest.GitRepositoryRef{}, head: true},
		{name: "head", ref: &manifest.GitRepositoryRef{Tag: "v2.0.0"}, head: true},
		{name: "older-base-head-tag", base: "v1.0.0", ref: &manifest.GitRepositoryRef{Tag: "v2.0.0"}},
		{name: "older-base-head-commit", base: "v1.0.0", commit: true},
		{name: "matching-base-head", base: "v2.0.0", ref: &manifest.GitRepositoryRef{Tag: "v2.0.0"}, head: true},
		{name: "older-base-head-sparse", base: "v1.0.0", ref: &manifest.GitRepositoryRef{Tag: "v2.0.0"}, sparse: true},
		{name: "older-base-head-submodules", base: "v1.0.0", ref: &manifest.GitRepositoryRef{Tag: "v2.0.0"}, submodules: true, fallback: true},
		{name: "non-head", ref: &manifest.GitRepositoryRef{Tag: "v1.0.0"}},
		{name: "conflicting", ref: &manifest.GitRepositoryRef{Tag: "v1.0.0", Branch: "bad branch"}},
		{name: "missing-tag", ref: &manifest.GitRepositoryRef{Tag: "missing"}, fallback: true},
		{name: "missing-name", ref: &manifest.GitRepositoryRef{Name: "refs/pull/9/head"}, fallback: true},
		{name: "missing-branch", ref: &manifest.GitRepositoryRef{Branch: "missing"}, fallback: true},
		{name: "missing-semver", ref: &manifest.GitRepositoryRef{SemVer: ">3.0.0"}, fallback: true},
		{name: "no-store", ref: &manifest.GitRepositoryRef{Tag: "v1.0.0"}, noStore: true, fallback: true},
		{name: "no-store-invalid", ref: &manifest.GitRepositoryRef{Commit: "abc"}, noStore: true, fail: true},
		{name: "no-store-no-ref", noStore: true, head: true},
		{name: "sparse", ref: &manifest.GitRepositoryRef{Tag: "v1.0.0"}, sparse: true},
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
			var revision string
			if tc.base != "" {
				repo, err := git.PlainOpen(root)
				if err != nil {
					t.Fatal(err)
				}
				head, err := repo.Head()
				if err != nil {
					t.Fatal(err)
				}
				revision = head.Hash().String()
				if tc.commit {
					repository.Reference = &manifest.GitRepositoryRef{Commit: revision}
				}
				ignore := "apps/excluded.txt\n"
				repository.Ignore = &ignore
				owner, err := sourcecache.New(t.Context(), "")
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := owner.Close(); err != nil {
						t.Error(err)
					}
				})
				result, err := baseline.AutoResolve(t.Context(), root, tc.base, owner)
				if err != nil {
					t.Fatal(err)
				}
				st.SetArtifact(manifest.BootstrapSourceID, result.BaseInput)
			}
			repository.RecurseSubmodules = tc.submodules
			if tc.sparse {
				repository.SparseCheckout = []string{"apps"}
			}
			st.AddObject(repository)
			d := discoverer{cfg: Config{Path: root, RepoRoot: root, Store: st, SelfURLs: []string{repository.URL}}}
			if _, err := d.seedBootstrapSource(t.Context()); err != nil {
				t.Fatal(err)
			}
			var logs bytes.Buffer
			previous := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
			t.Cleanup(func() { slog.SetDefault(previous) })
			bootstrap := st.GetArtifact(manifest.BootstrapSourceID).(*store.SourceArtifact)
			side := bootstrap.LocalPath
			d.cfg.Path = side
			if _, err := d.seedBootstrapSource(t.Context()); err != nil {
				t.Fatal(err)
			}
			aliased, err := d.overrideSelfReferentialGitRepositories(t.Context(), side)
			if st.GetArtifact(manifest.BootstrapSourceID) != bootstrap || st.GetObject(repository.Named()) != repository {
				t.Fatal("discovery replaced the bootstrap or authored source")
			}
			if tc.base != "" {
				want := "A"
				if tc.head {
					want = "B"
				}
				content, err := fs.ReadFile(bootstrap.FS, "value.txt")
				if err != nil || string(content) != want || bootstrap.Key == "" || bootstrap.LocalRoot != root {
					t.Fatalf("baseline content = %q, %v; want %s with immutable checkout identity: %+v", content, err, want, bootstrap)
				}
			}
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
				if strings.Count(logs.String(), "level=WARN") != 1 || !strings.Contains(logs.String(), "cluster") || !strings.Contains(logs.String(), manifest.GitRefString(*repository.Reference)) {
					t.Fatalf("fallback warning = %s", logs.String())
				}
				return
			}
			artifact := st.GetArtifact(repository.Named()).(*store.SourceArtifact)
			if tc.base != "" {
				for range 2 {
					content, err := fs.ReadFile(artifact.FS, "apps/value.txt")
					if err != nil || string(content) != "B" {
						t.Fatalf("pinned HEAD content = %q, %v; want B", content, err)
					}
				}
				if artifact.Revision != revision || (artifact.Key == "") != tc.head || artifact.LocalRoot != side || artifact.LocalPath == side || artifact.InputAlias != tc.head || d.hasPins == tc.head || (len(aliased) == 1) != tc.head {
					t.Fatalf("pinned HEAD identity/classification = %+v, pins=%t aliases=%v", artifact, d.hasPins, aliased)
				}
				if !tc.head && artifact.URL != repository.URL {
					t.Fatalf("committed URL = %q, want %q", artifact.URL, repository.URL)
				}
				status, ok := st.GetStatus(repository.Named())
				if !ok || status.Status != store.StatusReady {
					t.Fatalf("pinned HEAD status = %v, %t", status, ok)
				}
				for _, name := range []string{"apps/ignored.txt", "apps/excluded.txt"} {
					if _, err := fs.Stat(artifact.FS, name); !errors.Is(err, fs.ErrNotExist) {
						t.Fatalf("source retained excluded %s: %v", name, err)
					}
					if content, err := fs.ReadFile(bootstrap.FS, name); err != nil || len(content) == 0 {
						t.Fatalf("source shaping changed direct bootstrap %s: %q, %v", name, content, err)
					}
				}
			}
			want := "A"
			if tc.base != "" {
				want = "B"
			} else if tc.head {
				want = "dirty"
				if artifact.LocalRoot != root || artifact.LocalPath == root || artifact.FS == nil || artifact.Key != "" {
					t.Fatalf("mutable alias did not retain its admitted origin: %+v", artifact)
				}
			} else if artifact.FS == nil || artifact.Key == "" || artifact.URL != repository.URL {
				t.Fatalf("committed artifact did not use supplied cache: %+v", artifact)
			}
			if artifact.FS == nil || tc.head && tc.base == "" && artifact.Key != "" {
				t.Fatal("local admission violated the FS/Key state contract")
			}
			content, err := fs.ReadFile(artifact.FS, "value.txt")
			if tc.sparse {
				if !errors.Is(err, fs.ErrNotExist) {
					t.Fatalf("sparse source retained excluded file: %q, %v", content, err)
				}
				return
			}
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
				SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: id.Kind, Name: id.Name, Namespace: id.Namespace},
			}
			st.AddObject(ks)
			d := discoverer{cfg: Config{
				Path: root, RepoRoot: root, Store: st, SelfURLs: []string{"git://fixture.invalid/cluster"},
			}}
			if _, err := d.seedBootstrapSource(t.Context()); err != nil {
				t.Fatal(err)
			}
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
			l := loader.New(st)
			l.FinalizeIndex(root, nil)
			child := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "apps", Name: "release"}
			owners, _ := l.Index.OwnersOfFile("apps/release.yaml")
			parent := manifest.NamedResource{}
			for _, owner := range owners {
				if owner != child {
					parent = owner
					break
				}
			}
			owned := parent != (manifest.NamedResource{})
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
	if _, err := d.seedBootstrapSource(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := d.overrideSelfReferentialGitRepositories(t.Context(), root); err != nil {
		t.Fatal(err)
	}
	artifact := st.GetArtifact(repository.Named()).(*store.SourceArtifact)
	if artifact.LocalPath != root || st.GetObject(repository.Named()) != repository {
		t.Fatalf("canonical bootstrap or authored object changed: %+v", artifact)
	}
	t.Run("mutable alias refresh", func(t *testing.T) {
		testutil.WriteFile(t, root, "chart/Chart.yaml", "apiVersion: v2\nname: chart\nversion: 1.0.0\n")
		testutil.WriteFile(t, root, "chart/templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: main\n")
		testutil.WriteFile(t, root, "excluded/file", "hidden")
		alias := &manifest.GitRepository{Name: "alias", Namespace: "flux-system", URL: repository.URL}
		alias.Ignore = new("excluded/\n")
		st.AddObject(alias)
		ok, err := d.publishBootstrapAlias(t.Context(), alias.Named(), root)
		if err != nil || !ok {
			t.Fatalf("alias: %t, %v", ok, err)
		}
		art := st.GetArtifact(alias.Named()).(*store.SourceArtifact)
		if art.Key != "" || !art.InputAlias || art.LocalRoot != root || art.LocalPath == root {
			t.Fatalf("alias coordinates: %+v", art)
		}
		owner, err := sourcecache.New(t.Context(), "")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := owner.Close(); err != nil {
				t.Error(err)
			}
		})
		cli, err := helm.NewClient(owner)
		if err != nil {
			t.Fatal(err)
		}
		cli.SetSourceResolver(helm.NewStoreSourceResolver(st))
		hr := &manifest.HelmRelease{Name: "release", Namespace: "apps", Chart: manifest.HelmChart{Name: "chart", RepoKind: alias.Named().Kind, RepoName: alias.Name, RepoNamespace: alias.Namespace}}
		for _, exists := range []bool{false, true, false} {
			if exists {
				testutil.WriteFile(t, root, "chart/templates/arrived/deep/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: arrived\n")
			} else if err := os.RemoveAll(filepath.Join(root, "chart/templates/arrived")); err != nil {
				t.Fatal(err)
			}
			out, err := cli.Template(t.Context(), hr, nil, helm.Options{})
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(out, "name: arrived") != exists {
				t.Fatalf("alias membership stale: %s", out)
			}
			if st.GetArtifact(alias.Named()) != art {
				t.Fatal("Template republished alias")
			}
			view, pin, err := source.ReadView(t.Context(), art.FS, false)
			if err != nil || pin != "" {
				t.Fatalf("alias admission: %q, %v", pin, err)
			}
			if _, err := fs.Stat(view, "excluded/file"); !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("alias lost shaping: %v", err)
			}
		}
		if _, err := d.publishBootstrapAlias(t.Context(), alias.Named(), root); err != nil {
			t.Fatal(err)
		}
		if len(d.aliasViews) != 1 || st.GetObject(alias.Named()) != alias {
			t.Fatal("alias refresh changed registry or authored object")
		}
	})

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
		testutil.WriteFile(t, root, "apps/value.txt", value)
		testutil.WriteFile(t, root, "apps/ignored.txt", value)
		testutil.WriteFile(t, root, "apps/excluded.txt", value)
		testutil.WriteFile(t, root, ".sourceignore", "apps/ignored.txt\n")
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
	testutil.WriteFile(t, root, "apps/value.txt", "dirty")
	return root
}

func TestAliasBootstrapSources_NilCacheIsLazy(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("isolated default cache uses XDG_CACHE_HOME on Linux")
	}
	fallback := t.TempDir()
	t.Setenv("XDG_CACHE_HOME", fallback)
	root := discoveryRefFixture(t)
	defaultRoot := filepath.Join(fallback, "flate")
	for _, tag := range []string{"", "v2.0.0", "missing", "v1.0.0"} {
		repository := &manifest.GitRepository{Name: "cluster", Namespace: "flux-system"}
		repository.URL = "git://fixture.invalid/cluster"
		if tag != "" {
			repository.Reference = &manifest.GitRepositoryRef{Tag: tag}
		}
		st := store.New()
		st.AddObject(repository)
		d := discoverer{cfg: Config{Path: root, RepoRoot: root, Store: st, SelfURLs: []string{repository.URL}}}
		if _, err := d.seedBootstrapSource(t.Context()); err != nil {
			t.Fatal(err)
		}
		if _, err := d.overrideSelfReferentialGitRepositories(t.Context(), root); err != nil {
			t.Fatal(err)
		}
		_, err := os.Stat(defaultRoot)
		if !os.IsNotExist(err) {
			t.Fatalf("default cache created for %q: %v", tag, err)
		}
		if tag == "v1.0.0" {
			artifact := st.GetArtifact(repository.Named()).(*store.SourceArtifact)
			if artifact.FS == nil || artifact.Key == "" {
				t.Fatalf("committed source = %+v", artifact)
			}
		}
	}
}
