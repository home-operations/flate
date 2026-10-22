package discovery

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/baseline"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/source/safepath"
	"github.com/home-operations/flate/pkg/store"
)

func TestRun_PinnedArtifactDiscovery(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		revealed, broad bool
	}{{}, {revealed: true}, {broad: true}, {revealed: true, broad: true}} {
		t.Run(fmt.Sprintf("followed_source_%t_broad_%t", tc.revealed, tc.broad), func(t *testing.T) {
			t.Parallel()
			root := discoveryRefFixture(t)
			repo, err := git.PlainOpen(root)
			if err != nil {
				t.Fatal(err)
			}
			entry := "flux"
			if tc.revealed {
				entry = "sources"
				testutil.WriteFile(t, root, "flux/entry.yaml", testutil.KustomizationYAML("entry", "flux-system", "  {path: ./sources, sourceRef: {kind: GitRepository, name: flux-system}}\n"))
			}
			testutil.WriteFile(t, root, entry+"/pinned.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: cluster, namespace: flux-system}
spec: {url: 'git://fixture.invalid/cluster', ref: {tag: pinned}}
`+"---\n"+
				testutil.KustomizationYAML("parent", "flux-system", "  {path: ./apps, sourceRef: {kind: GitRepository, name: cluster}}\n"))
			child := testutil.KustomizationYAML("child", "", "  {path: ./leaf, sourceRef: {kind: GitRepository, name: cluster, namespace: flux-system}}\n")
			testutil.WriteFile(t, root, "apps/child.yaml", child)
			testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [child.yaml, cm.yaml]\nconfigMapGenerator:\n- name: generated\n  literals: [value=pinned]\n- name: file-generated\n  files: [value=input.txt]\n")
			testutil.WriteFile(t, root, "apps/input.txt", "pinned")
			testutil.WriteFile(t, root, "apps/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: flux-system}\ndata: {value: pinned}\n")
			hash := testutil.GitCommitAll(t, repo, "pinned", &git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(1, 0)}})
			if err := repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewTagReferenceName("pinned"), hash)); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(filepath.Join(root, "apps/child.yaml")); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(filepath.Join(root, "apps/cm.yaml")); err != nil {
				t.Fatal(err)
			}
			testutil.WriteFile(t, root, "apps/child.yaml", strings.Replace(child, manifest.FluxKustomizeDomain, "foreign.example", 1))
			testutil.WriteFile(t, root, "apps/phantom.yaml", strings.Replace(child, "name: child", "name: phantom", 1))
			testutil.WriteFile(t, root, "apps/input.txt", "dirty")
			testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [phantom.yaml, child.yaml]\nconfigMapGenerator:\n- name: generated\n  literals: [value=dirty]\n- name: phantom-generated\n  literals: [value=dirty]\n")
			testutil.GitCommitAll(t, repo, "head", &git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(2, 0)}})
			st := store.New()
			foreignSource := &manifest.RawObject{APIVersion: "foreign.example/v1", Kind: manifest.KindGitRepository, Namespace: "flux-system", Name: "cluster"}
			st.AddObject(foreignSource)
			foreignArtifact := &store.SourceArtifact{LocalPath: root, FS: os.DirFS(root)}
			st.SetArtifact(foreignSource.Named(), foreignArtifact)
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
			if st.GetArtifact(foreignSource.Named()) != foreignArtifact {
				t.Fatal("pinned source replaced a foreign artifact")
			}
			parent := testutil.KustomizationID("flux-system", "parent")
			childID := testutil.KustomizationID("", "child")
			phantom := testutil.KustomizationID("", "phantom")
			cm := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "settings"}
			artifact := st.GetArtifact(manifest.NamedResource{Group: "source.toolkit.fluxcd.io", Kind: manifest.KindGitRepository, Namespace: "flux-system", Name: "cluster"}).(*store.SourceArtifact)
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
			if got := testutil.Owners(st.Productions(cm, result.Index)); !slices.Equal(got, []manifest.NamedResource{parent}) {
				t.Fatalf("producers = %v, want [%s]", got, parent)
			}
		})
	}
}

func TestAliasBootstrapSources_DeclaredRefs(t *testing.T) {
	for _, tc := range []struct {
		name, base                                        string
		commit                                            bool
		ref                                               *sourcev1.GitRepositoryRef
		head, fallback, fail, noStore, sparse, submodules bool
	}{
		{name: "no-ref", head: true},
		{name: "empty-ref", ref: &sourcev1.GitRepositoryRef{}, head: true},
		{name: "head", ref: &sourcev1.GitRepositoryRef{Tag: "v2.0.0"}, head: true},
		{name: "older-base-head-tag", base: "v1.0.0", ref: &sourcev1.GitRepositoryRef{Tag: "v2.0.0"}},
		{name: "older-base-head-commit", base: "v1.0.0", commit: true},
		{name: "matching-base-head", base: "v2.0.0", ref: &sourcev1.GitRepositoryRef{Tag: "v2.0.0"}, head: true},
		{name: "older-base-head-sparse", base: "v1.0.0", ref: &sourcev1.GitRepositoryRef{Tag: "v2.0.0"}, sparse: true},
		{name: "older-base-head-submodules", base: "v1.0.0", ref: &sourcev1.GitRepositoryRef{Tag: "v2.0.0"}, submodules: true, fallback: true},
		{name: "non-head", ref: &sourcev1.GitRepositoryRef{Tag: "v1.0.0"}},
		{name: "conflicting", ref: &sourcev1.GitRepositoryRef{Tag: "v1.0.0", Branch: "bad branch"}},
		{name: "missing-tag", ref: &sourcev1.GitRepositoryRef{Tag: "missing"}, fallback: true},
		{name: "missing-name", ref: &sourcev1.GitRepositoryRef{Name: "refs/pull/9/head"}, fallback: true},
		{name: "missing-branch", ref: &sourcev1.GitRepositoryRef{Branch: "missing"}, fallback: true},
		{name: "missing-semver", ref: &sourcev1.GitRepositoryRef{SemVer: ">3.0.0"}, fallback: true},
		{name: "no-store", ref: &sourcev1.GitRepositoryRef{Tag: "v1.0.0"}, noStore: true, fallback: true},
		{name: "no-store-invalid", ref: &sourcev1.GitRepositoryRef{Commit: "abc"}, noStore: true, fail: true},
		{name: "no-store-no-ref", noStore: true, head: true},
		{name: "sparse", ref: &sourcev1.GitRepositoryRef{Tag: "v1.0.0"}, sparse: true},
		{name: "submodules", ref: &sourcev1.GitRepositoryRef{Tag: "v1.0.0"}, submodules: true, fallback: true},
		{name: "head-sparse", ref: &sourcev1.GitRepositoryRef{Tag: "v2.0.0"}, sparse: true, head: true},
		{name: "missing-commit", ref: &sourcev1.GitRepositoryRef{Commit: strings.Repeat("f", 40)}, sparse: true, fallback: true},
		{name: "missing-commit-branch", ref: &sourcev1.GitRepositoryRef{Commit: strings.Repeat("f", 40), Branch: "master"}, fallback: true},
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
					repository.Reference = &sourcev1.GitRepositoryRef{Commit: revision}
				}
				ignore := "apps/excluded.txt\n"
				repository.Ignore = &ignore
				owner := cachetest.New(t, "", sourcecache.New)
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
			d := discoverer{cfg: Config{Path: root, RepoRoot: root, Store: st, SelfURLs: []string{repository.URL}}}
			if _, err := d.resolveEntrySources(t.Context(), false); err != nil {
				t.Fatal(err)
			}
			st.AddObject(repository)
			bootstrap := st.GetArtifact(manifest.BootstrapSourceID).(*store.SourceArtifact)
			side := bootstrap.LocalPath
			d.cfg.Path = side
			aliased, err := d.resolveEntrySources(t.Context(), false)
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
				warnings := st.Warnings()
				if len(warnings) != 1 || warnings[0].Category != manifest.WarnSourceRefUnavailable || warnings[0].Resource != repository.Named() || warnings[0].Count != 1 || warnings[0].Detail[0] != (*manifest.GitRepositoryRef)(repository.Reference).String() {
					t.Fatalf("fallback warning = %+v", warnings)
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
				if artifact.Revision != revision || (artifact.Key == "") != tc.head || artifact.LocalRoot != side || artifact.LocalPath == side || artifact.InputAlias != tc.head || d.hasPins == tc.head || (artifact.InputAlias) != tc.head {
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
			if warnings := st.Warnings(); len(warnings) != 0 {
				t.Fatalf("materialization/HEAD unexpectedly warns: %+v", warnings)
			}
		})
	}
}

func TestAliasBootstrapSources_LocalPathOwnership(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, kind, url string
		local, late     bool
	}{
		{name: "pinned-local", kind: manifest.KindGitRepository, url: "git://fixture.invalid/cluster", local: true},
		{name: "external-git", kind: manifest.KindGitRepository, url: "git://fixture.invalid/other"},
		{name: "external-oci", kind: manifest.KindOCIRepository, url: "oci://fixture.invalid/cluster"},
		{name: "late-pinned-local", kind: manifest.KindGitRepository, url: "git://fixture.invalid/cluster", local: true, late: true},
		{name: "late-external-git", kind: manifest.KindGitRepository, url: "git://fixture.invalid/other", late: true},
		{name: "late-external-oci", kind: manifest.KindOCIRepository, url: "oci://fixture.invalid/cluster", late: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := discoveryRefFixture(t)
			st := store.New()
			id := manifest.NamedResource{Group: manifest.SourceDomain, Kind: tc.kind, Namespace: "flux-system", Name: "cluster"}
			if tc.kind == manifest.KindGitRepository {
				st.AddObject(&manifest.GitRepository{
					Name: id.Name, Namespace: id.Namespace, URL: tc.url,
					Reference: &sourcev1.GitRepositoryRef{Tag: "v1.0.0"},
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
			if tc.late {
				if _, err := d.resolveEntrySources(t.Context(), false); err != nil {
					t.Fatal(err)
				}
				st.DeleteObject(id)
				if _, err := d.resolveEntrySources(t.Context(), true); err != nil {
					t.Fatal(err)
				}
				if st.GetArtifact(id) == nil {
					t.Fatal("missing source did not acquire a bootstrap alias")
				}
				ref := ""
				if tc.kind == manifest.KindGitRepository {
					ref = "\n  ref: {tag: v1.0.0}"
				}
				testutil.WriteFile(t, root, "authored/source.yaml", fmt.Sprintf("apiVersion: source.toolkit.fluxcd.io/v1\nkind: %s\nmetadata: {name: cluster, namespace: flux-system}\nspec:\n  url: %q%s\n", tc.kind, tc.url, ref))
				l := loader.New(st)
				l.SourceRoot, l.PreferExisting = root, true
				if _, err := l.Load(t.Context(), filepath.Join(root, "authored")); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := d.resolveEntrySources(t.Context(), false); err != nil {
				t.Fatal(err)
			}
			if _, err := d.resolveEntrySources(t.Context(), false); err != nil {
				t.Fatal(err)
			}
			authored := st.GetObject(id)
			if aliases, err := d.resolveEntrySources(t.Context(), true); err != nil || aliases != 0 || st.GetObject(id) != authored {
				t.Fatalf("authored source replaced: %v, %v", aliases, err)
			}
			if !tc.local && st.GetArtifact(id) != nil {
				t.Fatal("external source acquired by aliasing")
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
			if err := l.FinalizeIndex(t.Context(), root); err != nil {
				t.Fatal(err)
			}
			child := testutil.HelmReleaseID("apps", "release")
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
	t.Parallel()
	for _, pinned := range []bool{false, true} {
		t.Run(fmt.Sprintf("shared_view_pinned_%t", pinned), func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			bootstrap := &store.SourceArtifact{Kind: manifest.KindGitRepository, LocalPath: root, LocalRoot: root, Revision: "one", Digest: "aaa", FS: fstest.MapFS{"file": {Data: []byte("first")}}}
			if pinned {
				bootstrap.Key = "family"
			}
			st.SetArtifact(manifest.BootstrapSourceID, bootstrap)
			d := discoverer{cfg: Config{Path: root, Store: st, SelfURLs: []string{"https://fixture.invalid/repo"}}}
			ids := []manifest.NamedResource{{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "one", Name: "source"}, {Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "two", Name: "alias"}}
			for _, id := range ids {
				if id == ids[1] {
					repository := &manifest.GitRepository{Name: id.Name, Namespace: id.Namespace, URL: "https://fixture.invalid/repo"}
					repository.SparseCheckout = []string{}
					st.AddObject(repository)
				}
				st.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace, SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: id.Kind, Name: id.Name}})
				if count, err := d.resolveEntrySources(t.Context(), true); err != nil || count != 1 {
					t.Fatalf("publish alias = %d, %v", count, err)
				}
			}
			first := st.GetArtifact(ids[0]).(*store.SourceArtifact)
			second := st.GetArtifact(ids[1]).(*store.SourceArtifact)
			if first.Key != "" || second.Key != "" || first.LocalPath == second.LocalPath || first.LocalRoot != root || second.LocalRoot != root || !first.InputAlias || !second.InputAlias {
				t.Fatalf("alias view/ownership = %+v, %+v", first, second)
			}
			generations := []struct{ revision, digest string }{{"two", "aaa"}, {"two", "bbb"}}
			if !pinned {
				generations = append(generations, generations[1])
			}
			for _, generation := range generations {
				next := *bootstrap
				next.Revision, next.Digest = generation.revision, generation.digest
				next.FS = fstest.MapFS{"file": {Data: []byte("other" + first.Revision + first.Digest)}}
				st.SetArtifact(manifest.BootstrapSourceID, &next)
				if _, err := d.resolveEntrySources(t.Context(), true); err != nil {
					t.Fatal(err)
				}
				got := st.GetArtifact(ids[0]).(*store.SourceArtifact)
				if got.FS == first.FS || got.Key != "" || got.Revision != generation.revision || got.Digest != generation.digest {
					t.Fatalf("generation reused alias identity: %+v", got)
				}
				data, err := fs.ReadFile(got.FS, "file")
				if err != nil || string(data) != "other"+first.Revision+first.Digest {
					t.Fatalf("alias generation bytes = %q, %v", data, err)
				}
				first, bootstrap = got, &next
			}
		})
	}
	for _, kind := range []string{manifest.KindOCIRepository, manifest.KindBucket} {
		t.Run("frozen_generation/"+kind, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			bootstrap := &store.SourceArtifact{LocalPath: root, LocalRoot: root, Revision: "one", Digest: "aaa", FS: fstest.MapFS{
				"kubernetes/clusters/c0/entry": {}, "kubernetes/apps/x/file": {Data: []byte("first")},
			}}
			st.SetArtifact(manifest.BootstrapSourceID, bootstrap)
			ks := &manifest.Kustomization{Name: "parent", Namespace: "apps", Path: "./apps/x", SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: kind, Name: "source"}}
			st.AddObject(ks)
			d := discoverer{cfg: Config{Path: filepath.Join(root, "kubernetes/clusters/c0"), Store: st}}
			if count, err := d.resolveEntrySources(t.Context(), true); err != nil || count != 1 {
				t.Fatalf("initial resolution = %d, %v", count, err)
			}
			for i, generation := range []struct{ revision, digest string }{{"two", "aaa"}, {"two", "bbb"}, {"two", "bbb"}} {
				previous := st.GetArtifact(ks.SourceID())
				object := st.GetObject(ks.SourceID())
				later := *ks
				later.Name, later.Path = fmt.Sprintf("later-%d", i), "./root-only"
				st.AddObject(&later)
				if count, err := d.resolveEntrySources(t.Context(), true); err != nil || count != 0 || st.GetArtifact(ks.SourceID()) != previous || st.GetObject(ks.SourceID()) != object || st.GetArtifact(manifest.BootstrapSourceID) != bootstrap {
					t.Fatalf("reference-only resolution changed publication: %d, %v", count, err)
				}
				next := *bootstrap
				next.Revision, next.Digest = generation.revision, generation.digest
				want := fmt.Sprintf("generation-%d", i)
				next.FS = fstest.MapFS{
					"kubernetes/apps/x/file": {Data: []byte(want)}, "apps/x/file": {Data: []byte("wrong root")}, "root-only/file": {},
				}
				st.SetArtifact(manifest.BootstrapSourceID, &next)
				if count, err := d.resolveEntrySources(t.Context(), true); err != nil || count != 1 {
					t.Fatalf("generation resolution = %d, %v", count, err)
				}
				artifact := st.GetArtifact(ks.SourceID()).(*store.SourceArtifact)
				data, err := fs.ReadFile(artifact.FS, "apps/x/file")
				if err != nil || string(data) != want || artifact == previous || artifact.LocalRoot != filepath.Join(root, "kubernetes") || artifact.Revision != next.Revision || artifact.Digest != next.Digest || st.GetObject(ks.SourceID()) != object || st.GetArtifact(manifest.BootstrapSourceID) != &next {
					t.Fatalf("frozen generation artifact=%+v bytes=%q error=%v", artifact, data, err)
				}
				bootstrap = &next
			}
		})
	}

	root := t.TempDir()
	st := store.New()
	repository := &manifest.GitRepository{Name: "flux-system", Namespace: "flux-system"}
	repository.URL = "git://fixture.invalid/cluster"
	repository.Reference = &sourcev1.GitRepositoryRef{Commit: "invalid"}
	st.AddObject(repository)
	d := discoverer{cfg: Config{Path: root, RepoRoot: root, Store: st, SelfURLs: []string{repository.URL}}}
	if _, err := d.resolveEntrySources(t.Context(), false); err != nil {
		t.Fatal(err)
	}
	if _, err := d.resolveEntrySources(t.Context(), false); err != nil {
		t.Fatal(err)
	}
	artifact := st.GetArtifact(repository.Named()).(*store.SourceArtifact)
	if artifact.LocalPath != root || st.GetObject(repository.Named()) != repository {
		t.Fatalf("canonical bootstrap or authored object changed: %+v", artifact)
	}
	t.Run("mutable alias refresh", func(t *testing.T) {
		t.Parallel()
		testutil.WriteFile(t, root, "chart/Chart.yaml", "apiVersion: v2\nname: chart\nversion: 1.0.0\n")
		testutil.WriteFile(t, root, "chart/templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: main\n")
		testutil.WriteFile(t, root, "excluded/file", "hidden")
		alias := &manifest.GitRepository{Name: "alias", Namespace: "flux-system", URL: repository.URL}
		alias.Ignore = new("excluded/\n")
		st.AddObject(alias)
		count, err := d.resolveEntrySources(t.Context(), true)
		if err != nil || count != 1 {
			t.Fatalf("alias: %d, %v", count, err)
		}
		art := st.GetArtifact(alias.Named()).(*store.SourceArtifact)
		if art.Key != "" || !art.InputAlias || art.LocalRoot != root || art.LocalPath == root {
			t.Fatalf("alias coordinates: %+v", art)
		}
		owner := cachetest.New(t, "", sourcecache.New)
		cli, err := helm.NewClient(owner)
		if err != nil {
			t.Fatal(err)
		}

		hr := &manifest.HelmRelease{Name: "release", Namespace: "apps", Chart: manifest.HelmChart{Name: "chart", Source: manifest.NamedResource{Group: alias.Named().Group, Kind: alias.Named().Kind, Namespace: alias.Namespace, Name: alias.Name}}}
		for _, exists := range []bool{false, true, false} {
			if exists {
				testutil.WriteFile(t, root, "chart/templates/arrived/deep/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: arrived\n")
			} else if err := os.RemoveAll(filepath.Join(root, "chart/templates/arrived")); err != nil {
				t.Fatal(err)
			}
			out, err := cli.Template(t.Context(), hr, nil, helm.RenderOptions{Options: helm.Options{}, Sources: helm.NewSources(st)})
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(out, "name: arrived") != exists {
				t.Fatalf("alias membership stale: %s", out)
			}
			if st.GetArtifact(alias.Named()) != art {
				t.Fatal("Template republished alias")
			}
			view, pin, err := source.OperationView(t.Context(), art.FS)
			if err != nil || pin != "" {
				t.Fatalf("alias admission: %q, %v", pin, err)
			}
			if _, err := fs.Stat(view, "excluded/file"); !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("alias lost shaping: %v", err)
			}
		}
		if _, err := d.resolveEntrySources(t.Context(), true); err != nil {
			t.Fatal(err)
		}
		if len(d.aliasViews) != 1 || st.GetObject(alias.Named()) != alias {
			t.Fatal("alias refresh changed registry or authored object")
		}
	})

	t.Run("OCI subtrees", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		st := store.New()
		input := fstest.MapFS{
			"kubernetes/apps/file":     {Data: []byte("subtree")},
			"kubernetes/.git/config":   {Data: []byte("metadata")},
			"kubernetes/.sourceignore": {Data: []byte("excluded\n")},
			"kubernetes/excluded":      {Data: []byte("excluded")},
			"other/apps/file":          {Data: []byte("other")},
		}
		bootstrap := &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: input, Key: "pinned", Revision: "one"}
		st.SetArtifact(manifest.BootstrapSourceID, bootstrap)
		d := discoverer{cfg: Config{Path: filepath.Join(root, "kubernetes"), Store: st}}
		for i, subtree := range []string{"kubernetes", "kubernetes", "other"} {
			id := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: "flux-system", Name: fmt.Sprintf("alias-%d", i)}
			d.cfg.Path = filepath.Join(root, subtree)
			st.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace, Path: "apps", SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: id.Kind, Name: id.Name}})
			if count, err := d.resolveEntrySources(t.Context(), true); count != 1 || err != nil {
				t.Fatalf("alias: %d %v", count, err)
			}
			art := st.GetArtifact(id).(*store.SourceArtifact)
			data, err := fs.ReadFile(art.FS, "apps/file")
			want := "subtree"
			if subtree == "other" {
				want = "other"
			}
			if err != nil || string(data) != want || art.Key != "" || art.LocalRoot != filepath.Join(root, subtree) {
				t.Fatalf("subtree artifact: %+v %q %v", art, data, err)
			}
			if subtree == "kubernetes" {
				for _, name := range []string{".git/config", "excluded"} {
					if _, err := fs.Stat(art.FS, name); !errors.Is(err, fs.ErrNotExist) {
						t.Fatalf("source policy %s: %v", name, err)
					}
				}
			}
			wantViews := 1
			if i == 2 {
				wantViews = 2
			}
			if len(d.aliasViews) != wantViews {
				t.Fatalf("views = %d", len(d.aliasViews))
			}
		}
		if st.GetArtifact(manifest.BootstrapSourceID) != bootstrap {
			t.Fatal("bootstrap changed")
		}
	})

}

func discoveryRefFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	repo := testutil.GitInit(t, root)
	for i, value := range []string{"A", "B"} {
		testutil.WriteFile(t, root, "value.txt", value)
		testutil.WriteFile(t, root, "apps/value.txt", value)
		testutil.WriteFile(t, root, "apps/ignored.txt", value)
		testutil.WriteFile(t, root, "apps/excluded.txt", value)
		testutil.WriteFile(t, root, ".sourceignore", "apps/ignored.txt\n")
		hash := testutil.GitCommitAll(t, repo, value, &git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(0, 0)}})
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
			repository.Reference = &sourcev1.GitRepositoryRef{Tag: tag}
		}
		st := store.New()
		st.AddObject(repository)
		d := discoverer{cfg: Config{Path: root, RepoRoot: root, Store: st, SelfURLs: []string{repository.URL}}}
		if _, err := d.resolveEntrySources(t.Context(), false); err != nil {
			t.Fatal(err)
		}
		if _, err := d.resolveEntrySources(t.Context(), false); err != nil {
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

type warningFailureFS struct{}

func (warningFailureFS) Open(name string) (fs.File, error) {
	return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrPermission}
}

func TestEntrySourceView_WarningEvidence(t *testing.T) {
	t.Parallel()
	for _, subtree := range []string{".", "app"} {
		t.Run(subtree, func(t *testing.T) {
			t.Parallel()
			for _, failed := range []bool{false, true} {
				t.Run(fmt.Sprint(failed), func(t *testing.T) {
					t.Parallel()
					root := t.TempDir()
					testutil.WriteFile(t, root, "app/file.yaml", "content")
					current := os.DirFS(root)
					if failed {
						current = warningFailureFS{}
					}
					input, _, err := gittree.New(t.Context(), nil, plumbing.ZeroHash, gittree.ViewOptions{Mode: gittree.Direct, Current: current})
					assert.NoError(t, err)
					raw := manifest.Warning{Category: manifest.WarnSkippedSubmodule, Message: "skipping submodule", Detail: []string{"app/module"}}
					input.Warnings = []manifest.Warning{raw}
					invocation := manifest.Warning{Category: manifest.WarnBaselineStatusUnavailable, Message: "current only"}
					bootstrap := &store.SourceArtifact{LocalPath: root, FS: input, Warnings: []manifest.Warning{invocation}}
					obj := &manifest.OCIRepository{Name: "alias", Namespace: "apps"}
					d := &discoverer{}
					for range 2 {
						artifact, err := d.entrySourceView(t.Context(), entryResolution{object: obj, input: bootstrap, subtree: subtree})
						if failed && !errors.Is(err, fs.ErrPermission) || !failed && err != nil {
							t.Fatalf("source view error=%v, failed=%t", err, failed)
						}
						if failed {
							if artifact != nil {
								t.Fatalf("failed source view = %+v", artifact)
							}
							continue
						}
						if artifact == nil || len(artifact.Warnings) != 0 {
							t.Fatalf("alias warning evidence = %+v", artifact)
						}
						if input := artifact.FS.(*gittree.FS); len(input.Warnings) != 0 {
							t.Fatalf("derived filesystem warnings = %+v", input.Warnings)
						}
					}
					assert.Diff(t, input.Warnings, []manifest.Warning{raw})
					assert.Diff(t, bootstrap.Warnings, []manifest.Warning{invocation})
				})
			}
		})
	}
}
