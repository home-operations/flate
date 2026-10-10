package e2e

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/google/go-cmp/cmp"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/storer"
	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/go-git/go-git/v5/plumbing/transport/client"
	"github.com/go-git/go-git/v5/plumbing/transport/server"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cacheroot"
	"github.com/home-operations/flate/pkg/store"
)

func TestE2E_SourceRef_EmptyPathMatchesRoot(t *testing.T) {
	fixture := func(t *testing.T, field string) (string, *store.Store, *discovery.Result) {
		t.Helper()
		root := t.TempDir()
		gitInit(t, root)
		testutil.WriteFile(t, root, "flux/root.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: root, namespace: flux-system}
spec:
`+field+`  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
  targetNamespace: apps
  postBuild: {substitute: {VALUE: source-root}}
`)
		testutil.WriteFile(t, root, "kustomization.yaml", "resources: [cm.yaml, child.yaml]\n")
		testutil.WriteFile(t, root, "cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: root-owned}
data: {value: '${VALUE}'}
`)
		testutil.WriteFile(t, root, "child.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: child, namespace: apps}
spec:
  path: ./leaf
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`)
		testutil.WriteFile(t, root, "leaf/kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(t, root, "leaf/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: leaf-owned}\n")
		st := store.New()
		res, err := discovery.Run(t.Context(), discovery.Config{
			Path: filepath.Join(root, "flux"), RepoRoot: root, Store: st,
		})
		if err != nil {
			t.Fatal(err)
		}
		return root, st, res
	}
	root, _, want := fixture(t, "  path: ./\n")
	wantOutput := runCLIStdout(t, "build", "all", "--path", filepath.Join(root, "flux"),
		"--concurrency", "2", "--cache-dir", t.TempDir())
	for _, path := range []struct{ name, field string }{
		{name: "explicit", field: "  path: ./\n"},
		{name: "omitted"},
	} {
		t.Run(path.name, func(t *testing.T) {
			root, st, res := fixture(t, path.field)
			parent := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "root"}
			child := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "apps", Name: "child"}
			if st.GetObject(child) == nil || res.ParentOf[child] != parent {
				t.Fatalf("source-root child not discovered or owned by root: parents=%v", res.ParentOf)
			}
			claims := loader.KSPathPrefixesLocalOnly(st, root, nil)
			owner, covered := loader.LongestParent(claims, "cm.yaml", manifest.NamedResource{})
			if !covered || owner != parent {
				t.Fatalf("source-root ConfigMap owner = %v, %t; want %v", owner, covered, parent)
			}
			assert.Diff(t, res.SourceFiles, want.SourceFiles)
			assert.Diff(t, res.ParentOf, want.ParentOf)
			for _, concurrency := range []string{"2", "8"} {
				t.Run("concurrency_"+concurrency, func(t *testing.T) {
					args := []string{"--path", filepath.Join(root, "flux"), "--concurrency", concurrency, "--cache-dir", t.TempDir()}
					owned := runCLIStdout(t, append([]string{"build", "ks", "root"}, args...)...)
					if !strings.Contains(owned, "name: root-owned") || !strings.Contains(owned, "value: source-root") ||
						!strings.Contains(owned, "namespace: apps") {
						t.Fatalf("root's rendered objects missing or untransformed:\n%s", owned)
					}
					out := runCLIStdout(t, append([]string{"build", "all"}, args...)...)
					if !strings.Contains(out, "name: leaf-owned") {
						t.Fatalf("child's rendered objects missing:\n%s", out)
					}
					assert.Equal(t, out, wantOutput)
				})
			}
		})
	}
}

func TestE2E_SourceRef_PatchedStubKustomizationKeepsParentPath(t *testing.T) {
	for _, concurrency := range []string{"2", "8"} {
		t.Run("concurrency_"+concurrency, func(t *testing.T) {
			root := t.TempDir()
			gitInit(t, root)
			testutil.WriteFile(t, root, "flux/kustomization.yaml", `resources: [root.yaml, tmpl.yaml]
patches:
  - target:
      group: kustomize.toolkit.fluxcd.io
      version: v1
      kind: Kustomization
      name: tmpl
    patch: |-
      - op: add
        path: /spec/path
        value: ./apps
      - op: add
        path: /spec/sourceRef
        value: {kind: GitRepository, name: flux-system}
`)
			testutil.WriteFile(t, root, "flux/root.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: root, namespace: flux-system}
spec:
  interval: 1m
  prune: true
  path: ./flux
  sourceRef: {kind: GitRepository, name: flux-system}
`)
			testutil.WriteFile(t, root, "flux/tmpl.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: tmpl, namespace: flux-system}
spec: {interval: 1m, prune: true}
`)
			testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(t, root, "apps/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: apps-cm, namespace: apps}\n")
			testutil.WriteFile(t, root, "unrelated/broken.yaml", "invalid: [\n")
			out, _ := requireCLIOK(t, "build", "all", "--path", filepath.Join(root, "flux"),
				"--concurrency", concurrency, "--cache-dir", t.TempDir())
			docs, err := manifest.DecodeDocs(strings.NewReader(out))
			if err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, len(docs), 3)
			seen := map[manifest.NamedResource]bool{}
			for _, doc := range docs {
				obj, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
				if err != nil {
					t.Fatal(err)
				}
				seen[obj.Named()] = true
				if ks, ok := obj.(*manifest.Kustomization); ok && ks.Name == "tmpl" {
					assert.Equal(t, ks.Path, "./apps")
					assert.Equal(t, ks.SourceName, "flux-system")
				}
			}
			assert.Diff(t, seen, map[manifest.NamedResource]bool{
				{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "root"}: true,
				{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "tmpl"}: true,
				{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "apps-cm"}:         true,
			})
		})
	}
}

func TestE2E_SourceRef_NonHEADTag(t *testing.T) {
	root, _, _, _ := sourceRefFixture(t)
	out, stderr := requireCLIOK(t, "build", "all", "--path", root+"/flux",
		"--concurrency", "2", "--cache-dir", t.TempDir())
	if !strings.Contains(out, "value: v1.0.0") || strings.Contains(out, "value: v2.0.0") {
		t.Fatalf("expected pinned v1.0.0 content:\n%s\nstderr:\n%s", out, stderr)
	}
}

func TestE2E_SourceRef_PinnedSourceDiscoveredAfterConsumer(t *testing.T) {
	for _, tt := range []struct {
		name, appsFile, sourcesFile, sourcesName, sourceRefName, sourceURL string
		noRef                                                              bool
	}{
		{name: "apps_first/flux-system", appsFile: "a.yaml", sourcesFile: "z.yaml", sourcesName: "z-sources", sourceRefName: "flux-system"},
		{name: "apps_first/cluster", appsFile: "a.yaml", sourcesFile: "z.yaml", sourcesName: "z-sources", sourceRefName: "cluster"},
		{name: "sources_first/flux-system", appsFile: "z.yaml", sourcesFile: "a.yaml", sourcesName: "a-sources", sourceRefName: "flux-system"},
		{name: "sources_first/cluster", appsFile: "z.yaml", sourcesFile: "a.yaml", sourcesName: "a-sources", sourceRefName: "cluster"},
		{name: "unpinned/no_ref", appsFile: "a.yaml", sourcesFile: "z.yaml", sourcesName: "z-sources", sourceRefName: "flux-system", noRef: true},
		{name: "unpinned/non_matching", appsFile: "a.yaml", sourcesFile: "z.yaml", sourcesName: "z-sources", sourceRefName: "flux-system", sourceURL: "git://fixture.invalid/other"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			repo := gitInit(t, root)
			if _, err := repo.CreateRemote(&config.RemoteConfig{
				Name: "origin", URLs: []string{"git://fixture.invalid/cluster"},
			}); err != nil {
				t.Fatal(err)
			}
			testutil.WriteFile(t, root, "flux/"+tt.appsFile, `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: apps, namespace: flux-system}
spec:
  interval: 10m
  path: ./apps
  sourceRef: {kind: GitRepository, name: pinned, namespace: flux-system}
`)
			testutil.WriteFile(t, root, "flux/"+tt.sourcesFile, `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: `+tt.sourcesName+`, namespace: flux-system}
spec:
  interval: 10m
  path: ./sources
  sourceRef: {kind: GitRepository, name: `+tt.sourceRefName+`, namespace: flux-system}
`)
			testutil.WriteFile(t, root, "sources/kustomization.yaml", "resources: [repo.yaml]\n")
			url, ref := tt.sourceURL, "  ref: {tag: v1.0.0}\n"
			if url == "" {
				url = "git://fixture.invalid/cluster"
			}
			if tt.noRef {
				ref = ""
			}
			testutil.WriteFile(t, root, "sources/repo.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: pinned, namespace: flux-system}
spec:
  interval: 10m
  url: `+url+"\n"+ref)
			testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [cm.yaml, deleted.yaml, extra.yaml]\n")
			testutil.WriteFile(t, root, "apps/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello, namespace: apps}\ndata: {value: pinned}\n")
			testutil.WriteFile(t, root, "apps/deleted.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: deleted, namespace: apps}\ndata: {value: pinned-deleted}\n")
			testutil.WriteFile(t, root, "apps/extra.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: extra, namespace: apps}\ndata: {value: pinned-extra}\n")
			gitCommitAll(t, repo)
			head, err := repo.Head()
			if err != nil {
				t.Fatal(err)
			}
			if err := repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewTagReferenceName("v1.0.0"), head.Hash())); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(filepath.Join(root, "apps/deleted.yaml")); err != nil {
				t.Fatal(err)
			}
			mutateFile(t, filepath.Join(root, "apps/cm.yaml"), "value: pinned", "value: newer")
			gitCommitAll(t, repo)
			mutateFile(t, filepath.Join(root, "apps/cm.yaml"), "value: newer", "value: dirty")
			testutil.WriteFile(t, root, "apps/extra.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: extra\n")
			transport := installSourceRefTransport(t, repo.Storer)
			var wantError string
			if tt.noRef || tt.sourceURL != "" {
				if _, err := loader.New(store.New()).Load(t.Context(), filepath.Join(root, "apps")); err != nil {
					wantError = "flate error: " + err.Error() + "\n"
				} else {
					t.Fatal("expected working-tree decode error")
				}
			}
			for _, command := range []string{"build", "diff"} {
				t.Run(command, func(t *testing.T) {
					var previous string
					for _, concurrency := range []string{"2", "8"} {
						t.Run("concurrency_"+concurrency, func(t *testing.T) {
							args := []string{command, "all", "--path", filepath.Join(root, "flux"),
								"--concurrency", concurrency, "--cache-dir", t.TempDir()}
							if command == "diff" {
								args = append(args, "--base", "HEAD", "-o", "diff")
							}
							out, stderr, code := runCLIBuffers(args...)
							if tt.sourceURL != "" {
								wantCode, wantReason := 0, "external source GitRepository/flux-system/pinned is outside the local tree; not rendered"
								if command == "build" {
									wantCode, wantReason = 1, "repository not found"
								}
								if code != wantCode || !strings.Contains(stderr, wantReason) || strings.Contains(stderr, wantError) ||
									transport.calls.Load() != 0 || (command == "diff" && out != "") {
									t.Fatalf("external source must release the working-tree error: exit=%d transport=%d\n%s\nstderr:\n%s", code, transport.calls.Load(), out, stderr)
								}
								return
							}
							if wantError != "" {
								if code != 1 || out != "" || stderr != wantError || transport.calls.Load() != 0 {
									t.Fatalf("unpinned decode error: exit=%d transport=%d\n%s\nstderr (-want +got):\n%s", code, transport.calls.Load(), out, cmp.Diff(wantError, stderr))
								}
								return
							}
							if code != 0 || transport.calls.Load() != 0 ||
								(command == "build" && (!strings.Contains(out, "value: pinned") ||
									!strings.Contains(out, "value: pinned-deleted") || !strings.Contains(out, "value: pinned-extra") ||
									strings.Contains(out, "value: dirty") || strings.Contains(out, "value: newer"))) ||
								(command == "diff" && out != "") {
								t.Fatalf("late source pin: exit=%d transport=%d\n%s\nstderr:\n%s", code, transport.calls.Load(), out, stderr)
							}
							if concurrency == "8" && out != previous {
								t.Fatalf("output differs across concurrency levels (-want +got):\n%s", cmp.Diff(previous, out))
							}
							previous = out
						})
					}
				})
			}
			if wantError != "" {
				return
			}
			st := store.New()
			res, err := discovery.Run(t.Context(), discovery.Config{
				Path: filepath.Join(root, "flux"), Store: st,
				SourceCache: source.NewCache(cacheroot.New(t.TempDir())),
			})
			if err != nil {
				t.Fatal(err)
			}
			artifact, ok := st.GetArtifact(manifest.NamedResource{
				Kind: manifest.KindGitRepository, Namespace: "flux-system", Name: "pinned",
			}).(*store.SourceArtifact)
			if !ok || artifact.LocalPath == root {
				t.Fatalf("expected committed source artifact, got %+v", artifact)
			}
			for _, file := range []struct{ name, path string }{{"hello", "cm.yaml"}, {"deleted", "deleted.yaml"}, {"extra", "extra.yaml"}} {
				id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: file.name}
				path, indexed := res.Existence.Get(id)
				if !indexed || path != filepath.Join(artifact.LocalPath, "apps", file.path) ||
					filepath.Join(root, filepath.FromSlash(res.SourceFiles[id])) != path {
					t.Fatalf("pinned discovery metadata for %s: indexed=%t path=%q source=%q", id, indexed, path, res.SourceFiles[id])
				}
			}
		})
	}
}

func TestE2E_SourceRef_HeldErrorScope(t *testing.T) {
	for _, tt := range []struct {
		name, kind, sourceName, url, ref    string
		unreadable, bothHeld, reversedPaths bool
	}{
		{name: "oci", kind: "OCIRepository", sourceName: "second"},
		{name: "bootstrap", kind: "GitRepository", sourceName: "flux-system"},
		{name: "resolved_unavailable_tag", kind: "GitRepository", sourceName: "second", url: "git://fixture.invalid/cluster", ref: "  ref: {tag: unavailable}\n"},
		{name: "non_matching_url", kind: "GitRepository", sourceName: "second", url: "git://fixture.invalid/other"},
		{name: "non_decode_error", kind: "GitRepository", sourceName: "second", unreadable: true},
		{name: "two_held", kind: "GitRepository", sourceName: "second", bothHeld: true},
		{name: "two_held_path_order", kind: "GitRepository", sourceName: "second", bothHeld: true, reversedPaths: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			repo := gitInit(t, root)
			if _, err := repo.CreateRemote(&config.RemoteConfig{
				Name: "origin", URLs: []string{"git://fixture.invalid/cluster"},
			}); err != nil {
				t.Fatal(err)
			}
			writeConsumer := func(name, path, kind, sourceName string) {
				testutil.WriteFile(t, root, "flux/"+name+".yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: `+name+`, namespace: flux-system}
spec:
  interval: 10m
  path: ./`+path+`
  sourceRef: {kind: `+kind+`, name: `+sourceName+`}
`)
			}
			heldPath, secondPath := "held", "second"
			if tt.reversedPaths {
				heldPath, secondPath = "z-held", "a-second"
			}
			writeConsumer("a-held", heldPath, "GitRepository", "held")
			writeConsumer("b-second", secondPath, tt.kind, tt.sourceName)
			writeConsumer("z-sources", "sources", "GitRepository", "flux-system")
			testutil.WriteFile(t, root, "sources/kustomization.yaml", "resources: [held.yaml]\n")
			testutil.WriteFile(t, root, "sources/held.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: held, namespace: flux-system}
spec: {url: 'git://fixture.invalid/cluster', interval: 10m}
`)
			if tt.kind == "OCIRepository" {
				testutil.WriteFile(t, root, "flux/repo.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: OCIRepository
metadata: {name: second, namespace: flux-system}
spec: {url: 'oci://fixture.invalid/second', interval: 10m}
`)
			} else if tt.url != "" {
				testutil.WriteFile(t, root, "flux/repo.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: second, namespace: flux-system}
spec:
  interval: 10m
  url: `+tt.url+"\n"+tt.ref)
			} else if tt.unreadable || tt.bothHeld {
				testutil.WriteFile(t, root, "sources/kustomization.yaml", "resources: [held.yaml, second.yaml]\n")
				testutil.WriteFile(t, root, "sources/second.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: second, namespace: flux-system}
spec: {url: 'git://fixture.invalid/cluster', interval: 10m}
`)
			}
			for _, path := range []string{heldPath, secondPath} {
				testutil.WriteFile(t, root, path+"/kustomization.yaml", "resources: [extra.yaml]\n")
				testutil.WriteFile(t, root, path+"/extra.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: extra}\n")
			}
			gitCommitAll(t, repo)
			testutil.WriteFile(t, root, heldPath+"/extra.yaml", "metadata: {name: held\n")
			if tt.unreadable {
				file := filepath.Join(root, secondPath+"/extra.yaml")
				if err := os.Chmod(file, 0); err != nil {
					t.Skipf("cannot make file unreadable: %v", err)
				}
				t.Cleanup(func() { _ = os.Chmod(file, 0o600) })
			} else {
				testutil.WriteFile(t, root, secondPath+"/extra.yaml", "metadata: {name: second\n")
			}
			_, secondError := loader.New(store.New()).Load(t.Context(), filepath.Join(root, secondPath))
			if tt.unreadable && secondError == nil {
				t.Skip("OS or current user permits reading a file with no permissions")
			}
			if secondError == nil || (tt.unreadable && errors.Is(secondError, manifest.ErrInput)) {
				t.Fatalf("expected second consumer's loader error, got %v", secondError)
			}
			wantError, repeats := "flate error: "+secondError.Error()+"\n", 1
			if tt.bothHeld {
				_, heldError := loader.New(store.New()).Load(t.Context(), filepath.Join(root, heldPath))
				if heldError == nil {
					t.Fatal("expected held consumer's decode error")
				}
				repeats = 16
				if !tt.reversedPaths {
					wantError = "flate error: " + heldError.Error() + "\n"
				}
			}
			transport := installSourceRefTransport(t, repo.Storer)
			for i := range repeats {
				out, stderr, code := runCLIBuffers("build", "all", "--path", filepath.Join(root, "flux"),
					"--concurrency", "2", "--cache-dir", t.TempDir())
				if code != 1 || out != "" || stderr != wantError || transport.calls.Load() != 0 {
					t.Fatalf("held-error scope: run=%d exit=%d transport=%d stdout=%q\nstderr (-want +got):\n%s",
						i, code, transport.calls.Load(), out, cmp.Diff(wantError, stderr))
				}
			}
		})
	}
}

func TestE2E_SourceRef_HeldErrorAcrossNamespaces(t *testing.T) {
	for _, tt := range []struct {
		name, betaURL        string
		workingTree, symlink bool
	}{
		{name: "external_source", betaURL: "git://fixture.invalid/other"},
		{name: "working_tree_source", betaURL: "git://fixture.invalid/cluster", workingTree: true},
		{name: "working_tree_symlink", betaURL: "git://fixture.invalid/cluster", workingTree: true, symlink: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			repo := gitInit(t, root)
			if _, err := repo.CreateRemote(&config.RemoteConfig{
				Name: "origin", URLs: []string{"git://fixture.invalid/cluster"},
			}); err != nil {
				t.Fatal(err)
			}
			for _, namespace := range []string{"alpha", "beta"} {
				suspend, path := "", "./broken"
				if namespace == "beta" {
					// The discovery error must surface even without reconciling this consumer.
					suspend = "  suspend: true\n"
					if tt.symlink {
						path = "./broken-alias"
					}
				}
				testutil.WriteFile(t, root, "flux/"+namespace+".yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: apps, namespace: `+namespace+`}
spec:
  interval: 10m
`+suspend+`  path: `+path+`
  sourceRef: {kind: GitRepository, name: cluster}
`)
			}
			testutil.WriteFile(t, root, "flux/beta-repo.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: cluster, namespace: beta}
spec: {interval: 10m, url: '`+tt.betaURL+`'}
`)
			testutil.WriteFile(t, root, "flux/sources.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: z-sources, namespace: alpha}
spec:
  interval: 10m
  path: ./sources
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`)
			testutil.WriteFile(t, root, "sources/kustomization.yaml", "resources: [repo.yaml]\n")
			testutil.WriteFile(t, root, "sources/repo.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: cluster, namespace: alpha}
spec:
  interval: 10m
  url: git://fixture.invalid/cluster
  ref: {tag: v1.0.0}
`)
			testutil.WriteFile(t, root, "broken/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(t, root, "broken/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello, namespace: apps}\ndata: {value: pinned}\n")
			if tt.symlink {
				if err := os.Symlink("broken", filepath.Join(root, "broken-alias")); err != nil {
					t.Skipf("symlinks unavailable: %v", err)
				}
			}
			gitCommitAll(t, repo)
			head, err := repo.Head()
			if err != nil {
				t.Fatal(err)
			}
			if err := repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewTagReferenceName("v1.0.0"), head.Hash())); err != nil {
				t.Fatal(err)
			}
			mutateFile(t, filepath.Join(root, "broken/cm.yaml"), "value: pinned", "value: newer")
			gitCommitAll(t, repo)
			testutil.WriteFile(t, root, "broken/cm.yaml", "metadata: {name: broken\n")
			_, heldError := loader.New(store.New()).Load(t.Context(), filepath.Join(root, "broken"))
			if !errors.Is(heldError, manifest.ErrInput) {
				t.Fatalf("expected working-tree decode error, got %v", heldError)
			}
			transport := installSourceRefTransport(t, repo.Storer)
			var previous string
			for _, concurrency := range []string{"2", "8"} {
				t.Run("concurrency_"+concurrency, func(t *testing.T) {
					out, stderr, code := runCLIBuffers("build", "ks", "apps", "-n", "alpha",
						"--path", filepath.Join(root, "flux"), "--concurrency", concurrency, "--cache-dir", t.TempDir())
					if tt.workingTree {
						wantError := "flate error: " + heldError.Error() + "\n"
						if code != 1 || out != "" || stderr != wantError || transport.calls.Load() != 0 {
							t.Fatalf("working-tree consumer: exit=%d transport=%d stdout=%q\nstderr (-want +got):\n%s",
								code, transport.calls.Load(), out, cmp.Diff(wantError, stderr))
						}
						return
					}
					if code != 0 || !strings.Contains(out, "value: pinned") || strings.Contains(out, "value: newer") ||
						strings.Contains(stderr, heldError.Error()) || transport.calls.Load() != 0 {
						t.Fatalf("external consumer: exit=%d transport=%d stdout=%q\nstderr:\n%s",
							code, transport.calls.Load(), out, stderr)
					}
					if concurrency == "8" && out != previous {
						t.Fatalf("output differs across concurrency levels (-want +got):\n%s", cmp.Diff(previous, out))
					}
					previous = out
				})
			}
			st := store.New()
			_, discoveryError := discovery.Run(t.Context(), discovery.Config{
				Path: filepath.Join(root, "flux"), Store: st,
				SourceCache: source.NewCache(cacheroot.New(t.TempDir())),
			})
			for _, namespace := range []string{"alpha", "beta"} {
				ks, ok := st.Get[*manifest.Kustomization](manifest.NamedResource{
					Kind: manifest.KindKustomization, Namespace: namespace, Name: "apps",
				})
				wantPath := "./broken"
				if namespace == "beta" && tt.symlink {
					wantPath = "./broken-alias"
				}
				if !ok || ks.SourceNamespace != namespace || ks.SourceRef.Namespace != "" || ks.Path != wantPath {
					t.Fatalf("expected implicit source in consumer namespace, got %+v", ks)
				}
			}
			art, ok := st.GetArtifact(manifest.NamedResource{
				Kind: manifest.KindGitRepository, Namespace: "alpha", Name: "cluster",
			}).(*store.SourceArtifact)
			if !ok || art.LocalRoot != root || art.LocalPath == root {
				t.Fatalf("expected alpha's committed source artifact, got %+v", art)
			}
			if tt.workingTree {
				betaArtifact, ok := st.GetArtifact(manifest.NamedResource{
					Kind: manifest.KindGitRepository, Namespace: "beta", Name: "cluster",
				}).(*store.SourceArtifact)
				if !ok || betaArtifact.LocalRoot == root || betaArtifact.LocalPath != root {
					t.Fatalf("expected beta's working-tree artifact, got %+v", betaArtifact)
				}
				if !errors.Is(discoveryError, manifest.ErrInput) {
					t.Fatalf("expected retained working-tree error, got %v", discoveryError)
				}
			} else if discoveryError != nil {
				t.Fatalf("expected alpha's error to be discarded, got %v", discoveryError)
			}
		})
	}
}

func TestE2E_SourceRef_HeldErrorRetainedByEmptyPath(t *testing.T) {
	root := t.TempDir()
	repo := gitInit(t, root)
	if _, err := repo.CreateRemote(&config.RemoteConfig{
		Name: "origin", URLs: []string{"git://fixture.invalid/cluster"},
	}); err != nil {
		t.Fatal(err)
	}
	writeKS := func(file, name, path, sourceName string) {
		t.Helper()
		pathField := ""
		if path != "" {
			pathField = "  path: " + path + "\n"
		}
		testutil.WriteFile(t, root, "flux/"+file, `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: `+name+`, namespace: flux-system}
spec:
  interval: 10m
`+pathField+`  sourceRef: {kind: GitRepository, name: `+sourceName+`, namespace: flux-system}
`)
	}
	writeKS("a-x.yaml", "a-x", "./", "pinned")
	writeKS("b-z.yaml", "b-z", "", "flux-system")
	writeKS("z-sources.yaml", "z-sources", "./sources", "flux-system")
	testutil.WriteFile(t, root, "sources/kustomization.yaml", "resources: [repo.yaml]\n")
	testutil.WriteFile(t, root, "sources/repo.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: pinned, namespace: flux-system}
spec:
  interval: 10m
  url: git://fixture.invalid/cluster
  ref: {tag: v1.0.0}
`)
	testutil.WriteFile(t, root, "apps/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello, namespace: apps}\ndata: {value: pinned}\n")
	gitCommitAll(t, repo)
	head, err := repo.Head()
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewTagReferenceName("v1.0.0"), head.Hash())); err != nil {
		t.Fatal(err)
	}
	mutateFile(t, filepath.Join(root, "apps/cm.yaml"), "value: pinned", "value: newer")
	gitCommitAll(t, repo)
	testutil.WriteFile(t, root, "apps/cm.yaml", "metadata: {name: broken\n")
	_, wantError := loader.New(store.New()).Load(t.Context(), root)
	if !errors.Is(wantError, manifest.ErrInput) {
		t.Fatalf("expected a working-tree decode error, got %v", wantError)
	}
	_, err = discovery.Run(t.Context(), discovery.Config{
		Path: filepath.Join(root, "flux"), Store: store.New(),
		SourceCache: source.NewCache(cacheroot.New(t.TempDir())),
	})
	if !errors.Is(err, manifest.ErrInput) {
		t.Fatalf("empty-path working-tree reader lost the root error: %v", err)
	}
	assert.Equal(t, err.Error(), wantError.Error())
}

func TestE2E_SourceRef_HeldErrorSurvivesNamespaceInheritance(t *testing.T) {
	for _, tt := range []struct {
		name, namespace, wantNamespace        string
		pinned, external, namespaceFreeSource bool
	}{
		{name: "explicit_source_namespace", namespace: ", namespace: flux-system", wantNamespace: "flux-system"},
		{name: "implicit_source_namespace", wantNamespace: "shifted"},
		{name: "pinned_inherited_namespace", wantNamespace: "shifted", pinned: true},
		{name: "external_source_in_other_inherited_namespace", wantNamespace: "shifted", external: true, namespaceFreeSource: true},
		{name: "pinned_namespace_free_source", wantNamespace: "shifted", pinned: true, namespaceFreeSource: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			repo := gitInit(t, root)
			if _, err := repo.CreateRemote(&config.RemoteConfig{
				Name: "origin", URLs: []string{"git://fixture.invalid/cluster"},
			}); err != nil {
				t.Fatal(err)
			}
			for _, ks := range []struct{ name, path string }{
				{"probe", "ok"}, {"entry", "entries"}, {"stage", "stage"},
			} {
				testutil.WriteFile(t, root, "flux/"+ks.name+".yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: `+ks.name+`, namespace: flux-system}
spec:
  interval: 10m
  path: ./`+ks.path+`
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`)
			}
			testutil.WriteFile(t, root, "entries/kustomization.yaml", "resources: [nested/child.yaml]\n")
			testutil.WriteFile(t, root, "entries/nested/kustomization.yaml", "resources: [child.yaml]\n")
			// Suspension prevents reconciliation from masking a lost discovery error.
			testutil.WriteFile(t, root, "entries/nested/child.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: a-held}
spec:
  interval: 10m
  suspend: true
  path: ./held
  sourceRef: {kind: GitRepository, name: unknown`+tt.namespace+`}
`)
			testutil.WriteFile(t, root, "stage/kustomization.yaml", "resources: [sources.yaml]\n")
			sourceTargetNamespace := ""
			if tt.namespaceFreeSource {
				sourceTargetNamespace = "  targetNamespace: beta\n"
				if tt.pinned {
					sourceTargetNamespace = "  targetNamespace: shifted\n"
				}
			}
			testutil.WriteFile(t, root, "stage/sources.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: z-sources, namespace: flux-system}
spec:
  interval: 10m
  path: ./sources
`+sourceTargetNamespace+`  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`)
			testutil.WriteFile(t, root, "sources/kustomization.yaml", "resources: [claim.yaml, repo.yaml]\n")
			testutil.WriteFile(t, root, "sources/claim.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: claim, namespace: flux-system}
spec:
  interval: 10m
  path: ./entries/nested
  targetNamespace: shifted
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`)
			sourceName, sourceMetadataNamespace := "pulse", ", namespace: flux-system"
			if tt.pinned || tt.external {
				sourceName, sourceMetadataNamespace = "unknown", ", namespace: shifted"
			}
			if tt.namespaceFreeSource {
				sourceMetadataNamespace = ""
			}
			url, ref := "git://fixture.invalid/cluster", "  ref: {branch: master}\n"
			if tt.external {
				url, ref = "git://fixture.invalid/external", ""
			}
			testutil.WriteFile(t, root, "sources/repo.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: `+sourceName+sourceMetadataNamespace+`}
spec:
  interval: 10m
  url: `+url+"\n"+ref)
			testutil.WriteFile(t, root, "ok/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(t, root, "ok/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: ok, namespace: apps}\n")
			testutil.WriteFile(t, root, "held/kustomization.yaml", "resources: [broken.yaml]\n")
			testutil.WriteFile(t, root, "held/broken.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: held}\n")
			gitCommitAll(t, repo)
			head, err := repo.Head()
			if err != nil {
				t.Fatal(err)
			}
			if tt.pinned {
				testutil.WriteFile(t, root, "extra.txt", "second commit\n")
				gitCommitAll(t, repo)
			}
			if !tt.external {
				mutateFile(t, filepath.Join(root, "sources/repo.yaml"), "{branch: master}", "{commit: "+head.Hash().String()+"}")
			}
			testutil.WriteFile(t, root, "held/broken.yaml", "metadata: {name: held\n")
			_, heldError := loader.New(store.New()).Load(t.Context(), filepath.Join(root, "held"))
			if !errors.Is(heldError, manifest.ErrInput) {
				t.Fatalf("expected held consumer's decode error, got %v", heldError)
			}
			wantError, wantCode := "flate error: "+heldError.Error()+"\n", 1
			if tt.pinned {
				wantError, wantCode = "", 0
			}
			transport := installSourceRefTransport(t, repo.Storer)
			for _, concurrency := range []string{"2", "8"} {
				t.Run("concurrency_"+concurrency, func(t *testing.T) {
					out, stderr, code := runCLIBuffers("build", "ks", "probe", "-n", "flux-system",
						"--path", filepath.Join(root, "flux"), "--concurrency", concurrency, "--cache-dir", t.TempDir())
					if code != wantCode || stderr != wantError || transport.calls.Load() != 0 ||
						(!tt.pinned && out != "") || (tt.pinned && !strings.Contains(out, "name: ok")) {
						t.Fatalf("re-keyed held consumer: exit=%d transport=%d stdout=%q\nstderr (-want +got):\n%s",
							code, transport.calls.Load(), out, cmp.Diff(wantError, stderr))
					}
				})
			}
			st := store.New()
			_, discoveryError := discovery.Run(t.Context(), discovery.Config{
				Path: filepath.Join(root, "flux"), Store: st,
				SourceCache: source.NewCache(cacheroot.New(t.TempDir())),
			})
			id := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "shifted", Name: "a-held"}
			ks, ok := st.Get[*manifest.Kustomization](id)
			if !ok || ks.Path != "./held" || ks.SourceName != "unknown" || ks.SourceNamespace != tt.wantNamespace {
				t.Fatalf("expected surviving consumer with inherited namespace and unchanged path/source, got %+v", ks)
			}
			if tt.pinned {
				ref := manifest.NamedResource{Kind: ks.SourceKind, Namespace: ks.SourceNamespace, Name: ks.SourceName}
				art, ok := st.GetArtifact(ref).(*store.SourceArtifact)
				if discoveryError != nil || !ok || art.LocalRoot != root || art.LocalPath == root {
					t.Fatalf("expected genuine inherited source pin without a held error: error=%v artifact=%+v", discoveryError, art)
				}
			} else if !errors.Is(discoveryError, manifest.ErrInput) {
				t.Fatalf("expected discovery to retain the decode error, got %v", discoveryError)
			}
		})
	}
}

func TestE2E_SourceRef_PinnedTreeReplacesHeldConsumer(t *testing.T) {
	for _, tt := range []struct {
		name, held, want, heldSource               string
		survivorName, survivorPath, survivorSource string
		keepError                                  bool
	}{
		{
			name: "removed",
			held: "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: restored, namespace: apps}\ndata: {value: tagged-consumer}\n",
			want: "value: tagged-consumer",
		},
		{
			name: "replaced",
			held: `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: a-held, namespace: flux-system}
spec:
  interval: 10m
  path: ./ok
  sourceRef: {kind: GitRepository, name: flux-system}
`,
			want: "value: ok-consumer",
		},
		{
			name: "different_name", heldSource: "unknown", keepError: true,
			survivorName: "b-survivor", survivorPath: "./apps/broken", survivorSource: "unknown",
			held: `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: b-survivor, namespace: flux-system}
spec:
  interval: 10m
  suspend: true
  path: ./apps/broken
  sourceRef: {kind: GitRepository, name: unknown}
`,
			want: "value: tagged",
		},
		{
			name: "different_path", heldSource: "unknown",
			survivorName: "a-held", survivorPath: "./ok", survivorSource: "unknown",
			held: `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: a-held, namespace: flux-system}
spec:
  interval: 10m
  suspend: true
  path: ./ok
  sourceRef: {kind: GitRepository, name: unknown}
`,
			want: "value: tagged",
		},
		{
			name: "different_source_ref", heldSource: "unknown", keepError: true,
			survivorName: "a-held", survivorPath: "./apps/broken", survivorSource: "flux-system",
			held: `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: a-held, namespace: flux-system}
spec:
  interval: 10m
  suspend: true
  path: ./apps/broken
  sourceRef: {kind: GitRepository, name: flux-system}
`,
			want: "value: tagged",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			repo := gitInit(t, root)
			if _, err := repo.CreateRemote(&config.RemoteConfig{
				Name: "origin", URLs: []string{"git://fixture.invalid/cluster"},
			}); err != nil {
				t.Fatal(err)
			}
			for _, ks := range []struct{ name, path, source string }{
				{"a-entry", "apps", "flux-system"},
				{"z-pinned", "apps", "pinned"},
				{"z-stage", "stage", "flux-system"},
			} {
				testutil.WriteFile(t, root, "flux/"+ks.name+".yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: `+ks.name+`, namespace: flux-system}
spec:
  interval: 10m
  path: ./`+ks.path+`
  sourceRef: {kind: GitRepository, name: `+ks.source+`}
`)
			}
			mutateFile(t, filepath.Join(root, "flux/a-entry.yaml"), "interval: 10m", "interval: 10m\n  suspend: true")
			testutil.WriteFile(t, root, "stage/kustomization.yaml", "resources: [ks.yaml]\n")
			testutil.WriteFile(t, root, "stage/ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: z-sources, namespace: flux-system}
spec:
  interval: 10m
  path: ./sources
  sourceRef: {kind: GitRepository, name: flux-system}
`)
			testutil.WriteFile(t, root, "sources/kustomization.yaml", "resources: [repo.yaml]\n")
			testutil.WriteFile(t, root, "sources/repo.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: pinned, namespace: flux-system}
spec:
  interval: 10m
  url: git://fixture.invalid/cluster
  ref: {tag: v1.0.0}
`)
			testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [cm.yaml, broken/held.yaml]\n")
			testutil.WriteFile(t, root, "apps/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello, namespace: apps}\ndata: {value: tagged}\n")
			testutil.WriteFile(t, root, "apps/broken/held.yaml", tt.held)
			testutil.WriteFile(t, root, "ok/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(t, root, "ok/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: ok, namespace: apps}\ndata: {value: ok-consumer}\n")
			testutil.WriteFile(t, root, "apps/broken/kustomization.yaml", "resources: [held.yaml, extra.yaml]\n")
			testutil.WriteFile(t, root, "apps/broken/extra.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: sibling, namespace: apps}\n")
			gitCommitAll(t, repo)
			head, err := repo.Head()
			if err != nil {
				t.Fatal(err)
			}
			if err := repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewTagReferenceName("v1.0.0"), head.Hash())); err != nil {
				t.Fatal(err)
			}
			mutateFile(t, filepath.Join(root, "apps/cm.yaml"), "value: tagged", "value: newer")
			gitCommitAll(t, repo)
			heldSource := tt.heldSource
			if heldSource == "" {
				heldSource = "pinned"
			}
			testutil.WriteFile(t, root, "apps/broken/held.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: a-held, namespace: flux-system}
spec:
  interval: 10m
  path: ./apps/broken
  sourceRef: {kind: GitRepository, name: `+heldSource+`}
`)
			testutil.WriteFile(t, root, "apps/broken/extra.yaml", "metadata: {name: extra\n")
			mutateFile(t, filepath.Join(root, "apps/cm.yaml"), "value: newer", "value: dirty")
			_, heldError := loader.New(store.New()).Load(t.Context(), filepath.Join(root, "apps/broken"))
			if !errors.Is(heldError, manifest.ErrInput) {
				t.Fatalf("expected held consumer's decode error, got %v", heldError)
			}
			transport := installSourceRefTransport(t, repo.Storer)
			for _, concurrency := range []string{"2", "8"} {
				out, stderr, code := runCLIBuffers("build", "all", "--path", filepath.Join(root, "flux"),
					"--concurrency", concurrency, "--cache-dir", t.TempDir())
				if tt.keepError {
					wantError := "flate error: " + heldError.Error() + "\n"
					if code != 1 || out != "" || stderr != wantError || transport.calls.Load() != 0 {
						t.Fatalf("surviving working-tree consumer: concurrency=%s exit=%d transport=%d stdout=%q\nstderr (-want +got):\n%s",
							concurrency, code, transport.calls.Load(), out, cmp.Diff(wantError, stderr))
					}
					continue
				}
				if code != 0 || strings.Contains(stderr, heldError.Error()) ||
					!strings.Contains(out, "value: tagged") || !strings.Contains(out, tt.want) ||
					strings.Contains(out, "value: dirty") || strings.Contains(out, "value: newer") || transport.calls.Load() != 0 {
					t.Fatalf("discarded held consumer: concurrency=%s exit=%d transport=%d\n%s\nstderr:\n%s",
						concurrency, code, transport.calls.Load(), out, stderr)
				}
			}
			if tt.survivorName != "" {
				st := store.New()
				_, err := discovery.Run(t.Context(), discovery.Config{
					Path: filepath.Join(root, "flux"), Store: st,
					SourceCache: source.NewCache(cacheroot.New(t.TempDir())),
				})
				if tt.keepError {
					if !errors.Is(err, manifest.ErrInput) {
						t.Fatalf("expected surviving consumer's working-tree error, got %v", err)
					}
				} else if err != nil {
					t.Fatalf("expected replaced consumer's held error to be dropped, got %v", err)
				}
				id := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: tt.survivorName}
				ks, ok := st.Get[*manifest.Kustomization](id)
				if !ok || ks.Path != tt.survivorPath || ks.SourceName != tt.survivorSource || ks.SourceRef.Namespace != "" {
					t.Fatalf("expected survivor with independently changed identity field, got %+v", ks)
				}
				if tt.survivorName != "a-held" {
					id.Name = "a-held"
					if _, ok := st.Get[*manifest.Kustomization](id); ok {
						t.Fatal("held consumer survived replacement")
					}
				}
			}
		})
	}
}

func TestE2E_SourceRef_PinnedFollowedSourceInheritsNamespace(t *testing.T) {
	for _, tt := range []struct{ name, namespace, sourceDir string }{
		{name: "explicit", namespace: ", namespace: flux-system", sourceDir: "clusters"},
		{name: "omitted", sourceDir: "clusters"},
		{name: "second_explicit", namespace: ", namespace: flux-system", sourceDir: "sub"},
		{name: "second_omitted", sourceDir: "sub"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			repo := gitInit(t, root)
			if _, err := repo.CreateRemote(&config.RemoteConfig{
				Name: "origin", URLs: []string{"git://fixture.invalid/cluster"},
			}); err != nil {
				t.Fatal(err)
			}
			testutil.WriteFile(t, root, "flux/ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: meta, namespace: flux-system}
spec:
  interval: 10m
  path: ./clusters
  sourceRef: {kind: GitRepository, name: bootstrap, namespace: flux-system}
`)
			if tt.sourceDir == "sub" {
				testutil.WriteFile(t, root, "clusters/kustomization.yaml", "namespace: flux-system\nresources: [ks2.yaml]\n")
				testutil.WriteFile(t, root, "clusters/ks2.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: sub}
spec:
  interval: 10m
  path: ./sub
  sourceRef: {kind: GitRepository, name: bootstrap, namespace: flux-system}
`)
			}
			testutil.WriteFile(t, root, tt.sourceDir+"/kustomization.yaml", "namespace: flux-system\nresources: [repo.yaml, apps.yaml]\n")
			testutil.WriteFile(t, root, tt.sourceDir+"/repo.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: cluster}
spec:
  interval: 10m
  url: git://fixture.invalid/cluster
  ref: {tag: v1.0.0}
`)
			testutil.WriteFile(t, root, tt.sourceDir+"/apps.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: apps}
spec:
  interval: 10m
  path: ./apps
  sourceRef: {kind: GitRepository, name: cluster`+tt.namespace+`}
`)
			testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [cm.yaml]\n")
			for _, version := range []string{"v1.0.0", "v2.0.0"} {
				testutil.WriteFile(t, root, "apps/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello, namespace: apps}\ndata:\n  value: "+version+"\n")
				gitCommitAll(t, repo)
				head, err := repo.Head()
				if err != nil {
					t.Fatal(err)
				}
				if err := repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewTagReferenceName(version), head.Hash())); err != nil {
					t.Fatal(err)
				}
			}
			mutateFile(t, filepath.Join(root, "apps/cm.yaml"), "value: v2.0.0", "value: dirty")
			transport := installSourceRefTransport(t, repo.Storer)
			for _, concurrency := range []string{"1", "2", "8"} {
				out, stderr, code := runCLIBuffers("build", "all", "--path", filepath.Join(root, "flux"),
					"--concurrency", concurrency, "--cache-dir", t.TempDir())
				if code != 0 || !strings.Contains(out, "value: v1.0.0") || strings.Contains(out, "value: dirty") || transport.calls.Load() != 0 {
					t.Fatalf("followed source pin: concurrency=%s exit=%d transport=%d\n%s\nstderr:\n%s", concurrency, code, transport.calls.Load(), out, stderr)
				}
			}
		})
	}
}

func TestE2E_SourceRef_PinnedRootPreservesSource(t *testing.T) {
	root, repo, older, head := sourceRefFixture(t)
	wt, err := repo.Worktree()
	if err != nil {
		t.Fatal(err)
	}
	for _, hash := range []plumbing.Hash{older, head} {
		if err := wt.Checkout(&gogit.CheckoutOptions{Hash: hash}); err != nil {
			t.Fatal(err)
		}
		mutateFile(t, filepath.Join(root, "flux/entry.yaml"), "path: ./apps", "path: ./")
		testutil.WriteFile(t, root, "kustomization.yaml", "resources: [apps, flux/entry.yaml]\n")
		gitCommitAll(t, repo)
		if hash == older {
			ref, err := repo.Head()
			if err != nil {
				t.Fatal(err)
			}
			if err := repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewTagReferenceName("v1.0.0"), ref.Hash())); err != nil {
				t.Fatal(err)
			}
		}
	}
	mutateFile(t, filepath.Join(root, "apps/cm.yaml"), "value: v2.0.0", "value: dirty")
	transport := installSourceRefTransport(t, repo.Storer)
	for _, scope := range []string{"flux", "."} {
		t.Run(scope, func(t *testing.T) {
			out, _ := requireCLIOK(t, "build", "all", "--path", filepath.Join(root, scope),
				"--concurrency", "2", "--cache-dir", t.TempDir())
			if !strings.Contains(out, "value: v1.0.0") || strings.Contains(out, "value: dirty") || strings.Contains(out, "value: v2.0.0") {
				t.Fatalf("pinned root rendered the working tree:\n%s", out)
			}
			if transport.calls.Load() != 0 {
				t.Fatalf("local pin accessed transport %d times", transport.calls.Load())
			}
		})
	}
}

func TestE2E_SourceRef_PinnedPathIgnoresAddedDiscoveryObjects(t *testing.T) {
	original, _, _, _ := sourceRefFixture(t)
	current, _, _, _ := sourceRefFixture(t)
	testutil.WriteFile(t, current, "apps/added-ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: added-ks, namespace: flux-system}
spec:
  path: ./apps/phantom
  sourceRef: {kind: GitRepository, name: working, namespace: flux-system}
---
apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: working, namespace: flux-system}
spec: {url: 'git://fixture.invalid/cluster'}
`)
	testutil.WriteFile(t, current, "apps/added-rs.yaml", `apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: added-rs, namespace: flux-system}
spec:
  resourcesTemplate: |
    apiVersion: v1
    kind: ConfigMap
    metadata: {name: added-rs-output, namespace: apps}
    data: {value: phantom}
`)
	testutil.WriteFile(t, current, "apps/phantom/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, current, "apps/phantom/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: added-ks-output, namespace: apps}\ndata: {value: phantom}\n")
	mutateFile(t, filepath.Join(current, "apps/kustomization.yaml"), "- cm.yaml\n", "- cm.yaml\n- added-ks.yaml\n- added-rs.yaml\n")
	for _, scope := range []string{"flux", "."} {
		t.Run(scope, func(t *testing.T) {
			path := filepath.Join(current, scope)
			out, _ := requireCLIOK(t, "build", "all", "--path", path,
				"--concurrency", "2", "--cache-dir", t.TempDir())
			if !strings.Contains(out, "value: v1.0.0") || strings.Contains(out, "phantom") || strings.Contains(out, "added-") {
				t.Fatalf("working-tree discovery objects escaped the pin:\n%s", out)
			}
			for _, paths := range [][2]string{{current, original}, {original, current}} {
				out, _ := requireCLIOK(t, "diff", "all", "--path", filepath.Join(paths[0], scope),
					"--path-orig", filepath.Join(paths[1], scope), "--concurrency", "2", "--cache-dir", t.TempDir(), "-o", "diff")
				if out != "" {
					t.Fatalf("working-tree discovery objects changed the pinned diff:\n%s", out)
				}
			}
		})
	}
}

func TestE2E_SourceRef_PinnedPathRendersDeletedKustomization(t *testing.T) {
	root, repo, _, _ := sourceRefFixture(t)
	testutil.WriteFile(t, root, "apps/child.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: child, namespace: flux-system}
spec:
  path: ./leaf
  sourceRef: {kind: GitRepository, name: cluster, namespace: flux-system}
`)
	testutil.WriteFile(t, root, "leaf/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, root, "leaf/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: child-output, namespace: apps}\ndata: {value: pinned-child}\n")
	gitCommitAll(t, repo)
	head, err := repo.Head()
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewTagReferenceName("v1.0.0"), head.Hash())); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(root, "apps/child.yaml")); err != nil {
		t.Fatal(err)
	}
	gitCommitAll(t, repo)
	out, _ := requireCLIOK(t, "build", "all", "--path", filepath.Join(root, "flux"),
		"--concurrency", "2", "--cache-dir", t.TempDir())
	if !strings.Contains(out, "name: child-output") || !strings.Contains(out, "value: pinned-child") {
		t.Fatalf("deleted committed child did not render:\n%s", out)
	}
}

func TestE2E_SourceRef_PinnedPathOwnsWorkingTreeReleases(t *testing.T) {
	root, repo, older, head := sourceRefFixture(t)
	wt, err := repo.Worktree()
	if err != nil {
		t.Fatal(err)
	}
	for _, hash := range []plumbing.Hash{older, head} {
		if err := wt.Checkout(&gogit.CheckoutOptions{Hash: hash}); err != nil {
			t.Fatal(err)
		}
		testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources:\n- cm.yaml\n- release.yaml\n")
		testutil.WriteFile(t, root, "apps/release.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: existing, namespace: apps}
spec:
  interval: 10m
  chart:
    spec:
      chart: charts/demo
      sourceRef: {kind: GitRepository, name: cluster, namespace: flux-system}
  values: {greeting: pinned}
`)
		testutil.WriteFile(t, root, "charts/demo/Chart.yaml", "apiVersion: v2\nname: demo\nversion: 0.1.0\n")
		testutil.WriteFile(t, root, "charts/demo/templates/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ .Release.Name }}-cm
  namespace: {{ .Release.Namespace }}
data:
  greeting: {{ .Values.greeting | quote }}
`)
		if hash == older {
			gitCommitAll(t, repo)
			ref, err := repo.Head()
			if err != nil {
				t.Fatal(err)
			}
			if err := repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewTagReferenceName("v1.0.0"), ref.Hash())); err != nil {
				t.Fatal(err)
			}
		}
	}
	loader := installSourceRefTransport(t, repo.Storer)
	mutateFile(t, filepath.Join(root, "apps/release.yaml"), "greeting: pinned", "greeting: dirty")
	release, err := os.ReadFile(filepath.Join(root, "apps/release.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	testutil.WriteFile(t, root, "apps/added.yaml", strings.Replace(string(release), "name: existing", "name: added", 1))
	mutateFile(t, filepath.Join(root, "apps/kustomization.yaml"), "- release.yaml\n", "- release.yaml\n- added.yaml\n")
	out, _ := requireCLIOK(t, "build", "all", "--path", root,
		"--concurrency", "2", "--cache-dir", t.TempDir(), "-o", "json")
	if loader.calls.Load() != 0 {
		t.Fatalf("local pin accessed transport %d times", loader.calls.Load())
	}
	var docs []struct {
		Kind     string
		Metadata struct{ Name, Namespace string }
		Data     map[string]string
		Spec     struct{ Values map[string]string }
	}
	if err := json.Unmarshal([]byte(out), &docs); err != nil {
		t.Fatal(err)
	}
	ids := make(map[manifest.NamedResource]int)
	for _, doc := range docs {
		ids[manifest.NamedResource{Kind: doc.Kind, Namespace: doc.Metadata.Namespace, Name: doc.Metadata.Name}]++
		if doc.Kind == manifest.KindHelmRelease && doc.Spec.Values["greeting"] != "pinned" ||
			doc.Kind == manifest.KindConfigMap && doc.Metadata.Name == "existing-cm" && doc.Data["greeting"] != "pinned" ||
			doc.Metadata.Name == "hello" && doc.Data["value"] != "v1.0.0" {
			t.Fatalf("expected pinned content:\n%s", out)
		}
	}
	want := map[manifest.NamedResource]int{
		{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "hello"}:       1,
		{Kind: manifest.KindHelmRelease, Namespace: "apps", Name: "existing"}:  1,
		{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "existing-cm"}: 1,
	}
	if diff := cmp.Diff(want, ids); diff != "" {
		t.Fatalf("pinned object ids (-want +got):\n%s\n%s", diff, out)
	}
}

func sourceRefFixture(t *testing.T) (string, *gogit.Repository, plumbing.Hash, plumbing.Hash) {
	t.Helper()
	root := t.TempDir()
	repo := gitInit(t, root)
	if _, err := repo.CreateRemote(&config.RemoteConfig{
		Name: "origin", URLs: []string{"git://fixture.invalid/cluster"},
	}); err != nil {
		t.Fatal(err)
	}
	testutil.WriteFile(t, root, "flux/entry.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: cluster, namespace: flux-system}
spec:
  interval: 10m
  url: git://fixture.invalid/cluster
  ref: {tag: v1.0.0}
  ignore: '/ignored.txt'
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: apps, namespace: flux-system}
spec:
  interval: 10m
  path: ./apps
  sourceRef: {kind: GitRepository, name: cluster, namespace: flux-system}
`)
	testutil.WriteFile(t, root, "ignored.txt", "ignored by the source artifact\n")
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources:\n- cm.yaml\n")
	var hashes [2]plumbing.Hash
	for i, version := range []string{"v1.0.0", "v2.0.0"} {
		testutil.WriteFile(t, root, "apps/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: hello, namespace: apps}
data:
  value: `+version+"\n")
		gitCommitAll(t, repo)
		head, err := repo.Head()
		if err != nil {
			t.Fatal(err)
		}
		hashes[i] = head.Hash()
		if err := repo.Storer.SetReference(plumbing.NewHashReference(
			plumbing.NewTagReferenceName(version), head.Hash())); err != nil {
			t.Fatal(err)
		}
	}
	return root, repo, hashes[0], hashes[1]
}

func TestE2E_SourceRef_NonHEADRendersPreserveCheckout(t *testing.T) {
	for _, kind := range []string{"tag", "commit", "semver", "head", "no-ref"} {
		t.Run(kind, func(t *testing.T) {
			root, repo, a, _ := sourceRefFixture(t)
			ref := "tag: v1.0.0"
			want := "v1.0.0"
			switch kind {
			case "commit":
				ref = "commit: " + a.String()
			case "semver":
				ref = "semver: '<2.0.0'"
			case "head":
				ref, want = "tag: v2.0.0", "dirty"
			case "no-ref":
				ref, want = "", "dirty"
			}
			setSourceRef(t, root, ref)
			mutateFile(t, filepath.Join(root, "apps/cm.yaml"), "value: v2.0.0", "value: dirty")
			wt, err := repo.Worktree()
			if err != nil {
				t.Fatal(err)
			}
			beforeHead, err := repo.Head()
			if err != nil {
				t.Fatal(err)
			}
			beforeStatus, err := wt.Status()
			if err != nil {
				t.Fatal(err)
			}
			before := make(map[string][]byte)
			for _, path := range []string{".git/HEAD", ".git/index", "flux/entry.yaml", "apps/cm.yaml", "apps/kustomization.yaml"} {
				before[path], err = os.ReadFile(filepath.Join(root, path))
				if err != nil {
					t.Fatal(err)
				}
			}
			out, _ := requireCLIOK(t, "build", "all", "--path", filepath.Join(root, "flux"),
				"--concurrency", "2", "--cache-dir", t.TempDir())
			if !strings.Contains(out, "value: "+want) || kind != "head" && kind != "no-ref" && strings.Contains(out, "dirty") {
				t.Fatalf("expected %s content:\n%s", want, out)
			}
			afterHead, err := repo.Head()
			if err != nil || afterHead.Hash() != beforeHead.Hash() || afterHead.Name() != beforeHead.Name() {
				t.Fatalf("HEAD changed: %v, %v", afterHead, err)
			}
			afterStatus, err := wt.Status()
			if diff := cmp.Diff(beforeStatus, afterStatus); err != nil || diff != "" {
				t.Fatalf("checkout status changed (-want +got):\n%s\n%v", diff, err)
			}
			for path, want := range before {
				got, err := os.ReadFile(filepath.Join(root, path))
				if err != nil || !bytes.Equal(got, want) {
					t.Fatalf("checkout file %s changed: %v", path, err)
				}
			}
		})
	}
}

func TestE2E_PinnedDiff_BaseAndCurrentRefs(t *testing.T) {
	root, repo, _, _ := sourceRefFixture(t)
	loader := installSourceRefTransport(t, repo.Storer)
	setSourceRef(t, root, "tag: v2.0.0")
	out, stderr := requireCLIOK(t, "diff", "all", "--path", filepath.Join(root, "flux"), "--base", "HEAD",
		"--concurrency", "2", "--cache-dir", t.TempDir(), "--git-depth", "0", "--log-level", "warn", "-o", "diff")
	assertSourceValueDiff(t, out, "v1.0.0", "v2.0.0")
	if loader.calls.Load() == 0 || strings.Count(stderr, "declared source ref cannot be satisfied locally") != 1 {
		t.Fatalf("exported declared-ref baseline did not warn/fetch: calls=%d\n%s", loader.calls.Load(), stderr)
	}
}

func TestE2E_PinnedDiff_StablePinIgnoresDirtyContent(t *testing.T) {
	root, repo, _, _ := sourceRefFixture(t)
	loader := installSourceRefTransport(t, repo.Storer)
	mutateFile(t, filepath.Join(root, "apps/cm.yaml"), "value: v2.0.0", "value: dirty")
	local, _ := requireCLIOK(t, "build", "all", "--path", filepath.Join(root, "flux"),
		"--concurrency", "2", "--cache-dir", t.TempDir())
	if loader.calls.Load() != 0 {
		t.Fatalf("local pin accessed transport %d times", loader.calls.Load())
	}
	exported := copyTree(t, root)
	if err := os.RemoveAll(filepath.Join(exported, ".git")); err != nil {
		t.Fatal(err)
	}
	fetched, _ := requireCLIOK(t, "build", "all", "--path", exported, "--concurrency", "2", "--cache-dir", t.TempDir(), "--git-depth", "0")
	if fetched != local || loader.calls.Load() == 0 {
		t.Fatalf("local/fetched pin bytes differ or no fetch occurred:\nlocal:\n%s\nfetched:\n%s", local, fetched)
	}
	out, stderr := requireCLIOK(t, "diff", "all", "--path", filepath.Join(root, "flux"), "--base", "HEAD",
		"--concurrency", "2", "--cache-dir", t.TempDir(), "--git-depth", "0", "--log-level", "warn", "-o", "diff")
	if out != "" || strings.Count(stderr, "declared source ref cannot be satisfied locally") != 1 {
		t.Fatalf("stable pin changed with dirty working files:\n%s\nstderr:\n%s", out, stderr)
	}
}

func TestE2E_PinnedDiff_RootPin(t *testing.T) {
	current, repo, older, head := sourceRefFixture(t)
	wt, err := repo.Worktree()
	if err != nil {
		t.Fatal(err)
	}
	for _, hash := range []plumbing.Hash{older, head} {
		if err := wt.Checkout(&gogit.CheckoutOptions{Hash: hash}); err != nil {
			t.Fatal(err)
		}
		testutil.WriteFile(t, current, "flux/entry.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: apps, namespace: flux-system}
spec:
  interval: 10m
  path: ./
  sourceRef: {kind: GitRepository, name: cluster, namespace: flux-system}
`)
		testutil.WriteFile(t, current, "flux/repo.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: cluster, namespace: flux-system}
spec:
  interval: 10m
  url: git://fixture.invalid/cluster
  ref: {tag: v1.0.0}
  ignore: '/ignored.txt'
`)
		testutil.WriteFile(t, current, "kustomization.yaml", "resources: [apps, flux/entry.yaml, flux/repo.yaml]\n")
		gitCommitAll(t, repo)
		if hash == older {
			ref, err := repo.Head()
			if err != nil {
				t.Fatal(err)
			}
			if err := repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewTagReferenceName("v1.0.0"), ref.Hash())); err != nil {
				t.Fatal(err)
			}
		}
	}
	original := copyTree(t, current)
	transport := installSourceRefTransport(t, repo.Storer)
	mutateFile(t, filepath.Join(current, "apps/cm.yaml"), "value: v2.0.0", "value: dirty")
	for _, stage := range []string{"dirty-only", "tag-change"} {
		t.Run(stage, func(t *testing.T) {
			if stage == "tag-change" {
				mutateFile(t, filepath.Join(current, "flux/repo.yaml"), "tag: v1.0.0", "tag: v2.0.0")
			}
			for _, scope := range []string{"flux", "."} {
				t.Run(scope, func(t *testing.T) {
					for _, side := range []struct{ name, current, original, before, after string }{
						{"forward", current, original, "v1.0.0", "v2.0.0"},
						{"reverse", original, current, "v2.0.0", "v1.0.0"},
					} {
						t.Run(side.name, func(t *testing.T) {
							args := []string{"diff", "all", "--path", filepath.Join(side.current, scope),
								"--path-orig", filepath.Join(side.original, scope), "--concurrency", "2", "--cache-dir", t.TempDir()}
							out, _ := requireCLIOK(t, append(args, "-o", "diff")...)
							if stage == "dirty-only" {
								if out != "" {
									t.Fatalf("dirty content changed the root-pin diff:\n%s", out)
								}
							} else {
								assertSourceValueDiff(t, out, side.before, side.after)
							}
							args[0] = "build"
							out, _ = requireCLIOK(t, args...)
							if stage == "dirty-only" {
								if out != "" {
									t.Fatalf("dirty content selected the pinned root:\n%s", out)
								}
							} else if !strings.Contains(out, "value: "+side.after) || strings.Contains(out, "value: dirty") {
								t.Fatalf("tag change did not select the pinned root:\n%s", out)
							}
							if transport.calls.Load() != 0 {
								t.Fatalf("root pin accessed transport %d times", transport.calls.Load())
							}
						})
					}
				})
			}
		})
	}
}

func TestE2E_PinnedDiff_SubstituteFromProducer(t *testing.T) {
	testPinnedProducerDiff(t, false)
}

func TestE2E_PinnedDiff_DeletedSubstituteFromProducer(t *testing.T) {
	testPinnedProducerDiff(t, true)
}

func testPinnedProducerDiff(t *testing.T, deleted bool) {
	t.Helper()
	var roots [2]string
	for i, mode := range []string{"before", "after"} {
		root, repo, older, head := sourceRefFixture(t)
		roots[i] = root
		wt, err := repo.Worktree()
		if err != nil {
			t.Fatal(err)
		}
		for _, hash := range []plumbing.Hash{older, head} {
			if err := wt.Checkout(&gogit.CheckoutOptions{Hash: hash}); err != nil {
				t.Fatal(err)
			}
			mutateFile(t, filepath.Join(root, "apps/cm.yaml"), "name: hello, namespace: apps", "name: hello")
			testutil.WriteFile(t, root, "apps/kustomization.yaml", "namespace: apps\nresources:\n- cm.yaml\n")
			if hash == older {
				gitCommitAll(t, repo)
				ref, err := repo.Head()
				if err != nil {
					t.Fatal(err)
				}
				if err := repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewTagReferenceName("v1.0.0"), ref.Hash())); err != nil {
					t.Fatal(err)
				}
			}
		}
		if deleted {
			if err := os.Remove(filepath.Join(root, "apps/cm.yaml")); err != nil {
				t.Fatal(err)
			}
		}
		testutil.WriteFile(t, root, "flux/consumer.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: working, namespace: flux-system}
spec:
  interval: 10m
  url: git://fixture.invalid/cluster
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: consumer, namespace: apps}
spec:
  interval: 10m
  path: ./consumer
  sourceRef: {kind: GitRepository, name: working, namespace: flux-system}
  postBuild:
    substitute: {mode: `+mode+`}
    substituteFrom:
      - kind: ConfigMap
        name: hello
`)
		testutil.WriteFile(t, root, "consumer/kustomization.yaml", "resources:\n- cm.yaml\n")
		testutil.WriteFile(t, root, "consumer/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: consumer, namespace: apps}
data:
  value: ${value}
  mode: ${mode}
`)
		out, _ := requireCLIOK(t, "build", "all", "--path", filepath.Join(root, "flux"),
			"--concurrency", "2", "--cache-dir", t.TempDir())
		if strings.Count(out, "value: v1.0.0") != 2 || !strings.Contains(out, "mode: "+mode) {
			t.Fatalf("expected pinned substitution in full render:\n%s", out)
		}
	}
	out, _ := requireCLIOK(t, "diff", "all", "--path", filepath.Join(roots[1], "flux"),
		"--path-orig", filepath.Join(roots[0], "flux"), "--concurrency", "2", "--cache-dir", t.TempDir(), "-o", "diff")
	if strings.Count(out, "-  mode: before") != 1 || strings.Count(out, "+  mode: after") != 1 ||
		!strings.Contains(out, "   value: v1.0.0") || strings.Contains(out, "${value}") || strings.Contains(out, "v2.0.0") {
		t.Fatalf("expected consumer diff with pinned substitution on both sides:\n%s", out)
	}
}

func TestE2E_SourceRef_PathOrigUsesOwnObjectStore(t *testing.T) {
	for _, tt := range []struct {
		name       string
		currentRef string
	}{
		{name: "different-spec", currentRef: "name: refs/tags/v1.0.0"},
		{name: "identical-spec", currentRef: "tag: v1.0.0"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.name == "identical-spec" {
				t.Skip("changed-only selection ignores divergent source revisions with identical authored files; tracked in fl-5c42t")
			}
			current, _, _, _ := sourceRefFixture(t)
			setSourceRef(t, current, tt.currentRef)
			original, originalRepo, _, b := sourceRefFixture(t)
			if err := originalRepo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewTagReferenceName("v1.0.0"), b)); err != nil {
				t.Fatal(err)
			}
			for _, side := range []struct {
				root  string
				value string
			}{
				{root: current, value: "v1.0.0"},
				{root: original, value: "v2.0.0"},
			} {
				out, _ := requireCLIOK(t, "build", "all", "--path", filepath.Join(side.root, "flux"),
					"--concurrency", "2", "--cache-dir", t.TempDir())
				if !strings.Contains(out, "value: "+side.value) {
					t.Fatalf("expected %s from %s:\n%s", side.value, side.root, out)
				}
			}
			out, _ := requireCLIOK(t, "diff", "all", "--path", filepath.Join(current, "flux"), "--path-orig", filepath.Join(original, "flux"),
				"--concurrency", "2", "--cache-dir", t.TempDir(), "--git-depth", "0", "--log-level", "warn", "-o", "diff")
			assertSourceValueDiff(t, out, "v2.0.0", "v1.0.0")
		})
	}
}

func assertSourceValueDiff(t *testing.T, out, before, after string) {
	t.Helper()
	if strings.Count(out, "-  value: "+before) != 1 || strings.Count(out, "+  value: "+after) != 1 ||
		strings.Contains(out, "+apiVersion:") || strings.Contains(out, "+kind: ConfigMap") {
		t.Fatalf("expected exact field removal/addition for %s -> %s:\n%s", before, after, out)
	}
}

func setSourceRef(t *testing.T, root, ref string) {
	t.Helper()
	path := filepath.Join(root, "flux/entry.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	line := ""
	if ref != "" {
		line = "  ref: {" + ref + "}\n"
	}
	testutil.WriteFileAt(t, path, strings.Replace(string(data), "  ref: {tag: v1.0.0}\n", line, 1))
}

type sourceRefLoader struct {
	store storer.Storer
	calls atomic.Int64
}

func (l *sourceRefLoader) Load(endpoint *transport.Endpoint) (storer.Storer, error) {
	if endpoint.Host != "fixture.invalid" || endpoint.Path != "/cluster" {
		return nil, transport.ErrRepositoryNotFound
	}
	l.calls.Add(1)
	return l.store, nil
}

func installSourceRefTransport(t *testing.T, storage storer.Storer) *sourceRefLoader {
	t.Helper()
	loader := &sourceRefLoader{store: storage}
	previous := client.Protocols["git"]
	client.InstallProtocol("git", server.NewClient(loader))
	t.Cleanup(func() { client.InstallProtocol("git", previous) })
	return loader
}
