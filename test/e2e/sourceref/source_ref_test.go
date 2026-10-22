// Package sourceref tests source selection, pinned trees and discovery.
package sourceref

import (
	"bytes"
	stdcmp "cmp"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"

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
	"github.com/home-operations/flate/pkg/baseline"
	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

func TestE2E_SourceRef_EmptyPathMatchesRoot(t *testing.T) {
	fixture := func(t *testing.T, field string) (string, *store.Store, *discovery.Result) {
		t.Helper()
		root := t.TempDir()
		testutil.GitInit(t, root)
		testutil.WriteFile(t, root, "flux/root.yaml", testutil.KustomizationYAML("root", "flux-system", "")+field+`  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
  targetNamespace: apps
  postBuild: {substitute: {VALUE: source-root}}
`)
		testutil.WriteFile(t, root, "kustomization.yaml", "resources: [cm.yaml, child.yaml]\n")
		testutil.WriteFile(t, root, "cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: root-owned}
data: {value: '${VALUE}'}
`)
		testutil.WriteFile(t, root, "child.yaml", testutil.KustomizationYAML("child", "apps", "  path: ./leaf\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n"))
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
	wantOutput := harness.RunCLIStdout(t, "build", "all", "--path", filepath.Join(root, "flux"),
		"--concurrency", "2", "--cache-dir", t.TempDir())
	for _, path := range []struct{ name, field string }{
		{name: "explicit", field: "  path: ./\n"},
		{name: "omitted"},
	} {
		t.Run(path.name, func(t *testing.T) {
			root, st, res := fixture(t, path.field)
			parent := testutil.KustomizationID("flux-system", "root")
			child := testutil.KustomizationID("apps", "child")
			record, _ := res.Index.Lookup(child)
			if st.GetObject(child) == nil || record.Parent != parent {
				t.Fatalf("source-root child not discovered or owned by root: parent=%v", record.Parent)
			}
			owners, _ := res.Index.OwnersOfFile("cm.yaml")
			if !slices.Equal(owners, []manifest.NamedResource{parent}) {
				t.Fatalf("source-root ConfigMap owners = %v; want [%v]", owners, parent)
			}
			for id, record := range res.Index.Files() {
				wantRecord, ok := want.Index.Lookup(id)
				if !ok {
					t.Fatalf("unexpected discovered resource %s", id)
				}
				assert.Equal(t, record.File, wantRecord.File)
				assert.Equal(t, record.Parent, wantRecord.Parent)
			}
			for _, concurrency := range []string{"2", "8"} {
				t.Run("concurrency_"+concurrency, func(t *testing.T) {
					args := []string{"--path", filepath.Join(root, "flux"), "--concurrency", concurrency, "--cache-dir", t.TempDir()}
					owned := harness.RunCLIStdout(t, append([]string{"build", "ks", "root"}, args...)...)
					if !strings.Contains(owned, "name: root-owned") || !strings.Contains(owned, "value: source-root") ||
						!strings.Contains(owned, "namespace: apps") {
						t.Fatalf("root's rendered objects missing or untransformed:\n%s", owned)
					}
					out := harness.RunCLIStdout(t, append([]string{"build", "all"}, args...)...)
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
			testutil.GitInit(t, root)
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
			testutil.WriteFile(t, root, "flux/root.yaml", testutil.KustomizationYAML("root", "flux-system", "  interval: 1m\n  prune: true\n  path: ./flux\n  sourceRef: {kind: GitRepository, name: flux-system}\n"))
			testutil.WriteFile(t, root, "flux/tmpl.yaml", testutil.KustomizationYAML("tmpl", "flux-system", "  {interval: 1m, prune: true}\n"))
			testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(t, root, "apps/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: apps-cm, namespace: apps}\n")
			testutil.WriteFile(t, root, "unrelated/broken.yaml", "invalid: [\n")
			out, _ := harness.RequireCLIOK(t, "build", "all", "--path", filepath.Join(root, "flux"),
				"--concurrency", concurrency, "--cache-dir", t.TempDir())
			docs, err := manifest.DecodeDocs(strings.NewReader(out))
			if err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, len(docs), 3)
			seen := map[manifest.NamedResource]bool{}
			for _, doc := range docs {
				obj, _, _, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{DefaultNamespace: ""})
				if err != nil {
					t.Fatal(err)
				}
				seen[obj.Named()] = true
				if ks, ok := obj.(*manifest.Kustomization); ok && ks.Name == "tmpl" {
					assert.Equal(t, ks.Path, "./apps")
					assert.Equal(t, ks.SourceRef.Name, "flux-system")
				}
			}
			assert.Diff(t, seen, map[manifest.NamedResource]bool{
				testutil.KustomizationID("flux-system", "root"):                    true,
				testutil.KustomizationID("flux-system", "tmpl"):                    true,
				{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "apps-cm"}: true,
			})
		})
	}
}

func TestE2E_SourceRef_NonHEADTag(t *testing.T) {
	root, repo, older, _ := sourceRefFixture(t)
	commit, err := repo.CommitObject(older)
	assert.NoError(t, err)
	tree, err := commit.Tree()
	assert.NoError(t, err)
	tree.Entries = append(tree.Entries, object.TreeEntry{Name: "module", Mode: filemode.Submodule, Hash: older})
	slices.SortFunc(tree.Entries, func(a, b object.TreeEntry) int { return strings.Compare(a.Name, b.Name) })
	encoded := repo.Storer.NewEncodedObject()
	assert.NoError(t, tree.Encode(encoded))
	commit.TreeHash, err = repo.Storer.SetEncodedObject(encoded)
	assert.NoError(t, err)
	encoded = repo.Storer.NewEncodedObject()
	assert.NoError(t, commit.Encode(encoded))
	pinned, err := repo.Storer.SetEncodedObject(encoded)
	assert.NoError(t, err)
	assert.NoError(t, repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewTagReferenceName("v1.0.0"), pinned)))
	for _, verb := range []string{"build", "test"} {
		var previous string
		for range 2 {
			out, stderr := harness.RequireCLIOK(t, verb, "all", "--path", root+"/flux", "--concurrency=2", "--cache-dir", t.TempDir())
			if strings.Count(stderr, "[SkippedSubmodule]") != 1 || !strings.Contains(stderr, "module") || !strings.Contains(stderr, "flux-system/cluster") || strings.Contains(out, "skipped submodule") {
				t.Fatalf("submodule destination: stdout=%s stderr=%s", out, stderr)
			}
			if verb == "build" && (!strings.Contains(out, "value: v1.0.0") || strings.Contains(out, "value: v2.0.0")) || verb == "test" && !strings.Contains(out, "passed") {
				t.Fatalf("pinned product: %s", out)
			}
			if previous != "" && stderr != previous {
				t.Fatal("submodule warning changed")
			}
			previous = stderr
		}
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
			repo := testutil.GitInit(t, root)
			if _, err := repo.CreateRemote(&config.RemoteConfig{
				Name: "origin", URLs: []string{"git://fixture.invalid/cluster"},
			}); err != nil {
				t.Fatal(err)
			}
			testutil.WriteFile(t, root, "flux/"+tt.appsFile, testutil.KustomizationYAML("apps", "flux-system", "  interval: 10m\n  path: ./apps\n  sourceRef: {kind: GitRepository, name: pinned, namespace: flux-system}\n"))
			testutil.WriteFile(t, root, "flux/"+tt.sourcesFile, testutil.KustomizationYAML(tt.sourcesName, "flux-system", "  interval: 10m\n  path: ./sources\n  sourceRef: {kind: GitRepository, name: "+tt.sourceRefName+", namespace: flux-system}\n"))
			testutil.WriteFile(t, root, "sources/kustomization.yaml", "resources: [repo.yaml]\n")
			url, ref := stdcmp.Or(tt.sourceURL, "git://fixture.invalid/cluster"), "  ref: {tag: v1.0.0}\n"
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
			testutil.GitCommitAll(t, repo, "init", &gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}})
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
			harness.MutateFile(t, filepath.Join(root, "apps/cm.yaml"), "value: pinned", "value: newer")
			testutil.GitCommitAll(t, repo, "init", &gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}})
			harness.MutateFile(t, filepath.Join(root, "apps/cm.yaml"), "value: newer", "value: dirty")
			testutil.WriteFile(t, root, "apps/extra.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: extra\n")
			transport := installSourceRefTransport(t, repo.Storer)
			var wantError string
			if tt.noRef || tt.sourceURL != "" {
				l := loader.New(store.New())
				l.SourceRoot = root
				if _, err := l.Load(t.Context(), filepath.Join(root, "apps")); err != nil {
					t.Fatal(err)
				}
				decodeError := l.Index.Errors["apps/extra.yaml"]
				if !errors.Is(decodeError, manifest.ErrInput) {
					t.Fatalf("expected working-tree decode error, got %v", decodeError)
				}
				wantError = "flate error: " + decodeError.Error() + "\n"
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
							out, stderr, code := harness.RunCLIBuffers(args...)
							if tt.sourceURL != "" {
								wantCode, wantReason := 0, "external source GitRepository.source.toolkit.fluxcd.io/flux-system/pinned is outside the local tree; not rendered"
								if command == "build" {
									wantCode, wantReason = 1, "repository not found"
								}
								if code != wantCode || !strings.Contains(stderr, wantReason) || strings.Contains(stderr, "loader: decode") ||
									transport.calls.Load() != 0 || (command == "diff" && out != "") {
									t.Fatalf("external source must release the working-tree error: exit=%d transport=%d\n%s\nstderr:\n%s", code, transport.calls.Load(), out, stderr)
								}
								return
							}
							if wantError != "" {
								if code != 1 || !strings.Contains(stderr, "kustomize build ./apps") || transport.calls.Load() != 0 {
									t.Fatalf("unpinned Kustomization failure: exit=%d transport=%d\n%s\nstderr:\n%s", code, transport.calls.Load(), out, stderr)
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
			})
			if err != nil {
				t.Fatal(err)
			}
			artifact, ok := st.GetArtifact(manifest.NamedResource{Group: "source.toolkit.fluxcd.io",
				Kind: manifest.KindGitRepository, Namespace: "flux-system", Name: "pinned",
			}).(*store.SourceArtifact)
			if !ok || artifact.LocalPath == root {
				t.Fatalf("expected committed source artifact, got %+v", artifact)
			}
			for _, file := range []struct{ name, path string }{{"hello", "cm.yaml"}, {"deleted", "deleted.yaml"}, {"extra", "extra.yaml"}} {
				id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: file.name}
				record, indexed := res.Index.Lookup(id)
				if !indexed || record.Path != filepath.Join(artifact.LocalPath, "apps", file.path) ||
					filepath.Join(root, filepath.FromSlash(record.File)) != record.Path {
					t.Fatalf("pinned discovery metadata for %s: indexed=%t path=%q source=%q", id, indexed, record.Path, record.File)
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
			repo := testutil.GitInit(t, root)
			if _, err := repo.CreateRemote(&config.RemoteConfig{
				Name: "origin", URLs: []string{"git://fixture.invalid/cluster"},
			}); err != nil {
				t.Fatal(err)
			}
			writeConsumer := func(name, path, kind, sourceName string) {
				testutil.WriteFile(t, root, "flux/"+name+".yaml", testutil.KustomizationYAML(name, "flux-system", "  interval: 10m\n  path: ./"+path+"\n  sourceRef: {kind: "+kind+", name: "+sourceName+"}\n"))
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
spec: {url: 'oci://fixture.invalid/second', interval: 10m, secretRef: {name: missing-auth}}
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
			testutil.GitCommitAll(t, repo, "init", &gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}})
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
			inputStore := store.New()
			if tt.unreadable {
				input, _, err := baseline.AdmitDirectory(t.Context(), root, root)
				if err != nil {
					t.Fatal(err)
				}
				obj := &manifest.GitRepository{Name: tt.sourceName, Namespace: "flux-system"}
				inputStore.AddObject(obj)
				inputStore.SetArtifact(obj.Named(), &store.SourceArtifact{LocalPath: root, FS: input.FS})
			}
			secondLoader := loader.New(inputStore)
			secondLoader.SourceRoot = root
			_, secondError := secondLoader.Load(t.Context(), filepath.Join(root, secondPath))
			if !tt.unreadable {
				if secondError != nil {
					t.Fatal(secondError)
				}
				secondError = secondLoader.Index.Errors[secondPath+"/extra.yaml"]
				if !errors.Is(secondError, manifest.ErrInput) {
					t.Fatalf("expected second consumer's decode error, got %v", secondError)
				}
			}
			if tt.unreadable && secondError == nil {
				t.Skip("OS or current user permits reading a file with no permissions")
			}
			if secondError == nil || (tt.unreadable && errors.Is(secondError, manifest.ErrInput)) {
				t.Fatalf("expected second consumer's loader error, got %v", secondError)
			}
			wantError, repeats := "flate error: "+secondError.Error()+"\n", 1
			if tt.bothHeld {
				heldLoader := loader.New(store.New())
				heldLoader.SourceRoot = root
				if _, err := heldLoader.Load(t.Context(), filepath.Join(root, heldPath)); err != nil {
					t.Fatal(err)
				}
				heldError := heldLoader.Index.Errors[heldPath+"/extra.yaml"]
				if !errors.Is(heldError, manifest.ErrInput) {
					t.Fatal("expected held consumer's decode error")
				}
				repeats = 2
				if !tt.reversedPaths {
					wantError = "flate error: " + heldError.Error() + "\n"
				}
			}
			transport := installSourceRefTransport(t, repo.Storer)
			for i := range repeats {
				out, stderr, code := harness.RunCLIBuffers("build", "all", "--path", filepath.Join(root, "flux"),
					"--concurrency", "2", "--cache-dir", t.TempDir())
				if tt.unreadable {
					if code != 1 || out != "" || !strings.HasSuffix(stderr, wantError) || strings.Count(stderr, "[RenderPersistenceDisabled]") > 1 || transport.calls.Load() != 0 {
						t.Fatalf("unreadable file: exit=%d stdout=%q stderr=%s", code, out, stderr)
					}
				} else if code != 1 || !strings.Contains(stderr, "kustomize build ./"+heldPath) || !strings.Contains(stderr, "flux-system/b-second") {
					t.Fatalf("scoped failures: run=%d exit=%d stdout=%q stderr=%s", i, code, out, stderr)
				}
			}
		})
	}
}

func TestE2E_SourceRef_HeldErrorAcrossNamespaces(t *testing.T) {
	for _, tt := range []struct {
		name, betaURL string
		workingTree   bool
	}{
		{name: "external_source", betaURL: "git://fixture.invalid/other"},
		{name: "working_tree_source", betaURL: "git://fixture.invalid/cluster", workingTree: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			repo := testutil.GitInit(t, root)
			if _, err := repo.CreateRemote(&config.RemoteConfig{
				Name: "origin", URLs: []string{"git://fixture.invalid/cluster"},
			}); err != nil {
				t.Fatal(err)
			}
			for _, namespace := range []string{"alpha", "beta"} {
				suspend := ""
				if namespace == "beta" {
					suspend = "  suspend: true\n"
				}
				testutil.WriteFile(t, root, "flux/"+namespace+".yaml", testutil.KustomizationYAML("apps", namespace, "  interval: 10m\n"+suspend+"  path: ./broken\n  sourceRef: {kind: GitRepository, name: cluster}\n"))
			}
			testutil.WriteFile(t, root, "flux/beta-repo.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: cluster, namespace: beta}
spec: {interval: 10m, url: '`+tt.betaURL+`'}
`)
			testutil.WriteFile(t, root, "flux/sources.yaml", testutil.KustomizationYAML("z-sources", "alpha", "  interval: 10m\n  path: ./sources\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n"))
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
			testutil.GitCommitAll(t, repo, "init", &gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}})
			head, err := repo.Head()
			if err != nil {
				t.Fatal(err)
			}
			if err := repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewTagReferenceName("v1.0.0"), head.Hash())); err != nil {
				t.Fatal(err)
			}
			harness.MutateFile(t, filepath.Join(root, "broken/cm.yaml"), "value: pinned", "value: newer")
			testutil.GitCommitAll(t, repo, "init", &gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}})
			testutil.WriteFile(t, root, "broken/cm.yaml", "metadata: {name: broken\n")
			l := loader.New(store.New())
			l.SourceRoot = root
			if _, err := l.Load(t.Context(), filepath.Join(root, "broken")); err != nil {
				t.Fatal(err)
			}
			heldError := l.Index.Errors["broken/cm.yaml"]
			if !errors.Is(heldError, manifest.ErrInput) {
				t.Fatalf("expected working-tree decode error, got %v", heldError)
			}
			transport := installSourceRefTransport(t, repo.Storer)
			var previous string
			for _, concurrency := range []string{"2", "4"} {
				t.Run("concurrency_"+concurrency, func(t *testing.T) {
					out, stderr, code := harness.RunCLIBuffers("build", "ks", "apps", "-n", "alpha",
						"--path", filepath.Join(root, "flux"), "--concurrency", concurrency, "--cache-dir", t.TempDir())
					if code != 0 || !strings.Contains(out, "value: pinned") || strings.Contains(out, "value: newer") ||
						strings.Contains(stderr, heldError.Error()) || transport.calls.Load() != 0 {
						t.Fatalf("suspended consumer: exit=%d transport=%d stdout=%q\nstderr:\n%s",
							code, transport.calls.Load(), out, stderr)
					}
					if concurrency == "4" && out != previous {
						t.Fatalf("output differs across concurrency levels (-want +got):\n%s", cmp.Diff(previous, out))
					}
					previous = out
				})
			}
			st := store.New()
			res, discoveryError := discovery.Run(t.Context(), discovery.Config{
				Path: filepath.Join(root, "flux"), Store: st,
			})
			for _, namespace := range []string{"alpha", "beta"} {
				ks, ok := st.Get[*manifest.Kustomization](testutil.KustomizationID(namespace, "apps"))
				if !ok || ks.SourceRef.Namespace != "" || ks.Path != "./broken" {
					t.Fatalf("expected implicit source in consumer namespace, got %+v", ks)
				}
				for ref := range manifest.References(ks) {
					if ref.Use == "source" {
						assert.Equal(t, ref.Namespace, namespace)
					}
				}
			}
			art, ok := st.GetArtifact(manifest.NamedResource{Group: "source.toolkit.fluxcd.io",
				Kind: manifest.KindGitRepository, Namespace: "alpha", Name: "cluster",
			}).(*store.SourceArtifact)
			if !ok || art.LocalRoot != root || art.LocalPath == root || art.Key == "" || art.FS == nil {
				t.Fatalf("expected alpha's committed source artifact, got %+v", art)
			}
			pinned, err := fs.ReadFile(art.FS, "broken/cm.yaml")
			if err != nil || !strings.Contains(string(pinned), "value: pinned") {
				t.Fatalf("alpha lost committed bytes: %q, %v", pinned, err)
			}
			if tt.workingTree {
				betaArtifact, ok := st.GetArtifact(manifest.NamedResource{Group: "source.toolkit.fluxcd.io",
					Kind: manifest.KindGitRepository, Namespace: "beta", Name: "cluster",
				}).(*store.SourceArtifact)
				if !ok || betaArtifact.LocalRoot != root || betaArtifact.LocalPath == root || betaArtifact.LocalPath == art.LocalPath || betaArtifact.Key != "" || betaArtifact.FS == nil {
					t.Fatalf("expected beta's working-tree artifact, got %+v", betaArtifact)
				}
				current, err := fs.ReadFile(betaArtifact.FS, "broken/cm.yaml")
				if err != nil || string(current) != "metadata: {name: broken\n" {
					t.Fatalf("beta lost malformed working-tree bytes: %q, %v", current, err)
				}
				if discoveryError != nil || !errors.Is(res.Index.Errors["broken/cm.yaml"], manifest.ErrInput) {
					t.Fatalf("expected retained working-tree error, got %v", discoveryError)
				}
			} else if discoveryError != nil {
				t.Fatalf("expected alpha's error to be discarded, got %v", discoveryError)
			}
		})
	}
}

func TestE2E_SourceRef_HeldErrorRetainedByEmptyPath(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	repo := testutil.GitInit(t, root)
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
		testutil.WriteFile(t, root, "flux/"+file, testutil.KustomizationYAML(name, "flux-system", "  interval: 10m\n"+pathField+"  sourceRef: {kind: GitRepository, name: "+sourceName+", namespace: flux-system}\n"))
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
	testutil.GitCommitAll(t, repo, "init", &gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}})
	head, err := repo.Head()
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewTagReferenceName("v1.0.0"), head.Hash())); err != nil {
		t.Fatal(err)
	}
	harness.MutateFile(t, filepath.Join(root, "apps/cm.yaml"), "value: pinned", "value: newer")
	testutil.GitCommitAll(t, repo, "init", &gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}})
	testutil.WriteFile(t, root, "apps/cm.yaml", "metadata: {name: broken\n")
	l := loader.New(store.New())
	l.SourceRoot = root
	if _, err := l.Load(t.Context(), root); err != nil {
		t.Fatal(err)
	}
	wantError := l.Index.Errors["apps/cm.yaml"]
	if !errors.Is(wantError, manifest.ErrInput) {
		t.Fatalf("expected a working-tree decode error, got %v", wantError)
	}
	res, err := discovery.Run(t.Context(), discovery.Config{
		Path: filepath.Join(root, "flux"), Store: store.New(),
	})
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, res.Index.Errors["apps/cm.yaml"].Error(), wantError.Error())
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
			repo := testutil.GitInit(t, root)
			if _, err := repo.CreateRemote(&config.RemoteConfig{
				Name: "origin", URLs: []string{"git://fixture.invalid/cluster"},
			}); err != nil {
				t.Fatal(err)
			}
			for _, ks := range []struct{ name, path string }{
				{"probe", "ok"}, {"entry", "entries"}, {"stage", "stage"},
			} {
				testutil.WriteFile(t, root, "flux/"+ks.name+".yaml", testutil.KustomizationYAML(ks.name, "flux-system", "  interval: 10m\n  path: ./"+ks.path+"\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n"))
			}
			testutil.WriteFile(t, root, "entries/kustomization.yaml", "resources: [nested/child.yaml]\n")
			testutil.WriteFile(t, root, "entries/nested/kustomization.yaml", "resources: [child.yaml]\n")
			testutil.WriteFile(t, root, "entries/nested/child.yaml", testutil.KustomizationYAML("a-held", "", "  interval: 10m\n  suspend: true\n  path: ./held\n  sourceRef: {kind: GitRepository, name: unknown"+tt.namespace+"}\n"))
			testutil.WriteFile(t, root, "stage/kustomization.yaml", "resources: [sources.yaml]\n")
			sourceTargetNamespace := ""
			if tt.namespaceFreeSource {
				sourceTargetNamespace = "  targetNamespace: beta\n"
				if tt.pinned {
					sourceTargetNamespace = "  targetNamespace: shifted\n"
				}
			}
			testutil.WriteFile(t, root, "stage/sources.yaml", testutil.KustomizationYAML("z-sources", "flux-system", "  interval: 10m\n  path: ./sources\n")+sourceTargetNamespace+`  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`)
			testutil.WriteFile(t, root, "sources/kustomization.yaml", "resources: [claim.yaml, repo.yaml]\n")
			testutil.WriteFile(t, root, "sources/claim.yaml", testutil.KustomizationYAML("claim", "flux-system", "  interval: 10m\n  path: ./entries/nested\n  targetNamespace: shifted\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n"))
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
			testutil.GitCommitAll(t, repo, "init", &gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}})
			head, err := repo.Head()
			if err != nil {
				t.Fatal(err)
			}
			if tt.pinned {
				testutil.WriteFile(t, root, "extra.txt", "second commit\n")
				testutil.GitCommitAll(t, repo, "init", &gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}})
			}
			if !tt.external {
				harness.MutateFile(t, filepath.Join(root, "sources/repo.yaml"), "{branch: master}", "{commit: "+head.Hash().String()+"}")
			}
			testutil.WriteFile(t, root, "held/broken.yaml", "metadata: {name: held\n")
			l := loader.New(store.New())
			l.SourceRoot = root
			if _, err := l.Load(t.Context(), filepath.Join(root, "held")); err != nil {
				t.Fatal(err)
			}
			heldError := l.Index.Errors["held/broken.yaml"]
			if !errors.Is(heldError, manifest.ErrInput) {
				t.Fatalf("expected held consumer's decode error, got %v", heldError)
			}
			transport := installSourceRefTransport(t, repo.Storer)
			out, stderr, code := harness.RunCLIBuffers("build", "ks", "probe", "-n", "flux-system",
				"--path", filepath.Join(root, "flux"), "--concurrency", "2", "--cache-dir", t.TempDir())
			aliases := 1
			if tt.pinned || tt.external {
				aliases = 0
			}
			persistence := strings.Count(stderr, "[RenderPersistenceDisabled]")
			if code != 0 || persistence > 1 ||
				strings.Count(stderr, "[BootstrapAliases]") != aliases || strings.Count(stderr, "warnings (") != aliases+persistence ||
				strings.Contains(stderr, "held/broken.yaml") || transport.calls.Load() != 0 || !strings.Contains(out, "name: ok") {
				t.Fatalf("re-keyed held consumer: exit=%d transport=%d stdout=%q\nstderr (-want +got):\n%s",
					code, transport.calls.Load(), out, cmp.Diff("", stderr))
			}
			st := store.New()
			res, discoveryError := discovery.Run(t.Context(), discovery.Config{
				Path: filepath.Join(root, "flux"), Store: st,
			})
			id := testutil.KustomizationID("shifted", "a-held")
			ks, ok := st.Get[*manifest.Kustomization](id)
			if !ok || ks.Path != "./held" || ks.SourceRef.Name != "unknown" {
				t.Fatalf("expected surviving consumer with inherited namespace and unchanged path/source, got %+v", ks)
			}
			var sourceRef manifest.NamedResource
			for ref := range manifest.References(ks) {
				if ref.Use == "source" {
					sourceRef = ref.NamedResource
					break
				}
			}
			assert.Equal(t, sourceRef.Namespace, tt.wantNamespace)
			if tt.pinned {
				art, ok := st.GetArtifact(sourceRef).(*store.SourceArtifact)
				if discoveryError != nil || !ok || art.LocalRoot != root || art.LocalPath == root {
					t.Fatalf("expected genuine inherited source pin without a held error: error=%v artifact=%+v", discoveryError, art)
				}
			} else if discoveryError != nil || !errors.Is(res.Index.Errors["held/broken.yaml"], manifest.ErrInput) {
				t.Fatalf("expected discovery to retain the decode error, got %v", discoveryError)
			}
		})
	}
}

func TestE2E_SourceRef_PinnedTreeReplacesHeldConsumer(t *testing.T) {
	for _, tt := range []struct {
		name, held, want, heldSource               string
		survivorName, survivorPath, survivorSource string
	}{
		{
			name: "removed",
			held: "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: restored, namespace: apps}\ndata: {value: tagged-consumer}\n",
			want: "value: tagged-consumer",
		},
		{
			name: "replaced",
			held: testutil.KustomizationYAML("a-held", "flux-system", "  interval: 10m\n  path: ./ok\n  sourceRef: {kind: GitRepository, name: flux-system}\n"),
			want: "value: ok-consumer",
		},
		{
			name: "different_name", heldSource: "unknown",
			survivorName: "b-survivor", survivorPath: "./apps/broken", survivorSource: "unknown",
			held: testutil.KustomizationYAML("b-survivor", "flux-system", "  interval: 10m\n  suspend: true\n  path: ./apps/broken\n  sourceRef: {kind: GitRepository, name: unknown}\n"),
			want: "value: tagged",
		},
		{
			name: "different_path", heldSource: "unknown",
			survivorName: "a-held", survivorPath: "./ok", survivorSource: "unknown",
			held: testutil.KustomizationYAML("a-held", "flux-system", "  interval: 10m\n  suspend: true\n  path: ./ok\n  sourceRef: {kind: GitRepository, name: unknown}\n"),
			want: "value: tagged",
		},
		{
			name: "different_source_ref", heldSource: "unknown",
			survivorName: "a-held", survivorPath: "./apps/broken", survivorSource: "flux-system",
			held: testutil.KustomizationYAML("a-held", "flux-system", "  interval: 10m\n  suspend: true\n  path: ./apps/broken\n  sourceRef: {kind: GitRepository, name: flux-system}\n"),
			want: "value: tagged",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			repo := testutil.GitInit(t, root)
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
				testutil.WriteFile(t, root, "flux/"+ks.name+".yaml", testutil.KustomizationYAML(ks.name, "flux-system", "  interval: 10m\n  path: ./"+ks.path+"\n  sourceRef: {kind: GitRepository, name: "+ks.source+"}\n"))
			}
			harness.MutateFile(t, filepath.Join(root, "flux/a-entry.yaml"), "interval: 10m", "interval: 10m\n  suspend: true")
			testutil.WriteFile(t, root, "stage/kustomization.yaml", "resources: [ks.yaml]\n")
			testutil.WriteFile(t, root, "stage/ks.yaml", testutil.KustomizationYAML("z-sources", "flux-system", "  interval: 10m\n  path: ./sources\n  sourceRef: {kind: GitRepository, name: flux-system}\n"))
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
			testutil.GitCommitAll(t, repo, "init", &gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}})
			head, err := repo.Head()
			if err != nil {
				t.Fatal(err)
			}
			if err := repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewTagReferenceName("v1.0.0"), head.Hash())); err != nil {
				t.Fatal(err)
			}
			harness.MutateFile(t, filepath.Join(root, "apps/cm.yaml"), "value: tagged", "value: newer")
			testutil.GitCommitAll(t, repo, "init", &gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}})
			heldSource := stdcmp.Or(tt.heldSource, "pinned")
			testutil.WriteFile(t, root, "apps/broken/held.yaml", testutil.KustomizationYAML("a-held", "flux-system", "  interval: 10m\n  path: ./apps/broken\n  sourceRef: {kind: GitRepository, name: "+heldSource+"}\n"))
			testutil.WriteFile(t, root, "apps/broken/extra.yaml", "metadata: {name: extra\n")
			harness.MutateFile(t, filepath.Join(root, "apps/cm.yaml"), "value: newer", "value: dirty")
			l := loader.New(store.New())
			l.SourceRoot = root
			if _, err := l.Load(t.Context(), filepath.Join(root, "apps/broken")); err != nil {
				t.Fatal(err)
			}
			heldError := l.Index.Errors["apps/broken/extra.yaml"]
			if !errors.Is(heldError, manifest.ErrInput) {
				t.Fatalf("expected held consumer's decode error, got %v", heldError)
			}
			transport := installSourceRefTransport(t, repo.Storer)
			for _, concurrency := range []string{"2", "8"} {
				out, stderr, code := harness.RunCLIBuffers("build", "all", "--path", filepath.Join(root, "flux"),
					"--concurrency", concurrency, "--cache-dir", t.TempDir())
				if code != 0 || strings.Contains(stderr, heldError.Error()) ||
					!strings.Contains(out, "value: tagged") || !strings.Contains(out, tt.want) ||
					strings.Contains(out, "value: dirty") || strings.Contains(out, "value: newer") || transport.calls.Load() != 0 {
					t.Fatalf("discarded held consumer: concurrency=%s exit=%d transport=%d\n%s\nstderr:\n%s",
						concurrency, code, transport.calls.Load(), out, stderr)
				}
			}
			if tt.survivorName != "" {
				st := store.New()
				res, err := discovery.Run(t.Context(), discovery.Config{
					Path: filepath.Join(root, "flux"), Store: st,
				})
				if err != nil || !errors.Is(res.Index.Errors["apps/broken/extra.yaml"], manifest.ErrInput) {
					t.Fatalf("expected retained working-tree diagnostic, got %v", err)
				}
				id := testutil.KustomizationID("flux-system", tt.survivorName)
				ks, ok := st.Get[*manifest.Kustomization](id)
				if !ok || ks.Path != tt.survivorPath || ks.SourceRef.Name != tt.survivorSource || ks.SourceRef.Namespace != "" {
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
			repo := testutil.GitInit(t, root)
			if _, err := repo.CreateRemote(&config.RemoteConfig{
				Name: "origin", URLs: []string{"git://fixture.invalid/cluster"},
			}); err != nil {
				t.Fatal(err)
			}
			testutil.WriteFile(t, root, "flux/ks.yaml", testutil.KustomizationYAML("meta", "flux-system", "  interval: 10m\n  path: ./clusters\n  sourceRef: {kind: GitRepository, name: bootstrap, namespace: flux-system}\n"))
			if tt.sourceDir == "sub" {
				testutil.WriteFile(t, root, "clusters/kustomization.yaml", "namespace: flux-system\nresources: [ks2.yaml]\n")
				testutil.WriteFile(t, root, "clusters/ks2.yaml", testutil.KustomizationYAML("sub", "", "  interval: 10m\n  path: ./sub\n  sourceRef: {kind: GitRepository, name: bootstrap, namespace: flux-system}\n"))
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
			testutil.WriteFile(t, root, tt.sourceDir+"/apps.yaml", testutil.KustomizationYAML("apps", "", "  interval: 10m\n  path: ./apps\n  sourceRef: {kind: GitRepository, name: cluster"+tt.namespace+"}\n"))
			testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [cm.yaml]\n")
			for _, version := range []string{"v1.0.0", "v2.0.0"} {
				testutil.WriteFile(t, root, "apps/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello, namespace: apps}\ndata:\n  value: "+version+"\n")
				testutil.GitCommitAll(t, repo, "init", &gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}})
				head, err := repo.Head()
				if err != nil {
					t.Fatal(err)
				}
				if err := repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewTagReferenceName(version), head.Hash())); err != nil {
					t.Fatal(err)
				}
			}
			harness.MutateFile(t, filepath.Join(root, "apps/cm.yaml"), "value: v2.0.0", "value: dirty")
			transport := installSourceRefTransport(t, repo.Storer)
			for _, concurrency := range []string{"1", "2", "8"} {
				out, stderr, code := harness.RunCLIBuffers("build", "all", "--path", filepath.Join(root, "flux"),
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
		harness.MutateFile(t, filepath.Join(root, "flux/entry.yaml"), "path: ./apps", "path: ./")
		testutil.WriteFile(t, root, "kustomization.yaml", "resources: [apps, flux/entry.yaml]\n")
		testutil.GitCommitAll(t, repo, "init", &gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}})
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
	harness.MutateFile(t, filepath.Join(root, "apps/cm.yaml"), "value: v2.0.0", "value: dirty")
	transport := installSourceRefTransport(t, repo.Storer)
	for _, scope := range []string{"flux", "."} {
		t.Run(scope, func(t *testing.T) {
			out, _ := harness.RequireCLIOK(t, "build", "all", "--path", filepath.Join(root, scope),
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
	testutil.WriteFile(t, current, "apps/added-ks.yaml", testutil.KustomizationYAML("added-ks", "flux-system", `  path: ./apps/phantom
  sourceRef: {kind: GitRepository, name: working, namespace: flux-system}
`)+"---\n"+
		`apiVersion: source.toolkit.fluxcd.io/v1
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
	harness.MutateFile(t, filepath.Join(current, "apps/kustomization.yaml"), "- cm.yaml\n", "- cm.yaml\n- added-ks.yaml\n- added-rs.yaml\n")
	for _, scope := range []string{"flux", "."} {
		t.Run(scope, func(t *testing.T) {
			path := filepath.Join(current, scope)
			out, _ := harness.RequireCLIOK(t, "build", "all", "--path", path,
				"--concurrency", "2", "--cache-dir", t.TempDir())
			if !strings.Contains(out, "value: v1.0.0") || strings.Contains(out, "phantom") || strings.Contains(out, "added-") {
				t.Fatalf("working-tree discovery objects escaped the pin:\n%s", out)
			}
			for _, paths := range [][2]string{{current, original}, {original, current}} {
				out, _ := harness.RequireCLIOK(t, "diff", "all", "--path", filepath.Join(paths[0], scope),
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
	testutil.WriteFile(t, root, "apps/child.yaml", testutil.KustomizationYAML("child", "flux-system", "  path: ./leaf\n  sourceRef: {kind: GitRepository, name: cluster, namespace: flux-system}\n"))
	harness.MutateFile(t, filepath.Join(root, "apps/kustomization.yaml"), "- cm.yaml\n", "- cm.yaml\n- child.yaml\n")
	testutil.WriteFile(t, root, "leaf/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, root, "leaf/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: child-output, namespace: apps}\ndata: {value: pinned-child}\n")
	testutil.GitCommitAll(t, repo, "init", &gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}})
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
	testutil.GitCommitAll(t, repo, "init", &gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}})
	out, _ := harness.RequireCLIOK(t, "build", "all", "--path", filepath.Join(root, "flux"),
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
			testutil.GitCommitAll(t, repo, "init", &gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}})
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
	harness.MutateFile(t, filepath.Join(root, "apps/release.yaml"), "greeting: pinned", "greeting: dirty")
	release, err := os.ReadFile(filepath.Join(root, "apps/release.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	testutil.WriteFile(t, root, "apps/added.yaml", strings.Replace(string(release), "name: existing", "name: added", 1))
	harness.MutateFile(t, filepath.Join(root, "apps/kustomization.yaml"), "- release.yaml\n", "- release.yaml\n- added.yaml\n")
	out, _ := harness.RequireCLIOK(t, "build", "all", "--path", root,
		"--concurrency", "2", "--cache-dir", t.TempDir(), "-o", "json")
	if loader.calls.Load() != 0 {
		t.Fatalf("local pin accessed transport %d times", loader.calls.Load())
	}
	var docs []struct {
		APIVersion string
		Kind       string
		Metadata   struct{ Name, Namespace string }
		Data       map[string]string
		Spec       struct{ Values map[string]string }
	}
	if err := json.Unmarshal([]byte(out), &docs); err != nil {
		t.Fatal(err)
	}
	ids := make(map[manifest.NamedResource]int)
	for _, doc := range docs {
		group, _, hasGroup := strings.Cut(doc.APIVersion, "/")
		if !hasGroup {
			group = ""
		}
		ids[manifest.NamedResource{Group: group, Kind: doc.Kind, Namespace: doc.Metadata.Namespace, Name: doc.Metadata.Name}]++
		if doc.Kind == manifest.KindHelmRelease && doc.Spec.Values["greeting"] != "pinned" ||
			doc.Kind == manifest.KindConfigMap && doc.Metadata.Name == "existing-cm" && doc.Data["greeting"] != "pinned" ||
			doc.Metadata.Name == "hello" && doc.Data["value"] != "v1.0.0" {
			t.Fatalf("expected pinned content:\n%s", out)
		}
	}
	want := map[manifest.NamedResource]int{
		{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "hello"}:       1,
		testutil.HelmReleaseID("apps", "existing"):                             1,
		{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "existing-cm"}: 1,
	}
	if diff := cmp.Diff(want, ids); diff != "" {
		t.Fatalf("pinned object ids (-want +got):\n%s\n%s", diff, out)
	}
}

func sourceRefFixture(t *testing.T) (string, *gogit.Repository, plumbing.Hash, plumbing.Hash) {
	t.Helper()
	root := t.TempDir()
	repo := testutil.GitInit(t, root)
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
`+"---\n"+
		testutil.KustomizationYAML("apps", "flux-system", `  interval: 10m
  path: ./apps
  sourceRef: {kind: GitRepository, name: cluster, namespace: flux-system}
`))
	testutil.WriteFile(t, root, "ignored.txt", "ignored by the source artifact\n")
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources:\n- cm.yaml\n")
	var hashes [2]plumbing.Hash
	for i, version := range []string{"v1.0.0", "v2.0.0"} {
		testutil.WriteFile(t, root, "apps/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: hello, namespace: apps}
data:
  value: `+version+"\n")
		testutil.GitCommitAll(t, repo, "init", &gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}})
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
			harness.MutateFile(t, filepath.Join(root, "apps/cm.yaml"), "value: v2.0.0", "value: dirty")
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
			out, _ := harness.RequireCLIOK(t, "build", "all", "--path", filepath.Join(root, "flux"),
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
	for _, concurrency := range []string{"2", "4"} {
		t.Run("concurrency_"+concurrency, func(t *testing.T) {
			out, stderr := harness.RequireCLIOK(t, "diff", "all", "--path", filepath.Join(root, "flux"), "--base", "HEAD",
				"--concurrency", concurrency, "--cache-dir", t.TempDir(), "--git-depth", "0", "--log-level", "warn", "-o", "diff")
			assertSourceValueDiff(t, out, "v1.0.0", "v2.0.0")
			if loader.calls.Load() != 0 || strings.Contains(stderr, "declared source ref cannot be satisfied locally") {
				t.Fatalf("locally available side refs warned/fetched: calls=%d\n%s", loader.calls.Load(), stderr)
			}
		})
	}
}

func TestE2E_PinnedDiff_StablePinIgnoresDirtyContent(t *testing.T) {
	root, repo, _, _ := sourceRefFixture(t)
	loader := installSourceRefTransport(t, repo.Storer)
	harness.MutateFile(t, filepath.Join(root, "apps/cm.yaml"), "value: v2.0.0", "value: dirty")
	local, _ := harness.RequireCLIOK(t, "build", "all", "--path", filepath.Join(root, "flux"),
		"--concurrency", "2", "--cache-dir", t.TempDir())
	if loader.calls.Load() != 0 {
		t.Fatalf("local pin accessed transport %d times", loader.calls.Load())
	}
	exported := t.TempDir()
	testutil.CopyTree(t, root, exported)
	if err := os.RemoveAll(filepath.Join(exported, ".git")); err != nil {
		t.Fatal(err)
	}
	fetched, _ := harness.RequireCLIOK(t, "build", "all", "--path", exported, "--concurrency", "2", "--cache-dir", t.TempDir(), "--git-depth", "0")
	if fetched != local || loader.calls.Load() == 0 {
		t.Fatalf("local/fetched pin bytes differ or no fetch occurred:\nlocal:\n%s\nfetched:\n%s", local, fetched)
	}
	for _, concurrency := range []string{"2", "4"} {
		t.Run("concurrency_"+concurrency, func(t *testing.T) {
			before := loader.calls.Load()
			out, stderr := harness.RequireCLIOK(t, "diff", "all", "--path", filepath.Join(root, "flux"), "--base", "HEAD",
				"--concurrency", concurrency, "--cache-dir", t.TempDir(), "--git-depth", "0", "--log-level", "warn", "-o", "diff")
			if out != "" || loader.calls.Load() != before || strings.Contains(stderr, "declared source ref cannot be satisfied locally") {
				t.Fatalf("stable local pin changed, warned or fetched with dirty working files: calls=%d\n%s\nstderr:\n%s", loader.calls.Load()-before, out, stderr)
			}
		})
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
		testutil.WriteFile(t, current, "flux/entry.yaml", testutil.KustomizationYAML("apps", "flux-system", "  interval: 10m\n  path: ./\n  sourceRef: {kind: GitRepository, name: cluster, namespace: flux-system}\n"))
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
		testutil.GitCommitAll(t, repo, "init", &gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}})
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
	original := t.TempDir()
	testutil.CopyTree(t, current, original)
	transport := installSourceRefTransport(t, repo.Storer)
	harness.MutateFile(t, filepath.Join(current, "apps/cm.yaml"), "value: v2.0.0", "value: dirty")
	for _, stage := range []string{"dirty-only", "tag-change"} {
		t.Run(stage, func(t *testing.T) {
			if stage == "tag-change" {
				harness.MutateFile(t, filepath.Join(current, "flux/repo.yaml"), "tag: v1.0.0", "tag: v2.0.0")
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
							out, _ := harness.RequireCLIOK(t, append(args, "-o", "diff")...)
							if stage == "dirty-only" {
								if out != "" {
									t.Fatalf("dirty content changed the root-pin diff:\n%s", out)
								}
							} else {
								assertSourceValueDiff(t, out, side.before, side.after)
							}
							args[0] = "build"
							out, _ = harness.RequireCLIOK(t, args...)
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
			harness.MutateFile(t, filepath.Join(root, "apps/cm.yaml"), "name: hello, namespace: apps", "name: hello")
			testutil.WriteFile(t, root, "apps/kustomization.yaml", "namespace: apps\nresources:\n- cm.yaml\n")
			if hash == older {
				testutil.GitCommitAll(t, repo, "init", &gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}})
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
`+"---\n"+
			testutil.KustomizationYAML("consumer", "apps", `  interval: 10m
  path: ./consumer
  sourceRef: {kind: GitRepository, name: working, namespace: flux-system}
  postBuild:
    substitute: {mode: `+mode+`}
    substituteFrom:
      - kind: ConfigMap
        name: hello
`))
		testutil.WriteFile(t, root, "consumer/kustomization.yaml", "resources:\n- cm.yaml\n")
		testutil.WriteFile(t, root, "consumer/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: consumer, namespace: apps}
data:
  value: ${value}
  mode: ${mode}
`)
		out, _ := harness.RequireCLIOK(t, "build", "all", "--path", filepath.Join(root, "flux"),
			"--concurrency", "2", "--cache-dir", t.TempDir())
		if strings.Count(out, "value: v1.0.0") != 2 || !strings.Contains(out, "mode: "+mode) {
			t.Fatalf("expected pinned substitution in full render:\n%s", out)
		}
	}
	out, _ := harness.RequireCLIOK(t, "diff", "all", "--path", filepath.Join(roots[1], "flux"),
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
				out, _ := harness.RequireCLIOK(t, "build", "all", "--path", filepath.Join(side.root, "flux"),
					"--concurrency", "2", "--cache-dir", t.TempDir())
				if !strings.Contains(out, "value: "+side.value) {
					t.Fatalf("expected %s from %s:\n%s", side.value, side.root, out)
				}
			}
			out, _ := harness.RequireCLIOK(t, "diff", "all", "--path", filepath.Join(current, "flux"), "--path-orig", filepath.Join(original, "flux"),
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
