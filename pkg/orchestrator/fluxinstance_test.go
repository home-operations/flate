package orchestrator

import (
	"context"
	"encoding/json"
	"io/fs"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gitlib "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
)

func TestRun_EntrySourceReplacement(t *testing.T) {
	for _, field := range []string{"url", "ref", "ignore"} {
		t.Run(field, func(t *testing.T) {
			root, remote := t.TempDir(), t.TempDir()
			repo := testutil.GitInit(t, root)
			assert.NoError(t, repo.Storer.SetReference(plumbing.NewSymbolicReference(plumbing.HEAD, plumbing.NewBranchReferenceName("main"))))
			const url = "https://example.test/entry-source"
			op := map[string]any{"op": "replace", "path": "/spec/url", "value": "file://" + remote}
			want := "remote"
			switch field {
			case "ref":
				op["path"], op["value"] = "/spec/ref/name", "refs/tags/pinned"
				want = "pinned"
			case "ignore":
				op["op"], op["path"], op["value"] = "add", "/spec/ignore", "entry/omitted.yaml"
				want = "current"
			}
			patch, err := json.Marshal([]any{op})
			assert.NoError(t, err)
			declaration := "apiVersion: fluxcd.controlplane.io/v1\nkind: FluxInstance\nmetadata: {name: flux, namespace: apps}\nspec:\n  sync: {name: self, kind: GitRepository, url: " + url + ", ref: refs/heads/main, path: ./entry}\n  kustomize:\n    patches:\n    - target: {kind: GitRepository}\n      patch: '" + string(patch) + "'\n"
			testutil.WriteFile(t, root, "entry/instance.yaml", declaration)
			testutil.WriteFile(t, root, "entry/kustomization.yaml", "resources: [instance.yaml, cm.yaml]\n")
			testutil.WriteFile(t, root, "entry/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: workload, namespace: apps}\ndata: {value: pinned}\n")
			testutil.WriteFile(t, root, "entry/omitted.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: omitted, namespace: apps}\n")
			options := &gitlib.CommitOptions{Author: &object.Signature{Name: "test", Email: "test@example.test", When: time.Unix(0, 0)}}
			pin := testutil.GitCommitAll(t, repo, "pinned", options)
			assert.NoError(t, repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewTagReferenceName("pinned"), pin)))
			testutil.WriteFile(t, root, "entry/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: workload, namespace: apps}\ndata: {value: current}\n")
			testutil.GitCommitAll(t, repo, "current", &gitlib.CommitOptions{Author: options.Author})
			remoteRepo := testutil.GitInit(t, remote)
			assert.NoError(t, remoteRepo.Storer.SetReference(plumbing.NewSymbolicReference(plumbing.HEAD, plumbing.NewBranchReferenceName("main"))))
			testutil.WriteFile(t, remote, "entry/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(t, remote, "entry/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: workload, namespace: apps}\ndata: {value: remote}\n")
			testutil.GitCommitAll(t, remoteRepo, "remote", &gitlib.CommitOptions{Author: options.Author})
			o, err := newTestOrchestrator(t, Tree{Path: filepath.Join(root, "entry"), RepoRoot: root, SelfURLs: []string{url}}, Config{Concurrency: 2})
			assert.NoError(t, err)
			t.Cleanup(o.stop)
			assert.NoError(t, o.bootstrap(t.Context()))
			source := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "self"}
			before := o.store.GetArtifact(source, store.ArtifactContent)
			result, err := o.collectResult(o.run(t.Context()))
			assert.NoError(t, err)
			artifact, ok := o.store.GetArtifact(source, store.ArtifactContent).(*store.SourceArtifact)
			if !ok || before == artifact {
				t.Fatalf("source patch reused stale bootstrap material: %+v", artifact)
			}
			data, err := fs.ReadFile(artifact.FS, "entry/cm.yaml")
			assert.NoError(t, err)
			if !strings.Contains(string(data), "value: "+want) {
				t.Fatalf("wrong source-side content: %s", data)
			}
			if field == "ignore" {
				_, err := fs.Stat(artifact.FS, "entry/omitted.yaml")
				if err == nil {
					t.Fatal("source ignore retained excluded input")
				}
			}
			rootID := testutil.KustomizationID("apps", "self")
			if field == "ignore" {
				assert.Equal(t, o.entryBinding.Producer, rootID)
				assert.Equal(t, o.entryBinding.Source, source)
			} else {
				assert.Equal(t, o.entryBinding.Producer.ReportHidden(), true)
				assert.Equal(t, o.entryBinding.Source, manifest.BootstrapSourceID)
			}
			assert.Equal(t, o.entryBinding.Path, "entry")
			found := false
			for _, doc := range outputByParent(result.Manifests)[rootID] {
				if doc["kind"] == "ConfigMap" && manifest.DocIdentity(doc).Name == "workload" {
					found = true
					assert.Equal(t, doc["data"].(map[string]any)["value"], any(want))
				}
			}
			assert.Equal(t, found, true)
		})
	}
}

func TestRender_FluxInstanceEntry(t *testing.T) {
	for _, mode := range []string{"self declaration", "unavailable named branch", "patched rename", "patched coordinates"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			repo := testutil.GitInit(t, root)
			assert.NoError(t, repo.Storer.SetReference(plumbing.NewSymbolicReference(plumbing.HEAD, plumbing.NewBranchReferenceName("main"))))
			const url = "https://example.test/self"
			declaration := `apiVersion: fluxcd.controlplane.io/v1
kind: FluxInstance
metadata: {name: flux, namespace: control}
spec:
  distribution: {version: 2.x}
  sync: {kind: GitRepository, url: https://example.test/self, ref: refs/heads/main, path: ./entry}
  kustomize:
    patches:
    - target: {kind: Kustomization}
      patch: |
        - op: add
          path: /spec/patches
          value:
          - target: {kind: ConfigMap}
            patch: '[{"op":"add","path":"/data","value":{"patched":"yes"}}]'
`
			if mode == "patched rename" {
				declaration += `    - target: {kind: Kustomization}
      patch: '[{"op":"replace","path":"/metadata/name","value":"final"}]'
`
			}
			if mode == "patched coordinates" {
				declaration += `    - target: {kind: Kustomization}
      patch: '[{"op":"replace","path":"/metadata/name","value":"final"},{"op":"replace","path":"/spec/path","value":"./leaf"}]'
`
			}
			if mode == "unavailable named branch" {
				declaration = strings.ReplaceAll(declaration, "refs/heads/main", "refs/heads/unavailable")
			}
			testutil.WriteFile(t, root, "entry/instance.yaml", declaration)
			testutil.WriteFile(t, root, "entry/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: workload, namespace: apps}\n")
			testutil.WriteFile(t, root, "entry/unrelated.yaml", "apiVersion: fluxcd.controlplane.io/v1\nkind: FluxInstance\nmetadata: {name: installation, namespace: other}\nspec: {distribution: {version: 2.x}}\n")
			testutil.WriteFile(t, root, "leaf/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(t, root, "leaf/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: workload, namespace: apps}\n")
			testutil.GitCommitAll(t, repo, "fixture", &gitlib.CommitOptions{Author: &object.Signature{Name: "test", Email: "test@example.test", When: time.Unix(0, 0)}})
			urls := []string{url}
			if mode == "unavailable named branch" {
				urls = nil
			}
			o, err := newTestOrchestrator(t, Tree{Path: filepath.Join(root, "entry"), RepoRoot: root, SelfURLs: urls}, Config{Concurrency: 2})
			assert.NoError(t, err)
			fetcher := &advisorySourceFetcher{root: root}
			o.src.Fetchers[manifest.KindGitRepository] = fetcher
			t.Cleanup(o.stop)
			assert.NoError(t, o.bootstrap(t.Context()))
			assert.Equal(t, o.entryBinding.Producer, manifest.NamedResource{})
			for _, obj := range o.store.ListObjects(manifest.FluxKustomizeDomain, manifest.KindKustomization) {
				if obj.Named().ReportHidden() {
					t.Fatalf("pending generated entry has synthetic sibling: %s", obj.Named())
				}
			}
			result, err := o.collectResult(o.run(t.Context()))
			assert.NoError(t, err)
			assert.Equal(t, fetcher.calls.Load(), int64(0))
			instance := manifest.NamedResource{Group: manifest.FluxOperatorDomain, Kind: "FluxInstance", Namespace: "control", Name: "flux"}
			name, path := "control", "entry"
			if mode == "patched rename" {
				name = "final"
			}
			if mode == "patched coordinates" {
				name, path = "final", "leaf"
			}
			ks := testutil.KustomizationID("control", name)
			if mode == "patched coordinates" {
				if o.entryBinding.Producer == ks {
					t.Fatal("retargeted root still applies the entry")
				}
			} else {
				assert.Equal(t, strings.TrimPrefix(o.entryBinding.Path, "./"), path)
				assert.Equal(t, o.entryBinding.Producer, ks)
			}
			assert.Equal(t, o.store.EmissionOwner(ks), instance)
			count := 0
			for _, batch := range result.Manifests {
				for _, doc := range batch.Documents {
					if manifest.DocIdentity(doc) == ks {
						count++
					}
				}
			}
			assert.Equal(t, count, 1)
			status, _ := o.store.GetStatus(ks)
			assert.Equal(t, status.Status, store.StatusReady)
			docs := outputByParent(result.Manifests)[ks]
			found := false
			for _, doc := range docs {
				if manifest.DocKind(doc) == manifest.KindConfigMap && manifest.DocIdentity(doc).Name == "workload" {
					found = true
					assert.Equal(t, doc["data"].(map[string]any)["patched"], any("yes"))
				}
			}
			if !found {
				t.Fatalf("ordinary root did not render its patched workload: %v", docs)
			}
			for _, obj := range o.store.ListObjects(manifest.FluxKustomizeDomain, manifest.KindKustomization) {
				if obj.Named().ReportHidden() {
					t.Fatalf("bound root retained synthetic sibling: %s", obj.Named())
				}
			}
		})
	}
}

func TestDAG_EntrySourceDuringOwnerRebinding(t *testing.T) {
	root := t.TempDir()
	repo := testutil.GitInit(t, root)
	assert.NoError(t, repo.Storer.SetReference(plumbing.NewSymbolicReference(plumbing.HEAD, plumbing.NewBranchReferenceName("main"))))
	testutil.WriteFile(t, root, "entry/instance.yaml", `apiVersion: fluxcd.controlplane.io/v1
kind: FluxInstance
metadata: {name: flux, namespace: control}
spec:
  distribution: {version: 2.x}
  sync: {kind: GitRepository, url: https://example.test/self, ref: refs/heads/unavailable, path: ./entry}
  kustomize:
    patches:
    - target: {kind: ConfigMap}
      patch: '[{"op":"add","path":"/data","value":{"patched":"yes"}}]'
`)
	testutil.WriteFile(t, root, "entry/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: workload, namespace: apps}\n")
	testutil.WriteFile(t, root, "entry/unrelated.yaml", "apiVersion: fluxcd.controlplane.io/v1\nkind: FluxInstance\nmetadata: {name: installation, namespace: other}\nspec: {distribution: {version: 2.x}}\n")
	testutil.GitCommitAll(t, repo, "fixture", &gitlib.CommitOptions{Author: &object.Signature{Name: "test", Email: "test@example.test", When: time.Unix(0, 0)}})
	o, err := newTestOrchestrator(t, Tree{Path: filepath.Join(root, "entry"), RepoRoot: root}, Config{Concurrency: 2})
	assert.NoError(t, err)
	defer o.stop()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	fetcher := &advisorySourceFetcher{root: root}
	o.src.Fetchers[manifest.KindGitRepository] = fetcher
	assert.NoError(t, o.bootstrap(ctx))
	o.configureControllers()
	o.src.StartLifecycle()
	o.ksc.StartLifecycle()
	o.hrc.StartLifecycle()
	o.rsc.StartLifecycle()
	o.fic.StartLifecycle()
	o.agc.StartLifecycle()
	source := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "control", Name: "control"}
	instance := manifest.NamedResource{Group: manifest.FluxOperatorDomain, Kind: "FluxInstance", Namespace: "control", Name: "flux"}
	installation := manifest.NamedResource{Group: manifest.FluxOperatorDomain, Kind: "FluxInstance", Namespace: "other", Name: "installation"}
	var committed, ordered atomic.Bool
	rootCommitted, ownerRebound := make(chan struct{}), make(chan struct{})
	rebind := sync.OnceFunc(func() { close(ownerRebound) })
	attempted := make(chan struct{})
	attempt := sync.OnceFunc(func() { close(attempted) })
	var declaration, ownerDeclaration manifest.BaseManifest
	var initialOutput atomic.Pointer[store.Emission]
	dispatcher := entryRebindingDispatcher{dagDispatcher: dagDispatcher{o}}
	dispatcher.dispatch = func(ctx context.Context, id schedule.NodeID, level manifest.DrainLevel) schedule.Result {
		if id == installation {
			o.tasks.YieldSlot(func() {
				select {
				case <-rootCommitted:
				case <-ctx.Done():
				}
			})
		}
		if id == source {
			if declaration == nil {
				declaration = o.store.GetDeclaration(source)
				ownerDeclaration = o.store.GetDeclaration(instance)
				output, _ := o.store.GetArtifact(instance, store.ArtifactContent).(*store.Emission)
				initialOutput.Store(output)
			}
			if committed.Load() && ordered.CompareAndSwap(false, true) {
				defer attempt()
				o.tasks.YieldSlot(func() {
					select {
					case <-ownerRebound:
					case <-ctx.Done():
					}
				})
				assert.Equal(t, manifest.Equal(declaration, o.store.GetDeclaration(source)), true)
				assert.Equal(t, manifest.Equal(ownerDeclaration, o.store.GetDeclaration(instance)), true)
				output, _ := o.store.GetArtifact(instance, store.ArtifactContent).(*store.Emission)
				previous := initialOutput.Load()
				if output == nil || output == previous || !output.Complete || !output.OutputReads.Valid() {
					t.Errorf("owner output did not rebind: initial=%p current=%p", previous, output)
				}
				t.Logf("source retry: drain=%v old output=%p valid=%t current output=%p", level, previous, previous.OutputReads.Valid(), output)
			}
		}
		result := dispatcher.dagDispatcher.Dispatch(ctx, id, level)
		if id == instance && committed.Load() {
			output, _ := o.store.GetArtifact(instance, store.ArtifactContent).(*store.Emission)
			if previous := initialOutput.Load(); previous != nil && output != nil && output != previous && output.OutputReads.Valid() {
				rebind()
			}
		}
		if id == source {
			t.Logf("source completed: valid=%t outcome=%v fetches=%d", result.Assessment.Valid(), result.Assessment.Outcome(), fetcher.calls.Load())
		}
		return result
	}
	defer o.store.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) {
		if id.KindInfo().Render {
			o.updateDependencyGraphFor(id)
		}
	}, false)()
	scheduler := schedule.New(dispatcher, dispatcher)
	defer o.store.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) {
		if id != (manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "workload"}) {
			return
		}
		if !committed.CompareAndSwap(false, true) {
			return
		}
		close(rootCommitted)
		select {
		case <-attempted:
		case <-ctx.Done():
			t.Error(ctx.Err())
		}
	}, false)()
	defer o.watchDAG(scheduler)()
	scheduler.Seed(o.seedNodes())
	assert.NoError(t, scheduler.Run(ctx))
	assert.Equal(t, ordered.Load(), true)
	assert.Equal(t, fetcher.calls.Load(), int64(0))
}

type entryRebindingDispatcher struct {
	dagDispatcher
	dispatch func(context.Context, schedule.NodeID, manifest.DrainLevel) schedule.Result
}

func (d entryRebindingDispatcher) Dispatch(ctx context.Context, id schedule.NodeID, level manifest.DrainLevel) schedule.Result {
	return d.dispatch(ctx, id, level)
}
