package kustomization

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/controllers/artifactgenerator"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func newControllerWithFixture(t *testing.T) (*Controller, *store.Store, string) {
	t.Helper()
	root := t.TempDir()
	testutil.WriteFileAt(t, filepath.Join(root, "apps", "kustomization.yaml"),
		"resources:\n- cm.yaml\n")
	testutil.WriteFileAt(t, filepath.Join(root, "apps", "cm.yaml"), `---
apiVersion: v1
kind: ConfigMap
metadata: {name: hello, namespace: default}
data: {greeting: hi}
`)

	s := store.New()
	bootstrap := &manifest.GitRepository{
		Name: "flux-system", Namespace: "flux-system",
		URL: "file://" + root,
	}
	s.AddObject(bootstrap)
	location, err := gitrepo.Locate(t.Context(), root, "")
	assert.NoError(t, err)
	input, err := gittree.AdmitDirectory(t.Context(), location)
	assert.NoError(t, err)
	s.SetArtifact(bootstrap.Named(), input)
	s.UpdateStatus(bootstrap.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: "ready"})

	owner := cachetest.New(t, "", sourcecache.New)
	tasks := task.NewBounded(2)
	network := new(source.Network)
	t.Cleanup(network.Close)
	inputs := source.NewHTTPInputs(owner, network, tasks)
	t.Cleanup(inputs.Close)
	cache := kustomize.NewTreeCache(owner, &inputs, tasks)
	t.Cleanup(cache.Close)

	c := New(s, tasks, cache, true)
	c.Configure(Options{}, sourcecache.RetryConfig{})
	c.StartLifecycle()
	t.Cleanup(func() {

		cache.Close()
		tasks.BlockTillDone()
	})
	return c, s, root
}

const namespaceIdentityDocs = `apiVersion: v1
kind: ConfigMap
metadata: {name: hello}
data: {greeting: hi}
---
apiVersion: source.toolkit.fluxcd.io/v1
kind: OCIRepository
metadata: {name: chart}
spec: {url: oci://example.test/chart}
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: app}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata: {name: role}
---
apiVersion: foreign.example/v1
kind: ClusterRole
metadata: {name: foreign}
`

func TestReconcile_HappyPath(t *testing.T) {
	t.Parallel()
	c, s, root := newControllerWithFixture(t)
	list := "apiVersion: v1\nkind: List\nitems:\n"
	for doc := range strings.SplitSeq(namespaceIdentityDocs, "---\n") {
		list += "- " + strings.ReplaceAll(strings.TrimSpace(doc), "\n", "\n  ") + "\n"
	}
	testutil.WriteFile(t, root, "apps/cm.yaml", list)
	ks := &manifest.Kustomization{
		Name: "apps", Namespace: "flux-system",
		Path:            "./apps",
		TargetNamespace: "rendered",
		CommonMetadata:  &kustomizev1.CommonMetadata{Labels: map[string]string{"team": "flate"}},
		SourceRef: kustomizev1.CrossNamespaceSourceReference{
			Kind: manifest.KindGitRepository, Name: "flux-system", Namespace: "flux-system",
		},
	}
	s.AddObject(ks)
	info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: s, ID: ks.Named(), ReconcileNode: c.ReconcileNode})
	if info.Status != store.StatusReady {
		t.Fatalf("status = %+v, want StatusReady", info)
	}
	if info.Message != "" {
		t.Errorf("unexpected Ready message: %q", info.Message)
	}
	art, _ := s.GetArtifact(ks.Named()).(*store.Emission)
	if art == nil {
		t.Fatal("Emission not stored")
	}
	if art.Fingerprint == "" {
		t.Error("artifact missing fingerprint")
	}
	if len(art.Manifests) == 0 {
		t.Error("artifact has no rendered manifests")
	}
	assert.Equal(t, len(art.Manifests), 5)
	for _, doc := range art.Manifests {
		name, ns := manifest.DocMetadata(doc)
		labels := doc["metadata"].(map[string]any)["labels"].(map[string]any)
		assert.Equal(t, labels["team"], any("flate"))
		assert.Equal(t, labels["kustomize.toolkit.fluxcd.io/name"], any(ks.Name))
		want := ks.TargetNamespace
		if manifest.DocKind(doc) == "ClusterRole" && manifest.DocAPIVersion(doc) == "rbac.authorization.k8s.io/v1" {
			want = ""
		}
		assert.Equal(t, ns, want)
		id := manifest.NamedResource{Group: schema.FromAPIVersionAndKind(manifest.DocAPIVersion(doc), manifest.DocKind(doc)).Group, Kind: manifest.DocKind(doc), Namespace: want, Name: name}
		if !slices.Contains(testutil.Owners(s.Productions(id, nil)), ks.Named()) {
			t.Errorf("child %s lost parent edge", id)
		}
		if s.GetObject(id) == nil {
			t.Errorf("missing effective child %s/%s", want, name)
		}
	}
}

func TestReconcile_ArtifactGeneratorPersistedDocument(t *testing.T) {
	t.Parallel()
	if reason := sourcecache.RenderPersistenceDisabledReason(); reason != "" {
		t.Skipf("render persistence disabled: %s", reason)
	}
	root := testutil.WriteTree(t, map[string]string{
		"apps/kustomization.yaml": "resources: [generator.yaml]\n",
		"apps/generator.yaml": `apiVersion: source.extensions.fluxcd.io/v1beta1
kind: ArtifactGenerator
metadata:
  name: generated
  namespace: apps
  uid: constructed-document
  labels: {purpose: retained}
  annotations: {example.test/note: rendered}
spec:
  sources: [{alias: repo, kind: GitRepository, name: input}]
  commonMetadata: {labels: {purpose: generated}}
  artifacts:
    - name: output
      revision: "@repo"
      originRevision: "@repo"
      copy: [{from: "@repo/file", to: "@artifact/file"}]
`,
	})
	storage := t.TempDir()
	var fresh map[string]any
	for _, persisted := range []bool{false, true} {
		owner, err := sourcecache.New(t.Context(), storage)
		assert.NoError(t, err)
		t.Cleanup(func() { assert.NoError(t, owner.Close()) })
		s := store.New()
		tasks := task.NewBounded(2)
		t.Cleanup(tasks.BlockTillDone)
		network := new(source.Network)
		t.Cleanup(network.Close)
		inputs := source.NewHTTPInputs(owner, network, tasks)
		t.Cleanup(inputs.Close)
		trees := kustomize.NewTreeCache(owner, &inputs, tasks)
		t.Cleanup(trees.Close)
		c := New(s, tasks, trees, true)

		c.Configure(Options{}, sourcecache.RetryConfig{})
		c.StartLifecycle()
		g := artifactgenerator.New(s, tasks, owner)

		guard := &guardedSourceFS{FS: os.DirFS(root)}
		guard.deny.Store(persisted)
		renderSource := &manifest.GitRepository{Name: "render-source", Namespace: "apps"}
		s.AddObject(renderSource)
		s.SetArtifact(renderSource.Named(), &store.SourceArtifact{FS: guard, LocalPath: root, Key: "render-pin", Digest: "sha256:render", Revision: "render-revision"})
		s.UpdateStatus(renderSource.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
		ks := &manifest.Kustomization{Name: "apps", Namespace: "apps", Path: "./apps", SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: renderSource.Name}}
		s.AddObject(ks)
		info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: tasks, Store: s, ID: ks.Named(), ReconcileNode: c.ReconcileNode})
		if info.Status != store.StatusReady {
			t.Fatalf("persisted=%t render status=%+v", persisted, info)
		}
		id := manifest.NamedResource{Group: "source.extensions.fluxcd.io", Kind: "ArtifactGenerator", Namespace: "apps", Name: "generated"}
		var reads store.ReadSet
		reads.Init(s, id, nil)
		obj, doc, attempt := reads.GetEmission(id)
		if obj == nil || doc == nil || !reads.Valid() || s.EmissionOwner(id) != ks.Named() {
			t.Fatalf("persisted=%t missing authoritative generator: %v, %v", persisted, obj, doc)
		}
		if persisted {
			assert.Diff(t, doc, fresh)
			assert.Equal(t, guard.opened.Load(), int64(0))
			t.Log("persistence enabled; reopened disk-hit witness: rendered document with source reads denied, opens=0")
		} else {
			fresh = manifest.DeepCopyMap(doc)
			metadata := doc["metadata"].(map[string]any)
			assert.Equal(t, metadata["uid"], any("constructed-document"))
			assert.Equal(t, metadata["annotations"].(map[string]any)["example.test/note"], any("rendered"))
			if guard.opened.Load() == 0 {
				t.Fatal("fresh rendering did not consume source files")
			}
		}
		attempt.Finish()
		reads.Close()
		repo := &manifest.GitRepository{Name: "input", Namespace: "apps"}
		s.AddObject(repo)
		s.SetArtifact(repo.Named(), &store.SourceArtifact{FS: fstest.MapFS{"file": {Data: []byte("generated bytes")}}, Key: "input-pin", Digest: "sha256:input", Revision: "input-revision", Metadata: map[string]string{"org.opencontainers.image.revision": "input-origin"}})
		s.UpdateStatus(repo.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
		info = controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: tasks, Store: s, ID: id, ReconcileNode: g.ReconcileNode})
		if info.Status != store.StatusReady {
			t.Fatalf("persisted=%t generator status=%+v", persisted, info)
		}
		target := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindExternalArtifact, Namespace: "apps", Name: "output"}
		payload, ok := s.GetArtifact(target).(*store.SourceArtifact)
		if !ok {
			t.Fatal("generated payload missing")
		}
		data, err := fs.ReadFile(payload.FS, "file")
		assert.NoError(t, err)
		assert.Equal(t, string(data), "generated bytes")
		assert.Equal(t, payload.Revision, "input-revision")
		assert.Equal(t, payload.Metadata["org.opencontainers.image.revision"], "input-origin")
		_, generated, output := s.GetEmission(target)
		assert.Equal(t, generated["metadata"].(map[string]any)["labels"].(map[string]any)["source.extensions.fluxcd.io/generator"], "constructed-document")
		output.Finish()
		tasks.BlockTillDone()
		trees.Close()
		inputs.Close()
		network.Close()
		assert.NoError(t, owner.Close())
	}
}

func TestReconcile_EmptyPostBuildRespectsSubstituteStrategy(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		strategy  kustomizev1.SubstituteStrategy
		postBuild map[string]any
		disabled  string
		input     string
		want      string
	}{
		{name: "no postBuild", want: "${VAR:=default}"},
		{name: "unset", postBuild: map[string]any{}, want: "${VAR:=default}"},
		{name: "WithVariables", strategy: kustomizev1.SubstituteStrategyWithVariables, postBuild: map[string]any{}, want: "${VAR:=default}"},
		{name: "Always", strategy: kustomizev1.SubstituteStrategyAlways, postBuild: map[string]any{}, want: "default"},
		{name: "empty substitute", postBuild: map[string]any{"substitute": map[string]any{}}, want: "${VAR:=default}"},
		{
			name: "missing optional ConfigMap",
			postBuild: map[string]any{"substituteFrom": []any{
				map[string]any{"kind": "ConfigMap", "name": "missing", "optional": true},
			}},
			want: "${VAR:=default}",
		},
		{
			name: "missing optional Secret",
			postBuild: map[string]any{"substituteFrom": []any{
				map[string]any{"kind": "Secret", "name": "missing", "optional": true},
			}},
			want: "${VAR:=default}",
		},
		{
			name:      "inline substitute",
			postBuild: map[string]any{"substitute": map[string]any{"VAR": "configured"}},
			want:      "configured",
		},
		{name: "disabled label", strategy: kustomizev1.SubstituteStrategyAlways, postBuild: map[string]any{}, disabled: "labels", want: "${VAR:=default}"},
		{name: "disabled annotation", strategy: kustomizev1.SubstituteStrategyAlways, postBuild: map[string]any{}, disabled: "annotations", want: "${VAR:=default}"},
		{name: "double dollar without variables", input: "echo $$PID", want: "echo $$PID"},
		{name: "double dollar Always", input: "echo $$PID", strategy: kustomizev1.SubstituteStrategyAlways, postBuild: map[string]any{}, want: "echo $PID"},
		{name: "double dollar with variables", input: "echo $$PID", postBuild: map[string]any{"substitute": map[string]any{"VAR": "configured"}}, want: "echo $PID"},
		{name: "double dollar disabled label", input: "echo $$PID", postBuild: map[string]any{"substitute": map[string]any{"VAR": "configured"}}, disabled: "labels", want: "echo $$PID"},
		{name: "double dollar disabled annotation", input: "echo $$PID", postBuild: map[string]any{"substitute": map[string]any{"VAR": "configured"}}, disabled: "annotations", want: "echo $$PID"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, s, root := newControllerWithFixture(t)
			input := tc.input
			if input == "" {
				input = "${VAR:=default}"
			}
			optOut := ""
			if tc.disabled != "" {
				optOut = fmt.Sprintf(", %s: {kustomize.toolkit.fluxcd.io/substitute: disabled}", tc.disabled)
			}
			testutil.WriteFileAt(t, filepath.Join(root, "apps", "cm.yaml"), fmt.Sprintf(`apiVersion: v1
kind: List
items:
- apiVersion: v1
  kind: ConfigMap
  metadata: {name: hello, namespace: default%s}
  data: {greeting: '%s'}
`, optOut, input))

			spec := map[string]any{
				"path":      "./apps",
				"sourceRef": map[string]any{"kind": "GitRepository", "name": "flux-system"},
			}
			if tc.postBuild != nil {
				if tc.strategy != "" {
					tc.postBuild["substituteStrategy"] = string(tc.strategy)
				}
				spec["postBuild"] = tc.postBuild
			}
			obj, _, _, err := manifest.ParseDoc(map[string]any{
				"apiVersion": "kustomize.toolkit.fluxcd.io/v1",
				"kind":       "Kustomization",
				"metadata":   map[string]any{"name": "apps", "namespace": "flux-system"},
				"spec":       spec,
			}, manifest.ParseDocOptions{DefaultNamespace: ""})
			if err != nil {
				t.Fatalf("ParseDoc: %v", err)
			}
			ks := obj.(*manifest.Kustomization)
			s.AddObject(ks)
			if info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: s, ID: ks.Named(), ReconcileNode: c.ReconcileNode}); info.Status != store.StatusReady {
				t.Fatalf("status = %+v, want StatusReady", info)
			}
			art, _ := s.GetArtifact(ks.Named()).(*store.Emission)
			if art == nil || len(art.Manifests) != 1 {
				t.Fatalf("expected one rendered manifest, got %+v", art)
			}
			data := art.Manifests[0]["data"].(map[string]any)
			if got := data["greeting"]; got != tc.want {
				t.Errorf("greeting = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestReconcile_UnresolvedWarningRequiresData(t *testing.T) {
	t.Parallel()
	c, s, _ := newControllerWithFixture(t)
	ks := &manifest.Kustomization{
		Name: "apps", Namespace: "flux-system", Path: "./apps",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: "flux-system"},
		PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{
			{Kind: manifest.KindSecret, Name: "missing-secret"},
			{Kind: manifest.KindConfigMap, Name: "missing-configmap"},
		}},
	}
	s.AddObject(ks)
	info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: s, ID: ks.Named(), ReconcileNode: c.ReconcileNode})
	assert.Equal(t, info.Status, store.StatusFailed)
	if warnings := s.Warnings(); len(warnings) != 0 {
		t.Fatalf("failed data gate published warnings: %+v", warnings)
	}
	if artifact := s.GetArtifact(ks.Named()); artifact != nil {
		t.Fatalf("failed data gate published output: %+v", artifact)
	}
}

func TestReconcile_UnresolvedWarningReplay(t *testing.T) {
	t.Parallel()
	for _, replay := range []bool{false, true} {
		name := "metadata before dispatch"
		if replay {
			name = "metadata after completion"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			c, s, _ := newControllerWithFixture(t)
			consumers := make([]*manifest.Kustomization, 2)
			for i := range consumers {
				obj, _, _, err := manifest.ParseDoc(map[string]any{
					"apiVersion": "kustomize.toolkit.fluxcd.io/v1",
					"kind":       "Kustomization",
					"metadata":   map[string]any{"name": fmt.Sprintf("apps-%d", i), "namespace": "flux-system"},
					"spec": map[string]any{
						"path":      "./apps",
						"sourceRef": map[string]any{"kind": "GitRepository", "name": "flux-system"},
						"postBuild": map[string]any{"substituteFrom": []any{
							map[string]any{"kind": "Secret", "name": "cluster-secrets"},
						}},
					},
				}, manifest.ParseDocOptions{})
				assert.NoError(t, err)
				consumers[i] = obj.(*manifest.Kustomization)
				s.AddObject(consumers[i])
			}
			first := consumers[0].Named()
			start := make(chan struct{})
			completed := make(chan struct{})
			var accepted *store.Emission
			var authority store.ReadSet
			var replayEvents atomic.Int64
			var stopReplay func()
			var renders atomic.Int64
			stopRenders := s.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
				if id == first && payload.(store.StatusInfo).Message == "rendering" {
					renders.Add(1)
				}
			}, false)
			defer stopRenders()
			for i, ks := range consumers {
				c.Tasks.Go(t.Context(), ks.Named().String(), func(ctx context.Context) {
					<-start
					if deps := c.ReconcileNode(ctx, ks.Named(), manifest.DrainNone).Blocked; len(deps) != 0 {
						t.Errorf("blocked dependencies = %v", deps)
					}
					if i == 0 {
						accepted, _ = s.GetArtifact(first).(*store.Emission)
						close(completed)
					}
				})
			}
			stamped := consumers[0].Clone()
			stamped.Labels = map[string]string{"kustomize.toolkit.fluxcd.io/name": "parent"}
			if replay {
				close(start)
				<-completed
			}
			s.AddObject(stamped)
			assert.Equal(t, s.GetObject(first), manifest.BaseManifest(stamped))
			if replay {
				authority.Init(s, manifest.NamedResource{Name: "output-reader"}, nil)
				authority.GetArtifact(first)
				if !authority.Valid() {
					t.Fatal("captured output was already invalid before metadata replay")
				}
				var stops []store.Unsubscribe
				for _, kind := range []store.EventKind{store.EventObjectAdded, store.EventStatusUpdated, store.EventArtifactUpdated} {
					stops = append(stops, s.AddListener(kind, func(id manifest.NamedResource, _ any) {
						if id == first {
							t.Logf("metadata replay event=%v", kind)
							replayEvents.Add(1)
						}
					}, false))
				}
				stopReplay = func() {
					for _, stop := range stops {
						stop()
					}
				}
			}

			if !replay {
				close(start)
			} else {
				c.Tasks.Go(t.Context(), "metadata replay", func(ctx context.Context) {
					if deps := c.ReconcileNode(ctx, first, manifest.DrainNone).Blocked; len(deps) != 0 {
						t.Errorf("replay blocked dependencies = %v", deps)
					}
				})
			}
			c.Tasks.BlockTillDone()
			assert.Equal(t, c.Tasks.Failures(), int64(0))
			if accepted == nil || accepted.Fingerprint == "" || len(accepted.Manifests) != 1 {
				t.Fatalf("first emission = %+v", accepted)
			}
			current := s.GetArtifact(first).(*store.Emission)
			if current.Fingerprint != accepted.Fingerprint {
				t.Fatal("metadata replay changed accepted fingerprint")
			}
			assert.Diff(t, current.Manifests, accepted.Manifests)
			if replay {
				stopReplay()
				if !authority.Valid() || replayEvents.Load() != 0 {
					t.Fatalf("metadata replay changed output authority: valid=%t events=%d", authority.Valid(), replayEvents.Load())
				}
				authority.Retire()
			}
			if renders.Load() != 1 {
				t.Fatalf("metadata replay rendered %d times, want 1", renders.Load())
			}

			for _, ks := range consumers {
				info, _ := s.GetStatus(ks.Named())
				assert.Equal(t, info.Status, store.StatusReady)
				other := s.GetArtifact(ks.Named()).(*store.Emission)
				if other.Fingerprint == "" || len(other.Manifests) != 1 {
					t.Fatalf("consumer emission = %+v", other)
				}
				assert.Diff(t, other.Manifests[0]["data"], map[string]any{"greeting": "hi"})
			}
			warnings := s.Warnings()
			if len(warnings) != 1 || warnings[0].Category != manifest.WarnUnresolvedSubstitution {
				t.Fatalf("warnings = %+v, want one unresolved Secret group", warnings)
			}
			assert.Equal(t, warnings[0].Resource, manifest.NamedResource{})
			assert.Equal(t, warnings[0].Count, 2)
			t.Logf("replay=%t; both consumers Ready; accepted output reused; warning=%+v", replay, warnings[0])
		})
	}
}

// TestReconcile_FingerprintDedup_SkipsRender locks the dedup
// short-circuit from PR #220: a second AddObject with byte-equal
// post-Prepare spec must not re-run kustomize.Render. We force
// the issue by AddObject'ing the same KS twice and verifying the
// artifact's identity persists (SetArtifact dedupes via DeepEqual).
func TestReconcile_FingerprintDedup_SkipsRender(t *testing.T) {
	t.Parallel()
	c, s, root := newControllerWithFixture(t)
	testutil.WriteFile(t, root, "apps/cm.yaml", namespaceIdentityDocs)
	ks := &manifest.Kustomization{
		Name: "apps", Namespace: "flux-system",
		Path: "./apps", TargetNamespace: "release",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{
			Kind: manifest.KindGitRepository, Name: "flux-system", Namespace: "flux-system",
		},
	}
	s.AddObject(ks)
	if info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: s, ID: ks.Named(), ReconcileNode: c.ReconcileNode}); info.Status != store.StatusReady {
		t.Fatalf("status = %+v, want StatusReady", info)
	}
	firstFP := s.GetArtifact(ks.Named()).(*store.Emission).Fingerprint
	art := s.GetArtifact(ks.Named()).(*store.Emission)
	before := make([]map[string]any, len(art.Manifests))
	for i, doc := range art.Manifests {
		before[i] = manifest.DeepCopyMap(doc)
	}

	// Re-AddObject a copy with kustomize ownership labels stamped, then drive a
	// second reconcile: the labels differ so the body re-runs, but the
	// fingerprint matches → second render skipped, artifact fingerprint
	// unchanged.
	stamped := ks.Clone()
	stamped.Labels = map[string]string{
		"kustomize.toolkit.fluxcd.io/name":      "parent",
		"kustomize.toolkit.fluxcd.io/namespace": "flux-system",
	}
	s.AddObject(stamped)
	c.ReconcileNode(context.Background(), ks.Named(), 0)
	secondFP := s.GetArtifact(ks.Named()).(*store.Emission).Fingerprint
	if firstFP != secondFP {
		t.Errorf("fingerprint changed across label-only re-AddObject: %q vs %q", firstFP, secondFP)
	}
	assert.Diff(t, art.Manifests, before)
	for _, doc := range art.Manifests {
		name, ns := manifest.DocMetadata(doc)
		want := ks.TargetNamespace
		if manifest.DocAPIVersion(doc) == "rbac.authorization.k8s.io/v1" {
			want = ""
		}
		assert.Equal(t, ns, want)
		if s.GetObject(manifest.NamedResource{Group: schema.FromAPIVersionAndKind(manifest.DocAPIVersion(doc), manifest.DocKind(doc)).Group, Kind: manifest.DocKind(doc), Namespace: want, Name: name}) == nil {
			t.Errorf("replayed child %s/%s missing", want, name)
		}
		if want != "" && s.GetObject(manifest.NamedResource{Group: schema.FromAPIVersionAndKind(manifest.DocAPIVersion(doc), manifest.DocKind(doc)).Group, Kind: manifest.DocKind(doc), Name: name}) != nil {
			t.Errorf("replay published duplicate bare child %s", name)
		}
	}

}

// TestReconcile_AlreadyReady_NoTransientPending pins Part A of the
// re-emission-churn fix: a re-reconcile of an already-Ready KS that
// turns out to be a no-op (fingerprint unchanged, dedup-skip) must NOT
// transiently flip the status Ready→Pending. That transient window is
// exactly what a dependent's quiescence-bound depwait can re-read at a
// transient pool drain and give up on ("parent Kustomization not
// ready"). Pre-fix the unconditional UpdateStatus(Pending) at the top
// of reconcile fired on every re-run; post-fix it is suppressed when
// the object is already Ready.
func TestReconcile_AlreadyReady_NoTransientPending(t *testing.T) {
	t.Parallel()
	c, s, _ := newControllerWithFixture(t)
	ks := &manifest.Kustomization{
		Name: "apps", Namespace: "flux-system",
		Path: "./apps",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{
			Kind: manifest.KindGitRepository, Name: "flux-system", Namespace: "flux-system",
		},
	}
	s.AddObject(ks)
	if info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: s, ID: ks.Named(), ReconcileNode: c.ReconcileNode}); info.Status != store.StatusReady {
		t.Fatalf("status = %+v, want StatusReady", info)
	}

	var mu sync.Mutex
	var sawPending bool
	s.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
		if id != ks.Named() {
			return
		}
		if info, ok := payload.(store.StatusInfo); ok && info.Status == store.StatusPending {
			mu.Lock()
			sawPending = true
			mu.Unlock()
		}
	}, false)

	// Re-emit with kustomize ownership labels stamped, then drive a second
	// reconcile: the labels differ so the body re-runs, but the fingerprint
	// matches → dedup-skip no-op.
	stamped := ks.Clone()
	stamped.Labels = map[string]string{
		"kustomize.toolkit.fluxcd.io/name":      "parent",
		"kustomize.toolkit.fluxcd.io/namespace": "flux-system",
	}
	s.AddObject(stamped)
	c.ReconcileNode(context.Background(), ks.Named(), 0)

	mu.Lock()
	defer mu.Unlock()
	if sawPending {
		t.Error("no-op re-reconcile of an already-Ready KS transiently downgraded it to Pending (the quiescence-race window)")
	}
	if info, _ := s.GetStatus(ks.Named()); info.Status != store.StatusReady {
		t.Errorf("KS not Ready after no-op re-run: %+v", info)
	}
}

// TestReconcile_GenuineReRender_DoesDowngrade is the byte-determinism
// guard for Part A: a re-reconcile whose effective spec CHANGED
// (fingerprint differs, e.g. #102 structural-parent-injected
// targetNamespace) MUST still downgrade to Pending before re-rendering,
// so a dependent re-gates on the new output rather than reading a
// stale-Ready parent. Proves the wasReady guard suppresses only the
// pre-fingerprint progress writes, not the genuine "rendering"
// downgrade.
func TestReconcile_GenuineReRender_DoesDowngrade(t *testing.T) {
	t.Parallel()
	c, s, _ := newControllerWithFixture(t)
	ks := &manifest.Kustomization{
		Name: "apps", Namespace: "flux-system",
		Path: "./apps",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{
			Kind: manifest.KindGitRepository, Name: "flux-system", Namespace: "flux-system",
		},
	}
	s.AddObject(ks)
	if info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: s, ID: ks.Named(), ReconcileNode: c.ReconcileNode}); info.Status != store.StatusReady {
		t.Fatalf("status = %+v, want StatusReady", info)
	}
	firstFP := s.GetArtifact(ks.Named()).(*store.Emission).Fingerprint

	var mu sync.Mutex
	var sawPending bool
	s.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
		if id != ks.Named() {
			return
		}
		if info, ok := payload.(store.StatusInfo); ok && info.Status == store.StatusPending {
			mu.Lock()
			sawPending = true
			mu.Unlock()
		}
	}, false)

	// Re-emit with a genuine spec change (targetNamespace) → fingerprint
	// differs → real re-render, not a dedup-skip. ReconcileNode runs the
	// re-render synchronously, so the new artifact has landed on return.
	changed := ks.Clone()
	changed.TargetNamespace = "elsewhere"
	s.AddObject(changed)
	c.ReconcileNode(context.Background(), ks.Named(), 0)

	var secondFP string
	if art, ok := s.GetArtifact(ks.Named()).(*store.Emission); ok && art.Fingerprint != firstFP {
		secondFP = art.Fingerprint
	}

	mu.Lock()
	defer mu.Unlock()
	if !sawPending {
		t.Error("genuine re-render did not downgrade to Pending; dependents would read a stale-Ready parent")
	}
	if secondFP == "" || secondFP == firstFP {
		t.Errorf("fingerprint did not change after spec mutation (%q → %q); not a genuine re-render", firstFP, secondFP)
	}
}

// TestReconcile_MissingPathFails covers the error path: spec.path
// points at a directory that doesn't exist → kustomize.Render
// errors → reconcile returns it → status flips Failed.
func TestReconcile_MissingPathFails(t *testing.T) {
	t.Parallel()
	c, s, _ := newControllerWithFixture(t)
	ks := &manifest.Kustomization{
		Name: "broken", Namespace: "flux-system",
		Path: "./nonexistent",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{
			Kind: manifest.KindGitRepository, Name: "flux-system", Namespace: "flux-system",
		},
	}
	s.AddObject(ks)
	info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: s, ID: ks.Named(), ReconcileNode: c.ReconcileNode})
	if info.Status != store.StatusFailed {
		t.Fatalf("status = %+v, want StatusFailed", info)
	}
	if info.Message == "" {
		t.Error("expected non-empty failure message")
	}
}

// TestReconcile_DependsOnFailed cascades a dep failure: when a KS's
// dependsOn target is in StatusFailed, reconcile must return
// DependencyFailedError without rendering.
func TestReconcile_DependsOnFailed(t *testing.T) {
	t.Parallel()
	c, s, _ := newControllerWithFixture(t)
	dep := &manifest.Kustomization{Name: "dep", Namespace: "flux-system"}
	s.AddObject(dep)
	s.UpdateStatus(dep.Named(), store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "synthetic dep failure"})

	ks := &manifest.Kustomization{
		Name: "depender", Namespace: "flux-system",
		Path: "./apps",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{
			Kind: manifest.KindGitRepository, Name: "flux-system", Namespace: "flux-system",
		},
		DependsOn: []kustomizev1.DependencyReference{
			{Name: dep.Name, Namespace: dep.Namespace},
		},
	}
	s.AddObject(ks)
	info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: s, ID: ks.Named(), ReconcileNode: c.ReconcileNode})
	if info.Status != store.StatusFailed {
		t.Fatalf("status = %+v, want StatusFailed", info)
	}
	if info.Message == "" {
		t.Error("expected failure message from dep cascade")
	}
}

func TestReconcile_Suspension(t *testing.T) {
	t.Parallel()
	t.Run("initial suspension", func(t *testing.T) {
		t.Parallel()
		c, s, _ := newControllerWithFixture(t)
		ks := &manifest.Kustomization{
			Name: "suspended", Namespace: "flux-system",
			Path: "./apps", Suspend: true,
			SourceRef: kustomizev1.CrossNamespaceSourceReference{
				Kind: manifest.KindGitRepository, Name: "flux-system", Namespace: "flux-system",
			},
		}
		s.AddObject(ks)
		info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: s, ID: ks.Named(), ReconcileNode: c.ReconcileNode})
		if info.Status != store.StatusReady {
			t.Fatalf("status = %+v, want StatusReady", info)
		}
		if info.Outcome() != store.OutcomeSuspended || info.Message != "suspended" {
			t.Errorf("expected Ready/suspended, got %+v", info)
		}
		if s.GetArtifact(ks.Named()) != nil {
			t.Error("suspended KS should not write an artifact")
		}
	})
	for _, tt := range []struct {
		name      string
		suspended bool
		cascade   bool
	}{
		{name: "refreshed/suspended=false"},
		{name: "refreshed/suspended=true", suspended: true},
		{name: "refreshed/oci_cascade", suspended: true, cascade: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			suspended := tt.suspended
			c, st, root := newControllerWithFixture(t)
			testutil.WriteFile(t, root, "apps/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: ks-refresh-output}
data: {greeting: hi}
`)
			parent := &manifest.Kustomization{Name: "parent", Namespace: "flux-system"}
			st.AddObject(parent)
			st.UpdateStatus(parent.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
			doc := map[string]any{
				"apiVersion": "kustomize.toolkit.fluxcd.io/v1", "kind": manifest.KindKustomization,
				"metadata": map[string]any{"name": "apps", "namespace": "flux-system"},
				"spec": map[string]any{
					"path": "./apps", "sourceRef": map[string]any{"kind": manifest.KindGitRepository, "name": "flux-system"},
					"dependsOn": []any{map[string]any{"name": parent.Name}},
				},
			}
			if tt.cascade {
				doc["spec"].(map[string]any)["sourceRef"] = map[string]any{"kind": manifest.KindOCIRepository, "name": "missing"}
				st.UpdateStatus(manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: parent.Namespace, Name: "missing"}, store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
			}
			obj, _, _, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{DefaultNamespace: ""})
			if err != nil {
				t.Fatal(err)
			}
			ks := obj.(*manifest.Kustomization)
			original := ks.Clone()
			c.Emit(parent.Named(), &store.Emission{Children: []store.Child{{Obj: ks, Doc: doc}}, Complete: true}, nil)
			st.UpdateStatus(ks.Named(), store.StatusInfo{Status: store.StatusPending, Reason: "", Message: ""})
			replacement := manifest.DeepCopyMap(doc)
			spec := replacement["spec"].(map[string]any)
			spec["suspend"] = suspended
			spec["targetNamespace"] = "refreshed"
			var observations atomic.Int64
			var fired, published atomic.Bool
			unsub := st.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
				info, ok := payload.(store.StatusInfo)
				if id != ks.Named() || !ok || info.Message != "resolving dependencies" {
					return
				}
				observations.Add(1)
				if fired.Swap(true) {
					return
				}
				publishDocuments(c, parent.Named(), []map[string]any{replacement}, manifest.ParseDocOptions{WipeSecrets: false, DefaultNamespace: parent.Named().Namespace})
				published.Store(true)
			}, false)
			t.Cleanup(unsub)
			controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: st, ID: ks.Named(), ReconcileNode: c.ReconcileNode})
			if observations.Load() < 1 || !published.Load() || c.Tasks.Failures() != 0 {
				t.Fatalf("observations=%d published=%t panics=%d", observations.Load(), published.Load(), c.Tasks.Failures())
			}
			fresh, ok := st.Get[*manifest.Kustomization](ks.Named())
			if !ok || fresh == ks || fresh.Suspend != suspended || fresh.TargetNamespace != "refreshed" {
				t.Fatalf("canonical replacement = %+v", fresh)
			}
			if diff := cmp.Diff(original, ks, cmp.AllowUnexported(manifest.Kustomization{}), cmpopts.IgnoreFields(manifest.Kustomization{}, "projection")); diff != "" {
				t.Fatalf("original mutated (-want +got):\n%s", diff)
			}
			if owners := testutil.Owners(st.Productions(ks.Named(), nil)); !slices.Contains(owners, parent.Named()) {
				t.Fatalf("missing independent contributor: %v", owners)
			}
			info, _ := st.GetStatus(ks.Named())
			art, _ := st.GetArtifact(ks.Named()).(*store.Emission)
			output := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "refreshed", Name: "ks-refresh-output"}
			if suspended {
				if info.Outcome() != store.OutcomeSuspended || info.Message != "suspended" {
					t.Errorf("status = %+v, want Ready/suspended", info)
				}
				_, _, inventory := st.GetEmission(ks.Named())
				if art != nil || st.GetObject(output) != nil || len(inventory.Children) != 0 {
					t.Errorf("suspended replacement rendered: artifact=%+v output=%v", art, st.GetObject(output))
				}
			} else if info.Status != store.StatusReady || art == nil || len(art.Manifests) != 1 || st.GetObject(output) == nil {
				t.Fatalf("active replacement did not render %s: status=%+v artifact=%+v", output, info, art)
			}
		})
	}
}

func TestReconcile_ReleasedPinWaitsForSource(t *testing.T) {
	t.Parallel()
	c, st, root := newControllerWithFixture(t)
	ks := &manifest.Kustomization{Name: "apps", Namespace: "flux-system", Path: "./apps"}
	st.AddObject(ks)
	if info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: st, ID: ks.Named(), ReconcileNode: c.ReconcileNode}); info.Status != store.StatusReady {
		t.Fatalf("first reconcile: %+v", info)
	}
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, Key: "released"})
	_, err := c.newExecution(ks.Named()).run.RequireRefresh(t.Context(), ks, manifest.GateArtifacts, manifest.DrainNone)
	if blocked, ok := errors.AsType[*depwait.ErrBlocked](err); !ok || len(blocked.Deps) != 1 || blocked.Deps[0] != manifest.BootstrapSourceID {
		t.Fatalf("released pin did not wait for source acquisition: %v", err)
	}
	if _, err := c.newExecution(ks.Named()).run.RequireRefresh(t.Context(), ks, manifest.GateArtifacts, manifest.DrainForce); !errors.Is(err, manifest.ErrObjectNotFound) {
		t.Fatalf("unavailable content at fixpoint: %v", err)
	}
}

type guardedSourceFS struct {
	fs.FS
	once   sync.Once
	opened atomic.Int64
	deny   atomic.Bool
	onOpen func()
}

func (s *guardedSourceFS) Open(name string) (fs.File, error) {
	s.opened.Add(1)
	if s.deny.Load() {
		return nil, fs.ErrPermission
	}
	s.once.Do(func() {
		if s.onOpen != nil {
			s.onOpen()
		}
	})
	return s.FS.Open(name)
}

func TestRender_RejectedCapturedInputsSkipsRender(t *testing.T) {
	t.Parallel()
	c, s, _ := newControllerWithFixture(t)
	initial := s.GetArtifact(manifest.BootstrapSourceID).(*store.SourceArtifact)
	guard := &guardedSourceFS{FS: initial.FS}
	counted := *initial
	counted.FS = guard
	s.SetArtifact(manifest.BootstrapSourceID, &counted)
	ks := &manifest.Kustomization{
		Name: "apps", Namespace: "flux-system", Path: "./apps",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{
			Kind: manifest.KindGitRepository, Name: "flux-system", Namespace: "flux-system",
		},
	}
	s.AddObject(ks)
	execution := c.newExecution(ks.Named())
	captured := make(chan struct{})
	resume := make(chan struct{})
	release := sync.OnceFunc(func() { close(resume) })
	defer func() {
		release()
		c.Tasks.BlockTillDone()
		execution.run.Close()
	}()
	ks, ok := execution.run.Reads.Get[*manifest.Kustomization](ks.Named())
	if !ok {
		t.Fatal("Kustomization not captured")
	}
	source, applyIgnore, err := execution.resolveSource(ks)
	assert.NoError(t, err)
	if !execution.run.Reads.Valid() {
		t.Fatal("initial capture is invalid")
	}
	before := guard.opened.Load()
	type observation struct {
		valid    bool
		opens    int64
		err      error
		handled  bool
		dedupErr error
	}
	result := make(chan observation, 1)
	c.Tasks.Go(t.Context(), "render", func(ctx context.Context) {
		close(captured)
		c.Tasks.YieldSlot(func() { <-resume })
		valid := execution.run.Reads.Valid()
		err := execution.render(ctx, ks, source, applyIgnore)
		opens := guard.opened.Load() - before
		_, handled, dedupErr := execution.run.FingerprintDedup(ks.Named(), nil)
		result <- observation{valid: valid, opens: opens, err: err, handled: handled, dedupErr: dedupErr}
	})
	c.Tasks.Go(t.Context(), "replace", func(context.Context) {
		defer release()
		c.Tasks.YieldSlot(func() { <-captured })
		next := ks.Clone()
		next.NamePrefix = "updated-"
		s.AddObject(next)
	})
	got := <-result
	if got.valid {
		t.Fatal("replacement did not invalidate captured inputs before rendering")
	}
	if got.opens != 0 {
		t.Fatalf("rejected render opened source %d times, want 0", got.opens)
	}
	if !got.handled || got.dedupErr == nil {
		t.Fatalf("invalid capture dedup: handled=%t, err=%v", got.handled, got.dedupErr)
	}
	if got.err == nil || !errors.Is(got.err, got.dedupErr) {
		t.Fatalf("render did not return the admission rejection: %v", got.err)
	}
}

type admissionSuspension struct {
	*manifest.Kustomization
	checks int
}

func (s *admissionSuspension) Suspended() bool {
	s.checks++
	return s.checks == 3
}

func TestRender_UnhandledAdmissionErrorSkipsRender(t *testing.T) {
	t.Parallel()
	c, st, _ := newControllerWithFixture(t)
	initial := st.GetArtifact(manifest.BootstrapSourceID).(*store.SourceArtifact)
	guard := &guardedSourceFS{FS: initial.FS}
	counted := *initial
	counted.FS = guard
	st.SetArtifact(manifest.BootstrapSourceID, &counted)
	ks := &manifest.Kustomization{Name: "apps", Namespace: "flux-system", Path: "./apps",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{
			Kind: manifest.KindGitRepository, Name: "flux-system", Namespace: "flux-system",
		},
	}
	declaration := &admissionSuspension{Kustomization: ks}
	st.AddObject(declaration)
	sourceContent := struct{ Path, Key, Revision, Digest string }{counted.LocalPath, counted.Key, counted.Revision, counted.Digest}
	probe := c.newExecution(ks.Named())
	fp, handled, want := probe.run.FingerprintDedup(ks.Named(), ks.Content(sourceContent))
	probe.run.Close()
	if fp == "" || handled || want == nil || declaration.checks != 3 {
		t.Fatalf("admission contract: fingerprint=%q handled=%t err=%v checkpoints=%d", fp, handled, want, declaration.checks)
	}
	declaration.checks = 0
	execution := c.newExecution(ks.Named())
	defer execution.run.Close()
	source, applyIgnore, err := execution.resolveSource(ks)
	assert.NoError(t, err)
	before := guard.opened.Load()
	got := execution.render(t.Context(), ks, source, applyIgnore)
	if !errors.Is(got, want) {
		t.Errorf("render error = %v, want admission error %v", got, want)
	}
	if opens := guard.opened.Load() - before; opens != 0 {
		t.Errorf("rejected render opened source %d times, want 0", opens)
	}
	if artifact := st.GetArtifact(ks.Named()); artifact != nil {
		t.Errorf("rejected attempt published %T", artifact)
	}
}

func TestReconcile_SourceCoverageUsesRenderedArtifact(t *testing.T) {
	for _, swapDuringRender := range []bool{false, true} {
		t.Run(fmt.Sprint(swapDuringRender), func(t *testing.T) {
			root := t.TempDir()
			testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [objects.yaml]\n")
			testutil.WriteFile(t, root, "apps/objects.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: shared, namespace: apps, labels: {flag: true, code: 012}}\ndata: {value: '${VALUE}'}\n---\napiVersion: v1\nkind: Secret\nmetadata: {name: exclusive, namespace: apps}\nstringData: {value: original}\n")
			s := store.New()
			sourceID := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "source"}
			location, err := gitrepo.Locate(t.Context(), root, "")
			assert.NoError(t, err)
			input, err := gittree.AdmitDirectory(t.Context(), location)
			assert.NoError(t, err)
			guard := &guardedSourceFS{FS: input.FS}
			initial := *input
			initial.Key, initial.FS, initial.Revision = "initial-pin", guard, "initial-pin"
			next := initial
			next.Key, next.FS = "different-pin", input.FS
			s.AddObject(&manifest.GitRepository{Name: sourceID.Name, Namespace: sourceID.Namespace, URL: "file://" + root})
			s.SetArtifact(sourceID, &initial)
			s.UpdateStatus(sourceID, store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
			docs, err := manifest.DecodeDocs(strings.NewReader("apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: producer, namespace: apps}\nspec: {path: apps, namePrefix: 'first-', sourceRef: {kind: GitRepository, name: source, namespace: apps}, postBuild: {substitute: {VALUE: original, OWNER: accepted}}, commonMetadata: {labels: {kustomize.toolkit.fluxcd.io/name: '${OWNER}'}}}\n"))
			assert.NoError(t, err)
			obj, _, _, err := manifest.ParseDoc(docs[0], manifest.ParseDocOptions{})
			assert.NoError(t, err)
			ks := obj.(*manifest.Kustomization)
			s.AddObject(ks)
			l := loader.New(s)
			l.SourceRoot, l.Options.WipeSecrets = root, false
			_, err = l.Load(t.Context(), filepath.Join(root, "apps"))
			assert.NoError(t, err)
			l.FinalizeIndex(t.Context(), root)
			shared := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "shared"}
			exclusive := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "exclusive"}
			if len(s.Productions(exclusive, l.Index)) == 0 {
				t.Fatal("missing frozen coverage")
			}
			for _, id := range []manifest.NamedResource{shared, exclusive} {
				for _, p := range s.Productions(id, l.Index) {
					if p.Path != "" {
						loader.Promote(t.Context(), s, id, loader.PromoteOptions{Record: p, Admit: func(manifest.BaseManifest) bool { return true }})
					}
				}
				if s.GetObject(id) == nil {
					t.Fatalf("missing promoted seed %s", id)
				}
			}

			q := &manifest.Kustomization{Name: "q", Namespace: "apps"}
			s.AddObject(q)
			s.RecordEmitted(q.Named(), &store.Emission{Children: []store.Child{{Obj: s.GetObject(shared)}}, Complete: true}, nil)
			owner, err := sourcecache.New(t.Context(), t.TempDir())
			assert.NoError(t, err)
			t.Cleanup(func() { _ = owner.Close() })
			tasks := task.NewBounded(2)
			trees := kustomize.NewTreeCache(owner, nil, tasks)
			t.Cleanup(trees.Close)
			c := New(s, tasks, trees, false)
			c.Configure(Options{Index: l.Index}, sourcecache.RetryConfig{})
			if swapDuringRender {
				guard.once = sync.Once{}
				guard.onOpen = func() { s.SetArtifact(sourceID, &next) }
			} else {
				s.SetArtifact(sourceID, &next)
			}
			if info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: s, ID: ks.Named(), ReconcileNode: c.ReconcileNode}); info.Status != store.StatusReady {
				t.Fatalf("settled source render: %+v", info)
			}
			if s.GetObject(exclusive) == nil {
				t.Fatal("unaccepted source render retired prediction from another source view")
			}
			freshOutput := s.GetArtifact(ks.Named()).(*store.Emission)
			wantManifests := make([]map[string]any, len(freshOutput.Manifests))
			for i, doc := range freshOutput.Manifests {
				wantManifests[i] = manifest.DeepCopyMap(doc)
				labels := doc["metadata"].(map[string]any)["labels"].(map[string]any)
				assert.Equal(t, labels["kustomize.toolkit.fluxcd.io/name"], any("accepted"))
				if manifest.DocKind(doc) == manifest.KindConfigMap {
					assert.Equal(t, labels["flag"], any("true"))
					assert.Equal(t, labels["code"], any("012"))
					assert.Equal(t, doc["data"].(map[string]any)["value"], any("original"))
				}
			}

			if s.GetObject(shared) == nil {
				t.Fatal("coverage retired Q support")
			}
			if !swapDuringRender {
				s.SetArtifact(sourceID, &initial)
				if err := c.newExecution((ks).Named()).reconcile(t.Context(), ks, manifest.DrainNone); err != nil {
					t.Fatal(err)
				}
				if s.GetObject(exclusive) != nil {
					t.Fatal("matching source view did not retire its whole prediction")
				}
			}
			_, _, empty := s.GetEmission(ks.Named())
			empty.Children, empty.Manifests, empty.Fingerprint, empty.Complete = nil, nil, "", true
			s.RecordEmitted(ks.Named(), &empty, nil)
			empty.Finish()
			s.SetArtifact(sourceID, &initial)
			probe := sourcecache.BlobKey{Content: "source-coverage-persistence-probe", PersistOnly: true}
			probeReader, _, _, err := owner.Blob(t.Context(), probe, sourcecache.Policy{}, func(context.Context, sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
				return sourcecache.ProducerResult{}, nil
			})
			assert.NoError(t, err)
			assert.NoError(t, probeReader.Close())
			probeReader, _, _, probeErr := owner.Blob(t.Context(), probe, sourcecache.Policy{MaxAge: sourcecache.Forever}, nil)
			persistent := probeErr == nil
			if persistent {
				assert.NoError(t, probeReader.Close())
				guard.deny.Store(true)
			} else {
				t.Log("raw render persistence unavailable for this build identity")
			}
			t.Logf("persisted renderer hit enabled: %t", persistent)
			before := guard.opened.Load()
			prepared, err := kustomize.Prepare(ks, nil, nil)
			assert.NoError(t, err)
			rendered, err := kustomize.Render(t.Context(), trees, kustomize.RenderRequest{Source: &initial, SubPath: ks.Path, Spec: prepared}, sourcecache.RetryConfig{})
			assert.NoError(t, err)
			for _, doc := range rendered {
				metadata := doc["metadata"].(map[string]any)
				labels, _ := metadata["labels"].(map[string]any)
				if _, stamped := labels["kustomize.toolkit.fluxcd.io/name"]; stamped {
					t.Fatal("persisted renderer document already stamped")
				}
				metadata["name"] = "caller-mutation"
				if data, ok := doc["data"].(map[string]any); ok {
					data["value"] = "caller-mutation"
				}
			}
			if err := c.newExecution((ks).Named()).reconcile(t.Context(), ks, manifest.DrainNone); err != nil {
				t.Fatalf("raw cache hit: %v", err)
			}
			if persistent && guard.opened.Load() != before {
				t.Fatal("expected immutable-source raw cache hit without input reads")
			}
			guard.deny.Store(false)
			cachedOutput := s.GetArtifact(ks.Named()).(*store.Emission)
			assert.Diff(t, cachedOutput.Manifests, wantManifests)
			assert.Equal(t, len(cachedOutput.Children), len(freshOutput.Children))
			for _, child := range cachedOutput.Children {
				if configMap, ok := child.Obj.(*manifest.ConfigMap); ok {
					assert.Equal(t, configMap.Data["value"], any("original"))
				}
			}
			fresh := ks.Clone()
			fresh.NamePrefix = "arbitrary-second-"

			s.AddObject(fresh)
			if err := c.newExecution((fresh).Named()).reconcile(t.Context(), fresh, manifest.DrainNone); err != nil {
				t.Fatal(err)
			}
			old := shared
			old.Name = "first-shared"
			newID := shared
			newID.Name = "arbitrary-second-shared"
			if s.GetObject(old) != nil || s.GetObject(newID) == nil {
				t.Fatal("repeated rename retained obsolete output")
			}
			failed := initial
			failed.Key = "failed-pin"
			failed.FS = &guardedSourceFS{FS: input.FS}
			failed.FS.(*guardedSourceFS).deny.Store(true)
			s.SetArtifact(sourceID, &failed)
			if err := c.newExecution((fresh).Named()).reconcile(t.Context(), fresh, manifest.DrainNone); err == nil || s.GetObject(newID) == nil {
				t.Fatal("failed successor withdrew completed output")
			}
			_, _, empty = s.GetEmission(ks.Named())
			empty.Children, empty.Manifests, empty.Complete = nil, nil, true
			s.RecordEmitted(ks.Named(), &empty, nil)
			if s.GetObject(newID) != nil || s.GetObject(shared) == nil {
				t.Fatal("complete empty did not retire only its contribution")
			}
		})
	}
}

func TestReconcile_DeclarationChangeBeforeCapture(t *testing.T) {
	t.Parallel()
	for _, state := range []string{"withdrawn", "suspended", "replacement"} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			c, st, root := newControllerWithFixture(t)
			testutil.WriteFile(t, root, "b/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(t, root, "b/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: from-b, namespace: flux-system}\n")
			parent := &manifest.Kustomization{Name: "parent", Namespace: "flux-system"}
			peer := &manifest.Kustomization{Name: "peer", Namespace: "flux-system"}
			parsed, _, _, err := manifest.ParseDoc(map[string]any{"apiVersion": manifest.FluxKustomizeDomain + "/v1", "kind": manifest.KindKustomization, "metadata": map[string]any{"name": "input", "namespace": "flux-system"}, "spec": map[string]any{"path": "./apps", "sourceRef": map[string]any{"kind": manifest.KindGitRepository, "name": "flux-system"}}}, manifest.ParseDocOptions{})
			if err != nil {
				t.Fatal(err)
			}
			input := parsed.(*manifest.Kustomization)
			st.AddObjects([]manifest.BaseManifest{parent, peer, input})
			st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{input.Named(): {Productions: []manifest.Production{{Owner: parent.Named()}}}}})
			st.UpdateStatus(parent.Named(), store.StatusInfo{Status: store.StatusPending, Reason: "", Message: "materializing"})
			publish := func(obj *manifest.Kustomization) {
				t.Helper()
				next := captureEmission(st, peer.Named(), &store.Emission{Complete: true})
				defer next.Finish()
				if obj != nil {
					next.Children = []store.Child{{Obj: obj}}
				}
				st.RecordEmitted(peer.Named(), next, nil)
			}
			publish(input)
			entered, changed, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var once atomic.Bool
			var expansions atomic.Int64
			stop := st.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
				if id != input.Named() {
					return
				}
				info := payload.(store.StatusInfo)
				if info.Message == "expanding substitutions" {
					expansions.Add(1)
				}
				if info.Message == "resolving source artifact" && once.CompareAndSwap(false, true) {
					close(entered)
					c.Tasks.YieldSlot(func() { <-changed })
				}
			}, false)
			defer stop()
			var run *base.Execution
			c.Tasks.Go(t.Context(), "reconcile", func(ctx context.Context) {
				defer close(finished)
				run = c.ReconcileNode(ctx, input.Named(), manifest.DrainNone)
			})
			c.Tasks.Go(t.Context(), "replace", func(context.Context) {
				reached := false
				c.Tasks.YieldSlot(func() {
					select {
					case <-entered:
						reached = true
					case <-finished:
					}
				})
				if !reached {
					t.Error("reconcile never reached source capture")
					return
				}
				defer close(changed)
				if state == "withdrawn" {
					publish(nil)
				} else {
					replacement := input.Clone()
					replacement.Path, replacement.Suspend = "./b", state == "suspended"
					publish(replacement)
				}
			})
			c.Tasks.BlockTillDone()
			if c.Tasks.Failures() != 0 {
				t.Fatal("capture worker panicked")
			}
			if run.Valid() || expansions.Load() != 0 || st.GetArtifact(input.Named()) != nil {
				t.Fatalf("stale source capture continued: valid=%t expansions=%d", run.Valid(), expansions.Load())
			}
			run.Close()
			run = c.ReconcileNode(t.Context(), input.Named(), manifest.DrainNone)

			switch state {
			case "withdrawn":
				if !slices.Equal(run.Blocked, []manifest.NamedResource{parent.Named()}) || expansions.Load() != 0 || st.GetArtifact(input.Named()) != nil {
					t.Fatalf("lost authority: blocked=%v expansions=%d", run.Blocked, expansions.Load())
				}
			case "suspended":
				info, _ := st.GetStatus(input.Named())
				if run.Outcome() == schedule.Blocked || expansions.Load() != 0 || info.Outcome() != store.OutcomeSuspended || st.GetArtifact(input.Named()) != nil {
					t.Fatalf("suspended capture continued: blocked=%v expansions=%d status=%v", run.Blocked, expansions.Load(), info)
				}
			default:
				emission, ok := st.GetArtifact(input.Named()).(*store.Emission)
				if run.Outcome() == schedule.Blocked || !ok || !emission.Complete || len(emission.Manifests) != 1 || emission.Manifests[0]["metadata"].(map[string]any)["name"] != "from-b" {
					t.Fatalf("replacement did not prepare B: blocked=%v output=%v", run.Blocked, emission)
				}
			}
		})
	}
}
