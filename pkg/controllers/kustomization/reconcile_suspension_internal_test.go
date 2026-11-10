package kustomization

import (
	"context"
	"errors"
	"io/fs"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestReconcile_Suspension(t *testing.T) {
	t.Parallel()
	t.Run("initial suspension", func(t *testing.T) { t.Parallel(); testReconcileSuspensionInitialSuspension(t) })
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
			testReconcileSuspensionCase(t, &tt)
		},
		)
	}
}

func TestReconcile_ReleasedPinWaitsForSource(t *testing.T) {
	t.Parallel()
	c, st, root := newControllerWithFixture(t)
	ks := &manifest.Kustomization{Name: "apps", Namespace: "flux-system", Path: "./apps"}
	st.AddObject(ks)
	{
		info := controllertest.DispatchToFixpoint(
			t.Context(),
			t,
			controllertest.Dispatch{Tasks: c.Tasks, Store: st, ID: ks.Named(), ReconcileNode: c.ReconcileNode},
		)
		assert.FatalIf(t, info.Status != store.StatusReady, "first reconcile: %+v", info)
	}
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, Key: "released"})
	_, err := c.newExecution(ks.Named()).run.RequireRefresh(t.Context(), ks, manifest.GateArtifacts, manifest.DrainNone)
	{
		blocked, ok := errors.AsType[*depwait.ErrBlocked](
			err,
		)
		assert.FatalIf(t, !ok || len(blocked.Deps) != 1 ||
			blocked.Deps[0] != manifest.BootstrapSourceID, "released pin did not wait for source acquisition: %v", err)
	}
	{
		_, err := c.newExecution(
			ks.Named(),
		).run.RequireRefresh(
			t.Context(),
			ks,
			manifest.GateArtifacts,
			manifest.DrainForce,
		)
		assert.FatalIf(t, !errors.Is(
			err,
			manifest.ErrObjectNotFound,
		), "unavailable content at fixpoint: %v", err)
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
	initial := s.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
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
	assert.FatalIf(t, !ok, "%v", "Kustomization not captured")
	source, applyIgnore, err := kustomize.SourceInput(&execution.run.Reads, ks)
	assert.NoError(t, err)
	assert.FatalIf(t, !execution.run.Reads.Valid(), "%v", "initial capture is invalid")
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
		fingerprintDedupResult1, fingerprintDedupResult1Err := execution.run.FingerprintDedup(
			t.Context(),
			ks.Named(),
			nil,
			&store.Emission{},
		)
		_, handled, dedupErr := fingerprintDedupResult1.Fingerprint, fingerprintDedupResult1.Reused, fingerprintDedupResult1Err
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
	assert.FatalIf(t, got.valid, "%v", "replacement did not invalidate captured inputs before rendering")
	assert.FatalIf(t, got.opens != 0, "rejected render opened source %d times, want 0", got.opens)
	assert.FatalIf(
		t,
		!got.handled || got.dedupErr == nil,
		"invalid capture dedup: handled=%t, err=%v",
		got.handled,
		got.dedupErr,
	)
	assert.FatalIf(
		t,
		got.err == nil || !errors.Is(got.err, got.dedupErr),
		"render did not return the admission rejection: %v",
		got.err,
	)
}

func TestReconcile_SourceCoverageUsesRenderedArtifact(t *testing.T) {
	t.Parallel()

	for _, swapDuringRender := range []bool{false, true} {
		t.Run(strconv.FormatBool(swapDuringRender), func(t *testing.T) {
			t.Parallel()
			testReconcileSourceCoverageUsesRenderedArtifactCase(
				t,
				&swapDuringRender,
			)
		},
		)
	}
}

func testReconcileSuspensionInitialSuspension(t *testing.T) {
	t.Helper()

	c, s, _ := newControllerWithFixture(t)
	ks := &manifest.Kustomization{
		Name: "suspended", Namespace: "flux-system",
		Path: "./apps", Suspend: true,
		SourceRef: kustomizev1.CrossNamespaceSourceReference{
			Kind: manifest.KindGitRepository, Name: "flux-system", Namespace: "flux-system",
		},
	}
	s.AddObject(ks)
	info := controllertest.DispatchToFixpoint(
		t.Context(),
		t,
		controllertest.Dispatch{Tasks: c.Tasks, Store: s, ID: ks.Named(), ReconcileNode: c.ReconcileNode},
	)
	assert.FatalIf(t, info.Status != store.StatusReady, "status = %+v, want StatusReady", info)
	assert.ErrorIf(
		t,
		info.Outcome() != store.OutcomeSuspended || info.Message != "suspended",
		"expected Ready/suspended, got %+v",
		info,
	)
	assert.ErrorIf(
		t,
		s.GetArtifact(ks.Named(), store.ArtifactContent) != nil,
		"%v",
		"suspended KS should not write an artifact",
	)
}

func testReconcileSuspensionCase(t *testing.T, testCaseInput *struct {
	name string

	suspended bool
	cascade   bool
},
) {
	t.Helper()

	suspended := testCaseInput.suspended
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
			"path":      "./apps",
			"sourceRef": map[string]any{"kind": manifest.KindGitRepository, "name": "flux-system"},
			"dependsOn": []any{map[string]any{"name": parent.Name}},
		},
	}
	if testCaseInput.cascade {
		doc["spec"].(map[string]any)["sourceRef"] = map[string]any{
			"kind": manifest.KindOCIRepository,
			"name": "missing",
		}
		st.UpdateStatus(
			manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindOCIRepository,
				Namespace: parent.Namespace,
				Name:      "missing",
			},
			store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""},
		)
	}
	parsed, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{DefaultNamespace: ""})
	obj := parsed.Object
	assert.FatalIf(t, err != nil, "%v", err)
	ks := obj.(*manifest.Kustomization)
	original := ks.Clone()
	controllertest.Emit(
		t.Context(),
		c.Controller,
		parent.Named(),
		&store.Emission{Children: []store.Child{{Obj: ks, Doc: doc}}, Complete: true},
	)
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
		publishDocuments(
			c,
			parent.Named(),
			[]map[string]any{replacement},
			manifest.ParseDocOptions{DefaultNamespace: parent.Named().Namespace},
		)
		published.Store(true)
	}, false)
	t.Cleanup(unsub)
	controllertest.DispatchToFixpoint(
		t.Context(),
		t,
		controllertest.Dispatch{Tasks: c.Tasks, Store: st, ID: ks.Named(), ReconcileNode: c.ReconcileNode},
	)
	assert.FatalIf(
		t,
		observations.Load() < 1 || !published.Load() || c.Tasks.Failures() != 0,
		"observations=%d published=%t panics=%d",
		observations.Load(),
		published.Load(),
		c.Tasks.Failures(),
	)
	fresh, ok := st.Get[*manifest.Kustomization](ks.Named())
	assert.FatalIf(
		t,
		!ok || fresh == ks || fresh.Suspend != suspended || fresh.TargetNamespace != "refreshed",
		"canonical replacement = %+v",
		fresh,
	)
	{
		diff := cmp.Diff(
			original,
			ks,
			cmp.AllowUnexported(manifest.Kustomization{}),
			cmpopts.IgnoreFields(manifest.Kustomization{}, "projection"),
		)
		assert.FatalIf(t, diff != "", "original mutated (-want +got):\n%s", diff)
	}
	{
		owners := testutil.Owners(st.Productions(ks.Named(), nil))
		assert.FatalIf(
			t,
			!slices.Contains(owners, parent.Named()),
			"missing independent contributor: %v",
			owners,
		)
	}
	info, _ := st.GetStatus(ks.Named())
	art, _ := st.GetArtifact(ks.Named(), store.ArtifactContent).(*store.Emission)
	output := manifest.NamedResource{
		Kind:      manifest.KindConfigMap,
		Namespace: "refreshed",
		Name:      "ks-refresh-output",
	}
	if suspended {
		assert.ErrorIf(
			t,
			info.Outcome() != store.OutcomeSuspended || info.Message != "suspended",
			"status = %+v, want Ready/suspended",
			info,
		)
		_, _, inventory := st.GetEmission(ks.Named())
		assert.ErrorIf(
			t,
			art != nil || st.GetObject(output) != nil || len(inventory.Children) != 0,
			"suspended replacement rendered: artifact=%+v output=%v",
			art,
			st.GetObject(output),
		)
	} else {
		assert.FatalIf(
			t,
			info.Status != store.StatusReady || art == nil || len(art.Manifests) != 1 ||
				st.GetObject(output) == nil,
			"active replacement did not render %s: status=%+v artifact=%+v",
			output,
			info,
			art,
		)
	}
}

func testReconcileSourceCoverageUsesRenderedArtifactCase(
	t *testing.T,
	testCaseInput *bool,
) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [objects.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"apps/objects.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: shared, namespace: apps, labels: {flag: true, code: 012}}\ndata: {value: '${VALUE}'}\n---\napiVersion: v1\nkind: Secret\nmetadata: {name: exclusive, namespace: apps}\nstringData: {value: original}\n",
	)
	s := store.New()
	sourceID := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "apps",
		Name:      "source",
	}
	location, err := gitrepo.Locate(t.Context(), root, "")
	assert.NoError(t, err)
	inputDirectory, err := gittree.OpenDirectory(t.Context(), location)
	assert.NoError(t, err)
	input := inputDirectory.Artifact()
	assert.NoError(t, inputDirectory.Close())
	guard := &guardedSourceFS{FS: input.FS}
	initial := *input
	initial.Key, initial.Content, initial.FS, initial.Revision = "initial-pin", "initial-content", guard, "initial-pin"
	next := initial
	next.Key, next.Content, next.FS = "different-pin", "different-content", input.FS
	s.AddObject(
		&manifest.GitRepository{Name: sourceID.Name, Namespace: sourceID.Namespace, URL: "file://" + root},
	)
	s.SetArtifact(sourceID, &initial)
	s.UpdateStatus(sourceID, store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	docs, err := testutil.DecodeDocs(
		strings.NewReader(
			"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: producer, namespace: apps}\nspec: {path: apps, namePrefix: 'first-', sourceRef: {kind: GitRepository, name: source, namespace: apps}, postBuild: {substitute: {VALUE: original, OWNER: accepted}}, commonMetadata: {labels: {kustomize.toolkit.fluxcd.io/name: '${OWNER}'}}}\n",
		),
	)
	assert.NoError(t, err)
	parsed2, err := manifest.ParseDoc(docs[0], manifest.ParseDocOptions{})
	obj := parsed2.Object
	assert.NoError(t, err)
	ks := obj.(*manifest.Kustomization)
	s.AddObject(ks)
	l := loader.New(s)
	l.SourceRoot = root
	_, err = l.Load(t.Context(), filepath.Join(root, "apps"), manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	l.FinalizeIndex(t.Context(), root, true)
	shared := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "shared"}
	exclusive := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "exclusive"}
	assert.FatalIf(t, len(s.Productions(exclusive, l.Index)) == 0, "%v", "missing frozen coverage")
	for _, id := range []manifest.NamedResource{shared, exclusive} {
		promoteSourceCoverage(t, s, l.Index, id)
	}

	q := &manifest.Kustomization{Name: "q", Namespace: "apps"}
	s.AddObject(q)
	s.RecordEmitted(
		q.Named(),
		&store.Emission{Children: []store.Child{{Obj: s.GetObject(shared)}}, Complete: true},
		nil,
	)
	owner, err := sourcecache.New(t.Context(), t.TempDir())
	assert.NoError(t, err)
	t.Cleanup(func() { _ = owner.Close() })
	tasks := task.NewBounded(2)
	trees := kustomize.NewTreeCache(owner, nil, tasks)
	t.Cleanup(trees.Close)
	c := New(s, tasks, trees)
	c.Configure(Options{Index: l.Index}, sourcecache.RetryConfig{})
	reconcile := func(obj *manifest.Kustomization) error {
		var result error
		tasks.Go(t.Context(), "reconcile", func(ctx context.Context) {
			result = c.newExecution(obj.Named()).reconcile(ctx, obj, manifest.DrainNone)
		})
		tasks.BlockTillDone()
		return result
	}

	if *testCaseInput {
		guard.once = sync.Once{}
		guard.onOpen = func() { s.SetArtifact(sourceID, &next) }
	} else {
		s.SetArtifact(sourceID, &next)
	}
	{
		info := controllertest.DispatchToFixpoint(
			t.Context(),
			t,
			controllertest.Dispatch{
				Tasks:         c.Tasks,
				Store:         s,
				ID:            ks.Named(),
				ReconcileNode: c.ReconcileNode,
			},
		)
		assert.FatalIf(t, info.Status != store.StatusReady, "settled source render: %+v", info)
	}
	assert.FatalIf(
		t,
		s.GetObject(exclusive) == nil,
		"%v",
		"unaccepted source render retired prediction from another source view",
	)
	freshOutput := s.GetArtifact(ks.Named(), store.ArtifactContent).(*store.Emission)
	wantManifests := make([]map[string]any, len(freshOutput.Manifests))
	for i, doc := range freshOutput.Manifests {
		wantManifests[i] = manifest.DeepCopyMap(doc.Document)
		labels := doc.Document["metadata"].(map[string]any)["labels"].(map[string]any)
		assert.Equal(t, labels["kustomize.toolkit.fluxcd.io/name"], any("accepted"))
		if manifest.DocKind(doc.Document) == manifest.KindConfigMap {
			assert.Equal(t, labels["flag"], any("true"))
			assert.Equal(t, labels["code"], any("012"))
			assert.Equal(t, doc.Document["data"].(map[string]any)["value"], any("original"))
		}
	}

	assert.FatalIf(t, s.GetObject(shared) == nil, "%v", "coverage retired Q support")
	s.SetArtifact(sourceID, &initial)
	{
		localErr := reconcile(ks)
		assert.FatalIf(t, localErr != nil, "%v", localErr)
	}
	assert.FatalIf(
		t,
		s.GetObject(exclusive) != nil,
		"%v",
		"matching source view did not retire its whole prediction",
	)
	_, _, empty := s.GetEmission(ks.Named())
	empty.Children, empty.Manifests, empty.Fingerprint, empty.Complete = nil, nil, "", true
	s.RecordEmitted(ks.Named(), &empty, nil)
	empty.Finish()
	s.SetArtifact(sourceID, &initial)
	probe := sourcecache.BlobKey{
		Content: "source-coverage-persistence-probe",
		Render:  sourcecache.RenderAccepted,
	}
	_, _, _, err = owner.Blob(
		t.Context(),
		probe,
		&sourcecache.Policy{},
		func(context.Context, sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
			return sourcecache.ProducerResult{}, nil
		},
	)
	assert.NoError(t, err)

	_, _, _, probeErr := owner.Blob(t.Context(), probe, &sourcecache.Policy{MaxAge: sourcecache.Forever}, nil)
	persistent := probeErr == nil
	if persistent {
		guard.deny.Store(true)
	} else {
		t.Log("raw render persistence unavailable for this build identity")
	}
	t.Logf("persisted renderer hit enabled: %t", persistent)
	before := guard.opened.Load()
	prepared, err := kustomize.Prepare(ks, nil, depwait.NewMissingPolicy(nil))
	assert.NoError(t, err)
	rendered, _, err := testutil.RenderMaps(
		kustomize.Render(
			t.Context(),
			trees,
			kustomize.RenderRequest{Source: &initial, SubPath: ks.Path, Spec: prepared},
			sourcecache.RetryConfig{},
		),
	)
	assert.NoError(t, err)
	for _, doc := range rendered {
		metadata := doc["metadata"].(map[string]any)
		labels, _ := metadata["labels"].(map[string]any)
		{
			_, stamped := labels["kustomize.toolkit.fluxcd.io/name"]
			assert.FatalIf(t, stamped, "%v", "persisted renderer document already stamped")
		}
		metadata["name"] = "caller-mutation"
		if data, ok := doc["data"].(map[string]any); ok {
			data["value"] = "caller-mutation"
		}
	}
	{
		err := reconcile(ks)
		assert.FatalIf(t, err != nil, "raw cache hit: %v", err)
	}
	assert.FatalIf(
		t,
		persistent && guard.opened.Load() != before,
		"%v",
		"expected immutable-source raw cache hit without input reads",
	)
	guard.deny.Store(false)
	cachedOutput := s.GetArtifact(ks.Named(), store.ArtifactContent).(*store.Emission)
	assert.Diff(t, testutil.DocumentMaps(cachedOutput.Manifests), wantManifests)
	assert.Equal(t, len(cachedOutput.Children), len(freshOutput.Children))
	childValues := cachedOutput.Children
	for childIndex := range childValues {
		child := childValues[childIndex]
		if configMap, ok := child.Obj.(*manifest.ConfigMap); ok {
			assert.Equal(t, configMap.Data["value"], any("original"))
		}
	}
	fresh := ks.Clone()
	fresh.NamePrefix = "arbitrary-second-"

	s.AddObject(fresh)
	{
		err := reconcile(fresh)
		assert.FatalIf(t, err != nil, "%v", err)
	}
	old := shared
	old.Name = "first-shared"
	newID := shared
	newID.Name = "arbitrary-second-shared"
	assert.FatalIf(
		t,
		s.GetObject(old) != nil || s.GetObject(newID) == nil,
		"%v",
		"repeated rename retained obsolete output",
	)
	failed := initial
	failed.Key = "failed-pin"
	failed.FS = &guardedSourceFS{FS: input.FS}
	failed.FS.(*guardedSourceFS).deny.Store(true)
	s.SetArtifact(sourceID, &failed)
	{
		err := reconcile(fresh)
		assert.FatalIf(
			t,
			err == nil || s.GetObject(newID) == nil,
			"%v",
			"failed successor withdrew completed output",
		)
	}
	_, _, empty = s.GetEmission(ks.Named())
	empty.Children, empty.Manifests, empty.Complete = nil, nil, true
	s.RecordEmitted(ks.Named(), &empty, nil)
	assert.FatalIf(
		t,
		s.GetObject(newID) != nil || s.GetObject(shared) == nil,
		"%v",
		"complete empty did not retire only its contribution",
	)
}

func promoteSourceCoverage(t *testing.T, s *store.Store, index *manifest.DiscoveryIndex, id manifest.NamedResource) {
	t.Helper()
	productions := s.Productions(id, index)
	for i := range productions {
		production := &productions[i]
		if production.Path == "" {
			continue
		}
		children, err := loader.Promote(t.Context(), s, id, loader.PromoteOptions{Record: *production})
		assert.NoError(t, err)
		for child := range selection.Declarations(children) {
			s.AddObject(child.Obj, child.Doc)
		}
	}

	assert.FatalIf(t, s.GetObject(id) == nil, "missing promoted seed %s", id)
}
