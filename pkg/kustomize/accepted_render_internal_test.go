package kustomize

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"iter"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestPreviousRender_SourceBinding(t *testing.T) {
	t.Parallel()
	if !renderPersistenceEligible(t) {
		t.Skip("requires a clean VCS-stamped build for positive persistence")
	}
	root := testutil.WriteTree(
		t,
		map[string]string{"kustomization.yaml": existingKustomization("cm.yaml"), "cm.yaml": cmYAML("original")},
	)
	req := RenderRequest{
		Source: localSource(t, root),
		Spec: &manifest.Kustomization{
			Name: "emitter", Path: ".",
			SourceRef: kustomizev1.CrossNamespaceSourceReference{
				Kind: manifest.KindGitRepository, Name: "content", Namespace: "sources",
			},
		},
		SubPath: ".", Label: root,
	}
	owner := req.Spec.Named()
	for _, tc := range []struct {
		name   string
		mutate func(*RenderRequest)
	}{
		{"source name", func(r *RenderRequest) { r.Spec.SourceRef.Name = "foreign" }},
		{"source namespace", func(r *RenderRequest) { r.Spec.SourceRef.Namespace = "foreign" }},
		{"source kind", func(r *RenderRequest) { r.Spec.SourceRef.Kind = manifest.KindBucket }},
		{"artifact kind", func(r *RenderRequest) { r.Source.Kind = "foreign" }},
		{"url", func(r *RenderRequest) { r.Source.URL = "foreign" }},
		{"path", func(r *RenderRequest) { r.Source.LocalPath = "foreign" }},
		{"origin", func(r *RenderRequest) { r.Source.LocalRoot = "foreign" }},
		{"key", func(r *RenderRequest) { r.Source.Key = "foreign" }},
		{"revision", func(r *RenderRequest) { r.Source.Revision = "foreign" }},
		{"digest", func(r *RenderRequest) { r.Source.Digest = "foreign" }},
		{"alias", func(r *RenderRequest) { r.Source.InputAlias = !r.Source.InputAlias }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			renderer := newTestTreeCache(t, t.TempDir())
			_, receipt, err := testutil.RenderMaps(Render(t.Context(), renderer, req, sourcecache.RetryConfig{}))
			assert.NoError(t, err)
			assert.NoError(t, renderer.RecordRender(t.Context(), RenderAcceptance{
				Owner:   owner,
				Receipt: receipt,
				Records: emptyRenderInventory,
				Content: acceptedInput,
			}))
			{
				_, complete := previousHistory(t.Context(), renderer, &req, owner)
				assert.FatalIf(t, !complete, "%s", "matching source binding missed")
			}
			changed := req
			source, spec := *req.Source, *req.Spec
			changed.Source, changed.Spec = &source, &spec
			tc.mutate(&changed)
			prepared, err := renderer.prepareRender(t.Context(), &changed)
			assert.NoError(t, err)
			frame := receipt.Value().(renderFrame)
			binding := frame.binding
			binding.Render = prepared.key
			header := frame.header
			header.Key, header.Binding = prepared.key, manifest.Fingerprint(binding)
			wire, err := testEncodeFrame(t, &header, frame.output)
			assert.NoError(t, err)
			stored, err := testDecodeFrame(t, wire)
			assert.NoError(t, err)
			accepted, err := testAcceptedFrame(t, owner, &stored, emptyRenderInventory)
			assert.NoError(t, err)
			_, _, _, err = renderer.owner.Blob(
				t.Context(),
				sourcecache.BlobKey{
					Content: prepared.key,
					Render:  sourcecache.RenderAccepted,
				},
				&sourcecache.Policy{MaxAge: 0},
				func(_ context.Context, w sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
					_, copyErr := io.Copy(w, bytes.NewReader(accepted))
					return sourcecache.ProducerResult{}, copyErr
				},
			)
			assert.NoError(t, err)

			{
				_, complete := previousHistory(t.Context(), renderer, &changed, owner)
				assert.FatalIf(t, complete, "%s", "foreign source coordinate borrowed history")
			}
		})
	}
}

func TestRecordRender_AcceptedLimit(t *testing.T) {
	t.Parallel()
	if !renderPersistenceEligible(t) {
		t.Skip("requires a clean VCS-stamped build for positive persistence")
	}
	renderer := newTestTreeCache(t, t.TempDir())
	owner := manifest.NamedResource{Group: "group", Kind: "owner", Namespace: "team", Name: "renderer"}
	payload, err := testutil.EncodeRender([]map[string]any{{"value": "original"}})
	assert.NoError(t, err)
	frame := renderFrame{header: renderHeader{Key: "oversized-accepted"}, output: payload}
	receipt := manifest.NewRenderReceipt(frame, equalRenderFrame)
	records := map[manifest.NamedResource]manifest.DiscoveryRecord{
		owner: {File: string(bytes.Repeat([]byte{'x'}, (16<<20)+1))},
	}
	assert.NoError(t, renderer.RecordRender(t.Context(), RenderAcceptance{
		Owner: owner, Receipt: receipt, Records: emptyRenderInventory, Content: acceptedInput,
	}))
	assert.NoError(t, renderer.RecordRender(t.Context(), RenderAcceptance{
		Owner: owner, Receipt: receipt, Records: maps.All(records), Content: acceptedInput,
	}))
	generation, _, _, err := renderer.owner.Blob(t.Context(),
		sourcecache.BlobKey{Content: frame.header.Key, Render: sourcecache.RenderAccepted},
		&sourcecache.Policy{MaxAge: sourcecache.Forever}, nil)
	assert.NoError(t, err)
	stored, valid, err := (*Preparation)(nil).readFrame(t.Context(), generation)
	assert.NoError(t, err)
	assert.Equal(t, valid, true)
	assert.Equal(t, bytes.Equal(stored.output, payload), true)
	history, complete := stored.stored.History(owner, acceptedInput)
	assert.Equal(t, complete, true)
	assert.Diff(t, maps.Collect(history), map[manifest.NamedResource]manifest.DiscoveryRecord{})
}

func TestRenderKey_DependsOnProjection(t *testing.T) {
	t.Parallel()
	root := testutil.WriteTree(
		t,
		map[string]string{"kustomization.yaml": existingKustomization("cm.yaml"), "cm.yaml": cmYAML("original")},
	)
	spec := &manifest.Kustomization{Name: "emitter", Namespace: "apps", Path: "."}
	spec.DependsOn = []kustomizev1.DependencyReference{{Name: "${PREFIX:-dev-}ready", Namespace: "${TEAM:-apps}"}}
	req := RenderRequest{Source: localSource(t, root), Spec: spec, SubPath: "."}
	parent := &manifest.Kustomization{
		PostBuild: &kustomizev1.PostBuild{Substitute: map[string]string{"PREFIX": "prod-", "TEAM": "control"}},
	}
	projected := req
	projected.Spec = shapeDependency(t, parent, spec).Object.(*manifest.Kustomization)
	cache := newTestTreeCache(t, t.TempDir())
	before, err := cache.prepareRender(t.Context(), &req)
	assert.NoError(t, err)
	after, err := cache.prepareRender(t.Context(), &projected)
	assert.NoError(t, err)
	assert.Equal(t, before.key, after.key)
	assert.Equal(t, spec.DependsOn[0].Name, "${PREFIX:-dev-}ready")
	for _, change := range []struct {
		name   string
		mutate func(*manifest.Kustomization)
	}{
		{"path", func(k *manifest.Kustomization) { k.Path = "other" }},
		{"name", func(k *manifest.Kustomization) { k.Name = "other" }},
		{"namespace", func(k *manifest.Kustomization) { k.Namespace = "other" }},
		{"target namespace", func(k *manifest.Kustomization) { k.TargetNamespace = "other" }},
		{"inline", func(k *manifest.Kustomization) {
			k.PostBuild = &kustomizev1.PostBuild{Substitute: map[string]string{"VALUE": "other"}}
		}},
	} {
		t.Run(change.name, func(t *testing.T) {
			t.Parallel()
			changed := req
			changed.Spec = spec.Clone()
			change.mutate(changed.Spec)
			next, err := cache.prepareRender(t.Context(), &changed)
			assert.NoError(t, err)
			assert.FatalIf(t, manifest.ValueEqual(
				PrepareShape(&req, []manifest.DocOccurrence{{Document: map[string]any{"value": "${VALUE}"}}}).Content(),
				PrepareShape(
					&changed,
					[]manifest.DocOccurrence{{Document: map[string]any{"value": "${VALUE}"}}},
				).Content(),
			), "%s", "accepted render change retained identity")
			assert.Equal(t, next.key == before.key, change.name == "inline")
		})
	}
}

func TestRecordRender_FinalDependencyBinding(t *testing.T) {
	t.Parallel()
	if !renderPersistenceEligible(t) {
		t.Skip("requires a clean VCS-stamped build for positive persistence")
	}
	root := testutil.WriteTree(
		t,
		map[string]string{"kustomization.yaml": existingKustomization("cm.yaml"), "cm.yaml": cmYAML("${DEP}")},
	)
	cacheDir := t.TempDir()
	renderer := newTestTreeCache(t, cacheDir)
	spec := &manifest.Kustomization{
		Name:      "owner",
		Namespace: "apps",
		Path:      ".",
		PostBuild: &kustomizev1.PostBuild{Substitute: map[string]string{"DEP": "before"}},
		DependsOn: []kustomizev1.DependencyReference{{Name: "${DEP}"}},
	}
	req := RenderRequest{Source: localSource(t, root), Spec: spec, SubPath: "."}
	rawDocs, pending, err := Render(t.Context(), renderer, req, sourcecache.RetryConfig{})
	assert.NoError(t, err)
	initialContent := PrepareShape(&req, rawDocs).Content()
	history := func(request *RenderRequest) (iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord], bool) {
		docs, receipt, found := renderer.PreviousRender(t.Context(), *request)
		if !found {
			return nil, false
		}
		content := PrepareShape(request, docs).Content()
		return renderer.RenderHistory(spec.Named(), receipt, content)
	}
	assert.NoError(t, renderer.RecordRender(t.Context(), RenderAcceptance{
		Owner:   spec.Named(),
		Receipt: pending,
		Records: emptyRenderInventory,
		Content: initialContent,
	}))
	renderer = newTestTreeCache(t, cacheDir)
	_, pending, found := renderer.PreviousRender(t.Context(), req)
	assert.Equal(t, found, true)
	before := pending.Value().(renderFrame)
	assert.FatalIf(
		t,
		before.stored == nil || before.lookup == "",
		"%s",
		"warm receipt lacks publication memos",
	)
	_, complete := before.stored.History(spec.Named(), initialContent)
	assert.Equal(t, complete, true)
	beforeWire, err := testEncodeFrame(t, &before.header, before.output)
	assert.NoError(t, err)
	_, _, before.lookup, err = renderer.owner.Blob(
		t.Context(),
		sourcecache.BlobKey{
			Content: before.header.Key,
			Render:  sourcecache.RenderAccepted,
		},
		&sourcecache.Policy{MaxAge: sourcecache.Forever},
		nil,
	)
	assert.NoError(t, err)
	pending = manifest.NewRenderReceipt(before, equalRenderFrame)
	wire, output := bytes.Clone(beforeWire), bytes.Clone(before.output)
	projected := req
	projected.Spec = spec.Clone()
	projected.Spec.PostBuild.Substitute["DEP"] = "ready"
	finalContent := PrepareShape(&projected, rawDocs).Content()
	{
		_, complete := history(&projected)
		assert.FatalIf(t, complete, "%s", "unaccepted snapshot supplied history")
	}
	acceptance := RenderAcceptance{
		Owner:   spec.Named(),
		Receipt: pending,
		Records: emptyRenderInventory,
		Content: finalContent,
	}
	assert.NoError(t, renderer.RecordRender(t.Context(), acceptance))
	{
		records, complete := history(&projected)
		assert.FatalIf(t, !complete ||
			records == nil, "%s", "final projected binding missed accepted history")
	}
	{
		_, complete := history(&req)
		assert.FatalIf(t, complete, "%s", "displaced snapshot retained history")
	}
	original := pending.Value().(renderFrame)
	assert.Equal(t, original.binding.equal(&before.binding), true)
	assert.Equal(t, original.header.Binding, before.header.Binding)
	originalPublication, err := original.stored.PreparePublication(t.Context(), sourcecache.FramePublicationInput{
		Owner: spec.Named(), Content: acceptedInput, Records: emptyRenderInventory, Complete: true,
	})
	assert.NoError(t, err)
	beforePublication, err := before.stored.PreparePublication(t.Context(), sourcecache.FramePublicationInput{
		Owner: spec.Named(), Content: acceptedInput, Records: emptyRenderInventory, Complete: true,
	})
	assert.NoError(t, err)
	assert.Equal(t, originalPublication.Identity(), beforePublication.Identity())
	assert.Equal(t, original.lookup, before.lookup)
	assert.Equal(t, original.stored == before.stored, true)
	originalWire, err := testEncodeFrame(t, &original.header, original.output)
	assert.NoError(t, err)
	assert.Equal(t, bytes.Equal(originalWire, wire) && bytes.Equal(original.output, output), true)
	_, rebound, err := Render(t.Context(), renderer, projected, sourcecache.RetryConfig{})
	assert.NoError(t, err)
	after := rebound.Value().(renderFrame)
	assert.Equal(t, after.header.Key, before.header.Key)
	assert.Equal(t, bytes.Equal(after.output, output), true)
	assert.Diff(t, after.header.Reads, before.header.Reads)
	index := filepath.Join(cacheDir, "oci", "index.json")
	stamp := time.Unix(1, 0)
	assert.NoError(t, os.Chtimes(index, stamp, stamp))
	acceptance.Receipt = rebound
	assert.NoError(t, renderer.RecordRender(t.Context(), acceptance))
	info, err := os.Stat(index)
	assert.NoError(t, err)
	assert.Equal(t, info.ModTime().Equal(stamp), true)
}

type preparationReadFS struct {
	fs.FS

	prepared atomic.Int64
}

func (f *preparationReadFS) Open(name string) (fs.File, error) {
	var pcs [48]uintptr
	frames := runtime.CallersFrames(pcs[:runtime.Callers(1, pcs[:])])
	for {
		frame, more := frames.Next()
		if strings.HasSuffix(frame.Function, "/kustomize.generateManifest") ||
			strings.HasSuffix(frame.Function, "/kustomize.preflightRemoteResources") {
			f.prepared.Add(1)
			break
		}
		if !more {
			break
		}
	}
	return f.FS.Open(name)
}

func TestRender_AcceptedMutableInputPreparation(t *testing.T) {
	t.Parallel()
	if !renderPersistenceEligible(t) {
		t.Skip("requires clean VCS-stamped accepted render persistence")
	}
	root := testutil.WriteTree(
		t,
		map[string]string{"kustomization.yaml": existingKustomization("cm.yaml"), "cm.yaml": cmYAML("before")},
	)
	input := &preparationReadFS{FS: os.DirFS(root)}
	cacheDir := t.TempDir()
	c := newTestTreeCache(t, cacheDir)
	var preparation Preparation
	t.Cleanup(preparation.Retire)
	req := RenderRequest{
		Preparation: &preparation,
		Source:      &store.SourceArtifact{LocalPath: root, FS: input},
		SubPath:     ".",
		Spec:        demoSpec,
	}
	docs, receipt, err := testutil.RenderMaps(Render(t.Context(), c, req, sourcecache.RetryConfig{}))
	assert.NoError(t, err)
	assert.FatalIf(
		t,
		!hasRenderField(docs, "name", "before") || input.prepared.Load() == 0,
		"cold input was not rendered: %v",
		docs,
	)
	assert.NoError(t, c.RecordRender(t.Context(), acceptanceForEmission(
		demoSpec.Named(), acceptedEmission(t, demoSpec.Named(), receipt, nil),
	)))
	input.prepared.Store(0)
	fresh := newTestTreeCache(t, cacheDir)
	replay, _, err := testutil.RenderMaps(Render(t.Context(), fresh, req, sourcecache.RetryConfig{}))
	assert.NoError(t, err)
	assert.Diff(t, replay, docs)
	assert.FatalIf(t, input.prepared.Load() != 0, "validated replay prepared inputs %d times", input.prepared.Load())
	testutil.WriteFile(t, root, "cm.yaml", cmYAML("after"))
	changed, _, err := testutil.RenderMaps(Render(t.Context(), fresh, req, sourcecache.RetryConfig{}))
	assert.NoError(t, err)
	assert.FatalIf(
		t,
		!hasRenderField(changed, "name", "after") || input.prepared.Load() == 0,
		"changed input reused accepted output: %v",
		changed,
	)
}

func TestRecordRender_AcceptanceRequired(t *testing.T) {
	t.Parallel()
	if !renderPersistenceEligible(t) {
		t.Skip("requires identified accepted render persistence")
	}
	for _, state := range []string{"unemitted", "failed", "stale", "incomplete", "missing fingerprint", "accepted"} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			testRecordRenderAcceptanceRequiredCase(t, &state)
		},
		)
	}
}

func testRecordRenderAcceptanceRequiredCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	root := testutil.WriteTree(t, map[string]string{
		"kustomization.yaml": existingKustomization("cm.yaml"), "cm.yaml": cmYAML("rendered"),
	})
	cache := newTestTreeCache(t, t.TempDir())
	request := RenderRequest{Source: localSource(t, root), Spec: demoSpec, SubPath: "."}
	_, receipt, err := Render(t.Context(), cache, request, sourcecache.RetryConfig{})
	assert.NoError(t, err)
	st := store.New()
	st.AddObject(demoSpec)
	provider := &manifest.ConfigMap{
		Name:      "provider",
		Namespace: demoSpec.Namespace,
		Data:      map[string]any{"VALUE": "first"},
	}
	st.AddObject(provider)
	reads := &store.ReadSet{}
	reads.Init(st, demoSpec.Named(), nil)
	defer reads.Close()
	reads.GetObject(provider.Named())
	emission := &store.Emission{
		Fingerprint: manifest.Fingerprint(acceptedInput),
		Render:      receipt,
		Complete:    (*testCaseInput) != "incomplete",
	}
	reads.Bind(emission)
	controller := base.New(st, task.NewBounded(2))
	if (*testCaseInput) == "failed" {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		assert.Equal(
			t,
			errors.Is(controllertest.Emit(ctx, controller, demoSpec.Named(), emission), context.Canceled),
			true,
		)
	} else if (*testCaseInput) != "unemitted" {
		assert.NoError(t, controllertest.Emit(t.Context(), controller, demoSpec.Named(), emission))
	}
	if (*testCaseInput) == "stale" {
		next := *provider
		next.Data = map[string]any{"VALUE": "second"}
		st.AddObject(&next)
		assert.Equal(t, reads.Valid(), false)
	}
	if (*testCaseInput) == "missing fingerprint" {
		emission.Fingerprint = ""
	}
	assert.NoError(t, cache.RecordRender(t.Context(), acceptanceForEmission(demoSpec.Named(), emission)))
	_, complete := previousHistory(t.Context(), cache, &request, demoSpec.Named())
	assert.Equal(t, complete, (*testCaseInput) == "accepted")
}
