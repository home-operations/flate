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

	"oras.land/oras-go/v2/errdef"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/engine/controllers/base"
	sourcecache "github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/engine/task"
	"github.com/home-operations/flate/internal/engine/tree"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestPreviousRender_SourceBinding(t *testing.T) {
	t.Parallel()
	t.Logf("render persistence reason=%q backing=disk", sourcecache.RenderPersistenceDisabledReason())
	persistent := sourcecache.RenderPersistenceDisabledReason() == ""
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
	content := PrepareShape(&req, nil).Content()
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
			assert.NoError(t, renderer.owner.RecordRender(t.Context(), sourcecache.RenderAcceptance{
				Sources: []manifest.SourceEvidence{},
				Owner:   owner,
				Receipt: receipt,
				Records: emptyRenderInventory,
				Content: content,
			}))
			{
				_, complete := renderer.previousHistory(t.Context(), &req, owner, content)
				assert.Equal(t, complete, persistent)
			}
			changed := req
			source, spec := *req.Source, *req.Spec
			changed.Source, changed.Spec = &source, &spec
			tc.mutate(&changed)
			prepared, err := renderer.prepareRender(t.Context(), &changed)
			assert.NoError(t, err)
			frame := receipt.Value().(renderFrame)
			binding := frame.Inputs
			binding.Key = prepared.key
			frame.Key, frame.Binding = prepared.key, binding.Fingerprint()
			frame.Stored = nil
			wire, err := testEncodeFrame(t, &frame)
			assert.NoError(t, err)
			stored, err := testDecodeFrame(t, wire)
			assert.NoError(t, err)
			accepted, err := testAcceptedFrame(t, &stored, &sourcecache.FramePublicationInput{
				Owner: owner, Content: content, Records: emptyRenderInventory, Complete: true,
			})
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
				_, complete := renderer.previousHistory(t.Context(), &changed, owner, content)
				assert.FatalIf(t, complete, "%s", "foreign source coordinate borrowed history")
			}
		})
	}
	for _, field := range []string{"revision", "url"} {
		t.Run("reuse_"+field, func(t *testing.T) {
			t.Parallel()
			testPreviousRenderSourceBindingReuse(t, &req, field, content)
		})
	}
}

func testPreviousRenderSourceBindingReuse(t *testing.T, req *RenderRequest, field string, content any) {
	t.Helper()
	persistent := sourcecache.RenderPersistenceDisabledReason() == ""
	owner := req.Spec.Named()
	for _, lifetime := range []string{"retained", "transferred"} {
		t.Run(lifetime, func(t *testing.T) {
			t.Parallel()
			renderer := newTestTreeCache(t, t.TempDir())
			var preparation, successor Preparation
			t.Cleanup(func() { preparation = Preparation{} })
			t.Cleanup(func() { successor = Preparation{} })
			var work manifest.WorkRecorder
			work.Init(&manifest.WorkKey{Owner: owner}, manifest.WorkScope{})
			source := *req.Source
			source.Content, source.Revision, source.URL = "equal-tree-content", "before", "https://example.test/before"
			current := *req
			current.Source, current.Preparation, current.Work = &source, &preparation, work.Scope().RenderWork()
			docs, receipt, err := Render(t.Context(), renderer, current, sourcecache.RetryConfig{})
			assert.NoError(t, err)
			output, err := manifest.EncodeRender(docs)
			assert.NoError(t, err)
			prior := receipt.Value().(renderFrame)
			priorOutput := bytes.Clone(prior.Output)
			binding, key := preparation.binding, preparation.key
			assert.NoError(t, renderer.owner.RecordRender(t.Context(), sourcecache.RenderAcceptance{
				Sources: []manifest.SourceEvidence{},
				Owner:   owner, Receipt: receipt, Records: emptyRenderInventory, Content: content,
			}))
			history, complete := renderer.previousHistory(t.Context(), &current, owner, content)
			assert.Equal(t, complete, persistent)
			assert.Equal(t, history == nil, !persistent)
			if lifetime == "transferred" {
				assert.Equal(t, preparation.Matches(renderer, &current), true)
				successor, preparation = preparation, Preparation{}
				assert.Equal(t, preparation.complete || preparation.source != nil, false)
				current.Preparation = &successor
			}
			replacement := source
			if field == "revision" {
				replacement.Revision = "after"
			} else {
				replacement.URL = "https://example.test/after"
			}
			current.Source = &replacement
			_, complete = renderer.previousHistory(t.Context(), &current, owner, content)
			assert.Equal(t, complete, false)
			changed, rebound, err := Render(t.Context(), renderer, current, sourcecache.RetryConfig{})
			assert.NoError(t, err)
			encoded, err := manifest.EncodeRender(changed)
			assert.NoError(t, err)
			assert.Equal(t, bytes.Equal(encoded, output), true)
			frame := rebound.Value().(renderFrame)
			assert.Equal(t, bytes.Equal(frame.Output, priorOutput), true)
			assert.Equal(t, frame.Inputs.Artifact.Revision, replacement.Revision)
			assert.Equal(t, frame.Inputs.Artifact.URL, replacement.URL)
			assert.Equal(t, current.Preparation.key, key)
			assert.Equal(t, current.Preparation.source, &replacement)
			assert.NoError(t, renderer.owner.RecordRender(t.Context(), sourcecache.RenderAcceptance{
				Sources: []manifest.SourceEvidence{},
				Owner:   owner, Receipt: rebound, Records: emptyRenderInventory, Content: content,
			}))
			history, complete = renderer.previousHistory(t.Context(), &current, owner, content)
			assert.Equal(t, complete, persistent)
			assert.Equal(t, history == nil, !persistent)
			unchanged := receipt.Value().(renderFrame)
			assert.Equal(t, unchanged.Inputs.Equal(&binding, equalRenderPolicy), true)
			assert.Equal(t, bytes.Equal(unchanged.Output, priorOutput), true)
			assert.Equal(t, source.Revision, "before")
			assert.Equal(t, source.URL, binding.Artifact.URL)
			var engine manifest.WorkCounts
			rows := work.Scope().Snapshot(0)
			for i := range rows {
				row := &rows[i]
				if row.Key.Phase == manifest.WorkKustomize {
					engine.Add(&row.Counts)
				}
			}
			assert.Equal(t, engine, manifest.WorkCounts{Attempts: 1, Completed: 1})
		})
	}
}

func TestRecordRender_AcceptedLimit(t *testing.T) {
	t.Parallel()
	spec := &manifest.Kustomization{Name: "demo", Namespace: "flux-system", Path: "./"}
	content := PrepareShape(&RenderRequest{Source: &store.SourceArtifact{}, Spec: spec}, nil).Content()
	persistent := sourcecache.RenderPersistenceDisabledReason() == ""
	renderer := newTestTreeCache(t, t.TempDir())
	owner := manifest.NamedResource{Group: "group", Kind: "owner", Namespace: "team", Name: "renderer"}
	payload, err := testutil.EncodeRender([]map[string]any{{"value": "original"}})
	assert.NoError(t, err)
	frame := renderFrame{
		Key: "oversized-accepted", Output: payload,
		Header: renderHeader{Reads: readSetSnapshot{Files: []localEntry{}}},
	}
	receipt := renderDomain.Receipt(frame)
	records := map[manifest.NamedResource]manifest.DiscoveryRecord{
		owner: {File: string(bytes.Repeat([]byte{'x'}, (16<<20)+1))},
	}
	assert.NoError(t, renderer.owner.RecordRender(t.Context(), sourcecache.RenderAcceptance{
		Sources: []manifest.SourceEvidence{},
		Owner:   owner, Receipt: receipt, Records: emptyRenderInventory, Content: content,
	}))
	assert.NoError(t, renderer.owner.RecordRender(t.Context(), sourcecache.RenderAcceptance{
		Sources: []manifest.SourceEvidence{},
		Owner:   owner, Receipt: receipt, Records: maps.All(records), Content: content,
	}))
	generation, _, _, err := renderer.owner.Blob(t.Context(),
		sourcecache.BlobKey{Content: frame.Key, Render: sourcecache.RenderAccepted},
		&sourcecache.Policy{MaxAge: sourcecache.Forever}, nil)
	if !persistent {
		assert.Equal(t, errors.Is(err, errdef.ErrNotFound), true)
		assert.Equal(t, generation == nil, true)
		return
	}
	assert.NoError(t, err)
	stored, valid, err := (*Preparation)(nil).readFrame(t.Context(), generation)
	assert.NoError(t, err)
	assert.Equal(t, valid, true)
	assert.Equal(t, bytes.Equal(stored.Output, payload), true)
	history, complete := stored.Stored.History(owner, content)
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
	persistent := sourcecache.RenderPersistenceDisabledReason() == ""
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
	base.PrepareDocuments(spec, rawDocs)
	initialContent := PrepareShape(&req, rawDocs).Content()
	history := func(request *RenderRequest) (iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord], bool) {
		docs, receipt, found := renderer.PreviousRender(t.Context(), *request)
		if !found {
			return nil, false
		}
		base.PrepareDocuments(request.Spec, docs)
		content := PrepareShape(request, docs).Content()
		return renderer.RenderHistory(spec.Named(), receipt, content)
	}
	assert.NoError(t, renderer.owner.RecordRender(t.Context(), sourcecache.RenderAcceptance{
		Sources: []manifest.SourceEvidence{},
		Owner:   spec.Named(),
		Receipt: pending,
		Records: emptyRenderInventory,
		Content: initialContent,
	}))
	renderer = newTestTreeCache(t, cacheDir)
	_, pending, found := renderer.PreviousRender(t.Context(), req)
	assert.Equal(t, found, persistent)
	if !persistent {
		assert.Equal(t, pending == nil, true)
		_, pending, err = Render(t.Context(), renderer, req, sourcecache.RetryConfig{})
		assert.NoError(t, err)
	}
	before := pending.Value().(renderFrame)
	assert.FatalIf(
		t,
		before.Stored == nil || persistent && before.Lookup == "",
		"%s",
		"warm receipt lacks publication memos",
	)
	_, complete := before.Stored.History(spec.Named(), initialContent)
	assert.Equal(t, complete, persistent)
	beforeWire, err := testEncodeFrame(t, &before)
	assert.NoError(t, err)
	_, _, before.Lookup, err = renderer.owner.Blob(
		t.Context(),
		sourcecache.BlobKey{
			Content: before.Key,
			Render:  sourcecache.RenderAccepted,
		},
		&sourcecache.Policy{MaxAge: sourcecache.Forever},
		nil,
	)
	if persistent {
		assert.NoError(t, err)
	} else {
		assert.Equal(t, errors.Is(err, errdef.ErrNotFound), true)
		assert.Equal(t, before.Lookup, "")
	}
	pending = renderDomain.Receipt(before)
	wire, output := bytes.Clone(beforeWire), bytes.Clone(before.Output)
	projected := req
	projected.Spec = spec.Clone()
	projected.Spec.PostBuild.Substitute["DEP"] = "ready"
	finalContent := PrepareShape(&projected, rawDocs).Content()
	{
		_, complete := history(&projected)
		assert.FatalIf(t, complete, "%s", "unaccepted snapshot supplied history")
	}
	acceptance := sourcecache.RenderAcceptance{
		Sources: []manifest.SourceEvidence{},
		Owner:   spec.Named(),
		Receipt: pending,
		Records: emptyRenderInventory,
		Content: finalContent,
	}
	assert.NoError(t, renderer.owner.RecordRender(t.Context(), acceptance))
	{
		records, complete := history(&projected)
		assert.Equal(t, complete, persistent)
		assert.Equal(t, records != nil, persistent)
	}
	{
		_, complete := history(&req)
		assert.FatalIf(t, complete, "%s", "displaced snapshot retained history")
	}
	original := pending.Value().(renderFrame)
	assert.Equal(t, original.Inputs.Equal(&before.Inputs, equalRenderPolicy), true)
	assert.Equal(t, original.Binding, before.Binding)
	originalPublication, err := original.Stored.PreparePublication(t.Context(), sourcecache.FramePublicationInput{
		Owner: spec.Named(), Content: initialContent, Records: emptyRenderInventory, Complete: true,
	})
	assert.NoError(t, err)
	beforePublication, err := before.Stored.PreparePublication(t.Context(), sourcecache.FramePublicationInput{
		Owner: spec.Named(), Content: initialContent, Records: emptyRenderInventory, Complete: true,
	})
	assert.NoError(t, err)
	assert.Equal(t, originalPublication.Identity(), beforePublication.Identity())
	assert.Equal(t, original.Lookup, before.Lookup)
	assert.Equal(t, original.Stored == before.Stored, true)
	originalWire, err := testEncodeFrame(t, &original)
	assert.NoError(t, err)
	assert.Equal(t, bytes.Equal(originalWire, wire) && bytes.Equal(original.Output, output), true)
	_, rebound, err := Render(t.Context(), renderer, projected, sourcecache.RetryConfig{})
	assert.NoError(t, err)
	after := rebound.Value().(renderFrame)
	assert.Equal(t, after.Key, before.Key)
	assert.Equal(t, bytes.Equal(after.Output, output), true)
	assert.Diff(t, after.Header.Reads, before.Header.Reads)
	index := filepath.Join(cacheDir, "oci", "index.json")
	stamp := time.Unix(1, 0)
	assert.NoError(t, os.Chtimes(index, stamp, stamp))
	acceptance.Receipt = rebound
	assert.NoError(t, renderer.owner.RecordRender(t.Context(), acceptance))
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
	spec := &manifest.Kustomization{Name: "demo", Namespace: "flux-system", Path: "./"}
	content := PrepareShape(&RenderRequest{Source: &store.SourceArtifact{}, Spec: spec}, nil).Content()
	t.Logf("render persistence reason=%q backing=disk", sourcecache.RenderPersistenceDisabledReason())
	root := testutil.WriteTree(
		t,
		map[string]string{"kustomization.yaml": existingKustomization("cm.yaml"), "cm.yaml": cmYAML("before")},
	)
	input := &preparationReadFS{FS: os.DirFS(root)}
	cacheDir := t.TempDir()
	c := newTestTreeCache(t, cacheDir)
	var preparation Preparation
	t.Cleanup(func() { preparation = Preparation{} })
	req := RenderRequest{
		Preparation: &preparation,
		Source:      &store.SourceArtifact{LocalPath: root, FS: input},
		SubPath:     ".",
		Spec:        spec,
	}
	docs, receipt, err := testutil.RenderMaps(Render(t.Context(), c, req, sourcecache.RetryConfig{}))
	assert.NoError(t, err)
	assert.FatalIf(
		t,
		!hasRenderField(docs, "name", "before") || input.prepared.Load() == 0,
		"cold input was not rendered: %v",
		docs,
	)
	assert.NoError(t, c.owner.RecordRender(t.Context(), acceptanceForEmission(
		spec.Named(), acceptedEmission(t, spec.Named(), receipt, content), content,
	)))
	input.prepared.Store(0)
	fresh := newTestTreeCache(t, cacheDir)
	replay, _, err := testutil.RenderMaps(Render(t.Context(), fresh, req, sourcecache.RetryConfig{}))
	assert.NoError(t, err)
	assert.Diff(t, replay, docs)
	assert.Equal(t, input.prepared.Load(), int64(0))
	testutil.WriteFile(t, root, "cm.yaml", cmYAML("after"))
	changed, _, err := testutil.RenderMaps(Render(t.Context(), fresh, req, sourcecache.RetryConfig{}))
	assert.NoError(t, err)
	assert.FatalIf(
		t,
		!hasRenderField(changed, "name", "after") || input.prepared.Load() == 0,
		"changed input reused accepted output: %v",
		changed,
	)
	borrowed := req.Source
	withExtra, err := testutil.SplitDocs([]byte(cmYAML("after") + "---\n" + cmYAML("extra")))
	assert.NoError(t, err)
	for _, tc := range []struct {
		name, initial, resources string
		accepted, want           []map[string]any
		count                    int
	}{
		{"added aggregate resource", "resources: [cm.yaml]\n", "resources: [cm.yaml, added.yaml]\n", changed, withExtra, 2},
		{"removed aggregate resource", "resources: [cm.yaml, added.yaml]\n", "resources: [cm.yaml]\n", withExtra, changed, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			renderer := newTestTreeCache(t, t.TempDir())
			request, prepared := req, preparation
			request.Preparation = &prepared
			overlay := tree.NewBuilder(borrowed.FS)
			assert.NoError(t, overlay.AddFile("kustomization.yaml", []byte(tc.initial), 0o644))
			assert.NoError(t, overlay.AddFile("added.yaml", []byte(cmYAML("extra")), 0o644))
			inputFS, buildErr := overlay.Build()
			assert.NoError(t, buildErr)
			selected := *borrowed
			selected.FS = inputFS
			request.Source = &selected
			accepted, acceptedReceipt, acceptedErr := testutil.RenderMaps(
				Render(t.Context(), renderer, request, sourcecache.RetryConfig{}),
			)
			assert.NoError(t, acceptedErr)
			assert.Diff(t, accepted, tc.accepted)
			assert.NoError(t, renderer.owner.RecordRender(t.Context(), acceptanceForEmission(
				spec.Named(), acceptedEmission(t, spec.Named(), acceptedReceipt, content), content,
			)))
			overlay = tree.NewBuilder(selected.FS)
			assert.NoError(t, overlay.AddFile("kustomization.yaml", []byte(tc.resources), 0o644))
			inputFS, buildErr = overlay.Build()
			assert.NoError(t, buildErr)
			updated := selected
			updated.FS = inputFS
			request.Source = &updated
			output, renderReceipt, renderErr := testutil.RenderMaps(
				Render(t.Context(), renderer, request, sourcecache.RetryConfig{}),
			)
			assert.NoError(t, renderErr)
			assert.Diff(t, output, tc.want)
			assert.Equal(t, len(output), tc.count)
			assert.Equal(t, hasRenderField(output, "name", "extra"), tc.count == 2)
			assert.NoError(t, renderer.owner.RecordRender(t.Context(), acceptanceForEmission(
				spec.Named(), acceptedEmission(t, spec.Named(), renderReceipt, content), content,
			)))
			original, readErr := fs.ReadFile(borrowed.FS, "kustomization.yaml")
			assert.NoError(t, readErr)
			assert.Equal(t, string(original), existingKustomization("cm.yaml"))
		})
	}
}

func TestRecordRender_AcceptanceRequired(t *testing.T) {
	t.Parallel()
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
	spec := &manifest.Kustomization{Name: "demo", Namespace: "flux-system", Path: "./"}
	content := PrepareShape(&RenderRequest{Source: &store.SourceArtifact{}, Spec: spec}, nil).Content()
	persistent := sourcecache.RenderPersistenceDisabledReason() == ""

	root := testutil.WriteTree(t, map[string]string{
		"kustomization.yaml": existingKustomization("cm.yaml"), "cm.yaml": cmYAML("rendered"),
	})
	cache := newTestTreeCache(t, t.TempDir())
	request := RenderRequest{Source: localSource(t, root), Spec: spec, SubPath: "."}
	_, receipt, err := Render(t.Context(), cache, request, sourcecache.RetryConfig{})
	assert.NoError(t, err)
	st := store.New()
	st.AddObject(spec)
	provider := &manifest.ConfigMap{
		Name:      "provider",
		Namespace: spec.Namespace,
		Data:      map[string]any{"VALUE": "first"},
	}
	st.AddObject(provider)
	reads := &store.ReadSet{}
	reads.Init(st, spec.Named(), nil)
	defer reads.Close()
	reads.GetObject(provider.Named())
	emission := &store.Emission{
		Fingerprint: manifest.Fingerprint(content),
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
			errors.Is(controllertest.Emit(ctx, controller, spec.Named(), emission), context.Canceled),
			true,
		)
	} else if (*testCaseInput) != "unemitted" {
		assert.NoError(t, controllertest.Emit(t.Context(), controller, spec.Named(), emission))
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
	acceptance := acceptanceForEmission(spec.Named(), emission, content)
	assert.Equal(
		t,
		acceptance.Content == nil,
		(*testCaseInput) == "missing fingerprint" || (*testCaseInput) == "incomplete",
	)
	assert.NoError(t, cache.owner.RecordRender(t.Context(), acceptance))
	_, complete := cache.previousHistory(t.Context(), &request, spec.Named(), content)
	assert.Equal(t, complete, persistent && (*testCaseInput) == "accepted")
}
