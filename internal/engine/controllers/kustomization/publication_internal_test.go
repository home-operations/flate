package kustomization

import (
	"context"
	"io/fs"
	"maps"
	"slices"
	"sync"
	"testing"

	"github.com/home-operations/flate/internal/engine/controllers/base"
	"github.com/home-operations/flate/internal/engine/kustomize"
	sourcecache "github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

//nolint:containedctx // The wrapper invalidates authority when publication checks its context.
type publicationInvalidationContext struct {
	context.Context

	once       sync.Once
	invalidate func()
}

func (c *publicationInvalidationContext) Err() error {
	c.once.Do(c.invalidate)
	return c.Context.Err()
}

func TestRecordRender_CapturedEmission(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		branch  string
		capture string
	}{
		{branch: "emission", capture: "reads"},
		{branch: "emission", capture: "output reads"},
		{branch: "deduplication", capture: "reads"},
		{branch: "deduplication", capture: "output reads"},
	} {
		t.Run(tc.branch+"/"+tc.capture, func(t *testing.T) {
			t.Parallel()
			c, st, _ := newControllerWithFixture(t)
			c.Cache = cachetest.New(t, t.TempDir(), sourcecache.New)
			c.Trees = kustomize.NewTreeCache(c.Cache, nil, c.Tasks)
			t.Cleanup(c.Trees.Close)
			artifact := *st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
			artifact.Kind, artifact.URL = manifest.KindGitRepository, "https://fixture.invalid/first"
			st.SetArtifact(manifest.BootstrapSourceID, &artifact)
			owner := &manifest.Kustomization{Name: "renderer", Namespace: "flux-system", Path: "./apps"}
			st.AddObject(owner)
			first := c.newExecution(owner.Named())
			defer first.run.Close()
			first.run.Reads.GetObject(owner.Named())
			request := kustomize.RenderRequest{
				Spec:    owner,
				SubPath: owner.Path,
				Source:  first.run.Reads.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent|store.ArtifactRevision|store.ArtifactOrigin).(*store.SourceArtifact),
			}
			docs, receipt, err := kustomize.Render(t.Context(), c.Trees, request, sourcecache.RetryConfig{})
			assert.NoError(t, err)
			publisher, ok := receipt.Value().(interface {
				RenderPublication(context.Context, *sourcecache.RenderAcceptance) (sourcecache.RenderPublication, error)
			})
			assert.Equal(t, ok, true)
			base.PrepareDocuments(owner, docs)
			content := kustomize.PrepareShape(&request, docs).Content()
			projection, err := publisher.RenderPublication(
				t.Context(),
				&sourcecache.RenderAcceptance{Content: content, Sources: []manifest.SourceEvidence{}},
			)
			assert.NoError(t, err)
			acceptance := sourcecache.RenderAcceptance{
				Owner:   owner.Named(),
				Content: content,
				Source:  request.Source.HistoryIdentity(manifest.BootstrapSourceID),
			}
			parsed := first.run.ParseChildren(owner, docs, manifest.ParseDocOptions{})
			initial := store.Emission{
				Children: parsed.Children, Manifests: parsed.Visible, Failures: parsed.Failures,
				Render: receipt, Fingerprint: manifest.Fingerprint(content), Complete: true,
			}
			assert.NoError(t, first.run.Emit(t.Context(), owner.Named(), &initial, nil))
			assert.NoError(t, first.recordRender(t.Context(), &initial, &acceptance))
			priorSources := initial.SourcePaths()
			key := sourcecache.BlobKey{Content: projection.Frame.Key, Render: sourcecache.RenderAccepted}
			var before []byte
			if c.Cache.CanPersistRenders() {
				generation, _, _, lookupErr := c.Cache.Blob(
					t.Context(),
					key,
					&sourcecache.Policy{MaxAge: sourcecache.Forever},
					nil,
				)
				assert.NoError(t, lookupErr)
				before, err = fs.ReadFile(generation, "content")
				assert.NoError(t, err)
			}

			replacement := artifact
			replacement.URL = "https://fixture.invalid/current"
			st.SetArtifact(manifest.BootstrapSourceID, &replacement)
			current := c.newExecution(owner.Named())
			defer current.run.Close()
			current.run.Reads.GetObject(owner.Named())
			request.Source = current.run.Reads.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent|store.ArtifactRevision|store.ArtifactOrigin).(*store.SourceArtifact)
			docs, receipt, err = kustomize.Render(t.Context(), c.Trees, request, sourcecache.RetryConfig{})
			acceptance.Source = request.Source.HistoryIdentity(manifest.BootstrapSourceID)
			assert.NoError(t, err)
			attempt := store.Emission{Render: receipt}
			if tc.branch == "deduplication" {
				result, dedupErr := current.run.FingerprintDedup(t.Context(), owner.Named(), content, &attempt)
				assert.NoError(t, dedupErr)
				assert.Equal(t, result.Reused, true)
			} else {
				parsed = current.run.ParseChildren(owner, docs, manifest.ParseDocOptions{})
				attempt.Children, attempt.Manifests, attempt.Failures = parsed.Children, parsed.Visible, parsed.Failures
				attempt.Fingerprint, attempt.Complete = manifest.Fingerprint(content), true
				assert.NoError(t, current.run.Emit(t.Context(), owner.Named(), &attempt, nil))
			}
			defer attempt.Finish()
			assert.Equal(t, attempt.Footprint() != nil, true)
			assert.Equal(t, attempt.SourcePaths() != nil, true)
			allSources := append(slices.Clone(priorSources), attempt.SourcePaths()...)
			provenance, err := c.Cache.ProducersForPaths(t.Context(), allSources)
			assert.NoError(t, err)
			if c.Cache.CanPersistRenders() {
				assert.Equal(t, len(provenance[owner.Named()]) > 0, true)
			}
			dependency := &manifest.ConfigMap{
				Name:      "authority",
				Namespace: "apps",
				Data:      map[string]any{"value": "initial"},
			}
			st.AddObject(dependency)
			var captured store.ReadSet
			captured.Init(st, owner.Named(), nil)
			defer captured.Retire()
			captured.GetObject(dependency.Named())
			if tc.capture == "reads" {
				attempt.Reads = &captured
			} else {
				attempt.OutputReads = &captured
			}
			ctx := &publicationInvalidationContext{Context: t.Context(), invalidate: func() {
				changed := *dependency
				changed.Data = map[string]any{"value": "changed"}
				st.AddObject(&changed)
			}}
			assert.NoError(t, current.recordRender(ctx, &attempt, &acceptance))
			assert.Equal(t, captured.Valid(), false)
			afterProvenance, err := c.Cache.ProducersForPaths(t.Context(), allSources)
			assert.NoError(t, err)
			assert.Diff(t, afterProvenance, provenance)
			if c.Cache.CanPersistRenders() {
				generation, _, _, lookupErr := c.Cache.Blob(
					t.Context(),
					key,
					&sourcecache.Policy{MaxAge: sourcecache.Forever},
					nil,
				)
				assert.NoError(t, lookupErr)
				after, readErr := fs.ReadFile(generation, "content")
				assert.NoError(t, readErr)
				assert.Diff(t, after, before)
				var metadata any
				stored, openErr := sourcecache.OpenRenderFrame(
					t.Context(),
					"kustomize-render",
					generation,
					&metadata,
				)
				assert.NoError(t, openErr)
				records, complete := stored.Stored.History(owner.Named(), content)
				assert.Equal(t, complete, true)
				assert.Equal(t, len(maps.Collect(records)), 1)
				attempt.Reads, attempt.OutputReads = &current.run.Reads, &current.run.Reads
				assert.NoError(t, current.recordRender(t.Context(), &attempt, &acceptance))
				generation, _, _, lookupErr = c.Cache.Blob(
					t.Context(),
					key,
					&sourcecache.Policy{MaxAge: sourcecache.Forever},
					nil,
				)
				assert.NoError(t, lookupErr)
				accepted, readErr := fs.ReadFile(generation, "content")
				assert.NoError(t, readErr)
				assert.Equal(t, string(accepted) != string(before), true)
			}
		})
	}
}
