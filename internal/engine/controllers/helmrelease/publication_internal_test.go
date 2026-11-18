package helmrelease

import (
	"context"
	"io/fs"
	"maps"
	"os"
	"slices"
	"sync"
	"testing"

	"github.com/home-operations/flate/internal/engine/helm"
	sourcecache "github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
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
			testRecordRenderCapturedEmission(t, tc.branch, tc.capture)
		})
	}
}

func testRecordRenderCapturedEmission(t *testing.T, branch, capture string) {
	t.Helper()
	c, st := newTestController(t, nil)
	c.Cache = cachetest.New(t, t.TempDir(), sourcecache.New)
	c.Helm = helm.NewClient(c.Cache)
	root := testutil.WriteTree(t, map[string]string{
		"chart/Chart.yaml": "apiVersion: v2\nname: fixture\nversion: 1.0.0\n",
		"chart/templates/child.yaml": testutil.KustomizationYAML(
			"output", "apps", "  path: .\n  sourceRef: {kind: GitRepository, name: charts, namespace: apps}\n",
		),
	})
	source := &manifest.GitRepository{Name: "charts", Namespace: "apps"}
	artifact := &store.SourceArtifact{
		Kind:      manifest.KindGitRepository,
		URL:       "https://fixture.invalid/first",
		LocalPath: root,
		FS:        os.DirFS(root),
	}
	owner := &manifest.HelmRelease{
		Name:      "renderer",
		Namespace: "apps",
		Chart:     manifest.HelmChart{Name: "chart", Source: source.Named()},
	}
	for _, object := range []manifest.BaseManifest{source, owner} {
		st.AddObject(object)
	}
	st.SetArtifact(source.Named(), artifact)
	first := c.newExecution(owner.Named())
	defer first.run.Close()
	first.run.Reads.GetObject(owner.Named())
	docs, receipt, err := c.Helm.Template(
		t.Context(),
		owner,
		nil,
		helm.RenderOptions{Sources: helm.NewSources(&first.run.Reads)},
	)
	assert.NoError(t, err)
	publisher, ok := receipt.Value().(interface {
		RenderPublication(context.Context, *sourcecache.RenderAcceptance) (sourcecache.RenderPublication, error)
	})
	assert.Equal(t, ok, true)
	projection, err := publisher.RenderPublication(
		t.Context(),
		&sourcecache.RenderAcceptance{Sources: []manifest.SourceEvidence{}},
	)
	assert.NoError(t, err)
	content := owner.Content(projection.Frame.Key, nil)
	parsed := first.run.ParseChildren(owner, docs, manifest.ParseDocOptions{})
	initial := store.Emission{
		Children: parsed.Children, Manifests: parsed.Visible, Failures: parsed.Failures,
		Render: receipt, Fingerprint: manifest.Fingerprint(content), Complete: true,
	}
	assert.NoError(t, first.run.Emit(t.Context(), owner.Named(), &initial, nil))
	assert.NoError(t, first.recordRender(t.Context(), owner.Named(), &initial))
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

	replacement := *artifact
	replacement.URL = "https://fixture.invalid/current"
	st.SetArtifact(source.Named(), &replacement)
	current := c.newExecution(owner.Named())
	defer current.run.Close()
	current.run.Reads.GetObject(owner.Named())
	docs, receipt, err = c.Helm.Template(
		t.Context(),
		owner,
		nil,
		helm.RenderOptions{Sources: helm.NewSources(&current.run.Reads)},
	)
	assert.NoError(t, err)
	attempt := store.Emission{Render: receipt}
	if branch == "deduplication" {
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
	if capture == "reads" {
		attempt.Reads = &captured
	} else {
		attempt.OutputReads = &captured
	}
	ctx := &publicationInvalidationContext{Context: t.Context(), invalidate: func() {
		changed := *dependency
		changed.Data = map[string]any{"value": "changed"}
		st.AddObject(&changed)
	}}
	assert.NoError(t, current.recordRender(ctx, owner.Named(), &attempt))
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
		stored, openErr := sourcecache.OpenRenderFrame(t.Context(), "helm-output", generation, &metadata)
		assert.NoError(t, openErr)
		records, complete := stored.Stored.History(owner.Named(), content)
		assert.Equal(t, complete, true)
		assert.Equal(t, len(maps.Collect(records)), 1)
		attempt.Reads, attempt.OutputReads = &current.run.Reads, &current.run.Reads
		assert.NoError(t, current.recordRender(t.Context(), owner.Named(), &attempt))
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
}
