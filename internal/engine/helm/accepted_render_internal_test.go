package helm

import (
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

const acceptedRenderAnnotation = "flate.accepted-render"

type historyCountingFS struct {
	fs.FS

	opens atomic.Int64
}

func (f *historyCountingFS) Open(name string) (fs.File, error) {
	f.opens.Add(1)
	return f.FS.Open(name)
}

type historyCountingSources struct {
	sourceReader

	calls atomic.Int64
}

func (s *historyCountingSources) GetArtifact(id manifest.NamedResource, read store.ArtifactRead) store.Artifact {
	s.calls.Add(1)
	return s.sourceReader.GetArtifact(id, read)
}

func TestPrepareTemplate_InputGeneration(t *testing.T) {
	t.Parallel()
	for _, first := range []string{"history", "render"} {
		t.Run(first, func(t *testing.T) {
			t.Parallel()
			testPrepareTemplateInputGeneration(t, first)
		})
	}
}

func testPrepareTemplateInputGeneration(t *testing.T, first string) {
	t.Helper()
	root := testutil.WriteTree(
		t,
		map[string]string{
			"outer.yaml":         "marker: first\n",
			"mychart/Chart.yaml": "apiVersion: v2\nname: mychart\nversion: 0.1.0\n",
			"mychart/templates/cm.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: first}\ndata: {marker: " +
				"{{ .Values.marker | quote }}}\n",
		},
	)
	client := NewClient(cachetest.New(t, t.TempDir(), cache.New))
	hr := newHR()
	hr.ChartValuesFiles = []string{"outer.yaml"}
	var recorder manifest.WorkRecorder
	recorder.Init(&manifest.WorkKey{Owner: hr.Named()}, manifest.WorkScope{})
	var preparation Preparation
	defer func() { preparation = Preparation{} }()
	input := RenderOptions{
		Preparation: &preparation,
		Sources:     localChartResolver(t, "chart-repo", "flux-system", root),
		Work:        recorder.Scope().RenderWork(),
	}
	if first == "history" {
		client.PreviousRender(t.Context(), hr, nil, input)
	}
	initial, _, err := client.Template(t.Context(), hr, nil, input)
	assert.NoError(t, err)
	client.PreviousRender(t.Context(), hr, nil, input)
	_, _, err = client.Template(t.Context(), hr, nil, input)
	assert.NoError(t, err)
	testutil.WriteFile(t, root, "outer.yaml", "marker: second\n")
	hr = hr.Clone()
	input.Work = recorder.Scope().RenderWork()
	observed, _, err := client.Template(t.Context(), hr, nil, input)
	assert.NoError(t, err)
	assert.FatalIf(
		t,
		manifest.ValueEqual(initial, observed),
		"%v",
		"source values outside the chart reused stale bytes",
	)
	testutil.WriteFile(
		t,
		root,
		"mychart/templates/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: second}\n",
	)
	hr = hr.Clone()
	input.Work = recorder.Scope().RenderWork()
	changed, _, err := client.Template(t.Context(), hr, nil, input)
	assert.NoError(t, err)
	assert.FatalIf(t, manifest.ValueEqual(initial, changed), "%v", "mutable chart reused stale bytes")
	hr = hr.Clone()
	input.Work = recorder.Scope().RenderWork()
	supplied := map[string]any{"answer": "one"}
	_, _, err = client.Template(t.Context(), hr, supplied, input)
	assert.NoError(t, err)
	priorKey := preparation.key
	supplied = maps.Clone(supplied)
	supplied["answer"] = "two"
	hr = hr.Clone()
	input.Work = recorder.Scope().RenderWork()
	_, _, err = client.Template(t.Context(), hr, supplied, input)
	assert.NoError(t, err)
	assert.FatalIf(t, preparation.key == priorKey, "%v", "changed supplied values reused the previous preparation key")
	var counts manifest.WorkCounts
	records := recorder.Scope().Snapshot(0)
	for i := range records {
		record := &records[i]
		if record.Key.Phase == manifest.WorkPreparation && record.Key.Input == "" {
			counts.Add(&record.Counts)
		}
	}
	prepares := uint64(5)
	assert.Equal(
		t,
		counts,
		manifest.WorkCounts{Attempts: prepares, Completed: prepares, OwnerPrepares: prepares},
	)
}

func TestPrepareTemplate_MutableSuppliedValues(t *testing.T) {
	t.Parallel()
	for _, retained := range []bool{false, true} {
		t.Run(map[bool]string{false: "single_operation", true: "retained"}[retained], func(t *testing.T) {
			t.Parallel()
			root := testutil.WriteTree(t, map[string]string{
				"mychart/Chart.yaml": "apiVersion: v2\nname: mychart\nversion: 0.1.0\n",
			})
			client := NewClient(cachetest.New(t, t.TempDir(), cache.New))
			hr := newHR()
			var preparation Preparation
			defer func() { preparation = Preparation{} }()
			input := RenderOptions{Sources: localChartResolver(t, "chart-repo", "flux-system", root)}
			if retained {
				input.Preparation = &preparation
			}
			values := map[string]any{"answer": "first"}
			first, err := client.prepareTemplate(t.Context(), hr, values, &input)
			assert.NoError(t, err)
			if retained {
				preparation = Preparation{}
			}
			values["answer"] = "second"
			second, err := client.prepareTemplate(t.Context(), hr.Clone(), values, &input)
			assert.NoError(t, err)
			assert.FatalIf(
				t,
				first.key == second.key,
				"%v",
				"changed supplied values reused the previous preparation key",
			)
		})
	}
}

func TestPrepareTemplate_RetainedValuesReplacement(t *testing.T) {
	t.Parallel()
	root := testutil.WriteTree(
		t,
		map[string]string{"mychart/Chart.yaml": "apiVersion: v2\nname: mychart\nversion: 0.1.0\n"},
	)
	client := NewClient(cachetest.New(t, t.TempDir(), cache.New))
	hr := newHR()
	var preparation, successor Preparation
	defer func() { preparation = Preparation{} }()
	defer func() { successor = Preparation{} }()
	input := RenderOptions{Preparation: &preparation, Sources: localChartResolver(t, "chart-repo", "flux-system", root)}
	original := map[string]any{"nested": map[string]any{"answer": "first"}, "list": []any{"first"}}
	first, err := client.prepareTemplate(t.Context(), hr, original, &input)
	assert.NoError(t, err)
	assert.Equal(t, preparation.Matches(client, hr, &input), true)
	successor, preparation = preparation, Preparation{}
	assert.Equal(t, preparation.complete, false)
	input.Preparation = &successor
	repeated, err := client.prepareTemplate(t.Context(), hr.Clone(), original, &input)
	assert.NoError(t, err)
	assert.Equal(t, repeated.key, first.key)
	replacement := map[string]any{"nested": map[string]any{"answer": "second"}, "list": []any{"second"}}
	next, err := client.prepareTemplate(t.Context(), hr.Clone(), replacement, &input)
	assert.NoError(t, err)
	assert.Equal(t, next.key != first.key, true)
	assert.Diff(t, original, map[string]any{"nested": map[string]any{"answer": "first"}, "list": []any{"first"}})
	successor = Preparation{}
	assert.Equal(t, successor.values == nil, true)
	replacement["nested"].(map[string]any)["answer"] = "third"
	replacement["list"].([]any)[0] = "third"
	final, err := client.prepareTemplate(t.Context(), hr.Clone(), replacement, &input)
	assert.NoError(t, err)
	assert.Equal(t, final.key != next.key, true)
}

func TestPrepareTemplate_SourceBinding(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"revision", "url", "observed_content", "mutable_wrapper", "missing_source"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			testPrepareTemplateSourceBinding(t, name)
		})
	}
	for _, disabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("tracked revision/disabled=%t", disabled), func(t *testing.T) {
			t.Parallel()
			sources, hr, root := ociRenderFixture(t)
			assert.Equal(t, hr.TracksDigest(), true)
			const digest = "sha256:ff3d3e14728f75476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8"
			original := &store.SourceArtifact{
				Kind: manifest.KindOCIRepository, FS: chartFixtureFS(hr, os.DirFS(root)),
				Content: "immutable", Digest: digest, Revision: "6.15.0@" + digest,
			}
			sources.SetArtifact(hr.ChartSource(), original)
			client := NewClient(cachetest.New(t, t.TempDir(), cache.New))
			var preparation Preparation
			defer func() { preparation = Preparation{} }()
			var recorder manifest.WorkRecorder
			recorder.Init(&manifest.WorkKey{Owner: hr.Named()}, manifest.WorkScope{})
			input := RenderOptions{
				Preparation: &preparation, Sources: NewSources(sources), Work: recorder.Scope().RenderWork(),
				DisableChartDigestTracking: disabled,
			}
			first, err := client.prepareTemplate(t.Context(), hr, nil, &input)
			assert.NoError(t, err)
			replacement := *original
			replacement.Revision = "latest@" + digest
			sources.SetArtifact(hr.ChartSource(), &replacement)
			next, err := client.prepareTemplate(t.Context(), hr, nil, &input)
			assert.NoError(t, err)
			assert.Equal(t, next.source, &replacement)
			assert.Equal(t, next.loaded.oci.Revision, replacement.Revision)
			assert.Equal(t, next.loaded.oci.DisableTracking, disabled)
			assert.Equal(t, first.loaded.oci.Revision, original.Revision)
			assert.Equal(t, first.source, original)
			var prepares uint64
			for _, record := range recorder.Scope().Snapshot(0) {
				prepares += record.Counts.OwnerPrepares
			}
			assert.Equal(t, prepares, uint64(2))
		})
	}
}

func testPrepareTemplateSourceBinding(t *testing.T, name string) {
	t.Helper()
	root := testutil.WriteTree(t, map[string]string{
		"mychart/Chart.yaml": "apiVersion: v2\nname: mychart\nversion: 0.1.0\n",
	})
	client := NewClient(cachetest.New(t, t.TempDir(), cache.New))
	hr := newHR()
	assert.Equal(t, hr.TracksDigest(), false)
	sources := store.New()
	original := *localChartResolver(t, "chart-repo", "flux-system", root).chartInput(hr)
	original.Revision = "first"
	if name != "observed_content" && name != "mutable_wrapper" {
		original.Content = "immutable-source"
	}
	sources.SetArtifact(hr.ChartSource(), &original)
	var recorder manifest.WorkRecorder
	recorder.Init(&manifest.WorkKey{Owner: hr.Named()}, manifest.WorkScope{})
	var preparation Preparation
	defer func() { preparation = Preparation{} }()
	input := RenderOptions{
		Preparation: &preparation,
		Sources:     NewSources(sources),
		SkipKinds:   []string{"Pod"},
		Work:        recorder.Scope().RenderWork(),
	}
	first, err := client.prepareTemplate(t.Context(), hr, nil, &input)
	assert.NoError(t, err)
	assert.Equal(t, &first.binding.Policy.Options.SkipKinds[0] != &input.SkipKinds[0], true)
	current := &original
	if name == "missing_source" {
		sources.SetArtifact(hr.ChartSource(), nil)
		_, missing := client.prepareTemplate(t.Context(), hr, nil, &input)
		assert.Equal(t, errors.Is(missing, manifest.ErrObjectNotFound), true)
		assert.Equal(t, errors.Is(missing, manifest.ErrFlux), true)
		assert.Equal(t, missing.Error(), "GitRepository flux-system-chart-repo artifact content unavailable")
		assert.Equal(t, preparation.source, &original)
		assert.Equal(t, first.source, &original)
		return
	}
	switch name {
	case "mutable_wrapper":
		current = original.WithReads(&store.ReadSet{}).WithReads(&store.ReadSet{})
		sources.SetArtifact(hr.ChartSource(), current)
	case "observed_content":
		preparation.binding.Artifact.Revision, preparation.loaded.artifact.Revision = "stale", "stale"
		preparation.binding.Artifact.URL, preparation.loaded.artifact.URL = "stale", "stale"
		first.Preparation = preparation
	default:
		replacement := original
		if name == "revision" {
			replacement.Revision = "second"
		} else {
			replacement.URL = "https://example.com/second"
		}
		current = &replacement
		sources.SetArtifact(hr.ChartSource(), current)
	}
	priorRevision, priorURL := first.binding.Artifact.Revision, first.binding.Artifact.URL
	next, err := client.prepareTemplate(t.Context(), hr, nil, &input)
	assert.NoError(t, err)
	repeated, err := client.prepareTemplate(t.Context(), hr, nil, &input)
	assert.NoError(t, err)
	for _, prepared := range []*Preparation{&next.Preparation, &repeated.Preparation, &preparation} {
		assert.Equal(t, prepared.binding.Artifact.Revision, current.Revision)
		assert.Equal(t, prepared.binding.Artifact.URL, current.URL)
		assert.Equal(t, prepared.loaded.artifact.Revision, current.Revision)
		assert.Equal(t, prepared.loaded.artifact.URL, current.URL)
		assert.Equal(t, prepared.source, current)
		assert.Equal(t, prepared.key, first.key)
		assert.Equal(t, prepared.loaded.fingerprint, first.loaded.fingerprint)
		assert.Equal(t, prepared.loaded.values.key, first.loaded.values.key)
		assert.Equal(t, prepared.loaded.chart, first.loaded.chart)
		assert.Equal(t, prepared.loaded.record, first.loaded.record)
		assert.Equal(t, prepared.loaded.oci, first.loaded.oci)
		assert.Equal(t, prepared.caps, first.caps)
		assert.Equal(t, &prepared.binding.Policy.Options.SkipKinds[0], &first.binding.Policy.Options.SkipKinds[0])
	}
	assert.Equal(t, first.binding.Artifact.Revision, priorRevision)
	assert.Equal(t, first.binding.Artifact.URL, priorURL)
	assert.Equal(t, first.loaded.artifact.Revision, priorRevision)
	assert.Equal(t, first.loaded.artifact.URL, priorURL)
	assert.Equal(t, original.Revision, "first")
	assert.Equal(t, original.URL, "file://"+root)
	var counts manifest.WorkCounts
	records := recorder.Scope().Snapshot(0)
	for i := range records {
		record := &records[i]
		if record.Key.Phase == manifest.WorkPreparation {
			counts.Add(&record.Counts)
		}
	}
	assert.Equal(t, counts, manifest.WorkCounts{Attempts: 1, Completed: 1, OwnerPrepares: 1, RawKeyPrepares: 3})
	t.Logf("preparation work: %+v", counts)
	if name == "mutable_wrapper" {
		var renderRecorder manifest.WorkRecorder
		renderRecorder.Init(&manifest.WorkKey{Owner: hr.Named()}, manifest.WorkScope{})
		input.Work = renderRecorder.Scope().RenderWork()
		initial, _, err := client.Template(t.Context(), hr, nil, input)
		assert.NoError(t, err)
		initialBytes, err := manifest.EncodeRender(initial)
		assert.NoError(t, err)
		testutil.WriteFile(
			t,
			root,
			"mychart/templates/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: changed}\n",
		)
		changed, _, err := client.Template(t.Context(), hr, nil, input)
		assert.NoError(t, err)
		changedBytes, err := manifest.EncodeRender(changed)
		assert.NoError(t, err)
		repeated, _, err := client.Template(t.Context(), hr, nil, input)
		assert.NoError(t, err)
		repeatedBytes, err := manifest.EncodeRender(repeated)
		assert.NoError(t, err)
		assert.Equal(t, string(changedBytes) != string(initialBytes), true)
		assert.Equal(t, string(repeatedBytes), string(changedBytes))
		assert.Equal(t, preparation.key != first.key, true)
		assert.Equal(t, first.source, &original)
		var prepares, engines uint64
		records := renderRecorder.Scope().Snapshot(0)
		for i := range records {
			record := &records[i]
			prepares += record.Counts.OwnerPrepares
			if record.Key.Phase == manifest.WorkHelm {
				engines += record.Counts.Attempts
			}
		}
		assert.Equal(t, prepares, uint64(1))
		assert.Equal(t, engines, uint64(2))
	}
}
