package helm

import (
	"io/fs"
	"maps"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil"
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

//nolint:ireturn // The upstream interface contract requires this return type.
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
	defer preparation.Retire()
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
			defer preparation.Retire()
			input := RenderOptions{Sources: localChartResolver(t, "chart-repo", "flux-system", root)}
			if retained {
				input.Preparation = &preparation
			}
			values := map[string]any{"answer": "first"}
			first, err := client.prepareTemplate(t.Context(), hr, values, &input)
			assert.NoError(t, err)
			if retained {
				preparation.Retire()
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
	defer preparation.Retire()
	defer successor.Retire()
	input := RenderOptions{Preparation: &preparation, Sources: localChartResolver(t, "chart-repo", "flux-system", root)}
	original := map[string]any{"nested": map[string]any{"answer": "first"}, "list": []any{"first"}}
	first, err := client.prepareTemplate(t.Context(), hr, original, &input)
	assert.NoError(t, err)
	assert.Equal(t, successor.Take(&preparation, client, hr, &input), true)
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
	successor.Retire()
	assert.Equal(t, successor.values == nil, true)
	replacement["nested"].(map[string]any)["answer"] = "third"
	replacement["list"].([]any)[0] = "third"
	final, err := client.prepareTemplate(t.Context(), hr.Clone(), replacement, &input)
	assert.NoError(t, err)
	assert.Equal(t, final.key != next.key, true)
}
