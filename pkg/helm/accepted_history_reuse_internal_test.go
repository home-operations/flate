package helm

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"maps"
	"slices"
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"oras.land/oras-go/v2/errdef"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
)

func TestTemplate_AcceptedHistory(t *testing.T) {
	t.Parallel()
	chartDir := t.TempDir()
	testutil.WriteFile(t, chartDir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
	testutil.WriteFile(
		t,
		chartDir,
		"mychart/templates/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: original}\n",
	)
	root := t.TempDir()
	owner := cachetest.New(t, root, cache.New)
	client := NewClient(owner)
	hr := newHR()
	hr.TargetNamespace = "runtime"
	input := RenderOptions{
		SkipCRDs:  true,
		SkipKinds: []string{manifest.KindCustomResourceDefinition, manifest.KindSecret},
		Sources:   localChartResolver(t, "chart-repo", "flux-system", chartDir),
	}
	t.Run("disabled preparation", func(t *testing.T) {
		t.Parallel()
		testTemplateAcceptedHistoryDisabledPreparation(
			t,
			&testTemplateAcceptedHistoryDisabledPreparationInputs{hr: &hr, input: &input},
		)
	},
	)
	docs, receipt, err := testutil.RenderMaps(client.Template(t.Context(), hr, nil, input))
	assert.NoError(t, err)

	assert.FatalIf(t, receipt == nil, "%s", "successful raw template supplied no receipt")

	frame := receipt.Value().(renderFrame)
	frame.header.Binding = manifest.Fingerprint(frame.binding)
	frame.header.Analysis = (&chartDescription{chart: frame.chart, analysis: frame.header.Analysis}).analyze()
	frame.encoded, err = encodeFrame(frame.header, frame.output)
	assert.NoError(t, err)
	frame.raw = manifest.Fingerprint(frame.encoded[12 : 12+int(binary.BigEndian.Uint32(frame.encoded[8:12]))])
	original := bytes.Clone(frame.output)
	docs[0]["metadata"].(map[string]any)["name"] = "mutated"
	assert.Equal(t, bytes.Equal(receipt.Value().(renderFrame).output, original), true)
	raw, err := testutil.DecodeRender(frame.output)
	assert.NoError(t, err)
	assert.Equal(t, hasRenderField(raw, "name", "original"), true)
	key := cache.BlobKey{Content: frame.header.Key, Render: cache.RenderAccepted}
	{
		_, _, _, blobErr := owner.Blob(
			t.Context(),
			key,
			&cache.Policy{MaxAge: cache.Forever},
			nil,
		)
		assert.FatalIf(t, !errors.Is(
			blobErr,
			errdef.ErrNotFound,
		), "standalone template wrote durable output: %v", blobErr)
	}
	assert.NoError(
		t,
		client.RecordRender(
			t.Context(),
			RenderAcceptance{Owner: hr.Named(), Receipt: receipt, Sources: []manifest.SourceEvidence{}, Records: nil},
		),
	)
	assert.NoError(
		t,
		client.RecordRender(
			t.Context(),
			RenderAcceptance{
				Owner:   hr.Named(),
				Receipt: nil,
				Sources: []manifest.SourceEvidence{},
				Records: emptyRenderInventory,
			},
		),
	)
	input.Admit = func(RenderAdmission) (bool, error) { t.Fatal("history called Admit"); return false, nil }
	{
		previous, complete := client.PreviousRender(t.Context(), hr, nil, input)
		assert.FatalIf(t, complete || previous != nil, "%s", "unaccepted template supplied history")
	}
	t.Run("durable", func(t *testing.T) {
		t.Parallel()
		testTemplateAcceptedHistoryDurable(
			t,
			&testTemplateAcceptedHistoryDurableInputs{
				root:    &root,
				client:  &client,
				hr:      &hr,
				input:   &input,
				receipt: &receipt,
				err:     &err,
				frame:   &frame,
				raw:     &raw,
				key:     &key,
			},
		)
	},
	)
}

type testTemplateAcceptedHistoryDurableInputs struct {
	root    *string
	client  **Client
	hr      **manifest.HelmRelease
	input   *RenderOptions
	receipt **manifest.RenderReceipt
	err     *error
	frame   *renderFrame
	raw     *[]map[string]any
	key     *cache.BlobKey
}

func testTemplateAcceptedHistoryDurable(t *testing.T, testCaseInput *testTemplateAcceptedHistoryDurableInputs) {
	t.Helper()

	if !renderPersistenceEligible(t) {
		t.Skip("requires a clean VCS-stamped build for positive persistence")
	}
	child := manifest.NamedResource{
		Group:     manifest.FluxKustomizeDomain,
		Kind:      manifest.KindKustomization,
		Namespace: "apps",
		Name:      "child",
	}
	records := map[manifest.NamedResource]manifest.DiscoveryRecord{
		child: {
			Application:  `{"kind":"Kustomization","spec":{"path":"./entry"}}`,
			Refs:         []manifest.NamedResource{manifest.BootstrapSourceID},
			ContentRoots: map[manifest.NamedResource][]string{manifest.BootstrapSourceID: {"apps/blue"}},
		},
	}
	assert.NoError(
		t, (*testCaseInput.client).RecordRender(
			t.Context(),
			RenderAcceptance{
				Owner:   (*testCaseInput.hr).Named(),
				Receipt: (*testCaseInput.receipt),
				Sources: []manifest.SourceEvidence{},
				Records: maps.All(records),
			},
		),
	)
	assert.NoError(t, (*testCaseInput.client).RecordRender(t.Context(), RenderAcceptance{
		Owner: (*testCaseInput.hr).Named(), Receipt: *testCaseInput.receipt, Records: emptyRenderInventory,
	}))
	fresh := NewClient(cachetest.New(t, (*testCaseInput.root), cache.New))
	previous, complete := fresh.PreviousRender(t.Context(), (*testCaseInput.hr), nil, (*testCaseInput.input))
	assert.Equal(t, complete, true)
	got := maps.Collect(previous)
	assert.Diff(t, got, records)
	equal := *testCaseInput.input
	equal.SkipKinds = slices.Clone(equal.SkipKinds)
	equalPrevious, equalComplete := fresh.PreviousRender(t.Context(), *testCaseInput.hr, nil, equal)
	assert.Equal(t, equalComplete, true)
	assert.Diff(t, maps.Collect(equalPrevious), records)
	changed := equal
	changed.SkipKinds = []string{manifest.KindCustomResourceDefinition}
	_, changedComplete := fresh.PreviousRender(t.Context(), *testCaseInput.hr, nil, changed)
	assert.Equal(t, changedComplete, false)
	prepared, prepareErr := fresh.prepareTemplate(t.Context(), *testCaseInput.hr, nil, &changed)
	assert.NoError(t, prepareErr)
	assert.Equal(t, prepared.key, testCaseInput.frame.header.Key)
	observed := *testCaseInput.input
	calls := 0
	observed.Admit = func(admission RenderAdmission) (bool, error) {
		calls++
		assert.Equal(t, admission.Key, testCaseInput.frame.header.Key)
		assert.Equal(t, admission.Receipt != nil, true)
		assert.Equal(t, len(admission.Documents), 1)
		return true, nil
	}
	_, _, admitErr := fresh.Template(t.Context(), *testCaseInput.hr, nil, observed)
	assert.NoError(t, admitErr)
	assert.Equal(t, calls, 1)

	t.Run("static output", func(t *testing.T) {
		t.Parallel()
		testTemplateAcceptedHistoryDurableStaticOutput(t, &testCaseInput, records)
	})
	t.Run("source metadata replacement", func(t *testing.T) {
		t.Parallel()
		testTemplateAcceptedHistoryDurableSourceMetadataReplacement(t, &testCaseInput, records)
	})

	t.Run("preparation", func(t *testing.T) {
		t.Parallel()
		testTemplateAcceptedHistoryDurablePreparation(t, &testCaseInput, records)
	})

	for _, tc := range []struct {
		name   string
		mutate func(*renderHeader)
	}{
		{"request key", func(h *renderHeader) { h.Key = "foreign" }},
		{"missing analysis", func(h *renderHeader) { h.Analysis = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testTemplateAcceptedHistoryDurableName(t, &testCaseInput, &tc, records)
		})
	}
	t.Run("typed record key", func(t *testing.T) {
		t.Parallel()
		testTemplateAcceptedHistoryDurableTypedRecordKey(t, &testCaseInput, records)
	})
	t.Run("frozen preparation", func(t *testing.T) {
		t.Parallel()
		testTemplateAcceptedHistoryDurableFrozenPreparation(t, &testCaseInput, records)
	})

	t.Run("unaccepted preparation", func(t *testing.T) {
		t.Parallel()
		testTemplateAcceptedHistoryDurableUnacceptedPreparation(t, &testCaseInput, records)
	})

	t.Run("publication", func(t *testing.T) {
		t.Parallel()
		testTemplateAcceptedHistoryDurablePublication(t, &testCaseInput, records)
	})
	described, err := fresh.describeChart(
		t.Context(),
		(*testCaseInput.hr),
		testCaseInput.input.Sources.chartInput((*testCaseInput.hr)),
	)
	assert.NoError(t, err)
	assert.Equal(t, described.chart == nil, true)
	assert.Equal(t, described.analysis != nil && described.record != nil, true)
	for _, tc := range []struct {
		name   string
		mutate func(*RenderOptions)
	}{
		{"dependency substitutions", func(r *RenderOptions) {
			r.DependencySubstitutions = values.NewDependencySubstitutions([]*manifest.Kustomization{{PostBuild: &kustomizev1.PostBuild{Substitute: map[string]string{"DEP": "effective-child"}}}})
		}},
		{"skip kinds", func(o *RenderOptions) { o.SkipKinds = []string{manifest.KindKustomization} }},
		{"source binding", func(o *RenderOptions) {
			changed := *o.Sources.LocalSourceArtifact((*testCaseInput.hr).ChartSource(), store.ArtifactContent)
			changed.URL = "different"
			sources := store.New()
			sources.SetArtifact((*testCaseInput.hr).ChartSource(), &changed)
			o.Sources = NewSources(sources)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testTemplateAcceptedHistoryDurableName2(t, &testCaseInput, records, &tc)
		})
	}
	foreign := (*testCaseInput.hr).Clone()
	foreign.Namespace = "foreign"
	{
		_, previousRenderComplete9 := fresh.PreviousRender(
			t.Context(),
			foreign,
			nil,
			(*testCaseInput.input),
		)
		assert.FatalIf(t, previousRenderComplete9, "%s", "foreign effective owner borrowed history")
	}
	missing := (*testCaseInput.input)
	missing.Sources = NewSources(store.New())
	{
		_, previousRenderComplete10 := fresh.PreviousRender(
			t.Context(),
			(*testCaseInput.hr),
			nil,
			missing,
		)
		assert.FatalIf(t, previousRenderComplete10, "%s", "unmaterialized artifact supplied history")
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	{
		err := (*testCaseInput.client).RecordRender(
			canceled,
			RenderAcceptance{
				Owner:   (*testCaseInput.hr).Named(),
				Receipt: (*testCaseInput.receipt),
				Sources: []manifest.SourceEvidence{},
				Records: emptyRenderInventory,
			},
		)
		assert.FatalIf(t, !errors.Is(
			err,
			context.Canceled,
		), "canceled acceptance: %v", err)
	}
	previous, complete = fresh.PreviousRender(t.Context(), (*testCaseInput.hr), nil, (*testCaseInput.input))
	assert.Equal(t, complete, true)
	for id := range previous {
		assert.Equal(t, id, child)
	}
	assert.NoError(
		t, (*testCaseInput.client).RecordRender(
			t.Context(),
			RenderAcceptance{
				Owner:   (*testCaseInput.hr).Named(),
				Receipt: (*testCaseInput.receipt),
				Sources: []manifest.SourceEvidence{},
				Records: emptyRenderInventory,
			},
		),
	)
	previous, complete = fresh.PreviousRender(t.Context(), (*testCaseInput.hr), nil, (*testCaseInput.input))
	assert.Equal(t, complete && previous != nil, true)
	for range previous {
		t.Fatal("empty acceptance retained previous records")
	}
}

func testTemplateAcceptedHistoryDurableStaticOutput(
	t *testing.T,
	testCaseInput **testTemplateAcceptedHistoryDurableInputs,
	records map[manifest.NamedResource]manifest.DiscoveryRecord,
) {
	t.Helper()

	publisher := NewClient(cachetest.New(t, t.TempDir(), cache.New))
	assert.NoError(
		t,
		publisher.RecordRender(
			t.Context(),
			RenderAcceptance{
				Owner:   (*(*testCaseInput).hr).Named(),
				Receipt: (*(*testCaseInput).receipt),
				Records: maps.All(records),
			},
		),
	)
	previousRenderPrevious, previousRenderErr := publisher.PreviousRender(
		t.Context(),
		(*(*testCaseInput).hr),
		nil,
		(*(*testCaseInput).input),
	)
	assert.Equal(t, previousRenderPrevious == nil, true)
	assert.Equal(t, previousRenderErr, false)
}

func testTemplateAcceptedHistoryDurableSourceMetadataReplacement(
	t *testing.T,
	testCaseInput **testTemplateAcceptedHistoryDurableInputs,
	records map[manifest.NamedResource]manifest.DiscoveryRecord,
) {
	t.Helper()

	directory := t.TempDir()
	source := manifest.SourceIdentity{Root: "/history-input"}
	for _, selected := range []string{"apps/red", "apps/blue", ""} {
		sources := []manifest.SourceEvidence{}
		if selected != "" {
			sources = append(sources, manifest.SourceEvidence{Source: source, Path: selected, Subtree: true})
		}
		publisher := NewClient(cachetest.New(t, directory, cache.New))
		assert.NoError(
			t,
			publisher.RecordRender(
				t.Context(),
				RenderAcceptance{
					Owner:   (*(*testCaseInput).hr).Named(),
					Receipt: (*(*testCaseInput).receipt),
					Records: maps.All(records),
					Sources: sources,
				},
			),
		)
		assert.NoError(t, publisher.cache.Close())
		reopened := NewClient(cachetest.New(t, directory, cache.New))
		for _, candidate := range []string{"apps/red", "apps/blue"} {
			owners, producersForPathsErr := reopened.cache.ProducersForPaths(
				t.Context(),
				[]manifest.SourceEvidence{{Source: source, Path: candidate + "/cm.yaml"}},
			)
			assert.NoError(t, producersForPathsErr)
			want := map[manifest.NamedResource][]manifest.SourceEvidence{}
			if candidate == selected {
				want[(*(*testCaseInput).hr).Named()] = []manifest.SourceEvidence{
					{Source: source, Path: candidate + "/cm.yaml"},
				}
			}
			assert.Diff(t, owners, want)
		}
		assert.NoError(t, reopened.cache.Close())
	}
}

func testTemplateAcceptedHistoryDurablePreparation(
	t *testing.T,
	testCaseInput **testTemplateAcceptedHistoryDurableInputs,
	records map[manifest.NamedResource]manifest.DiscoveryRecord,
) {
	t.Helper()

	artifact := *(*testCaseInput).input.Sources.LocalSourceArtifact((*(*testCaseInput).hr).ChartSource(), store.ArtifactContent)
	counted := &historyCountingFS{FS: artifact.FS}
	artifact.FS = counted
	objects := store.New()
	objects.SetArtifact((*(*testCaseInput).hr).ChartSource(), &artifact)
	sources := &historyCountingSources{sourceReader: objects}
	observed := (*(*testCaseInput).input)
	observed.Sources, observed.Admit = NewSources(sources), nil
	directory := t.TempDir()
	publisher := NewClient(cachetest.New(t, directory, cache.New))
	_, pending, templateErr := publisher.Template(t.Context(), (*(*testCaseInput).hr), nil, observed)
	assert.NoError(t, templateErr)
	assert.NoError(
		t,
		publisher.RecordRender(
			t.Context(),
			RenderAcceptance{
				Owner:   (*(*testCaseInput).hr).Named(),
				Receipt: pending,
				Sources: []manifest.SourceEvidence{},
				Records: maps.All(records),
			},
		),
	)
	reader := NewClient(cachetest.New(t, directory, cache.New))
	sources.calls.Store(0)
	counted.opens.Store(0)
	previousRenderPrevious2, previousRenderErr := reader.PreviousRender(
		t.Context(),
		(*(*testCaseInput).hr),
		nil,
		observed,
	)
	assert.Equal(t, previousRenderErr && previousRenderPrevious2 != nil, true)
	assert.Diff(t, maps.Collect(previousRenderPrevious2), records)

	assert.FatalIf(
		t,
		sources.calls.Load() == 0 || counted.opens.Load() == 0,
		"%s",
		"enabled history did not prepare chart",
	)

	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	sources.calls.Store(0)
	counted.opens.Store(0)
	{
		previousRenderErr, complete := reader.PreviousRender(
			canceled,
			(*(*testCaseInput).hr),
			nil,
			observed,
		)
		assert.FatalIf(t, previousRenderErr != nil ||
			complete, "%s", "canceled enabled renderer supplied history")
	}
	assert.Equal(t, sources.calls.Load(), 0)
	assert.Equal(t, counted.opens.Load(), 0)
	{
		previousRenderErr, complete := reader.PreviousRender(
			t.Context(),
			nil,
			nil,
			observed,
		)
		assert.FatalIf(t, previousRenderErr != nil ||
			complete, "%s", "nil HelmRelease supplied history")
	}
}

func testTemplateAcceptedHistoryDurableName(
	t *testing.T,
	testCaseInput **testTemplateAcceptedHistoryDurableInputs,
	tc *struct {
		name   string
		mutate func(*renderHeader)
	},
	records map[manifest.NamedResource]manifest.DiscoveryRecord,
) {
	t.Helper()

	isolated := NewClient(cachetest.New(t, t.TempDir(), cache.New))
	invalid := (*testCaseInput).frame.header
	tc.mutate(&invalid)
	invalidReceipt, encodeFrameErr := encodeFrame(invalid, (*testCaseInput).frame.output)
	assert.NoError(t, encodeFrameErr)
	invalidFrame, encodeFrameErr := decodeFrame(invalidReceipt)
	assert.NoError(t, encodeFrameErr)
	accepted, encodeFrameErr := encodeAccepted((*(*testCaseInput).hr).Named(), &invalidFrame, maps.All(records))
	assert.NoError(t, encodeFrameErr)
	_, _, _, encodeFrameErr = isolated.cache.Blob(
		t.Context(),
		(*(*testCaseInput).key),
		&cache.Policy{MaxAge: 0},
		func(_ context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
			_, copyErr := io.Copy(w, io.MultiReader(bytes.NewReader(invalidReceipt), bytes.NewReader(accepted)))
			return cache.ProducerResult{}, copyErr
		},
	)
	assert.NoError(t, encodeFrameErr)

	prepared, encodeFrameErr := isolated.prepareTemplate(
		t.Context(),
		(*(*testCaseInput).hr),
		nil,
		(*testCaseInput).input,
	)
	assert.NoError(t, encodeFrameErr)
	defer func() {
		{
			problem := recover()
			assert.ErrorIf(
				t,
				problem != nil,
				"invalid raw metadata panicked instead of missing: %v",
				problem,
			)
		}
	}()
	found, encodeFrameErr := isolated.lookupRender(t.Context(), &prepared.loaded, prepared.key, &RenderOptions{})
	assert.NoError(t, encodeFrameErr)

	assert.FatalIf(t, found != nil, "%s", "invalid raw request metadata reused cached output")

	{
		_, previousRenderComplete := isolated.PreviousRender(
			t.Context(),
			(*(*testCaseInput).hr),
			nil,
			(*(*testCaseInput).input),
		)
		assert.FatalIf(t, previousRenderComplete, "%s", "invalid raw request metadata supplied history")
	}
}

func testTemplateAcceptedHistoryDurableTypedRecordKey(
	t *testing.T,
	testCaseInput **testTemplateAcceptedHistoryDurableInputs,
	records map[manifest.NamedResource]manifest.DiscoveryRecord,
) {
	t.Helper()

	isolated := NewClient(cachetest.New(t, t.TempDir(), cache.New))
	assert.NoError(
		t,
		isolated.RecordRender(
			t.Context(),
			RenderAcceptance{
				Owner:   (*(*testCaseInput).hr).Named(),
				Receipt: (*(*testCaseInput).receipt),
				Sources: []manifest.SourceEvidence{},
				Records: maps.All(records),
			},
		),
	)
	prepared, prepareTemplateErr := isolated.prepareTemplate(
		t.Context(),
		(*(*testCaseInput).hr),
		nil,
		(*testCaseInput).input,
	)
	assert.NoError(t, prepareTemplateErr)
	remembered, prepareTemplateErr := isolated.lookupRender(
		t.Context(),
		&prepared.loaded,
		prepared.key,
		&RenderOptions{},
	)
	assert.NoError(t, prepareTemplateErr)

	assert.FatalIf(t, remembered == nil, "%s", "matching request did not load a typed record")

	prepared.loaded.record = remembered
	reader, _, _, prepareTemplateErr := isolated.cache.Blob(
		t.Context(),
		(*(*testCaseInput).key),
		&cache.Policy{MaxAge: cache.Forever},
		nil,
	)
	assert.NoError(t, prepareTemplateErr)
	data, prepareTemplateErr := fs.ReadFile(reader, "content")
	assert.NoError(t, prepareTemplateErr)
	foreign := (*(*testCaseInput).key)
	foreign.Content = "foreign"
	reader, _, _, prepareTemplateErr = isolated.cache.Blob(
		t.Context(),
		foreign,
		&cache.Policy{MaxAge: 0},
		func(_ context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
			_, writeErr := w.Write(data)
			return cache.ProducerResult{}, writeErr
		},
	)
	assert.NoError(t, prepareTemplateErr)
	assert.Equal(t, cache.AcquisitionIdentity(reader), remembered.identity)

	found, prepareTemplateErr := isolated.lookupRender(t.Context(), &prepared.loaded, "foreign", &RenderOptions{})
	assert.NoError(t, prepareTemplateErr)

	assert.FatalIf(
		t,
		found != nil,
		"%s",
		"same acquisition identity lent a typed record to a foreign request key",
	)
}

func testTemplateAcceptedHistoryDurableFrozenPreparation(
	t *testing.T,
	testCaseInput **testTemplateAcceptedHistoryDurableInputs,
	records map[manifest.NamedResource]manifest.DiscoveryRecord,
) {
	t.Helper()

	renderer := NewClient(cachetest.New(t, t.TempDir(), cache.New))
	current := (*(*testCaseInput).hr).Clone()
	current.CommonMetadata = &helmv2.CommonMetadata{Annotations: map[string]string{"policy": "before"}}
	assert.NoError(
		t,
		json.Unmarshal(
			[]byte(
				`{"dependsOn":[{"name":"before"}],"valuesFrom":[{"kind":"ConfigMap","name":"before"}],"healthCheckExprs":[{"apiVersion":"v1","kind":"Pod","current":"true"}]}`,
			),
			&current.HelmReleaseSpec,
		),
	)
	before := current.Clone()
	options := RenderOptions{Sources: (*testCaseInput).input.Sources, SkipKinds: []string{"Skipped"}}
	_, pending, renderMapsErr := testutil.RenderMaps(renderer.Template(t.Context(), current, nil, options))
	assert.NoError(t, renderMapsErr)
	current.CommonMetadata.Annotations["policy"], options.SkipKinds[0] = "after", "Different"
	current.DependsOn[0].Name, current.ValuesFrom[0].Name, current.HealthCheckExprs[0].Current = "after", "after", "false"
	assert.NoError(
		t,
		renderer.RecordRender(
			t.Context(),
			RenderAcceptance{
				Owner:   (*(*testCaseInput).hr).Named(),
				Receipt: pending,
				Sources: []manifest.SourceEvidence{},
				Records: maps.All(records),
			},
		),
	)
	expected := RenderOptions{Sources: (*testCaseInput).input.Sources, SkipKinds: []string{"Skipped"}}
	{
		_, previousRenderComplete2 := renderer.PreviousRender(
			t.Context(),
			before,
			nil,
			expected,
		)
		assert.FatalIf(t, !previousRenderComplete2, "%s", "caller mutation changed the prepared receipt")
	}
	{
		_, previousRenderComplete3 := renderer.PreviousRender(
			t.Context(),
			current,
			nil,
			expected,
		)
		assert.FatalIf(t, previousRenderComplete3, "%s", "mutated declaration borrowed the prepared receipt")
	}
	{
		_, previousRenderComplete4 := renderer.PreviousRender(
			t.Context(),
			before,
			nil,
			options,
		)
		assert.FatalIf(t, previousRenderComplete4, "%s", "mutated policy borrowed the prepared receipt")
	}
}

func testTemplateAcceptedHistoryDurableUnacceptedPreparation(
	t *testing.T,
	testCaseInput **testTemplateAcceptedHistoryDurableInputs,
	records map[manifest.NamedResource]manifest.DiscoveryRecord,
) {
	t.Helper()

	renderer := NewClient(cachetest.New(t, t.TempDir(), cache.New))
	original := RenderOptions{Sources: (*testCaseInput).input.Sources}
	_, first, renderMapsErr2 := testutil.RenderMaps(
		renderer.Template(t.Context(), (*(*testCaseInput).hr), nil, original),
	)
	assert.NoError(t, renderMapsErr2)
	assert.NoError(
		t,
		renderer.RecordRender(
			t.Context(),
			RenderAcceptance{
				Owner:   (*(*testCaseInput).hr).Named(),
				Receipt: first,
				Sources: []manifest.SourceEvidence{},
				Records: maps.All(records),
			},
		),
	)
	changed := original
	changed.SkipKinds = []string{manifest.KindKustomization}
	_, pending, renderMapsErr2 := testutil.RenderMaps(
		renderer.Template(t.Context(), (*(*testCaseInput).hr), nil, changed),
	)
	assert.NoError(t, renderMapsErr2)

	assert.FatalIf(t, pending == nil, "%s", "raw successor supplied no receipt")

	{
		_, previousRenderComplete5 := renderer.PreviousRender(
			t.Context(),
			(*(*testCaseInput).hr),
			nil,
			original,
		)
		assert.ErrorIf(t, !previousRenderComplete5, "%s", "unaccepted preparation displaced the durable frame")
	}
	{
		_, previousRenderComplete6 := renderer.PreviousRender(
			t.Context(),
			(*(*testCaseInput).hr),
			nil,
			changed,
		)
		assert.ErrorIf(t, previousRenderComplete6, "%s", "unaccepted preparation became durable history authority")
	}
}

func testTemplateAcceptedHistoryDurableName2(
	t *testing.T,
	testCaseInput **testTemplateAcceptedHistoryDurableInputs,
	records map[manifest.NamedResource]manifest.DiscoveryRecord,
	tc *struct {
		name   string
		mutate func(*RenderOptions)
	},
) {
	t.Helper()

	isolated := NewClient(cachetest.New(t, t.TempDir(), cache.New))
	assert.NoError(
		t,
		isolated.RecordRender(
			t.Context(),
			RenderAcceptance{
				Owner:   (*(*testCaseInput).hr).Named(),
				Receipt: (*(*testCaseInput).receipt),
				Sources: []manifest.SourceEvidence{},
				Records: maps.All(records),
			},
		),
	)
	changed := (*(*testCaseInput).input)
	tc.mutate(&changed)
	{
		_, previousRenderComplete8 := isolated.PreviousRender(
			t.Context(),
			(*(*testCaseInput).hr),
			nil,
			changed,
		)
		assert.FatalIf(t, previousRenderComplete8, "%s", "incompatible emission policy borrowed history")
	}
	changed.Admit = nil
	out, _, err := testutil.RenderMaps(isolated.Template(t.Context(), (*(*testCaseInput).hr), nil, changed))
	assert.NoError(t, err)
	assert.Diff(t, out, (*(*testCaseInput).raw))
}
