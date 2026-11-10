package helm

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

func sourceValuesCacheFixture(t *testing.T) (string, *store.Store, *manifest.HelmRelease) {
	t.Helper()
	root := t.TempDir()
	testutil.WriteFile(t, root, "charts/demo/Chart.yaml", "apiVersion: v2\nname: demo\nversion: 1.0.0\n")
	testutil.WriteFile(t, root, "charts/demo/values.yaml", "marker: default\n")
	testutil.WriteFile(
		t,
		root,
		"charts/demo/templates/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: demo\ndata:\n  marker: {{ .Values.marker | quote }}\n",
	)
	testutil.WriteFile(t, root, "config/prod.yaml", "marker: first\n")
	location, err := gitrepo.Locate(t.Context(), root, root)
	assert.NoError(t, err)
	admitted, err := gittree.AdmitDirectory(t.Context(), location)
	assert.NoError(t, err)
	hr := &manifest.HelmRelease{
		Name:      "demo",
		Namespace: "apps",
		Chart: manifest.HelmChart{
			Name: "charts/demo",
			Source: manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindGitRepository,
				Namespace: "apps",
				Name:      "repo",
			},
		},
		ChartValuesFiles:         []string{"config/prod.yaml"},
		IgnoreMissingValuesFiles: true,
	}
	st := store.New()
	st.SetArtifact(hr.ChartSource(), &store.SourceArtifact{FS: admitted.FS})
	return root, st, hr
}

func TestTemplateCache_SourceRootValuesFiles(t *testing.T) {
	t.Parallel()
	root, st, hr := sourceValuesCacheFixture(t)
	cacheRoot := t.TempDir()
	owner := cachetest.New(t, cacheRoot, cache.New)
	cli := NewClient(owner)
	cliSources := NewSources(st)
	first, receipt34, err := testutil.RenderMaps(
		cli.Template(t.Context(), hr, nil, RenderOptions{Options: Options{}, Sources: cliSources}),
	)
	if err == nil {
		{
			recordErr := cli.RecordRender(
				t.Context(),
				RenderAcceptance{
					Owner:   hr.Named(),
					Receipt: receipt34,
					Records: emptyRenderInventory,
					Sources: []manifest.SourceEvidence{},
				},
			)
			assert.NoError(t, recordErr)
		}
	}

	assert.NoError(t, err)
	assert.FatalIf(t, !hasRenderField(first, "marker", "first"), "%v", first)
	t.Run(
		"persistent reuse",
		func(t *testing.T) { t.Parallel(); testTemplateCacheSourceRootValuesFilesPersistentReuse(t) },
	)
	t.Run("packaged cache cannot mask missing source values", func(t *testing.T) {
		t.Parallel()
		testTemplateCacheSourceRootValuesFilesPackagedCacheCannotMaskMissingSourceValues(t)
	},
	)
	cli.assertSourceValuesChanges(t, hr, root, cliSources)
}

func (c *Client) assertSourceValuesChanges(t *testing.T, hr *manifest.HelmRelease, root string, cliSources Sources) {
	t.Helper()
	initial, err := loadDecodedChart(t.Context(), c, hr, cliSources)
	assert.NoError(t, err)
	for _, marker := range []string{"changed", "", "added"} {
		if marker == "" {
			assert.NoError(t, os.Remove(filepath.Join(root, "config", "prod.yaml")))
		} else {
			testutil.WriteFile(t, root, "config/prod.yaml", "marker: "+marker+"\n")
		}
		out, receipt39, err := testutil.RenderMaps(
			c.Template(t.Context(), hr, nil, RenderOptions{Options: Options{}, Sources: cliSources}),
		)
		if err == nil {
			{
				recordErr := c.RecordRender(
					t.Context(),
					RenderAcceptance{
						Owner:   hr.Named(),
						Receipt: receipt39,
						Records: emptyRenderInventory,
						Sources: []manifest.SourceEvidence{},
					},
				)
				assert.NoError(t, recordErr)
			}
		}

		assert.NoError(t, err)
		want := marker
		if want == "" {
			want = "default"
		}
		assert.FatalIf(t, !hasRenderField(out, "marker", want), "%v", out)
		repeated, receipt40, err := testutil.RenderMaps(
			c.Template(t.Context(), hr, nil, RenderOptions{Options: Options{}, Sources: cliSources}),
		)
		if err == nil {
			{
				recordErr := c.RecordRender(
					t.Context(),
					RenderAcceptance{
						Owner:   hr.Named(),
						Receipt: receipt40,
						Records: emptyRenderInventory,
						Sources: []manifest.SourceEvidence{},
					},
				)
				assert.NoError(t, recordErr)
			}
		}

		assert.NoError(t, err)
		assert.Diff(t, repeated, out)
		loaded, err := loadDecodedChart(t.Context(), c, hr, cliSources)
		assert.NoError(t, err)
		assert.Equal(t, loaded.chart == initial.chart, true)
	}
}

func testTemplateCacheSourceRootValuesFilesPersistentReuse(t *testing.T) {
	t.Helper()

	if !renderPersistenceEligible(t) {
		t.Skipf("requires clean VCS-stamped test binary: %s", cache.RenderPersistenceDisabledReason())
	}
	sourceValuesCacheFixtureRoot, st, hr := sourceValuesCacheFixture(t)
	cacheRoot := t.TempDir()
	owner := cachetest.New(t, cacheRoot, cache.New)
	newClientCli := NewClient(owner)
	newSourcesCliSources := NewSources(st)
	first, receipt35, err := testutil.RenderMaps(
		newClientCli.Template(t.Context(), hr, nil, RenderOptions{Options: Options{}, Sources: newSourcesCliSources}),
	)
	if err == nil {
		{
			recordErr := newClientCli.RecordRender(
				t.Context(),
				RenderAcceptance{
					Owner:   hr.Named(),
					Receipt: receipt35,
					Records: emptyRenderInventory,
					Sources: []manifest.SourceEvidence{},
				},
			)
			assert.NoError(t, recordErr)
		}
	}

	assert.NoError(t, err)
	initial, err := loadDecodedChart(t.Context(), newClientCli, hr, newSourcesCliSources)
	assert.NoError(t, err)
	initial.values, initial.valuesErr = newClientCli.observeValuesFiles(
		t.Context(),
		&initial,
		hr.ChartValuesFiles,
		true,
	)
	assert.NoError(t, initial.valuesErr)
	key := computeTemplateKey(&initial, map[string]any{}, Options{}, hr)
	assert.FatalIf(t, key == "", "%s", "fixture has no completed-output key")
	r, _, _, err := owner.Blob(
		t.Context(),
		cache.BlobKey{Content: key, Render: cache.RenderAccepted},
		&cache.Policy{MaxAge: cache.Forever},
		nil,
	)
	assert.NoError(t, err)
	data, err := fs.ReadFile(r, "content")
	assert.NoError(t, err)

	var record renderRecord
	assert.NoError(t, decodeTestRecord(t.Context(), data, &record))
	decoded, err := testutil.DecodeRender(record.Output)
	assert.NoError(t, err)
	assert.Diff(t, decoded, first)
	cached := append(slices.Clone(first), map[string]any{"witness": "persistent hit witness"})
	_, _, _, err = owner.Blob(
		t.Context(),
		cache.BlobKey{Content: key, Render: cache.RenderAccepted},
		&cache.Policy{},
		func(_ context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
			writeTestRecordErr := writeTestRecord(t.Context(),
				w,
				cache.BlobKey{Content: key, Render: cache.RenderAccepted},
				&renderRecord{Output: encodedRender(t, cached), Analysis: initial.analyze()},
			)
			return cache.ProducerResult{}, writeTestRecordErr
		},
	)
	assert.NoError(t, err)

	fresh := NewClient(cachetest.New(t, cacheRoot, cache.New))
	freshSources := NewSources(st)
	hit, receipt36, err := testutil.RenderMaps(
		fresh.Template(t.Context(), hr, nil, RenderOptions{Options: Options{}, Sources: freshSources}),
	)
	if err == nil {
		{
			recordErr := fresh.RecordRender(
				t.Context(),
				RenderAcceptance{
					Owner:   hr.Named(),
					Receipt: receipt36,
					Records: emptyRenderInventory,
					Sources: []manifest.SourceEvidence{},
				},
			)
			assert.NoError(t, recordErr)
		}
	}

	assert.NoError(t, err)
	assert.Diff(t, hit, cached)
	fresh.assertSourceValuesChanges(t, hr, sourceValuesCacheFixtureRoot, freshSources)
}

func testTemplateCacheSourceRootValuesFilesPackagedCacheCannotMaskMissingSourceValues(t *testing.T) {
	t.Helper()

	if !renderPersistenceEligible(t) {
		t.Skipf("requires clean VCS-stamped test binary: %s", cache.RenderPersistenceDisabledReason())
	}
	sourceValuesCacheFixtureRoot2, st, hr := sourceValuesCacheFixture(t)
	artifact := st.GetArtifact(hr.ChartSource(), store.ArtifactContent).(*store.SourceArtifact)
	testutil.WriteFile(t, sourceValuesCacheFixtureRoot2, "charts/demo/prod.yaml", "marker: packaged\n")
	hr.Chart.Source.Kind = manifest.KindExternalArtifact
	hr.ChartValuesFiles = []string{"prod.yaml"}
	hr.IgnoreMissingValuesFiles = false
	st.SetArtifact(hr.ChartSource(), artifact)
	cacheRoot := t.TempDir()
	owner := cachetest.New(t, cacheRoot, cache.New)
	newClientCli2 := NewClient(owner)
	newSourcesCliSources2 := NewSources(st)
	first, receipt37, err := testutil.RenderMaps(
		newClientCli2.Template(t.Context(), hr, nil, RenderOptions{Options: Options{}, Sources: newSourcesCliSources2}),
	)
	if err == nil {
		{
			recordErr := newClientCli2.RecordRender(
				t.Context(),
				RenderAcceptance{
					Owner:   hr.Named(),
					Receipt: receipt37,
					Records: emptyRenderInventory,
					Sources: []manifest.SourceEvidence{},
				},
			)
			assert.NoError(t, recordErr)
		}
	}

	assert.NoError(t, err)
	assert.FatalIf(t, !hasRenderField(first, "marker", "packaged"), "%v", first)
	initial, err := newClientCli2.describeChart(t.Context(), hr, newSourcesCliSources2.chartInput(hr))
	assert.NoError(t, err)
	key := computeTemplateKey(&initial, map[string]any{}, Options{}, hr)
	assert.FatalIf(t, key == "", "%s", "fixture has no completed-output key")
	r, _, _, err := owner.Blob(
		t.Context(),
		cache.BlobKey{Content: key, Render: cache.RenderAccepted},
		&cache.Policy{MaxAge: cache.Forever},
		nil,
	)
	assert.NoError(t, err)
	data, err := fs.ReadFile(r, "content")
	assert.NoError(t, err)

	var record renderRecord
	assert.NoError(t, decodeTestRecord(t.Context(), data, &record))
	decoded, err := testutil.DecodeRender(record.Output)
	assert.NoError(t, err)
	assert.Diff(t, decoded, first)
	assert.FatalIf(t, record.Analysis == nil, "%s", "persisted record has no chart analysis")
	sourceHR := hr.Clone()
	sourceHR.Chart.Source.Kind = manifest.KindGitRepository
	st.SetArtifact(sourceHR.ChartSource(), artifact)
	fresh := NewClient(cachetest.New(t, cacheRoot, cache.New))
	freshSources := NewSources(st)
	out, receipt38, err := testutil.RenderMaps(
		fresh.Template(t.Context(), sourceHR, nil, RenderOptions{Options: Options{}, Sources: freshSources}),
	)
	if err == nil {
		{
			recordErr := fresh.RecordRender(
				t.Context(),
				RenderAcceptance{
					Owner:   sourceHR.Named(),
					Receipt: receipt38,
					Records: emptyRenderInventory,
					Sources: []manifest.SourceEvidence{},
				},
			)
			assert.NoError(t, recordErr)
		}
	}

	assert.Diff(t, out, []map[string]any(nil))
	assert.FatalIf(
		t,
		!errors.Is(err, fs.ErrNotExist) || !errors.Is(err, manifest.ErrInput) ||
			!errors.Is(err, manifest.ErrFlux),
		"missing source values error = %v, want fs.ErrNotExist, ErrInput and ErrFlux",
		err,
	)
	assert.FatalIf(t, !strings.Contains(err.Error(), `read values file "prod.yaml"`), "%v", err)
	missing, err := fresh.describeChart(t.Context(), sourceHR, freshSources.chartInput(sourceHR))
	assert.NoError(t, err)
	missing.values, missing.valuesErr = fresh.observeValuesFiles(
		t.Context(), &missing, sourceHR.ChartValuesFiles,
		false,
	)
	assert.FatalIf(t, !errors.Is(missing.valuesErr, fs.ErrNotExist), "%v", missing.valuesErr)
	assert.Equal(t, computeTemplateKey(&missing, map[string]any{}, Options{}, sourceHR), key)
}
