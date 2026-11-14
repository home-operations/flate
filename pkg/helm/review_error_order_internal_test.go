package helm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"log" //nolint:depguard // The fixture must capture standard-library logs emitted by Helm dependencies.
	"os"
	"strings"
	"testing"

	chart "helm.sh/helm/v4/pkg/chart/v2"
	"oras.land/oras-go/v2/errdef"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func TestReview_ErrorPrecedence(t *testing.T) {
	t.Parallel()
	t.Run(
		"chart before capability and values",
		func(t *testing.T) { t.Parallel(); testReviewErrorPrecedenceChartBeforeCapabilityAndValues(t) },
	)
	for _, disabled := range []bool{false, true} {
		t.Run(
			"OCI before capability/disabled="+map[bool]string{false: "false", true: "true"}[disabled],
			func(t *testing.T) {
				t.Parallel()
				testReviewErrorPrecedenceCase(t, &disabled)
			},
		)
	}
	for _, failure := range []string{"chart lookup", "chart malformed", "OCI tracked", "OCI disabled", "packaged malformed", "packaged missing"} {
		t.Run(failure+" before persisted fault", func(t *testing.T) {
			t.Parallel()
			testReviewErrorPrecedenceCase2(t, &failure)
		},
		)
	}
}

//nolint:paralleltest // test changes process-wide logging.
func TestReview_ErrorPrecedenceDiagnostics(t *testing.T) {
	t.Run("malformed raw frame misses", func(t *testing.T) {
		t.Logf("render persistence reason=%q backing=disk", cache.RenderPersistenceDisabledReason())
		root, st, hr := sourceValuesCacheFixture(t)
		testutil.WriteFile(t, root, "charts/demo/templates/fail.yaml", `{{ fail "action must wait for cache error" }}`)
		testutil.WriteFile(t, root, "charts/demo/requirements.yaml", "dependencies: []\n")
		owner := cachetest.New(t, t.TempDir(), cache.New)
		cli := NewClient(owner)
		cliSources := NewSources(st)
		d, err := cli.describeChart(t.Context(), hr, cliSources.chartInput(hr))
		assert.NoError(t, err)
		d.values, d.valuesErr = cli.observeValuesFiles(t.Context(), &d, hr.ChartValuesFiles, true)
		assert.NoError(t, d.valuesErr)
		_, _, _, err = owner.Blob(
			t.Context(),
			cache.BlobKey{
				Content: computeTemplateKey(&d, map[string]any{}, Options{}, hr),
				Render:  cache.RenderAccepted,
			},
			&cache.Policy{},
			func(_ context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
				_, writeStringErr2 := io.WriteString(w, "{")
				return cache.ProducerResult{}, writeStringErr2
			},
		)
		assert.NoError(t, err)

		cli = NewClient(owner)
		var diagnostics bytes.Buffer
		previous := log.Writer()
		log.SetOutput(&diagnostics)
		defer log.SetOutput(previous)
		var receipt11 *manifest.RenderReceipt
		_, receipt11, err = testutil.RenderMaps(
			cli.Template(t.Context(), hr, nil, RenderOptions{Options: Options{}, Sources: cliSources}),
		)
		if err == nil {
			{
				recordErr := cli.RecordRender(
					t.Context(),
					RenderAcceptance{
						Owner:   hr.Named(),
						Receipt: receipt11,
						Records: emptyRenderInventory,
						Sources: []manifest.SourceEvidence{},
					},
				)
				assert.NoError(t, recordErr)
			}
		}

		assert.FatalIf(t, err == nil || !strings.Contains(err.Error(), "action must wait") ||
			!strings.Contains(
				diagnostics.String(),
				"Warning:",
			), "malformed raw frame prevented ordinary rendering: %v", err)
	})
}

func testReviewErrorPrecedenceChartBeforeCapabilityAndValues(t *testing.T) {
	t.Helper()

	root, st, hr := sourceValuesCacheFixture(t)
	testutil.WriteFile(t, root, "charts/demo/Chart.yaml", "apiVersion: v2\nname: demo\nversion: invalid\n")
	hr = hr.Clone()
	hr.ChartValuesFiles = []string{"absent.yaml"}
	hr.IgnoreMissingValuesFiles = false
	cli := NewClient(cachetest.New(t, "", cache.New))
	cliSources := NewSources(st)
	var receipt8 *manifest.RenderReceipt
	var err error
	_, receipt8, err = testutil.RenderMaps(
		cli.Template(t.Context(), hr, nil, RenderOptions{KubeVersion: "invalid", Sources: cliSources}),
	)
	if err == nil {
		{
			recordErr := cli.RecordRender(
				t.Context(),
				RenderAcceptance{
					Owner:   hr.Named(),
					Receipt: receipt8,
					Records: emptyRenderInventory,
					Sources: []manifest.SourceEvidence{},
				},
			)
			assert.NoError(t, recordErr)
		}
	}

	assert.FatalIf(
		t,
		!errors.Is(err, manifest.ErrInput) || !strings.Contains(err.Error(), "load chart"),
		"chart validation lost precedence: %v",
		err,
	)
}

func testReviewErrorPrecedenceCase(t *testing.T, testCaseInput *bool,
) {
	t.Helper()

	st, hr, dir := ociRenderFixture(t)
	st.SetArtifact(
		hr.ChartSource(),
		&store.SourceArtifact{FS: chartFixtureFS(hr, os.DirFS(dir)), Revision: "invalid"},
	)
	cli := NewClient(cachetest.New(t, "", cache.New))
	cliSources := NewSources(st)
	var receipt9 *manifest.RenderReceipt
	var err error
	_, receipt9, err = testutil.RenderMaps(
		cli.Template(
			t.Context(),
			hr,
			nil,
			RenderOptions{
				KubeVersion:                "invalid",
				DisableChartDigestTracking: (*testCaseInput),
				Sources:                    cliSources,
			},
		),
	)
	if err == nil {
		{
			recordErr := cli.RecordRender(
				t.Context(),
				RenderAcceptance{
					Owner:   hr.Named(),
					Receipt: receipt9,
					Records: emptyRenderInventory,
					Sources: []manifest.SourceEvidence{},
				},
			)
			assert.NoError(t, recordErr)
		}
	}

	assert.FatalIf(
		t,
		!errors.Is(err, manifest.ErrInput) || !strings.Contains(err.Error(), "track oci chart version"),
		"OCI validation lost precedence: %v",
		err,
	)
}

func testReviewErrorPrecedenceCase2(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	persistent := cache.RenderPersistenceDisabledReason() == ""
	st, original, dir := ociRenderFixture(t)
	hr := original.Clone()
	art := &store.SourceArtifact{
		FS:        chartFixtureFS(original, os.DirFS(dir)),
		LocalPath: dir,
		Revision:  "6.15.0@sha256:0123456789abcdef",
	}
	opts := Options{}
	want := "load chart " + dir
	switch *testCaseInput {
	case "chart lookup", "chart malformed":
		testutil.WriteFile(t, dir, "Chart.yaml", "apiVersion: v2\nname: podinfo\nversion: invalid\n")
		hr.ChartValuesFiles = nil
	case "OCI tracked", "OCI disabled":
		art.Revision = "invalid"
		opts.DisableChartDigestTracking = (*testCaseInput) == "OCI disabled"
		want = "track oci chart version"
	case "packaged malformed", "packaged missing":
		hr.ChartRef = nil
		hr.Chart.Source.Kind = manifest.KindHelmChart
		hr.ChartValuesFiles = []string{"prod.yaml"}
		if (*testCaseInput) == "packaged malformed" {
			testutil.WriteFile(t, dir, "prod.yaml", "marker: [\n")
		} else {
			hr.ChartValuesFiles = []string{"absent.yaml"}
		}
		want = "chart valuesFiles"
	}
	st.SetArtifact(hr.ChartSource(), art)
	root := t.TempDir()
	owner := cachetest.New(t, root, cache.New)
	cli := NewClient(owner)
	cliSources := NewSources(st)
	d, err := cli.describeChart(t.Context(), hr, cliSources.chartInput(hr))
	assert.NoError(t, err)
	if hr.TracksDigest() {
		d.oci = &ociChartContent{
			Digest:          art.Digest,
			Revision:        art.Revision,
			DisableTracking: opts.DisableChartDigestTracking,
		}
	}
	d.values, d.valuesErr = cli.observeValuesFiles(
		t.Context(), &d, hr.ChartValuesFiles,
		hr.IgnoreMissingValuesFiles,
	)
	assert.NoError(t, d.valuesErr)
	key := cache.BlobKey{
		Content: computeTemplateKey(&d, map[string]any{}, opts, hr),
		Render:  cache.RenderAccepted,
	}
	_, _, _, err = owner.Blob(
		t.Context(),
		key,
		&cache.Policy{},
		func(_ context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
			_, writeStringErr := io.WriteString(w, "{")
			return cache.ProducerResult{}, writeStringErr
		},
	)
	assert.NoError(t, err)

	if (*testCaseInput) == "chart lookup" {
		assert.NoError(t, os.RemoveAll(root))
	}
	r, _, _, err := owner.Blob(t.Context(), key, &cache.Policy{MaxAge: cache.Forever}, nil)
	switch {
	case !persistent:
		assert.Equal(t, errors.Is(err, errdef.ErrNotFound), true)
		assert.Equal(t, r == nil, true)
	case (*testCaseInput) == "chart lookup":
		_, ok := errors.AsType[*cache.CacheError](
			err,
		)
		assert.FatalIf(t, !ok || !errors.Is(err, manifest.ErrFlux) ||
			errors.Is(err, errdef.ErrNotFound), "fixture did not reach lookup fault: %v", err)
	default:
		assert.NoError(t, err)
		data, readFileErr := fs.ReadFile(r, "content")
		assert.NoError(t, readFileErr)

		var record renderRecord
		{
			_, ok := errors.AsType[*json.SyntaxError](json.Unmarshal(data, &record))
			assert.FatalIf(t, !ok, "%s", "fixture did not reach malformed persisted record")
		}
	}
	var receipt10 *manifest.RenderReceipt
	_, receipt10, err = testutil.RenderMaps(
		cli.Template(t.Context(), hr, nil, RenderOptions{Options: opts, Sources: cliSources}),
	)
	if err == nil {
		{
			recordErr := cli.RecordRender(
				t.Context(),
				RenderAcceptance{
					Owner:   hr.Named(),
					Receipt: receipt10,
					Records: emptyRenderInventory,
					Sources: []manifest.SourceEvidence{},
				},
			)
			assert.NoError(t, recordErr)
		}
	}

	assert.FatalIf(t, !errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) ||
		!strings.Contains(err.Error(), want), "preparation lost precedence over persisted fault: %v", err)
	if strings.HasPrefix((*testCaseInput), "chart") {
		{
			_, ok := errors.AsType[chart.ValidationError](err)
			assert.FatalIf(t, !ok, "chart validation sentinel lost: %v", err)
		}
	}
	assert.FatalIf(
		t,
		strings.HasPrefix((*testCaseInput), "packaged") &&
			!strings.Contains(err.Error(), hr.ChartValuesFiles[0]),
		"values file context lost: %v",
		err,
	)
	assert.FatalIf(
		t, (*testCaseInput) ==
			"packaged missing" && !errors.Is(err, fs.ErrNotExist), "missing values sentinel lost: %v",
		err,
	)
}
