package helm

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	"github.com/opencontainers/go-digest"
	chartcommon "helm.sh/helm/v4/pkg/chart/common"
	chart "helm.sh/helm/v4/pkg/chart/v2"
	"oras.land/oras-go/v2/errdef"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

// minimalChart returns a tiny *chart.Chart fixture for the key-derivation
// tests; it shares the same shape the real chart-loading path produces.
func minimalChart() *chart.Chart {
	return &chart.Chart{
		Metadata: &chart.Metadata{Name: "test", Version: "0.1.0", APIVersion: "v2"},
		Templates: []*chartcommon.File{
			{Name: "templates/cm.yaml", Data: []byte("kind: ConfigMap\n")},
		},
		Values: map[string]any{"default": true},
	}
}

// minimalHR returns a tiny *manifest.HelmRelease for the key-derivation
// tests. Mirrors the structural shape Template's call sites observe.
func minimalHR() *manifest.HelmRelease {
	return &manifest.HelmRelease{
		Name:      "demo",
		Namespace: "default",
	}
}

func TestTemplateCache_ChartDigestTrackingOptions(t *testing.T) {
	t.Parallel()
	st, hr, dir := ociRenderFixture(t)
	const digest = "sha256:ff3d3e14728f75476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8"
	st.SetArtifact(
		manifest.NamedResource{
			Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: "apps", Name: "podinfo",
		},
		&store.SourceArtifact{
			Kind:      manifest.KindOCIRepository,
			FS:        chartFixtureFS(hr, os.DirFS(dir)),
			LocalPath: dir,
			Digest:    digest,
			Revision:  "6.15.0@" + digest,
		},
	)
	owner := cachetest.New(t, "", cache.New)
	newClient := func() (*Client, Sources) {
		cli := NewClient(owner)
		cliSources := NewSources(st)
		return cli, cliSources
	}
	cli, cliSources := newClient()
	for _, disable := range []bool{false, true, false, true, false} {
		opts := Options{DisableChartDigestTracking: disable}
		out, receipt28, err := testutil.RenderMaps(
			cli.Template(t.Context(), hr, nil, RenderOptions{Options: opts, Sources: cliSources}),
		)
		if err == nil {
			{
				recordErr := cli.RecordRender(
					t.Context(),
					RenderAcceptance{Owner: hr.Named(), Receipt: receipt28, Records: emptyRenderInventory},
				)
				assert.NoError(t, recordErr)
			}
		}

		{
			assert.NoError(t, err)
		}
		version := "6.15.0"
		if !disable {
			version += "+ff3d3e14728f"
		}
		{
			assert.FatalIf(
				t,
				!hasRenderField(out, "version", version),
				"disable=%v returned incompatible output:\n%s",
				disable,
				out,
			)
		}
		freshClient, freshSources := newClient()
		fresh, receipt29, err := testutil.RenderMaps(
			freshClient.Template(t.Context(), hr, nil, RenderOptions{Options: opts, Sources: freshSources}),
		)
		if err == nil {
			{
				recordErr := freshClient.RecordRender(
					t.Context(),
					RenderAcceptance{Owner: hr.Named(), Receipt: receipt29, Records: emptyRenderInventory},
				)
				assert.NoError(t, recordErr)
			}
		}

		{
			assert.FatalIf(
				t,
				err != nil || !reflect.DeepEqual(fresh, out),
				"fresh disable=%v result differs: err=%v output=%s",
				disable,
				err,
				fresh,
			)
		}
	}
}

func TestTemplateCache_StorageFailures(t *testing.T) {
	t.Parallel()
	for _, fault := range []string{"lookup", "read", "publication", "construction"} {
		t.Run(fault, func(t *testing.T) {
			t.Parallel()
			testTemplateCacheStorageFailuresCase(t, &fault)
		},
		)
	}
}

type changingChartReader struct {
	sourceReader

	artifacts []*store.SourceArtifact
	calls     int
}

//nolint:ireturn // The upstream interface contract requires this return type.
func (r *changingChartReader) GetArtifact(manifest.NamedResource, store.ArtifactRead) store.Artifact {
	artifact := r.artifacts[min(r.calls, len(r.artifacts)-1)]
	r.calls++
	return artifact
}

func TestTemplateCache_ActionHookPolicy(t *testing.T) {
	t.Parallel()
	if !renderPersistenceEligible(t) {
		t.Log("render persistence ineligible; running warm/fresh functional assertions without claiming disk hits")
	}
	resolver := localChartResolver(t, "chart-repo", "flux-system", helmChartDirectory(t))
	warm := NewClient(cachetest.New(t, t.TempDir(), cache.New))
	warmSources := resolver
	for i, state := range []struct {
		install, upgrade, wantHook bool
	}{
		{false, true, true},
		{true, false, false},
		{false, false, true},
	} {
		opts := Options{}
		want := []string{"ConfigMap/demo-cm"}
		if state.wantHook {
			want = append(want, "ConfigMap/demo-pre")
		}
		var first []map[string]any
		for repeat := range 2 {
			hr := newHR()
			hr.Install = &helmv2.Install{DisableHooks: state.install}
			hr.Upgrade = &helmv2.Upgrade{DisableHooks: state.upgrade}
			out, receipt32, err := testutil.RenderMaps(
				warm.Template(t.Context(), hr, nil, RenderOptions{Options: opts, Sources: warmSources}),
			)
			if err == nil {
				{
					recordErr := warm.RecordRender(
						t.Context(),
						RenderAcceptance{Owner: hr.Named(), Receipt: receipt32, Records: emptyRenderInventory},
					)
					assert.NoError(t, recordErr)
				}
			}

			assert.NoError(t, err)
			assert.Diff(t, templateResources(t, out), want)
			fresh := NewClient(cachetest.New(t, t.TempDir(), cache.New))
			freshSources := resolver
			freshHR := newHR()
			freshHR.Install = &helmv2.Install{DisableHooks: state.install}
			freshHR.Upgrade = &helmv2.Upgrade{DisableHooks: state.upgrade}
			uncached, receipt33, err := testutil.RenderMaps(
				fresh.Template(t.Context(), freshHR, nil, RenderOptions{Options: opts, Sources: freshSources}),
			)
			if err == nil {
				{
					recordErr := fresh.RecordRender(
						t.Context(),
						RenderAcceptance{Owner: freshHR.Named(), Receipt: receipt33, Records: emptyRenderInventory},
					)
					assert.NoError(t, recordErr)
				}
			}

			assert.NoError(t, err)
			assert.Diff(t, templateResources(t, uncached), want)
			assert.Diff(t, out, uncached)
			if repeat == 0 {
				first = out
			} else {
				assert.ErrorIf(
					t,
					!reflect.DeepEqual(out, first),
					"state %d repeated output differs: %q != %q",
					i,
					out,
					first,
				)
			}
		}
	}
}

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
				RenderAcceptance{Owner: hr.Named(), Receipt: receipt34, Records: emptyRenderInventory},
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
			assert.NoError(t, os.Remove(filepath.Join(root, "config/prod.yaml")))
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
					RenderAcceptance{Owner: hr.Named(), Receipt: receipt39, Records: emptyRenderInventory},
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
					RenderAcceptance{Owner: hr.Named(), Receipt: receipt40, Records: emptyRenderInventory},
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

func TestSources_SelectedRevision(t *testing.T) {
	t.Parallel()
	for _, tracked := range []bool{false, true} {
		t.Run(strconv.FormatBool(tracked), func(t *testing.T) {
			t.Parallel()
			st, hr, dir := ociRenderFixture(t)
			hr = hr.Clone()
			if !tracked {
				hr.ChartRef = nil
			}
			owner := cachetest.New(t, "", cache.New)
			client := NewClient(owner)
			input := acquireChartFS(t, owner, chartFixtureFS(hr, os.DirFS(dir)))
			artifact := &store.SourceArtifact{
				Kind:     manifest.KindOCIRepository,
				FS:       input,
				Key:      "chart",
				Content:  cache.AcquisitionIdentity(input),
				Digest:   "sha256:ff3d3e14728f75476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8",
				Revision: "6.15.0@sha256:ff3d3e14728f75476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8",
			}
			st.SetArtifact(hr.ChartSource(), artifact)
			var reads store.ReadSet
			reads.Init(st, hr.Named(), nil)
			defer reads.Close()
			_, _, err := testutil.RenderMaps(
				client.Template(t.Context(), hr, nil, RenderOptions{Sources: NewSources(&reads)}),
			)
			assert.NoError(t, err)
			next := *artifact
			next.Revision = "6.16.0"
			next.Metadata = map[string]string{"org.opencontainers.image.revision": "next"}
			st.SetArtifact(hr.ChartSource(), &next)
			assert.Equal(t, reads.Valid(), !tracked)
			var fresh store.ReadSet
			fresh.Init(st, hr.Named(), nil)
			defer fresh.Close()
			loaded, err := client.describeChart(t.Context(), hr, NewSources(&fresh))
			assert.NoError(t, err)
			assert.Equal(t, loaded.artifact.Revision, next.Revision)
			later := next
			later.Revision = "6.17.0"
			st.SetArtifact(hr.ChartSource(), &later)
			assert.Equal(t, fresh.Valid(), !tracked)
		})
	}
}

func testTemplateCacheStorageFailuresCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	if (*testCaseInput) != "construction" && !renderPersistenceEligible(t) {
		t.Skip("storage faults require an eligible build")
	}
	chartDir := t.TempDir()
	testutil.WriteFile(t, chartDir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
	testutil.WriteFile(
		t,
		chartDir,
		"mychart/templates/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: fresh}\n",
	)
	if (*testCaseInput) == "construction" {
		testutil.WriteFile(
			t,
			chartDir,
			"mychart/crds/a-valid.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: prefix}\n",
		)
		testutil.WriteFile(t, chartDir, "mychart/crds/b-invalid.yaml", "apiVersion: [\n")
	}
	root := t.TempDir()
	owner := cachetest.New(t, root, cache.New)
	cli := NewClient(owner)
	cliSources := localChartResolver(t, "chart-repo", "flux-system", chartDir)
	hr := newHR()
	if (*testCaseInput) == "lookup" || (*testCaseInput) == "read" {
		testTemplateCacheStorageFailuresCaseLoop(t, struct {
			cli        **Client
			hr         **manifest.HelmRelease
			cliSources *Sources
		}{cli: &cli, hr: &hr, cliSources: &cliSources})
		d, err := cli.describeChart(t.Context(), hr, cliSources)
		assert.NoError(t, err)
		{
			assert.FatalIf(t, d.record == nil, "%s", "lookup fault fixture did not warm a typed record")
		}
		if (*testCaseInput) == "read" {
			r, _, _, err := owner.Blob(
				t.Context(),
				cache.BlobKey{Content: d.record.key, Render: cache.RenderAccepted},
				cache.Policy{MaxAge: cache.Forever},
				nil,
			)
			assert.NoError(t, err)
			data, err := fs.ReadFile(r, "content")
			assert.NoError(t, err)

			sum := digest.FromBytes(data)
			blob := filepath.Join(root, "oci", "blobs", sum.Algorithm().String(), sum.Encoded())
			assert.NoError(t, os.Remove(blob))
			assert.NoError(t, os.Mkdir(blob, 0o750))
			cli = NewClient(owner)
			cliSources = localChartResolver(t, "chart-repo", "flux-system", chartDir)
		}
	}
	if (*testCaseInput) != "read" {
		assert.NoError(t, os.RemoveAll(root))
	}
	for range 2 {
		out, receipt31, err := testutil.RenderMaps(
			cli.Template(t.Context(), hr, nil, RenderOptions{Sources: cliSources}),
		)
		if err == nil {
			err = cli.RecordRender(
				t.Context(),
				RenderAcceptance{Owner: hr.Named(), Receipt: receipt31, Records: emptyRenderInventory},
			)
		}

		if (*testCaseInput) == "read" {
			{
				pathErr, ok := errors.AsType[*fs.PathError](err)
				assert.FatalIf(t, !ok || pathErr.Op != "read", "read fault did not precede decoding: %v", err)
			}
		}
		{
			assert.FatalIf(
				t, (*testCaseInput) ==
					"construction" && !errors.Is(err, manifest.ErrInput), "construction did not precede publication: %v",
				err,
			)
		}
		wantDocuments := 0
		if (*testCaseInput) == "publication" {
			wantDocuments = 1
		}
		{
			assert.FatalIf(t, len(out) != wantDocuments || !errors.Is(err, manifest.ErrFlux) ||
				errors.Is(
					err,
					errdef.ErrNotFound,
				), "%s fault returned output %q, error %v", (*testCaseInput), out, err)
		}
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
				RenderAcceptance{Owner: hr.Named(), Receipt: receipt35, Records: emptyRenderInventory},
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
	key := computeTemplateKey(initial, map[string]any{}, Options{}, hr)
	{
		assert.FatalIf(t, key == "", "%s", "fixture has no completed-output key")
	}
	r, _, _, err := owner.Blob(
		t.Context(),
		cache.BlobKey{Content: key, Render: cache.RenderAccepted},
		cache.Policy{MaxAge: cache.Forever},
		nil,
	)
	assert.NoError(t, err)
	data, err := fs.ReadFile(r, "content")
	assert.NoError(t, err)

	var record renderRecord
	assert.NoError(t, decodeTestRecord(data, &record))
	decoded, err := testutil.DecodeRender(record.Output)
	assert.NoError(t, err)
	assert.Diff(t, decoded, first)
	cached := append(slices.Clone(first), map[string]any{"witness": "persistent hit witness"})
	_, _, _, err = owner.Blob(
		t.Context(),
		cache.BlobKey{Content: key, Render: cache.RenderAccepted},
		cache.Policy{},
		func(_ context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
			writeTestRecordErr := writeTestRecord(
				w,
				cache.BlobKey{Content: key, Render: cache.RenderAccepted},
				renderRecord{Output: encodedRender(t, cached), Analysis: initial.analyze()},
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
				RenderAcceptance{Owner: hr.Named(), Receipt: receipt36, Records: emptyRenderInventory},
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
				RenderAcceptance{Owner: hr.Named(), Receipt: receipt37, Records: emptyRenderInventory},
			)
			assert.NoError(t, recordErr)
		}
	}

	assert.NoError(t, err)
	assert.FatalIf(t, !hasRenderField(first, "marker", "packaged"), "%v", first)
	initial, err := newClientCli2.describeChart(t.Context(), hr, newSourcesCliSources2)
	assert.NoError(t, err)
	key := computeTemplateKey(initial, map[string]any{}, Options{}, hr)
	{
		assert.FatalIf(t, key == "", "%s", "fixture has no completed-output key")
	}
	r, _, _, err := owner.Blob(
		t.Context(),
		cache.BlobKey{Content: key, Render: cache.RenderAccepted},
		cache.Policy{MaxAge: cache.Forever},
		nil,
	)
	assert.NoError(t, err)
	data, err := fs.ReadFile(r, "content")
	assert.NoError(t, err)

	var record renderRecord
	assert.NoError(t, decodeTestRecord(data, &record))
	decoded, err := testutil.DecodeRender(record.Output)
	assert.NoError(t, err)
	assert.Diff(t, decoded, first)
	{
		assert.FatalIf(t, record.Analysis == nil, "%s", "persisted record has no chart analysis")
	}
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
				RenderAcceptance{Owner: sourceHR.Named(), Receipt: receipt38, Records: emptyRenderInventory},
			)
			assert.NoError(t, recordErr)
		}
	}

	assert.Diff(t, out, []map[string]any(nil))
	{
		assert.FatalIf(
			t,
			!errors.Is(err, fs.ErrNotExist) || !errors.Is(err, manifest.ErrInput) ||
				!errors.Is(err, manifest.ErrFlux),
			"missing source values error = %v, want fs.ErrNotExist, ErrInput and ErrFlux",
			err,
		)
	}
	assert.FatalIf(t, !strings.Contains(err.Error(), `read values file "prod.yaml"`), "%v", err)
	missing, err := fresh.describeChart(t.Context(), sourceHR, freshSources)
	assert.NoError(t, err)
	missing.values, missing.valuesErr = fresh.observeValuesFiles(
		t.Context(), &missing, sourceHR.ChartValuesFiles,
		false,
	)
	assert.FatalIf(t, !errors.Is(missing.valuesErr, fs.ErrNotExist), "%v", missing.valuesErr)
	assert.Equal(t, computeTemplateKey(missing, map[string]any{}, Options{}, sourceHR), key)
}

func testTemplateCacheStorageFailuresCaseLoop(t *testing.T, loopInputs struct {
	cli        **Client
	hr         **manifest.HelmRelease
	cliSources *Sources
},
) {
	t.Helper()

	for range 2 {
		_, receipt30, err := testutil.RenderMaps(
			(*loopInputs.cli).Template(
				t.Context(),
				(*loopInputs.hr),
				nil,
				RenderOptions{Options: Options{}, Sources: (*loopInputs.cliSources)},
			),
		)
		if err == nil {
			{
				recordErr := (*loopInputs.cli).RecordRender(
					t.Context(),
					RenderAcceptance{
						Owner:   (*loopInputs.hr).Named(),
						Receipt: receipt30,
						Records: emptyRenderInventory,
					},
				)
				assert.NoError(t, recordErr)
			}
		}

		assert.NoError(t, err)
	}
}
