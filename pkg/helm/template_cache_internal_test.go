package helm

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"runtime/debug"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	chart "helm.sh/helm/v4/pkg/chart/v2"
	"oras.land/oras-go/v2/errdef"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func renderPersistenceEligible(tb testing.TB) bool {
	tb.Helper()
	info, ok := debug.ReadBuildInfo()
	eligible := false
	if ok {
		const module = "github.com/home-operations/flate"
		eligible = info.Main.Path == module && info.Main.Version != "" && info.Main.Version != "(devel)"
		for _, dep := range info.Deps {
			if dep.Path == module && dep.Version != "" && dep.Version != "(devel)" {
				eligible = true
			}
		}
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" && setting.Value != "" {
				eligible = true
			}
		}
		renderPersistenceEligibleLoop(struct {
			info     **debug.BuildInfo
			eligible *bool
		}{info: &info, eligible: &eligible})
		for _, setting := range info.Settings {
			if setting.Key == "vcs.modified" && setting.Value == "true" {
				eligible = false
			}
		}
	}
	return eligible
}

func TestTemplateCache_OCIIdentity(t *testing.T) {
	t.Parallel()
	const digest = "sha256:ff3d3e14728f75476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8"
	const next = "sha256:abcdef12345675476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8"
	shared := digest[:len(digest)-1] + "9"
	initial := store.SourceArtifact{Digest: digest, Revision: "6.15.0@" + digest}
	for _, disable := range []bool{false, true} {
		for _, tc := range []struct {
			name                                   string
			next                                   store.SourceArtifact
			wantSameOutput, wantFingerprintChanged bool
			marker                                 string
		}{
			{name: "unchanged", next: initial, wantSameOutput: true},
			{name: "same suffix", next: store.SourceArtifact{Digest: shared, Revision: "6.15.0@" + shared}, wantSameOutput: true, wantFingerprintChanged: true},
			{name: "digest only", next: store.SourceArtifact{Digest: shared, Revision: initial.Revision}, wantSameOutput: true, wantFingerprintChanged: true},
			{name: "revision only", next: store.SourceArtifact{Digest: digest, Revision: "latest@" + digest}, wantSameOutput: true, wantFingerprintChanged: true},
			{name: "changed suffix", next: store.SourceArtifact{Digest: next, Revision: "6.15.0@" + next}, wantSameOutput: disable, wantFingerprintChanged: true},
			{name: "new values", next: store.SourceArtifact{Digest: shared, Revision: "6.15.0@" + shared}, marker: "updated", wantFingerprintChanged: true},
		} {
			t.Run(fmt.Sprintf("disabled_%t/%s", disable, tc.name), func(t *testing.T) {
				t.Parallel()
				testTemplateCacheOCIIdentityCase(
					t,
					&testTemplateCacheOCIIdentityCaseInputs{initial: &initial, disable: &disable, tc: &tc},
				)
			},
			)
		}
	}

	t.Run(
		"one admitted artifact",
		func(t *testing.T) { t.Parallel(); testTemplateCacheOCIIdentityOneAdmittedArtifact(t) },
	)
}

func TestTemplateCache_SubchartPersistence(t *testing.T) {
	t.Parallel()
	if !renderPersistenceEligible(t) {
		t.Skip("requires a clean VCS-stamped build to exercise persisted renders")
	}
	t.Log("persistence eligible: true")
	st, hr, dir := ociRenderFixture(t)
	testutil.WriteFile(t, dir, "Chart.yaml", `apiVersion: v2
name: podinfo
version: 6.15.0
dependencies:
  - name: child
    version: 1.2.3+child
    condition: child.enabled
  - name: sibling
    version: 1.0.0
    condition: sibling.enabled
`)
	for _, name := range []string{"child", "sibling"} {
		version := "1.0.0"
		if name == "child" {
			version = "1.2.3+child"
		}
		testutil.WriteFile(
			t,
			dir,
			"charts/"+name+"/Chart.yaml",
			"apiVersion: v2\nname: "+name+"\nversion: "+version+"\n",
		)
		testutil.WriteFile(t, dir, "charts/"+name+"/values.yaml", "enabled: true\nmarker: original\n")
		testutil.WriteFile(t, dir, "charts/"+name+"/templates/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ .Chart.Name }}
data:
  marker: {{ .Values.marker | quote }}
`)
	}
	input := acquireChartFS(t, cachetest.New(t, t.TempDir(), cache.New), chartFixtureFS(hr, os.DirFS(dir)))
	const digest = "sha256:ff3d3e14728f75476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8"
	st.SetArtifact(hr.ChartSource(), &store.SourceArtifact{
		Kind: manifest.KindOCIRepository, FS: input, Digest: digest, Revision: "6.15.0@" + digest,
	})
	newClient := func(t *testing.T, owner *cache.Cache, reverse bool) (*Client, Sources) {
		t.Helper()
		_, pin, err := source.OperationView(t.Context(), input)
		assert.NoError(t, err)
		{
			assert.FatalIf(t, pin == "", "%s", "fixture did not acquire an immutable operation pin")
		}
		selector := manifest.Fingerprint(struct {
			Pin  string `json:"Pin"`
			Path string `json:"Path"`
			Auto bool   `json:"Auto"`
		}{Pin: pin, Auto: true})
		captured, _, err := captureChart(t.Context(), input, ".", true)
		assert.NoError(t, err)
		ch, err := captured.load()
		assert.NoError(t, err)
		dependencies := ch.Dependencies()
		slices.SortFunc(dependencies, func(a, b *chart.Chart) int { return strings.Compare(a.Name(), b.Name()) })
		if reverse {
			slices.Reverse(dependencies)
		}
		ch.SetDependencies(dependencies...)
		order := slices.Clone(ch.Dependencies())
		before := cloneChartForRender(ch)
		fingerprint := captured.fingerprint()
		t.Cleanup(func() {
			assert.Equal(t, slices.Equal(ch.Dependencies(), order), true)
			assert.Equal(t, reflect.DeepEqual(ch, before), true)
			assert.Equal(t, captured.fingerprint(), fingerprint)
		})
		cli := NewClient(owner)
		cliSources := NewSources(st)
		// Canonical charts MUST remain immutable; choose the loader order before admission.
		cli.chartCache[selector] = chartDescription{
			chart:       ch,
			captured:    captured,
			key:         selector,
			location:    ".",
			fingerprint: captured.fingerprint(),
			analysis: &chartAnalysis{
				Version:        ch.Metadata.Version,
				Templates:      chartTemplateText(ch),
				Allowed:        knownTopLevelKeys(ch),
				RequiresLoader: requiresChartLoader(ch),
			},
		}
		return cli, cliSources
	}
	keyFor := func(t *testing.T, cli *Client, cliSources Sources) cache.BlobKey {
		t.Helper()
		loaded, err := loadDecodedChart(t.Context(), cli, hr, cliSources)
		assert.NoError(t, err)
		loaded.oci = &ociChartContent{Digest: digest, Revision: "6.15.0@" + digest}
		_, err = cli.mergeChartValuesFiles(t.Context(), loaded, hr.ChartValuesFiles, hr.IgnoreMissingValuesFiles)
		assert.NoError(t, err)
		key := computeTemplateKey(loaded, map[string]any{}, Options{}, hr)
		t.Logf("chart fingerprint=%s template key=%s", loaded.fingerprint, key)
		{
			assert.FatalIf(t, key == "", "%s", "subchart fixture produced an empty completed-output key")
		}
		return cache.BlobKey{Content: key, Render: cache.RenderAccepted}
	}
	readBlob := func(t *testing.T, owner *cache.Cache, key cache.BlobKey) []map[string]any {
		t.Helper()
		r, _, _, err := owner.Blob(t.Context(), key, cache.Policy{MaxAge: cache.Forever}, nil)
		assert.NoError(t, err)
		data, err := fs.ReadFile(r, "content")
		assert.NoError(t, err)

		var record renderRecord
		assert.NoError(t, decodeTestRecord(data, &record))
		docs, err := testutil.DecodeRender(record.Output)
		assert.NoError(t, err)
		return docs
	}
	writeBlob := func(t *testing.T, owner *cache.Cache, key cache.BlobKey, data []map[string]any) {
		t.Helper()
		captured, _, err := captureChart(t.Context(), input, ".", true)
		assert.NoError(t, err)
		ch, err := captured.load()
		assert.NoError(t, err)
		analysis := &chartAnalysis{
			Version:        ch.Metadata.Version,
			Templates:      chartTemplateText(ch),
			Allowed:        knownTopLevelKeys(ch),
			RequiresLoader: requiresChartLoader(ch),
		}
		_, _, _, err = owner.Blob(
			t.Context(),
			key,
			cache.Policy{},
			func(_ context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
				writeTestRecordErr := writeTestRecord(
					w,
					key,
					renderRecord{Output: encodedRender(t, data), Analysis: analysis},
				)
				return cache.ProducerResult{}, writeTestRecordErr
			},
		)
		assert.NoError(t, err)
	}
	root := t.TempDir()
	owner := cachetest.New(t, root, cache.New)
	cli, cliSources := newClient(t, owner, false)
	key := keyFor(t, cli, cliSources)
	_, _, _, err := owner.Blob(t.Context(), key, cache.Policy{MaxAge: cache.Forever}, nil)
	{
		assert.FatalIf(t, !errors.Is(err, errdef.ErrNotFound), "cold lookup: got %v, want not found", err)
	}
	t.Log("direct cold lookup: not found")
	cold, receipt5, err := testutil.RenderMaps(
		cli.Template(t.Context(), hr, nil, RenderOptions{Options: Options{}, Sources: cliSources}),
	)
	if err == nil {
		{
			recordErr := cli.RecordRender(
				t.Context(),
				RenderAcceptance{Owner: hr.Named(), Receipt: receipt5, Records: emptyRenderInventory},
			)
			assert.NoError(t, recordErr)
		}
	}

	assert.NoError(t, err)
	for _, name := range []string{"child", "sibling"} {
		{
			assert.FatalIf(t, !hasRenderField(cold, "name", name), "cold render omitted enabled dependency %s", name)
		}
	}
	assert.Diff(t, readBlob(t, owner, key), cold)
	t.Log("direct cold publication: exact constructed documents")
	second, receipt6, err := testutil.RenderMaps(
		cli.Template(t.Context(), hr, nil, RenderOptions{Options: Options{}, Sources: cliSources}),
	)
	if err == nil {
		{
			recordErr := cli.RecordRender(
				t.Context(),
				RenderAcceptance{Owner: hr.Named(), Receipt: receipt6, Records: emptyRenderInventory},
			)
			assert.NoError(t, recordErr)
		}
	}

	assert.NoError(t, err)
	assert.Diff(t, second, cold)
	marked := append(slices.Clone(cold), map[string]any{"witness": "persisted-subchart-hit"})
	writeBlob(t, owner, key, marked)
	warm, receipt7, err := testutil.RenderMaps(
		cli.Template(t.Context(), hr, nil, RenderOptions{Options: Options{}, Sources: cliSources}),
	)
	if err == nil {
		{
			recordErr := cli.RecordRender(
				t.Context(),
				RenderAcceptance{Owner: hr.Named(), Receipt: receipt7, Records: emptyRenderInventory},
			)
			assert.NoError(t, recordErr)
		}
	}

	assert.NoError(t, err)
	assert.Diff(t, warm, marked)
	t.Log("same-client marked hit: verified")
	assert.NoError(t, owner.Close())
	owner = cachetest.New(t, root, cache.New)
	cli, cliSources = newClient(t, owner, true)
	reopenedKey := keyFor(t, cli, cliSources)
	_, _, _, lookupErr := owner.Blob(t.Context(), reopenedKey, cache.Policy{MaxAge: cache.Forever}, nil)
	t.Logf("fresh-owner direct lookup: %v", lookupErr)
	reopened, receipt8, err := testutil.RenderMaps(
		cli.Template(t.Context(), hr, nil, RenderOptions{Options: Options{}, Sources: cliSources}),
	)
	if err == nil {
		{
			recordErr := cli.RecordRender(
				t.Context(),
				RenderAcceptance{Owner: hr.Named(), Receipt: receipt8, Records: emptyRenderInventory},
			)
			assert.NoError(t, recordErr)
		}
	}

	assert.NoError(t, err)
	if !reflect.DeepEqual(reopened, marked) {
		t.Fatalf(
			"fresh owner/client rerendered unchanged subcharts: marker absent=%v, fresh bytes equal cold=%v",
			!hasRenderField(reopened, "witness", "persisted-subchart-hit"),
			reflect.DeepEqual(reopened, cold),
		)
	}
	writeBlob(t, owner, reopenedKey, cold)
	restored, receipt9, err := testutil.RenderMaps(
		cli.Template(t.Context(), hr, nil, RenderOptions{Options: Options{}, Sources: cliSources}),
	)
	if err == nil {
		{
			recordErr := cli.RecordRender(
				t.Context(),
				RenderAcceptance{Owner: hr.Named(), Receipt: receipt9, Records: emptyRenderInventory},
			)
			assert.NoError(t, recordErr)
		}
	}

	assert.NoError(t, err)
	assert.Diff(t, restored, cold)
	assert.Equal(t, reopenedKey.Content, key.Content)
	assert.NoError(t, lookupErr)
	t.Log("verified persisted subchart publication, marked hit, reopen, and restored document equality")
	writeBlob(t, owner, key, marked)
	for _, field := range []string{"template", "defaults"} {
		t.Run(field, func(t *testing.T) {
			t.Parallel()
			testTemplateCacheSubchartPersistenceCase(
				t,
				&testTemplateCacheSubchartPersistenceCaseInputs{
					hr:       &hr,
					dir:      &dir,
					keyFor:   &keyFor,
					readBlob: &readBlob,
					owner:    &owner,
					key:      &key,
					cold:     &cold,
					marked:   &marked,
					field:    &field,
				},
			)
		},
		)
	}
}

func TestTemplateCache_NonOCITrackingMode(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindOCIRepository, manifest.KindGitRepository, manifest.KindBucket, manifest.KindExternalArtifact, manifest.KindHelmChart} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			testTemplateCacheNonOCITrackingModeCase(t, &kind)
		},
		)
	}
}

func TestComputeTemplateKey_StableSameInputs(t *testing.T) {
	t.Parallel()
	ch := minimalChart()
	hr := minimalHR()
	values1 := map[string]any{"a": 1, "b": 2}
	values2 := map[string]any{"b": 2, "a": 1} // different in-memory order
	opts := Options{KubeVersion: "1.33"}

	k1 := computeTemplateKey(
		chartDescription{fingerprint: chartFixtureDescription(t, ch).fingerprint, chart: ch},
		values1,
		opts,
		hr,
	)
	k2 := computeTemplateKey(
		chartDescription{fingerprint: chartFixtureDescription(t, ch).fingerprint, chart: ch},
		values2,
		opts,
		hr,
	)
	{
		assert.ErrorIf(t, k1 != k2, "same logical values must produce same key:\nk1=%s\nk2=%s", k1, k2)
	}
}

type testTemplateCacheOCIIdentityCaseInputs struct {
	initial *store.SourceArtifact
	disable *bool
	tc      *struct {
		name                   string
		next                   store.SourceArtifact
		wantSameOutput         bool
		wantFingerprintChanged bool
		marker                 string
	}
}

func testTemplateCacheOCIIdentityCase(t *testing.T, testCaseInput *testTemplateCacheOCIIdentityCaseInputs) {
	t.Helper()
	const next = "sha256:abcdef12345675476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8"

	st, hr, dir := ociRenderFixture(t)
	original := hr.Clone()
	cli := NewClient(cachetest.New(t, t.TempDir(), cache.New))
	cliSources := NewSources(st)
	opts := Options{DisableChartDigestTracking: (*testCaseInput.disable)}
	held := acquireChartFS(t, cli.cache, chartFixtureFS(hr, os.DirFS(dir)))
	var canonical *chart.Chart
	render := func(identity store.SourceArtifact) ([]map[string]any, string, string) {
		t.Helper()
		identity.Kind, identity.LocalPath, identity.FS = manifest.KindOCIRepository, dir, held
		st.SetArtifact(
			manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindOCIRepository,
				Namespace: "apps",
				Name:      "podinfo",
			},
			&identity,
		)
		out, receipt1, err := testutil.RenderMaps(
			cli.Template(t.Context(), hr, nil, RenderOptions{Options: opts, Sources: cliSources}),
		)
		if err == nil {
			{
				recordErr := cli.RecordRender(
					t.Context(),
					RenderAcceptance{Owner: hr.Named(), Receipt: receipt1, Records: emptyRenderInventory},
				)
				assert.NoError(t, recordErr)
			}
		}

		{
			assert.NoError(t, err)
		}
		loaded, err := loadDecodedChart(t.Context(), cli, hr, cliSources)
		{
			assert.NoError(t, err)
		}
		if canonical == nil {
			canonical = loaded.chart
		}
		assert.Equal(t, loaded.chart == canonical, true)
		assert.Equal(t, loaded.artifact.Revision, identity.Revision)
		assert.Equal(t, loaded.artifact.Digest, identity.Digest)
		assert.Equal(t, loaded.chart.Metadata.Version, "6.15.0")
		assert.Equal(t, loaded.chart.Dependencies()[0].Metadata.Version, "1.2.3+child")
		loaded.oci = &ociChartContent{
			Digest:          identity.Digest,
			Revision:        identity.Revision,
			DisableTracking: (*testCaseInput.disable),
		}
		valuesKey := chartValuesCacheKey(loaded, hr.ChartValuesFiles, hr.IgnoreMissingValuesFiles, "")
		merged, ok := cli.chartValuesCache[valuesKey]
		{
			assert.FatalIf(
				t,
				!ok || valuesKey == "",
				"%s",
				"render did not cache values under its effective fingerprint",
			)
		}
		key := computeTemplateKey(loaded, merged.merged, opts, hr)
		repeated, receipt2, err := testutil.RenderMaps(
			cli.Template(t.Context(), hr, nil, RenderOptions{Options: opts, Sources: cliSources}),
		)
		if err == nil {
			{
				recordErr := cli.RecordRender(
					t.Context(),
					RenderAcceptance{Owner: hr.Named(), Receipt: receipt2, Records: emptyRenderInventory},
				)
				assert.NoError(t, recordErr)
			}
		}

		{
			assert.FatalIf(
				t,
				err != nil || !reflect.DeepEqual(repeated, out),
				"repeat render differs: %v",
				err,
			)
		}
		return out, key, valuesKey
	}
	first, key, valuesKey := render((*testCaseInput.initial))
	assert.FatalIf(t, !hasRenderField(first, "marker", "original"), "%v", first)
	if testCaseInput.tc.marker != "" {
		template, err := os.ReadFile(filepath.Join(dir, "templates/cm.yaml"))
		{
			assert.NoError(t, err)
		}
		dir = t.TempDir()
		writeChartFiles(t, dir, "podinfo", "6.15.0")
		testutil.WriteFile(t, dir, "values.yaml", "marker: default\n")
		testutil.WriteFile(t, dir, "prod.yaml", "marker: "+testCaseInput.tc.marker+"\n")
		testutil.WriteFile(
			t,
			dir,
			"charts/child/Chart.yaml",
			"apiVersion: v2\nname: child\nversion: 1.2.3+child\n",
		)
		testutil.WriteFile(t, dir, "templates/cm.yaml", string(template))
		t.Run(
			"updated acquisition",
			func(t *testing.T) { held = acquireChartFS(t, cli.cache, chartFixtureFS(hr, os.DirFS(dir))) },
		)
		canonical = nil
	}
	second, nextKey, nextValuesKey := render(testCaseInput.tc.next)
	assert.Equal(t, reflect.DeepEqual(first, second), testCaseInput.tc.wantSameOutput)
	assert.Equal(t, key != nextKey, testCaseInput.tc.wantFingerprintChanged)
	assert.Equal(t, valuesKey != nextValuesKey, testCaseInput.tc.wantFingerprintChanged)
	assert.FatalIf(
		t,
		testCaseInput.tc.marker != "" && !hasRenderField(second, "marker", testCaseInput.tc.marker),
		"%v",
		second,
	)
	{
		diff := cmp.Diff(original, hr, cmpopts.IgnoreUnexported(manifest.HelmRelease{}))
		assert.FatalIf(t, diff != "", "%v", diff)
	}
	for _, revision := range []string{"6.14.0@" + next, "invalid"} {
		st.SetArtifact(
			manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindOCIRepository,
				Namespace: "apps",
				Name:      "podinfo",
			},
			&store.SourceArtifact{
				Kind:      manifest.KindOCIRepository,
				FS:        held,
				LocalPath: dir,
				Digest:    next,
				Revision:  revision,
			},
		)
		_, receipt3, err := testutil.RenderMaps(
			cli.Template(t.Context(), hr, nil, RenderOptions{Options: opts, Sources: cliSources}),
		)
		if err == nil {
			{
				recordErr := cli.RecordRender(
					t.Context(),
					RenderAcceptance{Owner: hr.Named(), Receipt: receipt3, Records: emptyRenderInventory},
				)
				assert.NoError(t, recordErr)
			}
		}
		{
			assert.FatalIf(
				t,
				!errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux),
				"invalid revision bypassed validation: %v",
				err,
			)
		}
	}
}

func testTemplateCacheOCIIdentityOneAdmittedArtifact(t *testing.T) {
	t.Helper()
	const digest = "sha256:ff3d3e14728f75476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8"

	st, hr, dir := ociRenderFixture(t)
	reads := map[string]int{}
	input := chartFixtureFS(hr, os.DirFS(dir))
	observed := chartOpenFS{
		FS:   input,
		open: func(name string) (fs.File, error) { reads[name]++; return input.Open(name) },
	}
	first := &store.SourceArtifact{FS: observed, Key: "untrusted", Revision: "6.15.0@" + digest, Digest: digest}
	second := &store.SourceArtifact{Revision: "invalid", FS: fstest.MapFS{}}
	resolver := &changingChartReader{sourceReader: st, artifacts: []*store.SourceArtifact{first, second}}
	cli := NewClient(cachetest.New(t, "", cache.New))
	cliSources := NewSources(resolver)
	out, receipt4, err := testutil.RenderMaps(
		cli.Template(t.Context(), hr, nil, RenderOptions{Options: Options{}, Sources: cliSources}),
	)
	if err == nil {
		{
			recordErr := cli.RecordRender(
				t.Context(),
				RenderAcceptance{Owner: hr.Named(), Receipt: receipt4, Records: emptyRenderInventory},
			)
			assert.NoError(t, recordErr)
		}
	}

	assert.NoError(t, err)
	assert.Equal(t, resolver.calls, 1)
	assert.Equal(t, reads[".helmignore"], 0)
	assert.Equal(t, reads["chart/templates/cm.yaml"], 1)
	assert.Equal(t, hasRenderField(out, "version", "6.15.0+ff3d3e14728f"), true)
	assert.Equal(t, first.Key, "untrusted")
}

type testTemplateCacheSubchartPersistenceCaseInputs struct {
	hr       **manifest.HelmRelease
	dir      *string
	keyFor   *func(t *testing.T, cli *Client, cliSources Sources) cache.BlobKey
	readBlob *func(t *testing.T, owner *cache.Cache, key cache.BlobKey) []map[string]any
	owner    **cache.Cache
	key      *cache.BlobKey
	cold     *[]map[string]any
	marked   *[]map[string]any
	field    *string
}

func renderPersistenceEligibleLoop(loopInputs struct {
	info     **debug.BuildInfo
	eligible *bool
},
) {
	for _, dep := range (*loopInputs.info).Deps {
		if dep.Replace != nil && dep.Replace.Version == "" {
			(*loopInputs.eligible) = false
		}
	}
}
