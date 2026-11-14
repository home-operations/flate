package helm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/storage"
	chart "helm.sh/helm/v4/pkg/chart/v2"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

func TestLoadChart_CoalescesConcurrentFirstLoad(t *testing.T) {
	t.Parallel()
	for _, archive := range []bool{false, true} {
		for _, pinned := range []bool{false, true} {
			t.Run(fmt.Sprintf("archive_%t/pinned_%t", archive, pinned), func(t *testing.T) {
				t.Parallel()
				testLoadChartCoalescesConcurrentFirstLoadCase(
					t,
					&testLoadChartCoalescesConcurrentFirstLoadCaseInputs{archive: &archive, pinned: &pinned},
				)
			},
			)
		}
	}
}

func TestLoadChart_InvalidatesOnFileMtimeChange(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	files := map[string]string{
		"mychart/Chart.yaml":                     "apiVersion: v2\nname: mychart\nversion: 0.1.0\ndescription: first\n",
		"mychart/values.yaml":                    "k: v1\n",
		"mychart/templates/cm.yaml":              "kind: ConfigMap\n",
		"mychart/charts/child/Chart.yaml":        "apiVersion: v2\nname: child\nversion: 0.1.0\ndescription: first\n",
		"mychart/charts/child/templates/cm.yaml": "kind: ConfigMap\n",
	}
	for name, body := range files {
		testutil.WriteFile(t, dir, name, body)
	}
	cli := NewClient(cachetest.New(t, "", cache.New))
	cliSources := localChartResolver(t, "chart-repo", "flux-system", dir)
	hr := newHR()
	first, err := loadDecodedChart(t.Context(), cli, hr, cliSources)
	assert.NoError(t, err)
	stamp := time.Unix(100, 0)
	for name := range files {
		{
			chtimesErr := os.Chtimes(filepath.Join(dir, name), stamp, stamp)
			assert.NoError(t, chtimesErr)
		}
	}
	touched, err := loadDecodedChart(t.Context(), cli, hr, cliSources)
	assert.NoError(t, err)
	assert.FatalIf(
		t,
		touched.chart != first.chart || touched.fingerprint != first.fingerprint,
		"%s",
		"unchanged-byte touch reloaded chart",
	)
	for _, tc := range []struct{ name, path, from, to string }{
		{"metadata", "mychart/Chart.yaml", "first", "other"},
		{"defaults", "mychart/values.yaml", "v1", "v2"},
		{"template", "mychart/templates/cm.yaml", "ConfigMap", "Namespace"},
		{"dependency metadata", "mychart/charts/child/Chart.yaml", "first", "other"},
		{"dependency template", "mychart/charts/child/templates/cm.yaml", "ConfigMap", "Namespace"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testLoadChartInvalidatesOnFileMtimeChangeCase(
				t,
				&testLoadChartInvalidatesOnFileMtimeChangeCaseInputs{
					files: &files,
					hr:    &hr,
					stamp: &stamp,
					tc:    &tc,
				},
			)
		},
		)
	}
}

func TestLoadChart_PerCallCloneIsolatesValuesAndDependencies(t *testing.T) {
	t.Parallel()
	input := fstest.MapFS{
		"Chart.yaml": {
			Data: []byte(
				"apiVersion: v2\nname: parent\nversion: 1.0.0\ndependencies:\n- name: child\n  version: 1.0.0\n  condition: child.enabled\n",
			),
		},
		"values.yaml":              {Data: []byte("child:\n  enabled: true\n  value: canonical\n")},
		"charts/child/Chart.yaml":  {Data: []byte("apiVersion: v2\nname: child\nversion: 1.0.0\n")},
		"charts/child/values.yaml": {Data: []byte("value: canonical\n")},
		"charts/child/templates/cm.yaml": {
			Data: []byte(
				"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: child\ndata:\n  value: {{ .Values.value | quote }}\n",
			),
		},
	}
	cli := NewClient(cachetest.New(t, "", cache.New))
	st := store.New()
	hr := &manifest.HelmRelease{
		Name:      "demo",
		Namespace: "default",
		Chart: manifest.HelmChart{
			Name: ".",
			Source: manifest.NamedResource{
				Name:      "repo",
				Namespace: "default",
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindGitRepository,
			},
		},
	}
	st.SetArtifact(hr.ChartSource(), &store.SourceArtifact{FS: input})
	cliSources := NewSources(st)
	canonical, err := loadDecodedChart(t.Context(), cli, hr, cliSources)
	assert.NoError(t, err)
	snapshot := func() string {
		t.Helper()
		data, marshalErr := json.Marshal(
			[]any{
				canonical.chart,
				canonical.chart.Raw,
				canonical.chart.Dependencies()[0],
				canonical.chart.Dependencies()[0].Raw,
			},
		)
		assert.NoError(t, marshalErr)
		return string(data)
	}
	before := snapshot()
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range 16 {
		wg.Go(func() {
			<-start
			enabled := i%2 == 0
			value := fmt.Sprintf("rendered-%02d", i)
			local := store.New()
			local.SetArtifact(hr.ChartSource(), &store.SourceArtifact{FS: input})
			sources := NewSources(local)
			out, _, renderMapsErr := testutil.RenderMaps(
				cli.Template(
					t.Context(),
					hr,
					map[string]any{"child": map[string]any{"enabled": enabled, "value": value}},
					RenderOptions{Options: Options{}, Sources: sources},
				),
			)
			if renderMapsErr != nil {
				t.Error(renderMapsErr)
				return
			}
			assert.Equal(t, hasRenderField(out, "kind", "ConfigMap"), enabled)
			assert.Equal(t, hasRenderField(out, "value", value), enabled)
			loaded, renderMapsErr := loadDecodedChart(t.Context(), cli, hr, sources)
			assert.NoError(t, renderMapsErr)
			assert.Equal(t, loaded.chart == canonical.chart, true)
		})
	}
	close(start)
	wg.Wait()
	assert.Equal(t, snapshot(), before)
	loaded, err := loadDecodedChart(t.Context(), cli, hr, cliSources)
	assert.NoError(t, err)
	assert.Equal(t, loaded.chart == canonical.chart, true)
}

type chartBacking struct {
	billy.Filesystem

	reads atomic.Int32
}

//nolint:ireturn // The upstream interface contract requires this return type.
func (b *chartBacking) Open(name string) (billy.File, error) {
	file, err := b.Filesystem.Open(name)
	if err != nil {
		return nil, err
	}
	return &chartBackingFile{File: file, reads: &b.reads}, nil
}

type chartBackingFile struct {
	billy.File

	reads *atomic.Int32
}

// Go-git reuses pack descriptors, so later observations MUST count reads on held files.
func (f *chartBackingFile) Read(p []byte) (int, error) {
	f.reads.Add(1)
	return f.File.Read(p)
}

func (f *chartBackingFile) ReadAt(p []byte, offset int64) (int, error) {
	f.reads.Add(1)
	return f.File.ReadAt(p, offset)
}

func TestLoadChart_AcquiredGitWarmReuse(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	for _, dir := range []string{".", "one", "two"} {
		testutil.WriteFile(t, root, dir+"/Chart.yaml", "apiVersion: v2\nname: chart\nversion: 1.0.0\n")
		testutil.WriteFile(
			t,
			root,
			dir+"/templates/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: chart\ndata:\n  marker: {{ .Values.marker | quote }}\n",
		)
	}
	testutil.WriteFile(t, root, "config/prod.yaml", "marker: acquired\n")
	repo := testutil.GitInit(t, root)
	testutil.GitCommitAll(t, repo, "chart", &git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e"}})
	owner := cachetest.New(t, "", cache.New)
	produce := func(ctx context.Context, storer storage.Storer, worktree billy.Filesystem) (plumbing.Hash, error) {
		repo, err := git.CloneContext(ctx, storer, worktree, &git.CloneOptions{URL: "file://" + root, NoCheckout: true})
		if err != nil {
			return plumbing.ZeroHash, err
		}
		head, err := repo.Head()
		if err != nil {
			return plumbing.ZeroHash, err
		}
		return head.Hash(), nil
	}
	cli := NewClient(owner)
	st := store.New()
	cliSources := NewSources(st)
	hr := &manifest.HelmRelease{
		Name:      "first",
		Namespace: "default",
		Chart: manifest.HelmChart{
			Name: "one",
			Source: manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindGitRepository,
				Name:      "repo",
				Namespace: "default",
			},
		},
	}
	var previous *chart.Chart
	for _, id := range []string{"git-one", "git-two"} {
		generation, _, err := owner.Git(
			t.Context(),
			cache.GitKey{Content: id},
			&cache.Policy{MaxAge: cache.Forever},
			produce,
		)
		assert.NoError(t, err)

		for _, persistent := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/persistent_%t", id, persistent), func(t *testing.T) {
				t.Parallel()
				testLoadChartAcquiredGitWarmReuseCase(
					t,
					&testLoadChartAcquiredGitWarmReuseCaseInputs{
						hr:         &hr,
						id:         &id,
						generation: &generation,
						persistent: &persistent,
					},
				)
			},
			)
		}
		backing := &chartBacking{Filesystem: generation.Backing}
		input, pin, err := gittree.New(
			t.Context(),
			backing,
			generation.Commit,
			gittree.ViewOptions{Content: struct {
				Source string `json:"Source"`
			}{id}},
		)
		assert.NoError(t, err)
		assert.Equal(t, pin == "", false)
		art := &store.SourceArtifact{FS: input, Key: "untrusted", InputAlias: true, LocalPath: root, Revision: id}
		st.SetArtifact(hr.ChartSource(), art)
		first, err := loadDecodedChart(t.Context(), cli, hr, cliSources)
		assert.NoError(t, err)
		assert.FatalIf(t, first.chart == previous, "%s", "different acquisition pins shared parsed entries")
		previous = first.chart
		reads := backing.reads.Load()
		other := hr.Clone()
		other.Name = "second"
		warm, err := loadDecodedChart(t.Context(), cli, other, cliSources)
		assert.NoError(t, err)
		assert.Equal(t, warm.chart == first.chart, true)
		assert.Equal(t, backing.reads.Load(), reads)
		values, err := cli.mergeChartValuesFiles(t.Context(), &first, []string{"config/prod.yaml"}, false)
		assert.NoError(t, err)
		assert.Equal(t, values["marker"], any("acquired"))
		values, err = cli.mergeChartValuesFiles(t.Context(), &warm, []string{"config/prod.yaml"}, false)
		assert.NoError(t, err)
		assert.Equal(t, values["marker"], any("acquired"))
		other.Chart.Name = "two"
		second, err := loadDecodedChart(t.Context(), cli, other, cliSources)
		assert.NoError(t, err)
		assert.Equal(t, second.chart == first.chart, false)
		explicit := hr.Clone()
		explicit.Chart.Name = "."
		rootChart, err := loadDecodedChart(t.Context(), cli, explicit, cliSources)
		assert.NoError(t, err)
		auto := explicit.Clone()
		auto.Chart.Source.Kind = manifest.KindHelmChart
		automatic := *art
		automatic.FS, _, err = gittree.New(
			t.Context(),
			backing,
			generation.Commit,
			gittree.ViewOptions{Mode: gittree.Source, Sparse: []string{"one"}, Content: struct {
				Source string `json:"Source"`
			}{id}},
		)
		assert.NoError(t, err)
		st.SetArtifact(auto.ChartSource(), &automatic)
		autoChart, err := loadDecodedChart(t.Context(), cli, auto, cliSources)
		assert.NoError(t, err)
		assert.Equal(t, autoChart.chart == rootChart.chart, false)
		reads = backing.reads.Load()
		warm, err = loadDecodedChart(t.Context(), cli, auto, cliSources)
		assert.NoError(t, err)
		assert.Equal(t, warm.chart == autoChart.chart, true)
		assert.Equal(t, backing.reads.Load(), reads)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err = loadDecodedChart(ctx, cli, auto, cliSources)
		assert.Equal(t, errors.Is(err, context.Canceled), true)
		assert.Equal(t, art.Key, "untrusted")
	}

	for _, tc := range []struct{ name, kind, shape string }{
		{"OCI chart", manifest.KindHelmChart, "file"},
		{"HTTP chart", manifest.KindHelmChart, "file"},
		{"OCI directory", manifest.KindOCIRepository, "directory"},
		{"OCI archive", manifest.KindOCIRepository, "file"},
		{"Bucket directory", manifest.KindBucket, "directory"},
		{"Bucket archive", manifest.KindBucket, "archive"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testLoadChartAcquiredGitWarmReuseCase2(t, &tc)
		},

		// Stat opens the backing; poisoning below the view preserves acquisition authority.

		)
	}
}

type testLoadChartCoalescesConcurrentFirstLoadCaseInputs struct {
	archive *bool
	pinned  *bool
}

func testLoadChartCoalescesConcurrentFirstLoadCase(
	t *testing.T,
	testCaseInput *testLoadChartCoalescesConcurrentFirstLoadCaseInputs,
) {
	t.Helper()

	const callers = 16
	root := t.TempDir()
	testutil.WriteFile(t, root, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 1.0.0\n")
	testutil.WriteFile(t, root, "mychart/templates/cm.yaml", "kind: ConfigMap\n")
	testutil.WriteFile(
		t,
		root,
		"mychart/charts/child/Chart.yaml",
		"apiVersion: v2\nname: child\nversion: 1.0.0\n",
	)
	owner := cachetest.New(t, "", cache.New)
	local, err := gitrepo.Locate(t.Context(), root, root)
	assert.NoError(t, err)
	admittedDirectory, err := gittree.OpenDirectory(t.Context(), local)
	assert.NoError(t, err)
	admitted := admittedDirectory.Artifact()
	assert.NoError(t, admittedDirectory.Close())
	input := admitted.FS
	location := "mychart"
	if *testCaseInput.archive {
		sub, err := fs.Sub(input, "mychart")
		assert.NoError(t, err)
		input = fstest.MapFS{"chart.tgz": {Data: chartArchive(t, sub)}}
		location = "chart.tgz"
	}
	if *testCaseInput.pinned {
		input = acquireChartFS(t, owner, input)
	}
	cli := NewClient(owner)
	st := store.New()
	hr := &manifest.HelmRelease{
		Name:      "demo",
		Namespace: "default",
		Chart: manifest.HelmChart{
			Name: location,
			Source: manifest.NamedResource{
				Name:      "repo",
				Namespace: "default",
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindGitRepository,
			},
		},
	}
	st.SetArtifact(hr.ChartSource(), &store.SourceArtifact{FS: input, LocalPath: root, Key: "untrusted"})
	cliSources := NewSources(st)
	var charts [callers]*chart.Chart
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range callers {
		wg.Go(func() {
			<-start
			loaded, err := loadDecodedChart(t.Context(), cli, hr, cliSources)
			if err != nil {
				t.Error(err)
				return
			}
			charts[i] = loaded.chart
		})
	}
	close(start)
	wg.Wait()
	for _, ch := range charts {
		assert.FatalIf(
			t,
			ch == nil || ch != charts[0],
			"%s",
			"concurrent identical inputs did not share the canonical chart",
		)
	}
	assert.Equal(t, len(cli.chartCache), 1)
}

type testLoadChartInvalidatesOnFileMtimeChangeCaseInputs struct {
	files *map[string]string
	hr    **manifest.HelmRelease
	stamp *time.Time
	tc    *struct {
		name string
		path string
		from string
		to   string
	}
}

func testLoadChartInvalidatesOnFileMtimeChangeCase(
	t *testing.T,
	testCaseInput *testLoadChartInvalidatesOnFileMtimeChangeCaseInputs,
) {
	t.Helper()
	dir := testutil.WriteTree(t, *testCaseInput.files)
	for name := range *testCaseInput.files {
		err := os.Chtimes(filepath.Join(dir, name), *testCaseInput.stamp, *testCaseInput.stamp)
		assert.NoError(t, err)
	}
	cli := NewClient(cachetest.New(t, "", cache.New))
	cliSources := localChartResolver(t, "chart-repo", "flux-system", dir)
	before, err := loadDecodedChart(t.Context(), cli, (*testCaseInput.hr), cliSources)
	assert.NoError(t, err)
	body := strings.ReplaceAll(
		(*testCaseInput.files)[testCaseInput.tc.path],
		testCaseInput.tc.from,
		testCaseInput.tc.to,
	)
	testutil.WriteFile(t, dir, testCaseInput.tc.path, body)
	{
		chtimesErr2 := os.Chtimes(
			filepath.Join(dir, testCaseInput.tc.path),
			(*testCaseInput.stamp),
			(*testCaseInput.stamp),
		)
		assert.NoError(t, chtimesErr2)
	}
	after, err := loadDecodedChart(t.Context(), cli, (*testCaseInput.hr), cliSources)
	assert.NoError(t, err)
	assert.FatalIf(t, after.chart == before.chart ||
		computeTemplateKey(
			&after,
			nil,
			Options{},
			(*testCaseInput.hr),
		) == computeTemplateKey(
			&before,
			nil,
			Options{},
			(*testCaseInput.hr),
		), "%s", "same-size, same-mtime edit reused chart content")
}

type testLoadChartAcquiredGitWarmReuseCaseInputs struct {
	hr         **manifest.HelmRelease
	id         *string
	generation **cache.GitGeneration
	persistent *bool
}

func testLoadChartAcquiredGitWarmReuseCase(t *testing.T, testCaseInput *testLoadChartAcquiredGitWarmReuseCaseInputs) {
	t.Helper()

	persistent := cache.RenderPersistenceDisabledReason() == ""
	outputRoot := ""
	if *testCaseInput.persistent {
		outputRoot = t.TempDir()
	}
	outputOwner := cachetest.New(t, outputRoot, cache.New)
	assert.Equal(t, outputOwner.CanPersistRenders(), persistent && *testCaseInput.persistent)
	renderer := NewClient(outputOwner)
	backing := &chartBacking{Filesystem: (*testCaseInput.generation).Backing}
	input, _, newErr := gittree.New(
		t.Context(),
		backing, (*testCaseInput.generation).Commit, gittree.ViewOptions{Content: struct {
			Source string `json:"Source"`
		}{(*testCaseInput.id)}},
	)
	assert.NoError(t, newErr)
	selected := (*testCaseInput.hr).Clone()
	selected.ChartValuesFiles = []string{"config/prod.yaml"}
	sources := store.New()
	sources.SetArtifact(selected.ChartSource(), &store.SourceArtifact{FS: input})
	rendererSources := NewSources(sources)
	first, _, newErr := testutil.RenderMaps(
		renderer.Template(
			t.Context(),
			selected,
			nil,
			RenderOptions{Options: Options{}, Sources: rendererSources},
		),
	)
	assert.NoError(t, newErr)
	assert.FatalIf(t, !hasRenderField(first, "marker", "acquired"), "%v", first)
	reads := backing.reads.Load()
	assert.FatalIf(t, reads == 0, "%s", "first render did not read acquired backing")
	second, _, newErr := testutil.RenderMaps(
		renderer.Template(
			t.Context(),
			selected,
			nil,
			RenderOptions{Options: Options{}, Sources: rendererSources},
		),
	)
	assert.NoError(t, newErr)
	assert.Diff(t, second, first)
	assert.Equal(t, backing.reads.Load(), reads)
	if !(*testCaseInput.persistent) {
		return
	}
	d, newErr := renderer.describeChart(t.Context(), selected, rendererSources.chartInput(selected))
	assert.NoError(t, newErr)
	d.values, newErr = renderer.observeValuesFiles(t.Context(), &d, selected.ChartValuesFiles, false)
	assert.NoError(t, newErr)
	key := cache.BlobKey{
		Content: computeTemplateKey(&d, map[string]any{}, Options{}, selected),
		Render:  cache.RenderAccepted,
	}
	witness := manifest.DeepCopyMap(first[0])
	witness["cacheWitness"] = "pinned persistent hit witness"
	output := encodedRender(t, []map[string]any{witness})
	_, _, _, newErr = outputOwner.Blob(
		t.Context(),
		key,
		&cache.Policy{},
		func(_ context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
			writeTestRecordErr := writeTestRecord(
				t.Context(),
				w,
				key,
				&renderRecord{Output: output, Analysis: d.analyze()},
			)
			return cache.ProducerResult{}, writeTestRecordErr
		},
	)
	assert.NoError(t, newErr)

	fresh := NewClient(cachetest.New(t, outputRoot, cache.New))
	freshSources := NewSources(sources)
	before := backing.reads.Load()
	hit, _, newErr := testutil.RenderMaps(
		fresh.Template(
			t.Context(),
			selected,
			nil,
			RenderOptions{Options: Options{}, Sources: freshSources},
		),
	)
	assert.NoError(t, newErr)
	want := first
	if persistent {
		want = []map[string]any{witness}
	}
	assert.Diff(t, hit, want)
	reads = backing.reads.Load()
	assert.FatalIf(
		t,
		reads <= before || len(fresh.chartValuesCache) != 1,
		"%s",
		"fresh persisted hit did not observe its values",
	)
	for _, entry := range fresh.chartValuesCache {
		assert.FatalIf(
			t,
			entry.merged != nil || entry.inputs == nil,
			"%s",
			"persisted hit did not leave an observation-only entry",
		)
	}
	hit, _, newErr = testutil.RenderMaps(
		fresh.Template(
			t.Context(),
			selected,
			nil,
			RenderOptions{Options: Options{}, Sources: freshSources},
		),
	)
	assert.NoError(t, newErr)
	assert.Diff(t, hit, want)
	assert.Equal(t, backing.reads.Load(), reads)
	t.Log("verified pinned persisted hits and observation-only reuse")
}

func testLoadChartAcquiredGitWarmReuseCase2(t *testing.T, testCaseInput *struct {
	name  string
	kind  string
	shape string
},
) {
	t.Helper()

	cacheRoot := t.TempDir()
	owner := cachetest.New(t, cacheRoot, cache.New)
	cli := NewClient(owner)
	st := store.New()
	cliSources := NewSources(st)
	hr := &manifest.HelmRelease{
		Chart: manifest.HelmChart{
			Source: manifest.NamedResource{Group: manifest.SourceDomain, Kind: testCaseInput.kind, Name: "chart"},
		},
	}
	if testCaseInput.kind == manifest.KindBucket {
		hr.Chart.Name = "."
		if testCaseInput.shape == "archive" {
			hr.Chart.Name = "chart.tgz"
		}
	}
	var previous *chart.Chart
	for _, version := range []string{"1.0.0", "2.0.0"} {
		input := fstest.MapFS{
			"Chart.yaml": {Data: []byte("apiVersion: v2\nname: chart\nversion: " + version + "\n")},
		}
		var acquired fs.FS
		backingName := "Chart.yaml"
		if testCaseInput.shape == "directory" {
			input = fstest.MapFS{"nested/Chart.yaml": input["Chart.yaml"]}
			backingName = "nested/Chart.yaml"
			if testCaseInput.kind == manifest.KindBucket {
				hr.Chart.Name = "nested"
			}
		}
		if testCaseInput.shape == "file" {
			data := chartArchive(t, input)
			r, _, _, err := owner.Blob(
				t.Context(),
				cache.BlobKey{Content: version},
				&cache.Policy{MaxAge: cache.Forever},
				func(_ context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
					_, err := io.Copy(w, bytes.NewReader(data))
					return cache.ProducerResult{}, err
				},
			)
			assert.NoError(t, err)
			backingName = "unconventional.payload"
			acquired = cache.NameBlob(backingName, r)
		} else {
			if testCaseInput.shape == "archive" {
				input = fstest.MapFS{"chart.tgz": {Data: chartArchive(t, input)}}
				backingName = "chart.tgz"
			}
			t.Run(version, func(t *testing.T) { acquired = acquireChartFS(t, owner, input) })
		}
		art := &store.SourceArtifact{FS: acquired, Key: "untrusted", LocalPath: "same-label", Revision: version}
		st.SetArtifact(hr.ChartSource(), art)
		loaded, err := loadDecodedChart(t.Context(), cli, hr, cliSources)
		assert.NoError(t, err)
		assert.Equal(t, loaded.chart.Metadata.Version, version)
		assert.Equal(t, loaded.chart != previous, true)
		previous = loaded.chart

		assert.NoError(t, os.RemoveAll(filepath.Join(cacheRoot, "oci", "blobs")))
		_, err = fs.ReadFile(acquired, backingName)
		assert.FatalIf(t, err == nil, "%s", "backing poison did not prevent chart reads")
		if testCaseInput.shape == "file" {
			_, loadDecodedChartErr := loadDecodedChart(t.Context(), NewClient(owner), hr, cliSources)
			assert.FatalIf(
				t,
				!errors.Is(loadDecodedChartErr, fs.ErrNotExist) ||
					!errors.Is(loadDecodedChartErr, manifest.ErrInput),
				"lost acquired payload failure: %v",
				loadDecodedChartErr,
			)
		}
		warm, err := loadDecodedChart(t.Context(), cli, hr, cliSources)
		assert.NoError(t, err)
		assert.Equal(t, warm.chart == loaded.chart, true)
		assert.Equal(t, warm.fingerprint, loaded.fingerprint)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err = loadDecodedChart(ctx, cli, hr, cliSources)
		assert.Equal(t, errors.Is(err, context.Canceled), true)
		explicit := hr.Clone()
		explicit.Chart.Source.Kind, explicit.Chart.Name = manifest.KindGitRepository, "."
		st.SetArtifact(explicit.ChartSource(), art)
		{
			_, err := loadDecodedChart(t.Context(), cli, explicit, cliSources)
			assert.FatalIf(t, err == nil, "%s", "explicit root reused a different chart selector")
		}
	}
}
