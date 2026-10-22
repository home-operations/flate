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

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
	chart "helm.sh/helm/v4/pkg/chart/v2"
)

func TestLoadChart_CoalescesConcurrentFirstLoad(t *testing.T) {
	t.Parallel()
	for _, archive := range []bool{false, true} {
		for _, pinned := range []bool{false, true} {
			t.Run(fmt.Sprintf("archive_%t/pinned_%t", archive, pinned), func(t *testing.T) {
				t.Parallel()
				const callers = 16
				root := t.TempDir()
				testutil.WriteFile(t, root, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 1.0.0\n")
				testutil.WriteFile(t, root, "mychart/templates/cm.yaml", "kind: ConfigMap\n")
				testutil.WriteFile(t, root, "mychart/charts/child/Chart.yaml", "apiVersion: v2\nname: child\nversion: 1.0.0\n")
				owner := cachetest.New(t, "", cache.New)
				local, err := gitrepo.Locate(t.Context(), root, root)
				assert.NoError(t, err)
				admitted, err := gittree.AdmitDirectory(t.Context(), local)
				assert.NoError(t, err)
				input := admitted.FS
				location := "mychart"
				if archive {
					sub, err := fs.Sub(input, "mychart")
					assert.NoError(t, err)
					input = fstest.MapFS{"chart.tgz": {Data: chartArchive(t, sub)}}
					location = "chart.tgz"
				}
				if pinned {
					input = acquireChartFS(t, owner, input)
				}
				cli := NewClient(owner)
				st := store.New()
				hr := &manifest.HelmRelease{Name: "demo", Namespace: "default", Chart: manifest.HelmChart{Name: location, Source: manifest.NamedResource{Name: "repo", Namespace: "default", Group: manifest.SourceDomain, Kind: manifest.KindGitRepository}}}
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
					if ch == nil || ch != charts[0] {
						t.Fatal("concurrent identical inputs did not share the canonical chart")
					}
				}
				assert.Equal(t, len(cli.chartCache), 1)
			})
		}
	}
}

func TestLoadChart_InvalidatesOnFileMtimeChange(t *testing.T) {
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
	if err != nil {
		t.Fatal(err)
	}
	stamp := time.Unix(100, 0)
	for name := range files {
		if err := os.Chtimes(filepath.Join(dir, name), stamp, stamp); err != nil {
			t.Fatal(err)
		}
	}
	touched, err := loadDecodedChart(t.Context(), cli, hr, cliSources)
	if err != nil {
		t.Fatal(err)
	}
	if touched.chart != first.chart || touched.fingerprint != first.fingerprint {
		t.Fatal("unchanged-byte touch reloaded chart")
	}
	for _, tc := range []struct{ name, path, from, to string }{
		{"metadata", "mychart/Chart.yaml", "first", "other"},
		{"defaults", "mychart/values.yaml", "v1", "v2"},
		{"template", "mychart/templates/cm.yaml", "ConfigMap", "Namespace"},
		{"dependency metadata", "mychart/charts/child/Chart.yaml", "first", "other"},
		{"dependency template", "mychart/charts/child/templates/cm.yaml", "ConfigMap", "Namespace"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before, err := loadDecodedChart(t.Context(), cli, hr, cliSources)
			if err != nil {
				t.Fatal(err)
			}
			body := strings.ReplaceAll(files[tc.path], tc.from, tc.to)
			testutil.WriteFile(t, dir, tc.path, body)
			if err := os.Chtimes(filepath.Join(dir, tc.path), stamp, stamp); err != nil {
				t.Fatal(err)
			}
			after, err := loadDecodedChart(t.Context(), cli, hr, cliSources)
			if err != nil {
				t.Fatal(err)
			}
			if after.chart == before.chart || computeTemplateKey(after, nil, Options{}, hr) == computeTemplateKey(before, nil, Options{}, hr) {
				t.Fatal("same-size, same-mtime edit reused chart content")
			}
		})
	}
}

func TestLoadChart_PerCallCloneIsolatesValuesAndDependencies(t *testing.T) {
	t.Parallel()
	input := fstest.MapFS{
		"Chart.yaml":                     {Data: []byte("apiVersion: v2\nname: parent\nversion: 1.0.0\ndependencies:\n- name: child\n  version: 1.0.0\n  condition: child.enabled\n")},
		"values.yaml":                    {Data: []byte("child:\n  enabled: true\n  value: canonical\n")},
		"charts/child/Chart.yaml":        {Data: []byte("apiVersion: v2\nname: child\nversion: 1.0.0\n")},
		"charts/child/values.yaml":       {Data: []byte("value: canonical\n")},
		"charts/child/templates/cm.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: child\ndata:\n  value: {{ .Values.value | quote }}\n")},
	}
	cli := NewClient(cachetest.New(t, "", cache.New))
	st := store.New()
	hr := &manifest.HelmRelease{Name: "demo", Namespace: "default", Chart: manifest.HelmChart{Name: ".", Source: manifest.NamedResource{Name: "repo", Namespace: "default", Group: manifest.SourceDomain, Kind: manifest.KindGitRepository}}}
	st.SetArtifact(hr.ChartSource(), &store.SourceArtifact{FS: input})
	cliSources := NewSources(st)
	canonical, err := loadDecodedChart(t.Context(), cli, hr, cliSources)
	assert.NoError(t, err)
	snapshot := func() string {
		t.Helper()
		data, err := json.Marshal([]any{canonical.chart, canonical.chart.Raw, canonical.chart.Dependencies()[0], canonical.chart.Dependencies()[0].Raw})
		assert.NoError(t, err)
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
			out, err := cli.Template(t.Context(), hr, map[string]any{"child": map[string]any{"enabled": enabled, "value": value}}, RenderOptions{Options: Options{}, Sources: cliSources})
			if err != nil {
				t.Error(err)
				return
			}
			assert.Equal(t, hasRenderField(out, "kind", "ConfigMap"), enabled)
			assert.Equal(t, hasRenderField(out, "value", value), enabled)
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
	opens atomic.Int32
}

func (b *chartBacking) Open(name string) (billy.File, error) {
	b.opens.Add(1)
	return b.Filesystem.Open(name)
}

func TestLoadChart_AcquiredGitWarmReuse(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	for _, dir := range []string{".", "one", "two"} {
		testutil.WriteFile(t, root, dir+"/Chart.yaml", "apiVersion: v2\nname: chart\nversion: 1.0.0\n")
		testutil.WriteFile(t, root, dir+"/templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: chart\ndata:\n  marker: {{ .Values.marker | quote }}\n")
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
	hr := &manifest.HelmRelease{Name: "first", Namespace: "default", Chart: manifest.HelmChart{Name: "one", Source: manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Name: "repo", Namespace: "default"}}}
	var previous *chart.Chart
	for _, id := range []string{"git-one", "git-two"} {
		generation, _, err := owner.Git(t.Context(), cache.GitKey{Content: id}, cache.Policy{MaxAge: cache.Forever}, produce)
		assert.NoError(t, err)

		for _, persistent := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/persistent_%t", id, persistent), func(t *testing.T) {
				t.Parallel()
				if persistent && !renderPersistenceEligible(t) {
					t.Skipf("requires clean VCS-stamped test binary: %s", cache.RenderPersistenceDisabledReason())
				}
				outputRoot := ""
				if persistent {
					outputRoot = t.TempDir()
				}
				outputOwner := cachetest.New(t, outputRoot, cache.New)
				renderer := NewClient(outputOwner)
				backing := &chartBacking{Filesystem: generation.Backing}
				input, _, err := gittree.New(t.Context(), backing, generation.Commit, gittree.ViewOptions{Content: struct{ Source string }{id}})
				assert.NoError(t, err)
				selected := hr.Clone()
				selected.ChartValuesFiles = []string{"config/prod.yaml"}
				sources := store.New()
				sources.SetArtifact(selected.ChartSource(), &store.SourceArtifact{FS: input})
				rendererSources := NewSources(sources)
				first, err := renderer.Template(t.Context(), selected, nil, RenderOptions{Options: Options{}, Sources: rendererSources})
				assert.NoError(t, err)
				if !hasRenderField(first, "marker", "acquired") {
					t.Fatal(first)
				}
				reads := backing.opens.Load()
				if reads == 0 {
					t.Fatal("first render did not read acquired backing")
				}
				second, err := renderer.Template(t.Context(), selected, nil, RenderOptions{Options: Options{}, Sources: rendererSources})
				assert.NoError(t, err)
				assert.Diff(t, second, first)
				assert.Equal(t, backing.opens.Load(), reads)
				if !persistent {
					return
				}
				d, err := renderer.describeChart(t.Context(), selected, rendererSources)
				assert.NoError(t, err)
				d.values, err = renderer.observeValuesFiles(t.Context(), d, selected.ChartValuesFiles, false)
				assert.NoError(t, err)
				key := cache.BlobKey{Content: computeTemplateKey(d, map[string]any{}, Options{}, selected), PersistOnly: true}
				witness := manifest.DeepCopyMap(first[0])
				witness["cacheWitness"] = "pinned persistent hit witness"
				output := encodedRender(t, []map[string]any{witness})
				r, _, _, err := outputOwner.Blob(t.Context(), key, cache.Policy{}, func(_ context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
					err := json.NewEncoder(w).Encode(renderRecord{Output: output, Analysis: d.analyze()})
					return cache.ProducerResult{}, err
				})
				assert.NoError(t, err)
				assert.NoError(t, r.Close())
				fresh := NewClient(cachetest.New(t, outputRoot, cache.New))
				freshSources := NewSources(sources)
				before := backing.opens.Load()
				hit, err := fresh.Template(t.Context(), selected, nil, RenderOptions{Options: Options{}, Sources: freshSources})
				assert.NoError(t, err)
				assert.Diff(t, hit, []map[string]any{witness})
				reads = backing.opens.Load()
				if reads <= before || len(fresh.chartValuesCache) != 1 {
					t.Fatal("fresh persisted hit did not observe its values")
				}
				for _, entry := range fresh.chartValuesCache {
					if entry.merged != nil || entry.inputs == nil {
						t.Fatal("persisted hit did not leave an observation-only entry")
					}
				}
				hit, err = fresh.Template(t.Context(), selected, nil, RenderOptions{Options: Options{}, Sources: freshSources})
				assert.NoError(t, err)
				assert.Diff(t, hit, []map[string]any{witness})
				assert.Equal(t, backing.opens.Load(), reads)
				t.Log("verified pinned persisted hits and observation-only reuse")
			})
		}
		backing := &chartBacking{Filesystem: generation.Backing}
		input, pin, err := gittree.New(t.Context(), backing, generation.Commit, gittree.ViewOptions{Content: struct{ Source string }{id}})
		assert.NoError(t, err)
		assert.Equal(t, pin == "", false)
		art := &store.SourceArtifact{FS: input, Key: "untrusted", InputAlias: true, LocalPath: root, Revision: id}
		st.SetArtifact(hr.ChartSource(), art)
		first, err := loadDecodedChart(t.Context(), cli, hr, cliSources)
		assert.NoError(t, err)
		if first.chart == previous {
			t.Fatal("different acquisition pins shared parsed entries")
		}
		previous = first.chart
		reads := backing.opens.Load()
		other := hr.Clone()
		other.Name = "second"
		warm, err := loadDecodedChart(t.Context(), cli, other, cliSources)
		assert.NoError(t, err)
		assert.Equal(t, warm.chart == first.chart, true)
		assert.Equal(t, backing.opens.Load(), reads)
		values, err := cli.mergeChartValuesFiles(t.Context(), first, []string{"config/prod.yaml"}, false)
		assert.NoError(t, err)
		assert.Equal(t, values["marker"], any("acquired"))
		values, err = cli.mergeChartValuesFiles(t.Context(), warm, []string{"config/prod.yaml"}, false)
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
		st.SetArtifact(auto.ChartSource(), art)
		autoChart, err := loadDecodedChart(t.Context(), cli, auto, cliSources)
		assert.NoError(t, err)
		assert.Equal(t, autoChart.chart == rootChart.chart, false)
		reads = backing.opens.Load()
		warm, err = loadDecodedChart(t.Context(), cli, auto, cliSources)
		assert.NoError(t, err)
		assert.Equal(t, warm.chart == autoChart.chart, true)
		assert.Equal(t, backing.opens.Load(), reads)
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
		{"OCI archive", manifest.KindOCIRepository, "archive"},
		{"Bucket directory", manifest.KindBucket, "directory"},
		{"Bucket archive", manifest.KindBucket, "archive"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cacheRoot := t.TempDir()
			owner := cachetest.New(t, cacheRoot, cache.New)
			cli := NewClient(owner)
			st := store.New()
			cliSources := NewSources(st)
			hr := &manifest.HelmRelease{Chart: manifest.HelmChart{Source: manifest.NamedResource{Group: manifest.SourceDomain, Kind: tc.kind, Name: "chart"}}}
			if tc.kind == manifest.KindBucket {
				hr.Chart.Name = "."
				if tc.shape == "archive" {
					hr.Chart.Name = "chart.tgz"
				}
			}
			var previous *chart.Chart
			for _, version := range []string{"1.0.0", "2.0.0"} {
				input := fstest.MapFS{"Chart.yaml": {Data: []byte("apiVersion: v2\nname: chart\nversion: " + version + "\n")}}
				var acquired fs.FS
				backingName := "Chart.yaml"
				if tc.shape == "directory" {
					input = fstest.MapFS{"nested/Chart.yaml": input["Chart.yaml"]}
					backingName = "nested/Chart.yaml"
					if tc.kind == manifest.KindBucket {
						hr.Chart.Name = "nested"
					}
				}
				if tc.shape == "file" {
					data := chartArchive(t, input)
					r, _, _, err := owner.Blob(t.Context(), cache.BlobKey{Content: version}, cache.Policy{MaxAge: cache.Forever}, func(_ context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
						_, err := io.Copy(w, bytes.NewReader(data))
						return cache.ProducerResult{}, err
					})
					assert.NoError(t, err)
					acquired = cache.FileFS("chart.tgz", r)
					assert.NoError(t, r.Close())
					backingName = "chart.tgz"
				} else {
					if tc.shape == "archive" {
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
				// FileFS Stat opens its backing; poisoning below the view preserves real authority.
				assert.NoError(t, os.RemoveAll(filepath.Join(cacheRoot, "oci", "blobs")))
				_, err = fs.ReadFile(acquired, backingName)
				if err == nil {
					t.Fatal("backing poison did not prevent chart reads")
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
				if _, err := loadDecodedChart(t.Context(), cli, explicit, cliSources); err == nil {
					t.Fatal("explicit root reused a different chart selector")
				}
			}
		})
	}
}
