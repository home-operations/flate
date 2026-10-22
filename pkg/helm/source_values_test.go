package helm

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

func TestTemplate_SourceRootValuesFiles(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindGitRepository, manifest.KindBucket} {
		for _, ignore := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/ignore_%t", kind, ignore), func(t *testing.T) {
				t.Parallel()
				root := t.TempDir()
				testutil.WriteFile(t, root, "charts/demo/Chart.yaml", "apiVersion: v2\nname: demo\nversion: 1.0.0\n")
				testutil.WriteFile(t, root, "charts/demo/values.yaml", "marker: default\nkept: default\n")
				testutil.WriteFile(t, root, "charts/demo/values.prod.yaml", "marker: production\n")
				testutil.WriteFile(t, root, "charts/demo/prod.yaml", "marker: wrong-chart\n")
				testutil.WriteFile(t, root, "charts/demo/chart-only.yaml", "marker: wrong-chart\n")
				testutil.WriteFile(t, root, "config/empty.yaml", "")
				testutil.WriteFile(t, root, "config/prod.yaml", "marker: shared-production\n")
				testutil.WriteFile(t, root, "prod.yaml", "marker: root-production\n")
				testutil.WriteFile(t, root, "values.yaml", "marker: root-default\n")
				testutil.WriteFile(t, root, "charts/demo/templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: demo\ndata:\n  marker: {{ .Values.marker | quote }}\n  kept: {{ .Values.kept | quote }}\n")
				sub, err := fs.Sub(os.DirFS(root), "charts/demo")
				assert.NoError(t, err)
				testutil.WriteFile(t, root, "packaged.tgz", string(chartArchive(t, sub)))
				owner := cachetest.New(t, "", cache.New)
				location, err := gitrepo.Locate(t.Context(), root, root)
				assert.NoError(t, err)
				admitted, err := gittree.AdmitDirectory(t.Context(), location)
				assert.NoError(t, err)
				input := admitted.FS
				if kind == manifest.KindBucket {
					input = acquireChartFS(t, owner, os.DirFS(root))
				}
				cli, err := NewClient(owner)
				assert.NoError(t, err)
				st := store.New()
				hr := &manifest.HelmRelease{Name: "demo", Namespace: "apps", Chart: manifest.HelmChart{Name: "charts/demo", Source: manifest.NamedResource{Kind: kind, Group: manifest.SourceDomain, Name: "repo", Namespace: "apps"}}, IgnoreMissingValuesFiles: ignore}
				st.SetArtifact(hr.ChartSource(), &store.SourceArtifact{FS: input})
				cliSources := NewSources(st)
				for _, tc := range []struct {
					name      string
					files     []string
					overrides map[string]any
					want      string
				}{
					{"reported", []string{"./charts/demo/values.yaml", "./charts/demo/values.prod.yaml"}, nil, "production"},
					{"outside chart", []string{"./config/prod.yaml"}, nil, "shared-production"},
					{"bare collision", []string{"prod.yaml"}, nil, "root-production"},
					{"explicit defaults", []string{"values.yaml"}, nil, "root-default"},
					{"ordered", []string{"config/prod.yaml", "prod.yaml"}, nil, "root-production"},
					{"release overrides", []string{"prod.yaml"}, map[string]any{"marker": "release"}, "release"},
					{"omitted", nil, nil, "default"},
					{"empty", []string{"config/empty.yaml"}, nil, "default"},
					{"missing", []string{"absent.yaml"}, nil, "default"},
					{"no chart fallback", []string{"chart-only.yaml"}, nil, "default"},
					{"packaged", []string{"config/prod.yaml"}, nil, "shared-production"},
					{"standalone", []string{"config/prod.yaml"}, nil, "shared-production"},
				} {
					t.Run(tc.name, func(t *testing.T) {
						t.Parallel()
						selected := hr.Clone()
						selected.ChartValuesFiles = tc.files
						if tc.name == "packaged" {
							selected.Chart.Name = "packaged.tgz"
						}
						if tc.name == "standalone" {
							selected.ChartRef = &helmv2.CrossNamespaceSourceReference{Kind: manifest.KindHelmChart, Name: "standalone"}
							selected.Chart.Source.Kind, selected.Chart.Source.Name = manifest.KindHelmChart, "standalone"
							var err error
							selected, _, err = Prepare(selected, func(id manifest.NamedResource) *manifest.HelmChartSource {
								return &manifest.HelmChartSource{Name: id.Name, Namespace: id.Namespace, Chart: "charts/demo", SourceRef: sourcev1.LocalHelmChartSourceReference{Kind: kind, Name: "repo"}, ValuesFiles: tc.files, IgnoreMissingValuesFiles: ignore}
							}, nil, nil)
							assert.NoError(t, err)
						}
						first, err := cli.Template(t.Context(), selected, tc.overrides, RenderOptions{Options: Options{}, Sources: cliSources})
						if !ignore && (tc.name == "missing" || tc.name == "no chart fallback") {
							if !errors.Is(err, fs.ErrNotExist) || !errors.Is(err, manifest.ErrInput) {
								t.Fatalf("missing source values: %v", err)
							}
							return
						}
						if err != nil {
							t.Fatal(err)
						}
						if !strings.Contains(first, `marker: "`+tc.want+`"`) || !strings.Contains(first, `kept: "default"`) {
							t.Fatal(first)
						}
						second, err := cli.Template(t.Context(), selected, tc.overrides, RenderOptions{Options: Options{}, Sources: cliSources})
						assert.NoError(t, err)
						assert.Equal(t, second, first)
					})
				}
			})
		}
	}
}

func TestTemplate_ArchiveValuesFiles(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindHelmChart, manifest.KindOCIRepository, manifest.KindExternalArtifact} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			owner := cachetest.New(t, "", cache.New)
			input := fstest.MapFS{
				"charts/demo/Chart.yaml":        {Data: []byte("apiVersion: v2\nname: demo\nversion: 1.0.0\n")},
				"charts/demo/values.yaml":       {Data: []byte("marker: default\n")},
				"charts/demo/prod.yaml":         {Data: []byte("marker: archive\n")},
				"charts/demo/templates/cm.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: demo\ndata:\n  marker: {{ .Values.marker | quote }}\n")},
				"prod.yaml":                     {Data: []byte("marker: outside\n")},
			}
			hr := &manifest.HelmRelease{Name: "demo", Namespace: "apps", Chart: manifest.HelmChart{Name: "charts/demo", Source: manifest.NamedResource{Group: manifest.SourceDomain, Kind: kind, Name: "repo", Namespace: "apps"}}}
			var artifact fs.FS = input
			if kind != manifest.KindExternalArtifact {
				sub, err := fs.Sub(input, "charts/demo")
				assert.NoError(t, err)
				artifact = fstest.MapFS{"chart.tgz": {Data: chartArchive(t, sub)}, "prod.yaml": input["prod.yaml"]}
			}
			st := store.New()
			st.SetArtifact(hr.ChartSource(), &store.SourceArtifact{FS: acquireChartFS(t, owner, artifact), Revision: "1.0.0"})
			cli, err := NewClient(owner)
			assert.NoError(t, err)
			cliSources := NewSources(st)
			for _, tc := range []struct {
				name  string
				files []string
				want  string
			}{
				{"archive", []string{"./prod.yaml"}, "archive"},
				{"defaults last", []string{"prod.yaml", "./values.yaml"}, "default"},
				{"override last", []string{"./values.yaml", "prod.yaml"}, "archive"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()
					selected := hr.Clone()
					selected.ChartValuesFiles = tc.files
					out, err := cli.Template(t.Context(), selected, nil, RenderOptions{Options: Options{}, Sources: cliSources})
					assert.NoError(t, err)
					if !strings.Contains(out, `marker: "`+tc.want+`"`) {
						t.Fatal(out)
					}
				})
			}
		})
	}
}

type valuesFaultFile struct {
	fs.File
	readErr, statErr, closeErr error
}

func (f valuesFaultFile) Read(p []byte) (int, error) {
	if f.readErr != nil {
		return 0, f.readErr
	}
	return f.File.Read(p)
}
func (f valuesFaultFile) Stat() (fs.FileInfo, error) {
	if f.statErr != nil {
		return nil, f.statErr
	}
	return f.File.Stat()
}
func (f valuesFaultFile) Close() error { return errors.Join(f.File.Close(), f.closeErr) }

func TestTemplate_ValuesFilesFailures(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "regular", "marker: regular\n")
	assert.NoError(t, os.Symlink("../absent", filepath.Join(root, "link")))
	location, err := gitrepo.Locate(t.Context(), root, root)
	assert.NoError(t, err)
	admitted, err := gittree.AdmitDirectory(t.Context(), location)
	assert.NoError(t, err)
	owner := cachetest.New(t, "", cache.New)
	var archive bytes.Buffer
	w := tar.NewWriter(&archive)
	assert.NoError(t, w.WriteHeader(&tar.Header{Name: "link", Typeflag: tar.TypeSymlink, Linkname: "../absent"}))
	assert.NoError(t, w.Close())
	bucket, _, _, err := owner.Acquire(t.Context(), cache.ArchiveKey{Content: "denied-values"}, cache.Policy{}, func(_ context.Context, out cache.ProducerInput) (cache.ProducerResult, error) {
		_, err := out.Write(archive.Bytes())
		return cache.ProducerResult{}, err
	})
	assert.NoError(t, err)
	fault := errors.New("values read failure")
	base := fstest.MapFS{"file": {Data: []byte("marker: value\n")}, "bad": {Data: []byte("key: : invalid\n")}, "directory": {Mode: fs.ModeDir}}
	invalid := chartOpenFS{FS: base, open: func(name string) (fs.File, error) {
		t.Errorf("unsafe values path reached filesystem: %q", name)
		return nil, fs.ErrPermission
	}}
	for _, ignore := range []bool{false, true} {
		t.Run(fmt.Sprint(ignore), func(t *testing.T) {
			t.Parallel()
			for _, tc := range []struct {
				name, path string
				input      fs.FS
				want       error
			}{
				{"escape", "../../outside", invalid, fs.ErrInvalid},
				{"absolute", "/outside", invalid, fs.ErrInvalid},
				{"empty", "", invalid, fs.ErrInvalid},
				{"root directory", "./", invalid, fs.ErrInvalid},
				{"nul", "file\x00", invalid, fs.ErrInvalid},
				{"directory", "directory", base, fs.ErrInvalid},
				{"malformed", "bad", base, manifest.ErrInput},
				{"local final link", "link", admitted.FS, fs.ErrPermission},
				{"local ancestor link", "link/nested/file", admitted.FS, fs.ErrPermission},
				{"bucket final link", "link", bucket, fs.ErrPermission},
				{"bucket ancestor link", "link/nested/file", bucket, fs.ErrPermission},
				{"permission", "file", chartOpenFS{FS: base, open: func(string) (fs.File, error) { return nil, fs.ErrPermission }}, fs.ErrPermission},
				{"stat", "file", chartOpenFS{FS: base, open: func(name string) (fs.File, error) {
					f, err := base.Open(name)
					return valuesFaultFile{File: f, statErr: fault}, err
				}}, fault},
				{"read", "file", chartOpenFS{FS: base, open: func(name string) (fs.File, error) {
					f, err := base.Open(name)
					return valuesFaultFile{File: f, readErr: fault}, err
				}}, fault},
				{"close", "file", chartOpenFS{FS: base, open: func(name string) (fs.File, error) {
					f, err := base.Open(name)
					return valuesFaultFile{File: f, closeErr: fault}, err
				}}, fault},
				{"close reports missing", "file", chartOpenFS{FS: base, open: func(name string) (fs.File, error) {
					f, err := base.Open(name)
					return valuesFaultFile{File: f, closeErr: fs.ErrNotExist}, err
				}}, fs.ErrNotExist},
				{"read reports missing", "file", chartOpenFS{FS: base, open: func(name string) (fs.File, error) {
					f, err := base.Open(name)
					return valuesFaultFile{File: f, readErr: fs.ErrNotExist}, err
				}}, fs.ErrNotExist},
			} {
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()
					cli, err := NewClient(owner)
					assert.NoError(t, err)
					ch := minimalChart()
					_, err = cli.mergeChartValuesFiles(t.Context(), chartDescription{chart: ch, fingerprint: chartFixtureDescription(t, ch).fingerprint, valuesFS: tc.input}, []string{tc.path}, ignore)
					if err == nil || !errors.Is(err, tc.want) || !errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) || !strings.Contains(err.Error(), fmt.Sprintf("%q", tc.path)) {
						t.Fatalf("values error = %v; want %v and filename %q", err, tc.want, tc.path)
					}
					for _, entry := range cli.chartValuesCache {
						if entry.merged != nil || tc.name != "malformed" {
							t.Fatal("failed values result was cached")
						}
					}
				})
			}
		})
	}
}

func TestObserveValuesFiles_PinnedReuse(t *testing.T) {
	t.Parallel()
	for _, fingerprinted := range []bool{false, true} {
		t.Run(fmt.Sprintf("fingerprinted_%t", fingerprinted), func(t *testing.T) {
			t.Parallel()
			root, st, hr := sourceValuesCacheFixture(t)
			testutil.WriteFile(t, root, "config/other.yaml", "marker: other\nnested:\n  value: shared\n")
			owner := cachetest.New(t, "", cache.New)
			st.SetArtifact(hr.ChartSource(), &store.SourceArtifact{FS: acquireChartFS(t, owner, os.DirFS(root))})
			cli, err := NewClient(owner)
			assert.NoError(t, err)
			cliSources := NewSources(st)
			d, err := cli.describeChart(t.Context(), hr, cliSources)
			assert.NoError(t, err)
			if d.valuesPin == "" {
				t.Fatal("fixture has no acquired values pin")
			}
			if !fingerprinted {
				d.fingerprint = ""
			}
			reads := map[string]int{}
			input := d.valuesFS
			d.valuesFS = chartOpenFS{FS: input, open: func(name string) (fs.File, error) {
				reads[name]++
				return input.Open(name)
			}}
			names := []string{"./config/prod.yaml", "config/other.yaml", "config/prod.yaml"}
			before := append([]string(nil), names...)
			first, err := cli.observeValuesFiles(t.Context(), d, names, false)
			assert.NoError(t, err)
			assert.Diff(t, names, before)
			assert.Equal(t, reads["config/prod.yaml"], 1)
			assert.Equal(t, reads["config/other.yaml"], 1)
			assert.Equal(t, len(first.inputs), 3)
			assert.Equal(t, *first.fingerprint, manifest.Fingerprint(first.inputs))
			normalized := []string{"config/prod.yaml", "./config/other.yaml", "./config/prod.yaml"}
			second, err := cli.observeValuesFiles(t.Context(), d, normalized, false)
			assert.NoError(t, err)
			wantReads := 2
			if fingerprinted {
				wantReads = 1
				assert.Equal(t, &second.inputs[0] == &first.inputs[0], true)
			}
			assert.Equal(t, reads["config/prod.yaml"], wantReads)
			assert.Equal(t, reads["config/other.yaml"], wantReads)
			d.values = second
			merged, err := cli.mergeChartValuesFiles(t.Context(), d, normalized, false)
			assert.NoError(t, err)
			assert.Equal(t, merged["marker"], any("first"))
			merged["nested"].(map[string]any)["value"] = "release"
			d.values = chartValuesEntry{}
			repeated, err := cli.mergeChartValuesFiles(t.Context(), d, normalized, false)
			assert.NoError(t, err)
			assert.Equal(t, repeated["nested"].(map[string]any)["value"], any("shared"))
			reversed, err := cli.mergeChartValuesFiles(t.Context(), d, []string{"config/prod.yaml", "config/other.yaml"}, false)
			assert.NoError(t, err)
			assert.Equal(t, reversed["marker"], any("other"))
			for _, invalid := range []string{"", ".", "../config/prod.yaml", "/config/prod.yaml", "config/prod.yaml\x00"} {
				_, err := cli.observeValuesFiles(t.Context(), d, []string{invalid}, true)
				if !errors.Is(err, fs.ErrInvalid) || !strings.Contains(err.Error(), fmt.Sprintf("%q", invalid)) {
					t.Fatalf("invalid path %q: %v", invalid, err)
				}
			}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			_, err = cli.observeValuesFiles(ctx, d, normalized, false)
			assert.Equal(t, errors.Is(err, context.Canceled), true)
			d.values = second
			_, err = cli.mergeChartValuesFiles(ctx, d, normalized, false)
			assert.Equal(t, errors.Is(err, context.Canceled), true)
		})
	}
}

func TestObserveValuesFiles_UnpinnedChanges(t *testing.T) {
	t.Parallel()
	root, st, hr := sourceValuesCacheFixture(t)
	owner := cachetest.New(t, "", cache.New)
	cli, err := NewClient(owner)
	assert.NoError(t, err)
	cliSources := NewSources(st)
	d, err := cli.describeChart(t.Context(), hr, cliSources)
	assert.NoError(t, err)
	assert.Equal(t, d.valuesPin, "")
	reads := 0
	d.valuesFS = chartOpenFS{FS: os.DirFS(root), open: func(name string) (fs.File, error) {
		reads++
		return os.DirFS(root).Open(name)
	}}
	var previous string
	for _, marker := range []string{"first", "changed", "", "added"} {
		before := reads
		if marker == "" {
			assert.NoError(t, os.Remove(filepath.Join(root, "config/prod.yaml")))
		} else {
			testutil.WriteFile(t, root, "config/prod.yaml", "marker: "+marker+"\n")
		}
		entry, err := cli.observeValuesFiles(t.Context(), d, hr.ChartValuesFiles, true)
		assert.NoError(t, err)
		assert.Equal(t, reads, before+1)
		assert.Equal(t, *entry.fingerprint == previous, false)
		previous = *entry.fingerprint
		observed := d
		observed.values = entry
		merged, err := cli.mergeChartValuesFiles(t.Context(), observed, hr.ChartValuesFiles, true)
		assert.NoError(t, err)
		if marker == "" {
			assert.Equal(t, len(merged), 0)
			_, err = cli.observeValuesFiles(t.Context(), d, hr.ChartValuesFiles, false)
			assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
		} else {
			assert.Equal(t, merged["marker"], any(marker))
		}
	}
	cli.assertSourceValuesChanges(t, hr, root, cliSources)
}

func TestObserveValuesFiles_RetryFailures(t *testing.T) {
	t.Parallel()
	for _, failure := range []string{"open", "read", "stat", "close", "cancel"} {
		t.Run(failure, func(t *testing.T) {
			t.Parallel()
			root, st, hr := sourceValuesCacheFixture(t)
			owner := cachetest.New(t, "", cache.New)
			st.SetArtifact(hr.ChartSource(), &store.SourceArtifact{FS: acquireChartFS(t, owner, os.DirFS(root))})
			cli, err := NewClient(owner)
			assert.NoError(t, err)
			cliSources := NewSources(st)
			d, err := cli.describeChart(t.Context(), hr, cliSources)
			assert.NoError(t, err)
			input := d.valuesFS
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			fault := errors.New("transient values failure")
			reads := 0
			d.valuesFS = chartOpenFS{FS: input, open: func(name string) (fs.File, error) {
				reads++
				if reads != 1 {
					return input.Open(name)
				}
				if failure == "open" {
					return nil, fault
				}
				f, err := input.Open(name)
				if err != nil {
					return nil, err
				}
				switch failure {
				case "read":
					return valuesFaultFile{File: f, readErr: fault}, nil
				case "stat":
					return valuesFaultFile{File: f, statErr: fault}, nil
				case "close":
					return valuesFaultFile{File: f, closeErr: fault}, nil
				default:
					cancel()
					return f, nil
				}
			}}
			_, err = cli.observeValuesFiles(ctx, d, hr.ChartValuesFiles, false)
			if failure == "cancel" {
				fault = context.Canceled
			}
			assert.Equal(t, errors.Is(err, fault), true)
			assert.Equal(t, len(cli.chartValuesCache), 0)
			merged, err := cli.mergeChartValuesFiles(t.Context(), d, hr.ChartValuesFiles, false)
			assert.NoError(t, err)
			assert.Equal(t, merged["marker"], any("first"))
			assert.Equal(t, reads, 2)
		})
	}
}

func TestObserveValuesFiles_ConcurrentPublication(t *testing.T) {
	t.Parallel()
	root, st, hr := sourceValuesCacheFixture(t)
	owner := cachetest.New(t, "", cache.New)
	st.SetArtifact(hr.ChartSource(), &store.SourceArtifact{FS: acquireChartFS(t, owner, os.DirFS(root))})
	cli, err := NewClient(owner)
	assert.NoError(t, err)
	cliSources := NewSources(st)
	d, err := cli.describeChart(t.Context(), hr, cliSources)
	assert.NoError(t, err)
	input := d.valuesFS
	blocked, resume := make(chan struct{}), make(chan struct{})
	var reads atomic.Int32
	d.valuesFS = chartOpenFS{FS: input, open: func(name string) (fs.File, error) {
		if reads.Add(1) == 1 {
			close(blocked)
			<-resume
		}
		return input.Open(name)
	}}
	late := make(chan chartValuesEntry, 1)
	go func() {
		entry, err := cli.observeValuesFiles(t.Context(), d, hr.ChartValuesFiles, false)
		if err != nil {
			t.Error(err)
		}
		late <- entry
	}()
	<-blocked
	merged, err := cli.mergeChartValuesFiles(t.Context(), d, hr.ChartValuesFiles, false)
	close(resume)
	entry := <-late
	assert.NoError(t, err)
	if entry.merged == nil {
		t.Fatal("observation publisher discarded the merged entry")
	}
	assert.Diff(t, entry.merged, merged)
	start := make(chan struct{})
	var wg sync.WaitGroup
	var results [2]map[string]any
	for i := range results {
		wg.Go(func() {
			<-start
			var err error
			results[i], err = cli.mergeChartValuesFiles(t.Context(), d, hr.ChartValuesFiles, false)
			if err != nil {
				t.Error(err)
				return
			}
			results[i]["marker"] = i
		})
	}
	close(start)
	wg.Wait()
	assert.Equal(t, results[0]["marker"], any(0))
	assert.Equal(t, results[1]["marker"], any(1))
	assert.Equal(t, entry.merged["marker"], any("first"))
	assert.Equal(t, reads.Load(), int32(2))
}

func TestObserveValuesFiles_AcquiredChanges(t *testing.T) {
	t.Parallel()
	root, st, hr := sourceValuesCacheFixture(t)
	cli, err := NewClient(cachetest.New(t, "", cache.New))
	assert.NoError(t, err)
	cliSources := NewSources(st)
	var previousPin, chartFingerprint string
	for _, marker := range []string{"first", "changed", ""} {
		if marker == "" {
			assert.NoError(t, os.Remove(filepath.Join(root, "config/prod.yaml")))
		} else {
			testutil.WriteFile(t, root, "config/prod.yaml", "marker: "+marker+"\n")
		}
		acquired := acquireChartFS(t, cachetest.New(t, "", cache.New), os.DirFS(root))
		st.SetArtifact(hr.ChartSource(), &store.SourceArtifact{FS: acquired})
		d, err := cli.describeChart(t.Context(), hr, cliSources)
		assert.NoError(t, err)
		if d.valuesPin == "" || d.valuesPin == previousPin {
			t.Fatal("changed acquisition did not change its pin")
		}
		previousPin = d.valuesPin
		if chartFingerprint != "" {
			assert.Equal(t, d.fingerprint, chartFingerprint)
		}
		chartFingerprint = d.fingerprint
		reads := 0
		d.valuesFS = chartOpenFS{FS: acquired, open: func(name string) (fs.File, error) {
			reads++
			return acquired.Open(name)
		}}
		merged, err := cli.mergeChartValuesFiles(t.Context(), d, hr.ChartValuesFiles, false)
		assert.Equal(t, reads, 1)
		if marker == "" {
			assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
			omitted, err := cli.mergeChartValuesFiles(t.Context(), d, hr.ChartValuesFiles, true)
			assert.NoError(t, err)
			assert.Equal(t, len(omitted), 0)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, merged["marker"], any(marker))
		}
		first, err := cli.Template(t.Context(), hr, nil, RenderOptions{Options: Options{}, Sources: cliSources})
		assert.NoError(t, err)
		want := marker
		if want == "" {
			want = "default"
		}
		if !strings.Contains(first, `marker: "`+want+`"`) {
			t.Fatal(first)
		}
	}
}
