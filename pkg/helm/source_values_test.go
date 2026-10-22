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
				cli.SetSourceResolver(NewStoreSourceResolver(st))
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
							selected, err = Prepare(selected, func(id manifest.NamedResource) *manifest.HelmChartSource {
								return &manifest.HelmChartSource{Name: id.Name, Namespace: id.Namespace, Chart: "charts/demo", SourceRef: sourcev1.LocalHelmChartSourceReference{Kind: kind, Name: "repo"}, ValuesFiles: tc.files, IgnoreMissingValuesFiles: ignore}
							}, nil, nil)
							assert.NoError(t, err)
						}
						first, err := cli.Template(t.Context(), selected, tc.overrides, Options{})
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
						second, err := cli.Template(t.Context(), selected, tc.overrides, Options{})
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
			cli.SetSourceResolver(NewStoreSourceResolver(st))
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
					out, err := cli.Template(t.Context(), selected, nil, Options{})
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
	readErr, closeErr error
}

func (f valuesFaultFile) Read(p []byte) (int, error) {
	if f.readErr != nil {
		return 0, f.readErr
	}
	return f.File.Read(p)
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
					assert.Equal(t, len(cli.chartValuesCache), 0)
				})
			}
		})
	}
}
