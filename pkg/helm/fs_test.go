package helm

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cacheroot"
	"github.com/home-operations/flate/pkg/store"
	"helm.sh/helm/v4/pkg/chart/loader/archive"
	chart "helm.sh/helm/v4/pkg/chart/v2"
	"helm.sh/helm/v4/pkg/chart/v2/loader"
)

func TestLoadFSChart_MatchesHelmDirLoader(t *testing.T) {
	for _, irregular := range []bool{false, true} {
		name := "files"
		if irregular {
			name = "irregular"
		}
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			for name, data := range map[string]string{
				"Chart.yaml":            "\xef\xbb\xbfapiVersion: v2\nname: parent\nversion: 1.0.0\n",
				"values.yaml":           "\xef\xbb\xbfanswer: 42\n",
				"templates/config.yaml": "\xef\xbb\xbfkind: ConfigMap\n",
				"crds/example.yaml":     "kind: CustomResourceDefinition\n",
				".helmignore":           "drop.txt\nignored/\n*.tmp\n",
				"drop.txt":              "ignored", "ignored/Chart.yaml": "invalid",
				"other.tmp": "ignored", "templates/.hidden": "default ignore",
				"charts/child/Chart.yaml":            "apiVersion: v2\nname: child\nversion: 2.0.0\n",
				"charts/child/values.yaml":           "enabled: true\n",
				"charts/child/templates/config.yaml": "kind: Secret\n",
			} {
				testutil.WriteFile(t, root, name, data)
			}
			if irregular {
				listener, err := net.Listen("unix", filepath.Join(root, "socket"))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = listener.Close() })
			}
			want, wantErr := loader.LoadDir(root)
			got, gotErr := loadFSChart(os.DirFS(root), ".")
			if irregular {
				if wantErr == nil || gotErr == nil {
					t.Fatalf("irregular: upstream=%v FS=%v", wantErr, gotErr)
				}
				if !errors.Is(gotErr, manifest.ErrInput) {
					t.Fatalf("irregular error lacks input classification: %v", gotErr)
				}
				if want := strings.ReplaceAll(wantErr.Error(), filepath.Join(root, "socket"), "socket"); manifest.TrimSentinelPrefix(gotErr.Error()) != want {
					t.Fatalf("irregular error: got %q, want %q", gotErr, want)
				}
				return
			}
			if wantErr != nil || gotErr != nil {
				t.Fatalf("upstream=%v FS=%v", wantErr, gotErr)
			}
			if diff := cmp.Diff(want, got, cmp.AllowUnexported(chart.Chart{}), cmpopts.IgnoreFields(chart.Chart{}, "parent")); diff != "" {
				t.Fatalf("chart (-want +got):\n%s", diff)
			}
		})
	}
}

func TestLoadChart_ArtifactKeyIsolation(t *testing.T) {
	st := store.New()
	cli, err := NewClient(cacheroot.New(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	cli.SetSourceResolver(NewStoreSourceResolver(st))
	for _, name := range []string{"first", "second", "first", "missing-key", "invalid-metadata", "invalid-archive"} {
		key := name
		if name == "missing-key" {
			key = ""
		}
		version := "1.0.0"
		if name == "invalid-metadata" || name == "invalid-archive" {
			version = "invalid"
		}
		data := []byte("apiVersion: v2\nname: " + name + "\nversion: " + version + "\n")
		input := fstest.MapFS{"Chart.yaml": {Data: data}}
		if name == "invalid-archive" {
			var buf bytes.Buffer
			gw := gzip.NewWriter(&buf)
			tw := tar.NewWriter(gw)
			if err := tw.WriteHeader(&tar.Header{Name: name + "/Chart.yaml", Size: int64(len(data)), Mode: 0o644}); err != nil {
				t.Fatal(err)
			}
			if _, err := tw.Write(data); err != nil {
				t.Fatal(err)
			}
			if err := errors.Join(tw.Close(), gw.Close()); err != nil {
				t.Fatal(err)
			}
			input = fstest.MapFS{"chart.tgz": {Data: buf.Bytes()}}
		}
		art := &store.SourceArtifact{Kind: manifest.KindHelmChart, Key: key, FS: input}
		st.SetArtifact(manifest.NamedResource{Kind: manifest.KindHelmChart, Name: name, Namespace: "test"}, art)
		hr := &manifest.HelmRelease{Chart: manifest.HelmChart{RepoKind: manifest.KindHelmChart, RepoName: name, RepoNamespace: "test"}}
		loaded, err := cli.LoadChart(t.Context(), hr)
		if name == "invalid-metadata" || name == "invalid-archive" {
			if !errors.Is(err, manifest.ErrInput) || strings.Count(err.Error(), "input error") != 1 {
				t.Fatalf("invalid metadata must be classified once: %v", err)
			}
			continue
		}
		if key == "" {
			if !errors.Is(err, manifest.ErrInput) {
				t.Fatalf("missing key: %v", err)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if loaded.Chart.Name() != name {
			t.Fatalf("loaded %s, want %s", loaded.Chart.Name(), name)
		}
	}
}

func BenchmarkLoadChart_FSWarm(b *testing.B) {
	cli, err := NewClient(cacheroot.New(b.TempDir()))
	if err != nil {
		b.Fatal(err)
	}
	st := store.New()
	cli.SetSourceResolver(NewStoreSourceResolver(st))
	st.SetArtifact(manifest.NamedResource{Kind: manifest.KindHelmChart, Namespace: "test", Name: "chart"},
		&store.SourceArtifact{Kind: manifest.KindHelmChart, Key: "generation",
			FS: fstest.MapFS{"Chart.yaml": {Data: []byte("apiVersion: v2\nname: chart\nversion: 1.0.0\n")}}})
	hr := &manifest.HelmRelease{Chart: manifest.HelmChart{RepoKind: manifest.KindHelmChart, RepoName: "chart", RepoNamespace: "test"}}
	if _, err := cli.LoadChart(b.Context(), hr); err != nil {
		b.Fatal(err)
	}
	for b.Loop() {
		if _, err := cli.LoadChart(b.Context(), hr); err != nil {
			b.Fatal(err)
		}
	}
}

func TestLoadFSChart_DecompressedBudgetMatchesHelm(t *testing.T) {
	old := archive.MaxDecompressedChartSize
	archive.MaxDecompressedChartSize = 64
	defer func() { archive.MaxDecompressedChartSize = old }()
	input := fstest.MapFS{"Chart.yaml": {Data: []byte("apiVersion: v2\nname: chart\nversion: 1.0.0\n")}, "values.yaml": {Data: bytes.Repeat([]byte(" "), 65)}}
	root := t.TempDir()
	for name, file := range input {
		testutil.WriteFile(t, root, name, string(file.Data))
	}
	if _, err := loader.LoadDir(root); err == nil {
		t.Fatal("Helm accepted oversized fixture")
	}
	if _, err := loadFSChart(input, "."); !errors.Is(err, manifest.ErrInput) {
		t.Fatalf("oversized FS chart lacks input classification: %v", err)
	}
}

func TestChartPathInFS_InvalidArtifacts(t *testing.T) {
	for name, input := range map[string]fstest.MapFS{
		"missing-layer":   {},
		"multiple-charts": {"a/Chart.yaml": {}, "b/Chart.yaml": {}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := chartPathInFS(input); !errors.Is(err, manifest.ErrInput) {
				t.Fatalf("invalid artifact lacks input classification: %v", err)
			}
		})
	}
}

func TestLoadFSChart_InvalidInputs(t *testing.T) {
	for name, input := range map[string]fstest.MapFS{
		"missing":  {},
		"archive":  {"chart": {Data: []byte("invalid archive")}},
		"metadata": {"chart/Chart.yaml": {Data: []byte("name: chart\nversion: invalid\n")}},
		"ignore":   {"chart/.helmignore": {Data: []byte("**\n")}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := loadFSChart(input, "chart")
			if name == "archive" {
				if err == nil {
					t.Fatal("invalid archive must return an error")
				}
				return
			}
			if name == "metadata" {
				if _, ok := errors.AsType[chart.ValidationError](err); !ok {
					t.Fatalf("invalid metadata lacks validation error: %v", err)
				}
				return
			}
			if !errors.Is(err, manifest.ErrInput) {
				t.Fatalf("invalid chart lacks input classification: %v", err)
			}
		})
	}
}
