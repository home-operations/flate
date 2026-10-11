package helm

import (
	"bytes"
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
				if want := strings.ReplaceAll(wantErr.Error(), filepath.Join(root, "socket"), "socket"); gotErr.Error() != want {
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
	for _, name := range []string{"first", "second", "first", "missing-key"} {
		key := name
		if name == "missing-key" {
			key = ""
		}
		art := &store.SourceArtifact{Kind: manifest.KindHelmChart, Key: key,
			FS: fstest.MapFS{"Chart.yaml": {Data: []byte("apiVersion: v2\nname: " + name + "\nversion: 1.0.0\n")}}}
		st.SetArtifact(manifest.NamedResource{Kind: manifest.KindHelmChart, Name: name, Namespace: "test"}, art)
		hr := &manifest.HelmRelease{Chart: manifest.HelmChart{RepoKind: manifest.KindHelmChart, RepoName: name, RepoNamespace: "test"}}
		loaded, err := cli.LoadChart(t.Context(), hr)
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
	if _, err := loadFSChart(input, "."); err == nil {
		t.Fatal("FS chart bypassed Helm's decompressed-size budget")
	}
}
