package helm

import (
	"bytes"
	"compress/gzip"
	"context"
	"io/fs"
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func ociRenderFixture(tb testing.TB) (*store.Store, *manifest.HelmRelease, string) {
	tb.Helper()
	dir := tb.TempDir()
	writeChartFiles(tb, dir, "podinfo", "6.15.0")
	testutil.WriteFile(tb, dir, "values.yaml", "marker: default\n")
	testutil.WriteFile(tb, dir, "prod.yaml", "marker: original\n")
	testutil.WriteFile(tb, dir, "charts/child/Chart.yaml", "apiVersion: v2\nname: child\nversion: 1.2.3+child\n")
	testutil.WriteFile(tb, dir, "templates/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ .Release.Name }}
  labels:
    helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | quote }}
data:
  version: {{ .Chart.Version | quote }}
  marker: {{ .Values.marker | quote }}
`)
	st := store.New()
	src := &manifest.OCIRepository{Name: "podinfo", Namespace: "apps", URL: "oci://example.test/podinfo"}
	st.AddObject(src)
	hr := &manifest.HelmRelease{
		Name: "podinfo", Namespace: "apps",
		ChartRef:         &helmv2.CrossNamespaceSourceReference{Kind: manifest.KindOCIRepository, Name: src.Name},
		Chart:            manifest.HelmChart{Source: manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: src.Namespace, Name: src.Name}},
		ChartValuesFiles: []string{"prod.yaml"},
	}
	st.AddObject(hr)
	return st, hr, dir
}

func acquireChartFS(tb testing.TB, owner *cache.Cache, input fs.FS) fs.FS {
	tb.Helper()
	files := make(map[string]string)
	err := fs.WalkDir(input, ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		data, err := fs.ReadFile(input, name)
		if err != nil {
			return err
		}
		files[name] = string(data)
		return nil
	})
	if err != nil {
		tb.Fatal(err)
	}
	var buf bytes.Buffer
	testutil.WriteTar(tb, &buf, files, 0o600)
	data := buf.Bytes()
	acquired, _, _, err := owner.Acquire(tb.Context(), cache.ArchiveKey{Content: tb.Name()}, cache.Policy{MaxAge: cache.Forever}, func(_ context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
		_, err := w.Write(data)
		return cache.ProducerResult{}, err
	})
	if err != nil {
		tb.Fatal(err)
	}
	return acquired
}

func chartArchive(tb testing.TB, input fs.FS) []byte {
	tb.Helper()
	files := make(map[string]string)
	err := fs.WalkDir(input, ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		data, err := fs.ReadFile(input, name)
		if err != nil {
			return err
		}
		files["chart/"+name] = string(data)
		return nil
	})
	if err != nil {
		tb.Fatal(err)
	}
	return testutil.TarGz(tb, files, 0o600, gzip.NoCompression)
}

func hasRenderField(value any, key, want string) bool {
	switch value := value.(type) {
	case []map[string]any:
		for _, doc := range value {
			if hasRenderField(doc, key, want) {
				return true
			}
		}
	case map[string]any:
		if value[key] == want {
			return true
		}
		for _, item := range value {
			if hasRenderField(item, key, want) {
				return true
			}
		}
	case []any:
		for _, item := range value {
			if hasRenderField(item, key, want) {
				return true
			}
		}
	}
	return false
}

func encodedRender(tb testing.TB, docs []map[string]any) []byte {
	tb.Helper()
	data, err := manifest.EncodeRender(docs)
	if err != nil {
		tb.Fatal(err)
	}
	return data
}
