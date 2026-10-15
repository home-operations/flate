package helm

import (
	"archive/tar"
	"context"
	"errors"
	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"io"
	"io/fs"
	"testing"
)

func ociRenderFixture(t testing.TB) (*store.Store, *manifest.HelmRelease, string) {
	t.Helper()
	dir := t.TempDir()
	writeChartFiles(t, dir, "podinfo", "6.15.0")
	testutil.WriteFile(t, dir, "values.yaml", "marker: default\n")
	testutil.WriteFile(t, dir, "prod.yaml", "marker: original\n")
	testutil.WriteFile(t, dir, "charts/child/Chart.yaml", "apiVersion: v2\nname: child\nversion: 1.2.3+child\n")
	testutil.WriteFile(t, dir, "templates/cm.yaml", `apiVersion: v1
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
		Chart:            manifest.HelmChart{RepoKind: manifest.KindOCIRepository, RepoNamespace: src.Namespace, RepoName: src.Name},
		ChartValuesFiles: []string{"prod.yaml"},
	}
	st.AddObject(hr)
	return st, hr, dir
}

func newTestCache(t testing.TB, root string) *cache.Cache {
	t.Helper()
	owner, err := cache.New(t.Context(), root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	})
	return owner
}

func acquireChartFS(t testing.TB, owner *cache.Cache, input fs.FS) fs.FS {
	t.Helper()
	acquired, _, err := owner.Acquire(t.Context(), cache.ArchiveKey{ID: t.Name()}, cache.Policy{MaxAge: cache.Forever}, func(_ context.Context, w io.Writer) (ocispec.Descriptor, error) {
		tw := tar.NewWriter(w)
		err := fs.WalkDir(input, ".", func(name string, entry fs.DirEntry, err error) error {
			if err != nil || entry.IsDir() {
				return err
			}
			data, err := fs.ReadFile(input, name)
			if err != nil {
				return err
			}
			if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o600, Size: int64(len(data))}); err != nil {
				return err
			}
			_, err = tw.Write(data)
			return err
		})
		return ocispec.Descriptor{}, errors.Join(err, tw.Close())
	})
	if err != nil {
		t.Fatal(err)
	}
	return acquired
}
