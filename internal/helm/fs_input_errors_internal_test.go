package helm

import (
	"errors"
	"io/fs"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	sourcecache "github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

type chartErrorFile struct {
	fs.File

	err       error
	closeOnly bool
}

func (f *chartErrorFile) Read(p []byte) (int, error) {
	if f.closeOnly {
		return f.File.Read(p)
	}
	return 0, f.err
}

func (f *chartErrorFile) Close() error {
	if f.File != nil {
		_ = f.File.Close()
	}
	if f.closeOnly {
		return f.err
	}
	return nil
}

type chartStatFS struct {
	fs.FS

	broken *bool
}

func (f chartStatFS) Stat(name string) (fs.FileInfo, error) {
	if *f.broken {
		return nil, fs.ErrPermission
	}
	return fs.Stat(f.FS, name)
}

type chartReadDirFS struct {
	fs.FS

	broken *bool
}

func (f chartReadDirFS) ReadDir(name string) ([]fs.DirEntry, error) {
	if *f.broken {
		return nil, fs.ErrPermission
	}
	return fs.ReadDir(f.FS, name)
}

func TestLoadFSChart_AutomaticInputErrors(t *testing.T) {
	t.Parallel()
	for _, failure := range []string{"stat", "open", "enumeration", "read", "close", "read and close"} {
		t.Run(failure, func(t *testing.T) {
			t.Parallel()
			testLoadFSChartAutomaticInputErrorsCase(t, &failure)
		},
		)
	}
}

func testLoadFSChartAutomaticInputErrorsCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	files := fstest.MapFS{
		"a/Chart.yaml": {Data: []byte("apiVersion: v2\nname: first\nversion: 1.0.0\n")},
		"z/Chart.yaml": {Data: []byte("apiVersion: v2\nname: alternate\nversion: 1.0.0\n")},
	}
	broken := true
	closeErr := errors.New("close failed")
	var input fs.FS = chartOpenFS{FS: files, open: func(name string) (fs.File, error) {
		if broken && name == "a/Chart.yaml" && (*testCaseInput) == "open" {
			return nil, fs.ErrPermission
		}
		file, err := files.Open(name)
		if !broken || err != nil || name != "a/Chart.yaml" {
			return file, err
		}
		switch *testCaseInput {
		case "read":
			return valuesFaultFile{File: file, readErr: fs.ErrPermission}, nil
		case "close":
			return valuesFaultFile{File: file, closeErr: fs.ErrPermission}, nil
		case "read and close":
			return valuesFaultFile{File: file, readErr: fs.ErrPermission, closeErr: closeErr}, nil
		}
		return file, nil
	}}
	if (*testCaseInput) == "stat" {
		input = chartStatFS{FS: input, broken: &broken}
	}
	if (*testCaseInput) == "enumeration" {
		input = chartReadDirFS{FS: input, broken: &broken}
	}
	hr := newHR()
	hr.Chart = manifest.HelmChart{
		Source: manifest.NamedResource{
			Group:     manifest.SourceDomain,
			Kind:      manifest.KindHelmChart,
			Name:      "source",
			Namespace: hr.Namespace,
		},
	}
	st := store.New()
	st.SetArtifact(hr.ChartSource(), &store.SourceArtifact{FS: input})
	cli := NewClient(cachetest.New(t, "", sourcecache.New))
	docs, _, err := testutil.RenderMaps(
		cli.Template(t.Context(), hr, nil, RenderOptions{Sources: NewSources(st)}),
	)
	assert.FatalIf(
		t,
		!errors.Is(err, fs.ErrPermission) || !errors.Is(err, manifest.ErrInput) || docs != nil,
		"%s failure selected another chart: docs=%v, error=%v", (*testCaseInput), docs,
		err,
	)
	assert.FatalIf(
		t, (*testCaseInput) ==
			"read and close" && !errors.Is(err, closeErr), "lost joined close error: %v",
		err,
	)
	assert.Equal(t, len(cli.chartCache), 0)
	broken = false
	loaded, err := loadDecodedChart(t.Context(), cli, hr, NewSources(st))
	assert.NoError(t, err)
	assert.Equal(t, loaded.chart.Name(), "alternate")
	assert.Equal(t, len(cli.chartCache), 1)
}
