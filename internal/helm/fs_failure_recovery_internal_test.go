package helm

import (
	"bytes"
	"errors"
	"io/fs"
	"testing"
	"testing/fstest"

	"helm.sh/helm/v4/pkg/chart/loader/archive"

	"github.com/home-operations/flate/internal/assert"
	sourcecache "github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

//nolint:paralleltest // test replaces process-wide dependency configuration.
func TestLoadFSChart_FailureRecovery(t *testing.T) {
	for _, failure := range []string{"read", "close", "disappeared", "parse", "policy-limit"} {
		t.Run(failure, func(t *testing.T) {
			testLoadFSChartFailureRecoveryCase(t, &failure)
		},
		)
	}
}

//nolint:reassign // Serial fixtures must set and restore Helm's process-wide decompression limit.
func testLoadFSChartFailureRecoveryCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()
	input := fstest.MapFS{
		"Chart.yaml":        {Data: []byte("apiVersion: v2\nname: chart\nversion: 1.0.0\n")},
		"templates/cm.yaml": {Data: []byte("old")},
	}
	broken := true
	want := errors.New("capture failed")
	observed := chartOpenFS{FS: input, open: func(name string) (fs.File, error) {
		if broken && name == "templates/cm.yaml" {
			switch *testCaseInput {
			case "read":
				return &chartErrorFile{err: want}, nil
			case "close":
				f, err := input.Open(name)
				return &chartErrorFile{File: f, err: want, closeOnly: true}, err
			case "disappeared":
				return nil, fs.ErrNotExist
			}
		}
		return input.Open(name)
	}}
	if (*testCaseInput) == "parse" {
		input["Chart.yaml"].Data = []byte("name: chart\nversion: invalid\n")
	}
	if (*testCaseInput) == "policy-limit" {
		old := archive.MaxDecompressedChartSize
		archive.MaxDecompressedChartSize = 64
		t.Cleanup(func() { archive.MaxDecompressedChartSize = old })
		input[".helmignore"] = &fstest.MapFile{Data: bytes.Repeat([]byte("#"), 65)}
	}
	cli := NewClient(cachetest.New(t, "", sourcecache.New))
	st := store.New()
	hr := &manifest.HelmRelease{
		Chart: manifest.HelmChart{
			Name: ".",
			Source: manifest.NamedResource{
				Group: manifest.SourceDomain,
				Kind:  manifest.KindGitRepository,
				Name:  "repo",
			},
		},
	}
	st.SetArtifact(hr.ChartSource(), &store.SourceArtifact{FS: observed, Key: "untrusted"})
	cliSources := NewSources(st)
	_, err := loadDecodedChart(t.Context(), cli, hr, cliSources)
	assert.FatalIf(t, err == nil, "%s", "failed input accepted")
	assert.FatalIf(
		t,
		((*testCaseInput) == "read" || (*testCaseInput) == "close") && !errors.Is(err, want),
		"lost read/close error: %v",
		err,
	)
	if (*testCaseInput) == "disappeared" {
		_, missing, captureChartErr := captureChart(t.Context(), observed, ".", false)
		assert.Equal(t, missing, false)
		assert.Equal(t, errors.Is(captureChartErr, fs.ErrNotExist), true)
	}
	captured := 0
	if (*testCaseInput) == "parse" {
		captured = 1
		for dIndex := range cli.chartCache {
			d := cli.chartCache[dIndex]
			assert.Equal(t, d.chart == nil, true)
		}
	}
	assert.Equal(t, len(cli.chartCache), captured)
	broken = false
	input["Chart.yaml"].Data = []byte("apiVersion: v2\nname: chart\nversion: 1.0.0\n")
	delete(input, ".helmignore")
	_, err = loadDecodedChart(t.Context(), cli, hr, cliSources)
	assert.NoError(t, err)
	assert.Equal(t, len(cli.chartCache), captured+1)
}
