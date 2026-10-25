package helm

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"strings"

	"helm.sh/helm/v4/pkg/chart/loader/archive"
	chart "helm.sh/helm/v4/pkg/chart/v2"
	"helm.sh/helm/v4/pkg/chart/v2/loader"
	"helm.sh/helm/v4/pkg/chart/v2/util"

	"github.com/home-operations/flate/internal/chartinput"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
)

func chartPathInFS(input fs.FS) (string, error) {
	if _, err := fs.Stat(input, util.ChartfileName); err == nil {
		return ".", nil
	}
	for _, name := range []string{source.OCICopiedLayerFilename, source.HelmChartArchiveFilename} {
		if _, err := fs.Stat(input, name); err == nil {
			return name, nil
		}
	}
	entries, err := fs.ReadDir(input, ".")
	if err != nil {
		return "", fmt.Errorf("%w: read chart artifact: %w", manifest.ErrInput, err)
	}
	selected := ""
	for _, e := range entries {
		if !e.IsDir() || strings.HasPrefix(e.Name(), ".") {
			continue
		}
		if _, err := fs.Stat(input, path.Join(e.Name(), util.ChartfileName)); err != nil {
			continue
		}
		if selected != "" {
			return "", fmt.Errorf("%w: chart artifact contains multiple Chart.yaml-bearing subdirs; flate cannot disambiguate a bundle-of-charts artifact", manifest.ErrInput)
		}
		selected = e.Name()
	}
	if selected == "" {
		return "", fmt.Errorf("%w: chart layer missing or layerSelector misconfigured", manifest.ErrInput)
	}
	return selected, nil
}

type capturedChart struct {
	files   []*archive.BufferedFile
	archive []byte
}

func (c capturedChart) fingerprint() string {
	if c.files == nil {
		return manifest.Fingerprint(c.archive)
	}
	content := make([]chartFileContent, len(c.files))
	for i, f := range c.files {
		content[i] = chartFileContent{Name: f.Name, Data: f.Data}
	}
	return manifest.Fingerprint(content)
}

func (c capturedChart) load() (*chart.Chart, error) {
	if c.files != nil {
		return loader.LoadFiles(c.files)
	}
	return loader.LoadArchive(bytes.NewReader(c.archive))
}

// A missing requested location is distinct from a read failure within a chart.
func captureChart(ctx context.Context, input fs.FS, name string) (capturedChart, bool, error) {
	if err := ctx.Err(); err != nil {
		return capturedChart{}, false, err
	}
	info, err := fs.Stat(input, name)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return capturedChart{}, true, fmt.Errorf("%w: chart not found at %s", manifest.ErrInput, name)
		}
		return capturedChart{}, false, fmt.Errorf("%w: stat chart %s: %w", manifest.ErrInput, name, err)
	}
	if info.IsDir() {
		input, err = fs.Sub(input, name)
		if err != nil {
			return capturedChart{}, false, fmt.Errorf("%w: chart directory %s: %w", manifest.ErrInput, name, err)
		}
		files, err := chartFilesInFS(ctx, input)
		return capturedChart{files: files}, false, err
	}
	r, err := input.Open(name)
	if err != nil {
		return capturedChart{}, false, fmt.Errorf("%w: open chart %s: %w", manifest.ErrInput, name, err)
	}
	data, readErr := io.ReadAll(io.LimitReader(r, archive.MaxDecompressedChartSize+1))
	if err := errors.Join(readErr, r.Close()); err != nil {
		return capturedChart{}, false, fmt.Errorf("%w: read chart %s: %w", manifest.ErrInput, name, err)
	}
	if int64(len(data)) > archive.MaxDecompressedChartSize {
		return capturedChart{}, false, fmt.Errorf("%w: chart archive exceeds maximum decompressed size", manifest.ErrInput)
	}
	return capturedChart{archive: data}, false, nil
}

// Capture MUST finish before LoadFiles consumes the records.
func chartFilesInFS(ctx context.Context, input fs.FS) ([]*archive.BufferedFile, error) {
	files := []*archive.BufferedFile{}
	remaining := archive.MaxDecompressedChartSize
	err := chartinput.Walk(ctx, input, func(name string, info fs.FileInfo, data []byte) error {
		if data == nil {
			r, err := input.Open(name)
			if err != nil {
				return err
			}
			var readErr error
			data, readErr = io.ReadAll(io.LimitReader(r, remaining+1))
			if err := errors.Join(readErr, r.Close()); err != nil {
				return err
			}
		}
		remaining -= int64(len(data))
		if remaining < 0 {
			return fmt.Errorf("chart exceeds maximum decompressed size")
		}
		file := &archive.BufferedFile{Name: name, ModTime: info.ModTime(), Data: bytes.TrimPrefix(data, []byte{0xef, 0xbb, 0xbf})}
		files = append(files, file)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %w", manifest.ErrInput, err)
	}
	return files, nil
}
