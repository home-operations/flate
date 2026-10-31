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

	"github.com/home-operations/flate/internal/chartinput"
	"github.com/home-operations/flate/pkg/manifest"
)

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
func captureChart(ctx context.Context, input fs.FS, name string, archiveShape bool) (capturedChart, bool, error) {
	if err := ctx.Err(); err != nil {
		return capturedChart{}, false, err
	}
	info, err := fs.Stat(input, name)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return capturedChart{}, true, manifest.NewError(manifest.ErrInput, fmt.Errorf("chart not found at %s: %w", name, err), "")
		}
		return capturedChart{}, false, manifest.NewError(manifest.ErrInput, fmt.Errorf("stat chart %s: %w", name, err), "")
	}
	if info.IsDir() {
		input, err = fs.Sub(input, name)
		if err != nil {
			return capturedChart{}, false, manifest.NewError(manifest.ErrInput, fmt.Errorf("chart directory %s: %w", name, err), "")
		}
		files, err := chartFilesInFS(ctx, input, archiveShape)
		return capturedChart{files: files}, false, err
	}
	r, err := input.Open(name)
	if err != nil {
		return capturedChart{}, false, manifest.NewError(manifest.ErrInput, fmt.Errorf("open chart %s: %w", name, err), "")
	}
	data, readErr := io.ReadAll(r)
	if err := errors.Join(readErr, r.Close()); err != nil {
		return capturedChart{}, false, manifest.NewError(manifest.ErrInput, fmt.Errorf("read chart %s: %w", name, err), "")
	}
	return capturedChart{archive: data}, false, nil
}

// Capture MUST finish before LoadFiles consumes the records.
func chartFilesInFS(ctx context.Context, input fs.FS, archiveShape bool) ([]*archive.BufferedFile, error) {
	files := []*archive.BufferedFile{}
	remaining := archive.MaxDecompressedChartSize
	visit := func(name string, info fs.FileInfo, data []byte) error {
		rooted := name
		if archiveShape {
			delimiter := "/"
			if strings.ContainsRune(name, '\\') {
				delimiter = "\\"
			}
			base, relative, _ := strings.Cut(name, delimiter)
			relative = strings.ReplaceAll(relative, delimiter, "/")
			if path.IsAbs(relative) {
				return errors.New("chart illegally contains absolute paths")
			}
			relative = path.Clean(relative)
			if relative == "." {
				return fmt.Errorf("chart illegally contains content outside the base directory: %q", name)
			}
			if strings.HasPrefix(relative, "..") {
				return errors.New("chart illegally references parent directory")
			}
			if len(relative) >= 3 && relative[1:3] == ":/" && (relative[0] >= 'a' && relative[0] <= 'z' || relative[0] >= 'A' && relative[0] <= 'Z') {
				return errors.New("chart contains illegally named files")
			}
			if base == "Chart.yaml" {
				return errors.New("chart yaml not in base directory")
			}
			rooted = relative
		}
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
		if remaining < 0 || archiveShape && remaining == 0 {
			return fmt.Errorf("chart exceeds maximum decompressed size")
		}
		file := &archive.BufferedFile{Name: rooted, ModTime: info.ModTime(), Data: bytes.TrimPrefix(data, []byte{0xef, 0xbb, 0xbf})}
		files = append(files, file)
		return nil
	}
	err := chartinput.Walk(ctx, input, !archiveShape, visit)
	if err != nil {
		return nil, manifest.NewError(manifest.ErrInput, err, "")
	}
	return files, nil
}
