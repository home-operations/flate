package helm

import (
	"bytes"
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
	"helm.sh/helm/v4/pkg/ignore"

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

func loadFSChart(input fs.FS, name string) (*chart.Chart, string, bool, error) {
	files, fingerprint, missing, err := captureChartDir(input, name)
	if err != nil {
		return nil, "", missing, err
	}
	if files != nil {
		ch, err := loader.LoadFiles(files)
		return ch, fingerprint, false, err
	}
	r, err := input.Open(name)
	if err != nil {
		return nil, "", false, fmt.Errorf("%w: open chart %s: %w", manifest.ErrInput, name, err)
	}
	ch, loadErr := loader.LoadArchive(r)
	if err := errors.Join(loadErr, r.Close()); err != nil {
		return nil, "", false, err
	}
	return ch, chartFingerprint(ch), false, nil
}

// A missing requested location is distinct from a read failure within a chart.
func captureChartDir(input fs.FS, name string) ([]*archive.BufferedFile, string, bool, error) {
	info, err := fs.Stat(input, name)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, "", true, fmt.Errorf("%w: chart not found at %s", manifest.ErrInput, name)
		}
		return nil, "", false, fmt.Errorf("%w: stat chart %s: %w", manifest.ErrInput, name, err)
	}
	if !info.IsDir() {
		return nil, "", false, nil
	}
	input, err = fs.Sub(input, name)
	if err != nil {
		return nil, "", false, fmt.Errorf("%w: chart directory %s: %w", manifest.ErrInput, name, err)
	}
	files, fingerprint, err := chartFilesInFS(input)
	return files, fingerprint, false, err
}

// The records and their identity MUST be finalized before LoadFiles consumes them.
func chartFilesInFS(input fs.FS) ([]*archive.BufferedFile, string, error) {
	rules := ignore.Empty()
	var policy []byte
	present := false
	if r, err := input.Open(ignore.HelmIgnore); err == nil {
		data, readErr := io.ReadAll(io.LimitReader(r, archive.MaxDecompressedChartSize+1))
		if err := errors.Join(readErr, r.Close()); err != nil {
			return nil, "", fmt.Errorf("%w: chart ignore rules: %w", manifest.ErrInput, err)
		}
		if int64(len(data)) > archive.MaxDecompressedChartSize {
			return nil, "", fmt.Errorf("%w: chart ignore rules exceed maximum decompressed size", manifest.ErrInput)
		}
		policy, present = data, true
		rules, err = ignore.Parse(bytes.NewReader(policy))
		if err != nil {
			return nil, "", fmt.Errorf("%w: chart ignore rules: %w", manifest.ErrInput, err)
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return nil, "", fmt.Errorf("%w: chart ignore rules: %w", manifest.ErrInput, err)
	}
	rules.AddDefaults()
	files := []*archive.BufferedFile{}
	remaining := archive.MaxDecompressedChartSize
	err := fs.WalkDir(input, ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if name == "." {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if rules.Ignore(name, info) {
			if entry.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if entry.IsDir() {
			return nil
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("cannot load irregular file %s as it has file mode type bits set", name)
		}
		data := policy
		if name != ignore.HelmIgnore || !present {
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
		return nil, "", fmt.Errorf("%w: %w", manifest.ErrInput, err)
	}
	content := make([][2]any, len(files))
	for i, file := range files {
		content[i] = [2]any{file.Name, file.Data}
	}
	return files, manifest.Fingerprint(content), nil
}
