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
	"helm.sh/helm/v4/pkg/ignore"
)

func chartPathInFS(input fs.FS) (string, error) {
	if _, err := fs.Stat(input, chartYamlFilename); err == nil {
		return ".", nil
	}
	for _, name := range []string{copiedOCILayerFilename, httpChartFilename} {
		if _, err := fs.Stat(input, name); err == nil {
			return name, nil
		}
	}
	entries, err := fs.ReadDir(input, ".")
	if err != nil {
		return "", err
	}
	selected := ""
	for _, e := range entries {
		if !e.IsDir() || strings.HasPrefix(e.Name(), ".") {
			continue
		}
		if _, err := fs.Stat(input, path.Join(e.Name(), chartYamlFilename)); err != nil {
			continue
		}
		if selected != "" {
			return "", fmt.Errorf("chart artifact contains multiple Chart.yaml-bearing subdirs; flate cannot disambiguate a bundle-of-charts artifact")
		}
		selected = e.Name()
	}
	if selected == "" {
		return "", fmt.Errorf("chart layer missing or layerSelector misconfigured")
	}
	return selected, nil
}

func loadFSChart(input fs.FS, name string) (*chart.Chart, error) {
	info, err := fs.Stat(input, name)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		r, err := input.Open(name)
		if err != nil {
			return nil, err
		}
		ch, loadErr := loader.LoadArchive(r)
		return ch, errors.Join(loadErr, r.Close())
	}
	input, err = fs.Sub(input, name)
	if err != nil {
		return nil, err
	}
	rules := ignore.Empty()
	if r, err := input.Open(ignore.HelmIgnore); err == nil {
		parsed, parseErr := ignore.Parse(r)
		closeErr := r.Close()
		if err := errors.Join(parseErr, closeErr); err != nil {
			return nil, err
		}
		rules = parsed
	} else if !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	rules.AddDefaults()
	var files []*archive.BufferedFile
	remaining := archive.MaxDecompressedChartSize
	err = fs.WalkDir(input, ".", func(name string, entry fs.DirEntry, err error) error {
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
		r, err := input.Open(name)
		if err != nil {
			return err
		}
		data, readErr := io.ReadAll(io.LimitReader(r, remaining+1))
		if err := errors.Join(readErr, r.Close()); err != nil {
			return err
		}
		remaining -= int64(len(data))
		if remaining < 0 {
			return fmt.Errorf("chart exceeds maximum decompressed size")
		}
		files = append(files, &archive.BufferedFile{Name: name, ModTime: info.ModTime(), Data: bytes.TrimPrefix(data, []byte{0xef, 0xbb, 0xbf})})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return loader.LoadFiles(files)
}
