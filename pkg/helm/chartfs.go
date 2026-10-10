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

	"helm.sh/helm/v4/pkg/chart/common"
	"helm.sh/helm/v4/pkg/chart/loader/archive"
	chart "helm.sh/helm/v4/pkg/chart/v2"
	"helm.sh/helm/v4/pkg/chart/v2/loader"
	"helm.sh/helm/v4/pkg/ignore"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/tree"
)

var chartBOM = []byte{0xef, 0xbb, 0xbf}

// Helm owns archive validation and recursive dependency parsing. Trimming the
// parsed chart drops Raw aliases and oversized buffers before cache publication.
func archiveFiles(ctx context.Context, in io.Reader) ([]*archive.BufferedFile, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	files, err := archive.LoadArchiveFiles(in)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", manifest.ErrInput, err)
	}
	return files, ctx.Err()
}

func loadChartFS(ctx context.Context, input fs.FS, name string) (*chart.Chart, error) {
	info, err := fs.Stat(input, name)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		f, err := input.Open(name)
		if err != nil {
			return nil, err
		}
		defer func() { _ = f.Close() }()
		files, err := archiveFiles(ctx, f)
		if err != nil {
			return nil, err
		}
		return loader.LoadFiles(files)
	}
	rules := ignore.Empty()
	if f, err := input.Open(path.Join(name, ignore.HelmIgnore)); err == nil {
		rules, err = ignore.Parse(f)
		closeErr := f.Close()
		if err != nil {
			return nil, err
		}
		if closeErr != nil {
			return nil, closeErr
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	rules.AddDefaults()
	remaining := archive.MaxDecompressedChartSize
	var files []*archive.BufferedFile
	var walk func(string, string, int) error
	walk = func(p, n string, hops int) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		info, err := fs.Lstat(input, p)
		if err != nil {
			return err
		}
		if info.Mode()&fs.ModeSymlink != 0 {
			hops++
			if hops > 8 {
				return fmt.Errorf("%w: %w", manifest.ErrInput, tree.ErrLoop)
			}
			info, err = fs.Stat(input, p)
			if err != nil {
				return err
			}
		}
		if n != "" && rules.Ignore(n, info) {
			return nil
		}
		if info.IsDir() {
			entries, err := fs.ReadDir(input, p)
			if err != nil {
				return err
			}
			for _, e := range entries {
				if err := walk(path.Join(p, e.Name()), path.Join(n, e.Name()), hops); err != nil {
					return err
				}
			}
			return nil
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("%w: cannot load irregular chart file %s", manifest.ErrInput, p)
		}
		if info.Size() > remaining {
			return fmt.Errorf("%w: chart exceeds maximum decompressed size of %d bytes", manifest.ErrInput, archive.MaxDecompressedChartSize)
		}
		f, err := input.Open(p)
		if err != nil {
			return err
		}
		data := make([]byte, info.Size())
		_, err = io.ReadFull(f, data)
		closeErr := f.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		remaining -= int64(len(data))
		data = bytes.TrimPrefix(data, chartBOM)
		files = append(files, &archive.BufferedFile{Name: n, ModTime: info.ModTime(), Data: data})
		return nil
	}
	if err := walk(name, "", 0); err != nil {
		return nil, err
	}
	return loader.LoadFiles(files)
}

// Chart.Raw is used by Helm's show/save/create paths. Template, values and
// schema consumers use parsed fields; retaining Raw keeps their input copies.
func trimChart(ch *chart.Chart) {
	ch.Raw = nil
	for _, files := range [][]*common.File{ch.Templates, ch.Files} {
		for _, f := range files {
			if cap(f.Data) != len(f.Data) {
				data := make([]byte, len(f.Data))
				copy(data, f.Data)
				f.Data = data
			}
		}
	}
	if cap(ch.Schema) != len(ch.Schema) {
		data := make([]byte, len(ch.Schema))
		copy(data, ch.Schema)
		ch.Schema = data
	}
	for _, dep := range ch.Dependencies() {
		trimChart(dep)
	}
}

func memoryChartPath(input fs.FS, hr *manifest.HelmRelease) (string, error) {
	switch hr.Chart.RepoKind {
	case manifest.KindGitRepository, manifest.KindBucket, manifest.KindExternalArtifact:
		name := path.Clean(hr.Chart.Name)
		if !fs.ValidPath(name) {
			return "", fmt.Errorf("%w: invalid chart path %q", manifest.ErrInput, hr.Chart.Name)
		}
		_, err := fs.Stat(input, path.Join(name, chartYamlFilename))
		return name, err
	}
	for _, name := range []string{chartYamlFilename, copiedOCILayerFilename, httpChartFilename} {
		_, err := fs.Stat(input, name)
		if err == nil {
			if name == chartYamlFilename {
				return ".", nil
			}
			return name, nil
		}
		if !errors.Is(err, fs.ErrNotExist) {
			return "", err
		}
	}
	entries, err := fs.ReadDir(input, ".")
	if err != nil {
		return "", err
	}
	match := ""
	for _, e := range entries {
		if !e.IsDir() || strings.HasPrefix(e.Name(), ".") {
			continue
		}
		_, err := fs.Stat(input, path.Join(e.Name(), chartYamlFilename))
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return "", err
		}
		if match != "" {
			return "", fmt.Errorf("%w: chart artifact contains multiple Chart.yaml-bearing subdirs", manifest.ErrInput)
		}
		match = e.Name()
	}
	if match == "" {
		return "", fmt.Errorf("%w: chart layer missing or layerSelector misconfigured", manifest.ErrInput)
	}
	return match, nil
}
