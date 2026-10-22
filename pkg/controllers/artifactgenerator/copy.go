package artifactgenerator

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"strings"

	"github.com/bmatcuk/doublestar/v4"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/safepath"
	"github.com/home-operations/flate/pkg/tree"
	"helm.sh/helm/v4/pkg/chart/v2/loader"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/yaml"
)

func sourcePath(value string) (string, string, error) {
	alias, pattern, ok := strings.Cut(value, "/")
	if !ok || !strings.HasPrefix(alias, "@") || !aliasName.MatchString(strings.TrimPrefix(alias, "@")) {
		return "", "", fmt.Errorf("invalid source %q", value)
	}
	if _, err := safepath.ArchiveName(pattern); pattern != "" && err != nil {
		return "", "", err
	}
	return alias[1:], pattern, nil
}
func destinationPath(value string) (string, error) {
	destination, ok := strings.CutPrefix(value, "@artifact/")
	if !ok {
		return "", fmt.Errorf("invalid destination %q", value)
	}
	if destination == "" {
		return ".", nil
	}
	return safepath.ArchiveName(destination)
}

func (e *evaluation) apply(ctx context.Context, builder *tree.Builder, op operation) error {
	alias, pattern, err := sourcePath(op.From)
	if err != nil {
		return err
	}
	if pattern == "" {
		pattern = "."
	}
	if err := validateGlob(pattern); err != nil {
		return err
	}
	input := e.inputs[alias]
	if input == nil {
		return fmt.Errorf("unknown source alias %q", alias)
	}
	destination, err := destinationPath(op.To)
	if err != nil {
		return err
	}
	glob := strings.ContainsAny(pattern, "*?[]")
	root := path.Clean(pattern)
	if glob {
		root = "."
		for component := range strings.SplitSeq(path.Clean(pattern), "/") {
			if strings.ContainsAny(component, "*?[]") {
				break
			}
			root = path.Join(root, component)
		}
	}
	matches := []string{path.Clean(pattern)}
	if glob {
		if op.Strategy == "Extract" {
			matches, err = doublestar.Glob(input.FS, pattern, doublestar.WithFailOnIOErrors())
		} else {
			matches, err = fs.Glob(input.FS, pattern)
		}
		if err != nil {
			return err
		}
	}
	selected := 0
	for _, name := range matches {
		if err := ctx.Err(); err != nil {
			return err
		}
		if glob && name == "." || excluded(name, root, op.Exclude) {
			continue
		}
		selected++
		info, err := fs.Stat(input.FS, name)
		if err != nil {
			return err
		}
		target := destination
		if op.Strategy == "Extract" {
			lower := strings.ToLower(name)
			if info.IsDir() || !strings.HasSuffix(lower, ".tar.gz") && !strings.HasSuffix(lower, ".tgz") {
				if glob {
					continue
				}
				return fmt.Errorf("extract requires a tarball, got %q", name)
			}
			if err := e.extract(ctx, builder, archiveSelection{alias, name, target}); err != nil {
				return err
			}
			continue
		}
		if glob {
			relative := name
			if prefix, ok := strings.CutSuffix(pattern, "/**"); ok {
				relative = strings.TrimPrefix(name, prefix+"/")
			}
			target = path.Join(target, relative)
		} else if info.IsDir() || strings.HasSuffix(op.To, "/") {
			target = path.Join(target, path.Base(name))
		} else if previous, err := fs.Stat(builder, target); err == nil && previous.IsDir() {
			target = path.Join(target, path.Base(name))
		}
		if info.IsDir() {
			err = fs.WalkDir(input.FS, name, func(current string, entry fs.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if err := ctx.Err(); err != nil {
					return err
				}
				if current != name && excluded(current, root, op.Exclude) {
					if entry.IsDir() {
						return fs.SkipDir
					}
					return nil
				}
				relative := strings.TrimPrefix(strings.TrimPrefix(current, name), "/")
				to := path.Join(target, relative)
				if entry.IsDir() {
					return ensureDirectory(builder, to)
				}
				return copyFile(ctx, input.FS, builder, operation{From: current, To: to})
			})
		} else {
			err = copyFile(ctx, input.FS, builder, operation{From: name, To: target, Strategy: op.Strategy})
		}
		if err != nil {
			return err
		}
	}
	if glob && selected == 0 {
		return fmt.Errorf("no unexcluded files match %q", pattern)
	}
	return nil
}

func excluded(name, root string, patterns []string) bool {
	relative := strings.TrimPrefix(name, strings.TrimSuffix(root, "/")+"/")
	for _, pattern := range patterns {
		for _, value := range []string{name, relative} {
			if match, _ := doublestar.Match(pattern, value); match {
				return true
			}
		}
		if !strings.Contains(pattern, "/") {
			if match, _ := doublestar.Match(pattern, path.Base(name)); match {
				return true
			}
		}
	}
	return false
}

func ensureDirectory(builder *tree.Builder, name string) error {
	if info, err := fs.Stat(builder, name); err == nil {
		if !info.IsDir() {
			return tree.ErrConflict
		}
		return nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	empty, _ := tree.NewBuilder(nil).Build()
	return builder.Mount(name, empty)
}

func copyFile(ctx context.Context, input fs.FS, builder *tree.Builder, op operation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	info, err := fs.Stat(input, op.From)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("unsupported source file %q", op.From)
	}
	data, err := fs.ReadFile(input, op.From)
	if err != nil {
		return err
	}
	mode := info.Mode()
	if previous, err := fs.Stat(builder, op.To); op.Strategy == "Extract" && err == nil {
		mode = previous.Mode()
	} else if op.Strategy == "Merge" && err == nil {
		base, err := loadYAML(ctx, builder, op.To)
		if err != nil {
			return err
		}
		overlay, err := loadYAML(ctx, input, op.From)
		if err != nil {
			return err
		}
		data, err = yaml.Marshal(loader.MergeMaps(base, overlay))
		if err != nil {
			return err
		}
		mode = previous.Mode()
	}
	return builder.AddFile(op.To, data, mode)
}

func loadYAML(ctx context.Context, input fs.FS, name string) (map[string]any, error) {
	file, err := input.Open(name)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	reader := utilyaml.NewYAMLToJSONDecoder(file)
	out := map[string]any{}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var document map[string]any
		err := reader.Decode(&document)
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		out = loader.MergeMaps(out, document)
	}
}

type archiveSelection struct{ Alias, Path, Destination string }

func (e *evaluation) extract(ctx context.Context, builder *tree.Builder, selection archiveSelection) error {
	input := e.inputs[selection.Alias]
	key := cache.ArchiveKey{Policy: "extract", Content: struct {
		Namespace string
		Inputs    []inputIdentity
		Selection archiveSelection
	}{"artifactgenerator/extract/v1", e.identity, selection}}
	extracted, _, _, err := e.owner.Acquire(ctx, key, cache.Policy{MaxAge: e.maxAge}, func(_ context.Context, target cache.ProducerInput) (cache.ProducerResult, error) {
		file, err := input.FS.Open(selection.Path)
		if err != nil {
			return cache.ProducerResult{}, err
		}
		defer func() { _ = file.Close() }()
		compressed, err := gzip.NewReader(file)
		if err != nil {
			return cache.ProducerResult{}, err
		}
		defer func() { _ = compressed.Close() }()
		_, err = io.Copy(target, compressed) //nolint:gosec // Cache validates tar members; Extract preserves Flux's unbounded trailing-data semantics.
		return cache.ProducerResult{}, err
	})
	if err != nil {
		return err
	}
	if err := ensureDirectory(builder, selection.Destination); err != nil {
		return err
	}
	return fs.WalkDir(extracted, ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if name == "." {
			return nil
		}
		target := path.Join(selection.Destination, name)
		if entry.IsDir() {
			return ensureDirectory(builder, target)
		}
		return copyFile(ctx, extracted, builder, operation{From: name, To: target, Strategy: "Extract"})
	})
}

func (e *evaluation) produce(ctx context.Context, req request, target cache.ProducerInput) (cache.ProducerResult, error) {
	builder := tree.NewBuilder(nil)
	for _, op := range req.Output.Copy {
		if err := e.apply(ctx, builder, op); err != nil {
			return cache.ProducerResult{}, err
		}
	}
	writer := tar.NewWriter(target)
	if err := writer.AddFS(builder); err != nil {
		return cache.ProducerResult{}, err
	}
	return cache.ProducerResult{}, writer.Close()
}
