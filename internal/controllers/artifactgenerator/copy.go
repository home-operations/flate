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
	"helm.sh/helm/v4/pkg/chart/v2/loader"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/yaml"

	"github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/source/safepath"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/tree"
)

const strategyExtract = "Extract"

type sourceSelection struct {
	alias   string
	pattern string
}

func sourcePath(value string) (sourceSelection, error) {
	alias, pattern, ok := strings.Cut(value, "/")
	if !ok || !strings.HasPrefix(alias, "@") || !aliasName.MatchString(strings.TrimPrefix(alias, "@")) {
		return sourceSelection{alias: "", pattern: ""}, fmt.Errorf("invalid source %q", value)
	}
	if _, err := safepath.ArchiveName(pattern); pattern != "" && err != nil {
		return sourceSelection{alias: "", pattern: ""}, err
	}
	return sourceSelection{alias: alias[1:], pattern: pattern}, nil
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
	plan, err := e.prepareCopy(op)
	if err != nil {
		return err
	}
	selected := 0
	for _, name := range plan.matches {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("context: %w", err)
		}
		if plan.glob && name == "." || excluded(name, plan.root, op.Exclude) {
			continue
		}
		selected++
		if err := plan.apply(ctx, builder, name); err != nil {
			return err
		}
	}
	if plan.glob && selected == 0 {
		return fmt.Errorf("no unexcluded files match %q", plan.pattern)
	}
	return nil
}

type copyPlan struct {
	evaluation                        *evaluation
	op                                operation
	input                             *store.SourceArtifact
	alias, pattern, destination, root string
	glob                              bool
	matches                           []string
}

func (e *evaluation) prepareCopy(op operation) (copyPlan, error) {
	sourcePathResult1, sourcePathResult1Err := sourcePath(op.From)
	alias, pattern, err := sourcePathResult1.alias, sourcePathResult1.pattern, sourcePathResult1Err
	if err != nil {
		return copyPlan{}, err
	}
	if pattern == "" {
		pattern = "."
	}
	if localErr := validateGlob(pattern); localErr != nil {
		return copyPlan{}, localErr
	}
	input := e.inputs[alias]
	if input == nil {
		return copyPlan{}, fmt.Errorf("unknown source alias %q", alias)
	}
	destination, err := destinationPath(op.To)
	if err != nil {
		return copyPlan{}, err
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
		if op.Strategy == strategyExtract {
			matches, err = doublestar.Glob(input.FS, pattern, doublestar.WithFailOnIOErrors())
		} else {
			matches, err = fs.Glob(input.FS, pattern)
		}
		if err != nil {
			return copyPlan{}, fmt.Errorf("glob: %w", err)
		}
	}
	return copyPlan{e, op, input, alias, pattern, destination, root, glob, matches}, nil
}

func (p *copyPlan) apply(ctx context.Context, builder *tree.Builder, name string) error {
	info, err := fs.Stat(p.input.FS, name)
	if err != nil {
		return fmt.Errorf("stat: %w", err)
	}
	if p.op.Strategy == strategyExtract {
		lower := strings.ToLower(name)
		if info.IsDir() || !strings.HasSuffix(lower, ".tar.gz") && !strings.HasSuffix(lower, ".tgz") {
			if p.glob {
				return nil
			}
			return fmt.Errorf("extract requires a tarball, got %q", name)
		}
		return p.evaluation.extract(ctx, builder, archiveSelection{p.alias, name, p.destination})
	}
	target := p.copyDestination(builder, name, info)
	if info.IsDir() {
		return p.copyDirectory(ctx, builder, operation{From: name, To: target})
	}
	return copyFile(ctx, p.input.FS, builder, operation{From: name, To: target, Strategy: p.op.Strategy})
}

func (p *copyPlan) copyDestination(builder *tree.Builder, name string, info fs.FileInfo) string {
	target := p.destination
	if p.glob {
		relative := name
		if prefix, ok := strings.CutSuffix(p.pattern, "/**"); ok {
			relative = strings.TrimPrefix(name, prefix+"/")
		}
		target = path.Join(target, relative)
	} else if info.IsDir() || strings.HasSuffix(p.op.To, "/") {
		target = path.Join(target, path.Base(name))
	} else if previous, err := fs.Stat(builder, target); err == nil && previous.IsDir() {
		target = path.Join(target, path.Base(name))
	}
	return target
}

func (p *copyPlan) copyDirectory(ctx context.Context, builder *tree.Builder, selection operation) error {
	err := fs.WalkDir(p.input.FS, selection.From, func(current string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("context: %w", err)
		}
		if current != selection.From && excluded(current, p.root, p.op.Exclude) {
			if entry.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		relative := strings.TrimPrefix(strings.TrimPrefix(current, selection.From), "/")
		to := path.Join(selection.To, relative)
		if entry.IsDir() {
			return ensureDirectory(builder, to)
		}
		return copyFile(ctx, p.input.FS, builder, operation{From: current, To: to})
	})
	if err != nil {
		return fmt.Errorf("walkdir: %w", err)
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
		return fmt.Errorf("stat: %w", err)
	}
	empty, _ := tree.NewBuilder(nil).Build()
	return builder.Mount(name, empty)
}

func copyFile(ctx context.Context, input fs.FS, builder *tree.Builder, op operation) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("context: %w", err)
	}
	info, err := fs.Stat(input, op.From)
	if err != nil {
		return fmt.Errorf("stat: %w", err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("unsupported source file %q", op.From)
	}
	data, err := fs.ReadFile(input, op.From)
	if err != nil {
		return fmt.Errorf("readfile: %w", err)
	}
	mode := info.Mode()
	if previous, err := fs.Stat(builder, op.To); op.Strategy == strategyExtract && err == nil {
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
			return fmt.Errorf("marshal: %w", err)
		}
		mode = previous.Mode()
	}
	return builder.AddFile(op.To, data, mode)
}

func loadYAML(ctx context.Context, input fs.FS, name string) (map[string]any, error) {
	file, err := input.Open(name)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	defer func() { _ = file.Close() }()
	reader := utilyaml.NewYAMLToJSONDecoder(file)
	out := map[string]any{}
	for {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("context: %w", err)
		}
		var document map[string]any
		err := reader.Decode(&document)
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return nil, fmt.Errorf("decode: %w", err)
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
	extracted, _, _, err := e.owner.Acquire(
		ctx,
		key,
		&cache.Policy{MaxAge: e.maxAge, Work: &e.work},
		func(_ context.Context, target cache.ProducerInput) (cache.ProducerResult, error) {
			file, err := input.FS.Open(selection.Path)
			if err != nil {
				return cache.ProducerResult{}, fmt.Errorf("open: %w", err)
			}
			defer func() { _ = file.Close() }()
			compressed, err := gzip.NewReader(file)
			if err != nil {
				return cache.ProducerResult{}, fmt.Errorf("newreader: %w", err)
			}
			defer func() { _ = compressed.Close() }()
			_, err = io.Copy( //nolint:gosec // Flux archive extraction has no uncompressed size limit.
				target,
				compressed,
			)
			if err != nil {
				return cache.ProducerResult{}, fmt.Errorf("copy: %w", err)
			}
			return cache.ProducerResult{}, nil
		},
	)
	if err != nil {
		return err
	}
	if directoryErr := ensureDirectory(builder, selection.Destination); directoryErr != nil {
		return directoryErr
	}
	err = fs.WalkDir(extracted, ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("context: %w", err)
		}
		if name == "." {
			return nil
		}
		target := path.Join(selection.Destination, name)
		if entry.IsDir() {
			return ensureDirectory(builder, target)
		}
		return copyFile(ctx, extracted, builder, operation{From: name, To: target, Strategy: strategyExtract})
	})
	if err != nil {
		return fmt.Errorf("walkdir: %w", err)
	}
	return nil
}

func (e *evaluation) produce(
	ctx context.Context,
	req *request,
	target cache.ProducerInput,
) error {
	builder := tree.NewBuilder(nil)
	for _, op := range req.Output.Copy {
		if err := e.apply(ctx, builder, op); err != nil {
			return err
		}
	}
	writer := tar.NewWriter(target)
	if err := writer.AddFS(builder); err != nil {
		return fmt.Errorf("addfs: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("close: %w", err)
	}
	return nil
}
