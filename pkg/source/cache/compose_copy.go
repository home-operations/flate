package cache

import (
	"archive/tar"
	"cmp"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"strings"
	"time"

	"github.com/bmatcuk/doublestar/v4"
	"helm.sh/helm/v4/pkg/chart/v2/loader"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/yaml"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/safepath"
	"github.com/home-operations/flate/pkg/tree"
)

func (e *composition) apply(ctx context.Context, builder *tree.Builder, op *Copy) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("context: %w", err)
	}
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
	owner                      *Cache
	work                       manifest.AcquisitionWork
	maxAge                     time.Duration
	op                         *Copy
	input                      fs.FS
	pattern, destination, root string
	glob                       bool
	matches                    []string
}

func (e *composition) prepareCopy(op *Copy) (copyPlan, error) {
	if op.Input < 0 || op.Input >= len(e.request.Inputs) {
		return copyPlan{}, fmt.Errorf("invalid input index %d", op.Input)
	}
	input := e.request.Inputs[op.Input].FS
	if input == nil {
		return copyPlan{}, fmt.Errorf("missing input filesystem %d", op.Input)
	}
	switch op.Mode {
	case CopyFiles, MergeYAML:
	case ExtractArchive:
		if op.ExtractContent == nil {
			return copyPlan{}, fmt.Errorf("extraction requires content identity")
		}
	default:
		return copyPlan{}, fmt.Errorf("unsupported copy mode %d", op.Mode)
	}
	pattern := cmp.Or(op.From, ".")
	if _, err := safepath.ArchiveName(pattern); err != nil {
		return copyPlan{}, err
	}
	destination, err := safepath.ArchiveName(op.To)
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
		if op.Mode == ExtractArchive {
			matches, err = doublestar.Glob(input, pattern, doublestar.WithFailOnIOErrors())
		} else {
			matches, err = fs.Glob(input, pattern)
		}
		if err != nil {
			return copyPlan{}, fmt.Errorf("glob: %w", err)
		}
	}
	return copyPlan{
		owner: e.owner, work: e.work, maxAge: e.maxAge,
		op: op, input: input, pattern: pattern, destination: destination,
		root: root, glob: glob, matches: matches,
	}, nil
}

func (p *copyPlan) apply(ctx context.Context, builder *tree.Builder, name string) error {
	info, err := fs.Stat(p.input, name)
	if err != nil {
		return fmt.Errorf("stat: %w", err)
	}
	if p.op.Mode == ExtractArchive {
		lower := strings.ToLower(name)
		if info.IsDir() || !strings.HasSuffix(lower, ".tar.gz") && !strings.HasSuffix(lower, ".tgz") {
			if p.glob {
				return nil
			}
			return fmt.Errorf("extract requires a tarball, got %q", name)
		}
		return p.extract(ctx, builder, name)
	}
	target := p.copyDestination(builder, name, info)
	if info.IsDir() {
		return p.copyDirectory(ctx, builder, &Copy{From: name, To: target})
	}
	return copyFile(ctx, p.input, builder, &Copy{From: name, To: target, Mode: p.op.Mode})
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

func (p *copyPlan) copyDirectory(ctx context.Context, builder *tree.Builder, selection *Copy) error {
	err := fs.WalkDir(p.input, selection.From, func(current string, entry fs.DirEntry, err error) error {
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
		return copyFile(ctx, p.input, builder, &Copy{From: current, To: to})
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

func copyFile(ctx context.Context, input fs.FS, builder *tree.Builder, op *Copy) error {
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
	if previous, err := fs.Stat(builder, op.To); op.Mode == ExtractArchive && err == nil {
		mode = previous.Mode()
	} else if op.Mode == MergeYAML && err == nil {
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

func (p *copyPlan) extract(ctx context.Context, builder *tree.Builder, name string) error {
	input := p.input
	work := p.work
	key := ArchiveKey{Policy: extractPolicy, Content: p.op.ExtractContent(name, p.destination)}
	extracted, _, _, err := p.owner.Acquire(
		ctx,
		key,
		&Policy{MaxAge: p.maxAge, Work: &work},
		func(_ context.Context, target ProducerInput) (ProducerResult, error) {
			file, err := input.Open(name)
			if err != nil {
				return ProducerResult{}, fmt.Errorf("open: %w", err)
			}
			defer func() { _ = file.Close() }()
			compressed, err := gzip.NewReader(file)
			if err != nil {
				return ProducerResult{}, fmt.Errorf("newreader: %w", err)
			}
			defer func() { _ = compressed.Close() }()
			_, err = io.Copy( //nolint:gosec // Flux archive extraction has no uncompressed size limit.
				target,
				compressed,
			)
			if err != nil {
				return ProducerResult{}, fmt.Errorf("copy: %w", err)
			}
			return ProducerResult{}, nil
		},
	)
	if err != nil {
		return err
	}
	if directoryErr := ensureDirectory(builder, p.destination); directoryErr != nil {
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
		target := path.Join(p.destination, name)
		if entry.IsDir() {
			return ensureDirectory(builder, target)
		}
		return copyFile(ctx, extracted, builder, &Copy{From: name, To: target, Mode: ExtractArchive})
	})
	if err != nil {
		return fmt.Errorf("walkdir: %w", err)
	}
	return nil
}

func (e *composition) produce(ctx context.Context, target ProducerInput) error {
	builder := tree.NewBuilder(nil)
	for i := range e.request.Copies {
		op := &e.request.Copies[i]
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("context: %w", err)
		}
		if err := e.apply(ctx, builder, op); err != nil {
			return manifest.NewError(manifest.ErrInput,
				fmt.Errorf("copy %d %q to %q: %w", i, op.From, op.To, err), err.Error())
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
