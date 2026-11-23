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
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/bmatcuk/doublestar/v4"
	securejoin "github.com/cyphar/filepath-securejoin"
	"helm.sh/helm/v4/pkg/chart/v2/loader"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/yaml"

	"github.com/home-operations/flate/internal/engine/source/safepath"
	"github.com/home-operations/flate/internal/engine/source/sourceignore"
	"github.com/home-operations/flate/internal/engine/tree"
	"github.com/home-operations/flate/pkg/manifest"
)

type copyPlan struct {
	owner                      *Cache
	work                       manifest.AcquisitionWork
	maxAge                     time.Duration
	op                         *Copy
	input                      fs.FS
	identity                   compositionIdentity
	pattern, destination, root string
	glob                       bool
	matches                    []string
}

func (e *composition) apply(ctx context.Context, builder *tree.Builder, op *Copy) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("context: %w", err)
	}
	plan, err := e.prepareCopy(op)
	if err != nil {
		return err
	}
	if !plan.glob {
		return plan.applySingle(ctx, builder)
	}
	return plan.applyGlob(ctx, builder)
}

func (p *copyPlan) applySingle(ctx context.Context, builder *tree.Builder) error {
	name, op := p.matches[0], p.op
	if op.Mode == PlaceTree && excluded(name, p.root, op.Exclude) {
		return nil
	}
	info, err := fs.Stat(p.input, name)
	if err != nil {
		if op.Mode == PlaceTree {
			return fmt.Errorf("stat: %w", err)
		}
		if !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("failed to stat source '%s': %w", name, err)
		}
		if op.Optional {
			return nil
		}
		return manifest.NewError(manifest.ErrInput, err, fmt.Sprintf("source '%s' does not exist", name))
	}
	if op.Mode != PlaceTree && !info.IsDir() && excluded(name, ".", op.Exclude) {
		return nil
	}
	return p.apply(ctx, builder, name, info)
}

func (p *copyPlan) applyGlob(ctx context.Context, builder *tree.Builder) error {
	if len(p.matches) == 0 && !p.op.Optional {
		return fmt.Errorf("no files match pattern '%s' in source '%s'", p.pattern, p.identity.Alias)
	}
	selected := false
	for _, name := range p.matches {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("context: %w", err)
		}
		if name == "." || name == ".." || excluded(name, p.root, p.op.Exclude) {
			continue
		}
		selected = true
		if err := p.applyMatch(ctx, builder, name); err != nil {
			return err
		}
	}
	if !selected && !p.op.Optional {
		return fmt.Errorf("all files matching pattern '%s' in source '%s' were excluded", p.pattern, p.identity.Alias)
	}
	return nil
}

func (p *copyPlan) applyMatch(ctx context.Context, builder *tree.Builder, name string) error {
	if p.op.Mode == ExtractArchive {
		if !isTarball(name) {
			return nil
		}
		if err := p.extract(ctx, builder, name); err != nil {
			return fmt.Errorf("failed to extract tarball '%s' to '%s': %w", name, p.destination, err)
		}
		return nil
	}
	info, err := fs.Stat(p.input, name)
	if err != nil {
		return fmt.Errorf("failed to stat source '%s': %w", name, err)
	}
	return p.apply(ctx, builder, name, info)
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
	case CopyFiles, MergeYAML, ExtractArchive:
	case PlaceTree:
		name, err := resolvePlacement(input, cmp.Or(op.From, "."))
		if err != nil {
			return copyPlan{}, err
		}
		return copyPlan{op: op, input: input, root: name, matches: []string{name}}, nil
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
	if wildcard := strings.IndexAny(root, "*?[]"); wildcard >= 0 {
		prefix := root[:wildcard]
		root = "."
		if parent, _, found := strings.CutLast(prefix, "/"); found {
			root = parent
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
			return copyPlan{}, fmt.Errorf("invalid glob pattern '%s': %w", pattern, err)
		}
	}
	return copyPlan{
		owner: e.owner, work: e.work, maxAge: e.maxAge,
		op: op, input: input, identity: e.request.Inputs[op.Input].identity(),
		pattern: pattern, destination: destination, root: root, glob: glob, matches: matches,
	}, nil
}

func isTarball(name string) bool {
	lower := strings.ToLower(name)
	return strings.HasSuffix(lower, ".tar.gz") || strings.HasSuffix(lower, ".tgz")
}

func (p *copyPlan) apply(ctx context.Context, builder *tree.Builder, name string, info fs.FileInfo) error {
	if p.op.Mode == PlaceTree {
		return p.place(ctx, builder, name, info)
	}
	if p.op.Mode == ExtractArchive {
		if info.IsDir() {
			return fmt.Errorf("extract strategy is not supported for directories, got '%s'", name)
		}
		if !isTarball(name) {
			return fmt.Errorf("extract strategy requires tarball file (.tar.gz or .tgz), got '%s'", name)
		}
		return p.extract(ctx, builder, name)
	}
	target := p.copyDestination(builder, name, info)
	if info.IsDir() {
		return p.copyDirectory(ctx, builder, &Copy{From: name, To: target})
	}
	return copyFile(ctx, p.input, builder, &Copy{From: name, To: target, Mode: p.op.Mode})
}

type placementView struct{ fs.FS }

func (v placementView) Lstat(name string) (fs.FileInfo, error) {
	info, err := fs.Lstat(v.FS, cmp.Or(strings.TrimLeft(filepath.ToSlash(name), "/"), "."))
	if err != nil {
		return info, fmt.Errorf("lstat placement: %w", err)
	}
	return info, nil
}

func (v placementView) Readlink(name string) (string, error) {
	link, err := fs.ReadLink(v.FS, cmp.Or(strings.TrimLeft(filepath.ToSlash(name), "/"), "."))
	if err != nil {
		return link, fmt.Errorf("readlink placement: %w", err)
	}
	return link, nil
}

func resolvePlacement(input fs.FS, name string) (string, error) {
	resolved, err := securejoin.SecureJoinVFS("/", name, placementView{input})
	if err != nil {
		return "", fmt.Errorf("resolve placement: %w", err)
	}
	return safepath.ArchiveName(cmp.Or(strings.TrimPrefix(filepath.ToSlash(resolved), "/"), "."))
}

func (p *copyPlan) place(ctx context.Context, builder *tree.Builder, name string, info fs.FileInfo) error {
	destination, err := resolvePlacement(builder, cmp.Or(p.op.To, "."))
	if err != nil {
		return err
	}
	previous, err := fs.Stat(builder, destination)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("stat destination: %w", err)
	}
	if err == nil {
		if previous.IsDir() {
			return fmt.Errorf("destination %q: %w", destination, fs.ErrExist)
		}
		if info.IsDir() {
			return tree.ErrConflict
		}
	}
	if !info.IsDir() {
		return copyFile(ctx, p.input, builder, &Copy{From: name, To: destination})
	}
	selected, err := fs.Sub(p.input, name)
	if err != nil {
		return fmt.Errorf("select directory: %w", err)
	}
	return builder.Mount(destination, selected)
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
	key := p.extractionKey(name)
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

func (p *copyPlan) extractionKey(name string) ArchiveKey {
	input := p.identity
	input.Alias = ""
	return ArchiveKey{Policy: extractPolicy, Content: struct {
		Input compositionIdentity
		Path  string
	}{input, name}}
}

func (e *composition) produce(ctx context.Context, target ProducerInput) (ProducerResult, error) {
	builder := tree.NewBuilder(e.request.Base.FS)
	for i := range e.request.Copies {
		op := &e.request.Copies[i]
		if err := ctx.Err(); err != nil {
			return ProducerResult{}, fmt.Errorf("context: %w", err)
		}
		if err := e.apply(ctx, builder, op); err != nil {
			return ProducerResult{}, manifest.NewError(manifest.ErrInput,
				fmt.Errorf("copy %d %q to %q: %w", i, op.From, op.To, err), err.Error())
		}
	}
	var result ProducerResult
	if e.request.Capture != nil {
		captured, err := e.request.Capture(ctx, builder)
		if err != nil {
			return ProducerResult{}, err
		}
		result.Annotations = map[string]string{CompositionCapture: captured}
	}
	var input fs.FS = builder
	if e.request.Policy == fixedPolicy {
		matcher, err := sourceignore.NewFS(builder, []string{}, e.request.Ignore, true)
		if err != nil {
			return ProducerResult{}, fmt.Errorf("composition membership: %w", err)
		}
		input = compositionArchive{FS: builder, matcher: matcher}
	}
	writer := tar.NewWriter(target)
	if err := writer.AddFS(input); err != nil {
		return ProducerResult{}, fmt.Errorf("addfs: %w", err)
	}
	if err := writer.Close(); err != nil {
		return ProducerResult{}, fmt.Errorf("close: %w", err)
	}
	return result, nil
}

type compositionArchive struct {
	fs.FS

	matcher *sourceignore.Matcher
}

func (a compositionArchive) ReadDir(dir string) ([]fs.DirEntry, error) {
	entries, err := fs.ReadDir(a.FS, dir)
	if err != nil {
		return nil, fmt.Errorf("read staged directory: %w", err)
	}
	return slices.DeleteFunc(entries, func(entry fs.DirEntry) bool {
		return a.matcher.Match(path.Join(dir, entry.Name()), entry.IsDir())
	}), nil
}
