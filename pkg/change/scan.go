package change

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/source/sourceignore"
	"github.com/home-operations/flate/pkg/store"
)

type fileScan struct {
	policy      *sourceignore.Matcher
	entries     map[string]fileMeta
	ignoreFiles []string
}

type fileMeta struct {
	size int64
	mode fs.FileMode
}

type scanEvidence struct {
	digests    map[string]string
	candidates map[string]bool
}

func (e scanEvidence) membershipChanged() bool {
	for name := range e.candidates {
		if path.Base(name) == ".sourceignore" {
			return true
		}
	}
	return false
}

func completedDigests(inputs [2]*store.SourceArtifact) [2]map[string]string {
	var digests [2]map[string]string
	for side, input := range inputs {
		if source, ok := input.FS.(*gittree.FS); ok {
			digests[side] = source.FileDigests()
		}
	}
	if digests[0] == nil || digests[1] == nil {
		return [2]map[string]string{}
	}
	return digests
}

// Excluded directories MUST still be traversed so deeper .sourceignore re-includes survive.
func scanTree(
	ctx context.Context,
	inputs [2]*store.SourceArtifact,
	evidence scanEvidence,
	work *manifest.WorkRecord,
) (fileScan, error) {
	input, opposite := inputs[0], inputs[1]
	source, native := input.FS.(*gittree.FS)
	oppositeName, err := filepath.Rel(input.LocalPath, opposite.LocalPath)
	if err == nil {
		oppositeName = filepath.ToSlash(oppositeName)
	}
	var scanned fileScan
	switch {
	case evidence.digests != nil || evidence.candidates != nil && !evidence.membershipChanged():
		scanned, err = evidence.observeFiles(ctx, input.FS, oppositeName, work)
	default:
		scanned, err = scanFiles(ctx, input.FS, oppositeName, work)
	}
	if err != nil {
		return fileScan{}, err
	}
	if evidence.digests != nil {
		scanned.policy, err = source.SourcePolicy(ctx)
		return scanned, err
	}
	var matcher *sourceignore.Matcher
	if native {
		matcher, err = source.SourcePolicy(ctx)
	} else {
		files := scanned.ignoreFiles
		if evidence.candidates != nil {
			files = nil
		}
		matcher, err = sourceignore.NewFS(input.FS, files, "", true)
	}
	if err != nil {
		return fileScan{}, err
	}
	files := scanned.ignoreFiles
	if evidence.candidates != nil && !evidence.membershipChanged() {
		files = nil
	}
	sourceignore.Select(matcher, scanned.entries, files)
	scanned.policy = matcher
	return scanned, nil
}

func (e scanEvidence) observeFiles(
	ctx context.Context,
	input fs.FS,
	oppositeName string,
	work *manifest.WorkRecord,
) (fileScan, error) {
	names := maps.Keys(e.digests)
	if e.digests == nil || e.candidates != nil && !e.membershipChanged() {
		names = maps.Keys(e.candidates)
	}
	out := fileScan{entries: make(map[string]fileMeta), ignoreFiles: []string{}}
	directories := directoryObservations{input: input}
	for _, name := range slices.Sorted(names) {
		work.Add(&manifest.WorkCounts{FileEntries: 1})
		if err := ctx.Err(); err != nil {
			return fileScan{}, fmt.Errorf("context: %w", err)
		}
		if name == oppositeName || strings.HasPrefix(name, oppositeName+"/") {
			continue
		}
		info, err := e.fileInfo(ctx, name, &directories)
		if err != nil {
			return fileScan{}, err
		}
		if info != nil {
			out.addFile(name, info)
		}
	}
	return out, nil
}

func (e scanEvidence) fileInfo(
	ctx context.Context,
	name string,
	directories *directoryObservations,
) (fs.FileInfo, error) {
	var info fs.FileInfo
	var err error
	if e.digests != nil {
		if _, selected := e.digests[name]; !selected {
			return nil, nil //nolint:nilnil // Completed evidence records admitted absence.
		}
		info, err = fs.Stat(directories.input, name)
	} else {
		entry, lookupErr := directories.lookup(ctx, name)
		if lookupErr != nil || entry == nil {
			return nil, lookupErr
		}
		info, err = entry.Info()
	}
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil //nolint:nilnil // Both observation routes preserve ordinary absence.
	}
	if err != nil {
		return nil, fmt.Errorf("stat %s: %w", name, err)
	}
	return info, nil
}

type directoryObservations struct {
	input   fs.FS
	entries map[string][]fs.DirEntry
}

func (d *directoryObservations) lookup(ctx context.Context, name string) (fs.DirEntry, error) {
	entries, err := d.read(ctx, path.Dir(name))
	if err != nil {
		return nil, err
	}
	i, present := slices.BinarySearchFunc(entries, path.Base(name), func(entry fs.DirEntry, name string) int {
		return strings.Compare(entry.Name(), name)
	})
	if !present {
		return nil, nil //nolint:nilnil // The admitted parent omits this path.
	}
	return entries[i], nil
}

func (d *directoryObservations) read(ctx context.Context, name string) ([]fs.DirEntry, error) {
	if entries, known := d.entries[name]; known {
		return entries, nil
	}
	if name != "." {
		entry, err := d.lookup(ctx, name)
		if err != nil {
			return nil, err
		}
		if entry == nil || !entry.IsDir() {
			return nil, nil
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("context: %w", err)
	}
	entries, err := fs.ReadDir(d.input, name)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read directory %s: %w", name, err)
	}
	if d.entries == nil {
		d.entries = make(map[string][]fs.DirEntry)
	}
	d.entries[name] = entries
	return entries, nil
}

func (s *fileScan) addFile(name string, info fs.FileInfo) {
	if info.Mode().IsRegular() {
		s.entries[name] = fileMeta{size: info.Size(), mode: info.Mode()}
	}
}

func scanFiles(
	ctx context.Context,
	scoped fs.FS,
	oppositeName string,
	work *manifest.WorkRecord,
) (fileScan, error) {
	out := fileScan{entries: make(map[string]fileMeta), ignoreFiles: []string{}}
	err := fs.WalkDir(scoped, ".", func(name string, d fs.DirEntry, err error) error {
		work.Add(&manifest.WorkCounts{FileEntries: 1})
		if err != nil {
			return err
		}
		if localErr := ctx.Err(); localErr != nil {
			return fmt.Errorf("context: %w", localErr)
		}
		if name != "." && (d.Name() == ".git" || name == oppositeName) {
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if d.Name() == ".sourceignore" {
			out.ignoreFiles = append(out.ignoreFiles, name)
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return fmt.Errorf("stat %s: %w", name, err)
		}
		out.addFile(name, info)
		return nil
	})
	if err != nil {
		return fileScan{}, fmt.Errorf("walk: %w", err)
	}
	return out, nil
}
