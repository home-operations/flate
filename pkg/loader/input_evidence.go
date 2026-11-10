package loader

import (
	"context"
	"io/fs"
	"maps"
	"path"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/gittree"
)

type inputEvidence struct {
	projection *inputPayload
	digest     string
	coverage   manifest.DocumentRole
}

func (s *inputCache) certify(ctx context.Context, frame *inputFrame) error {
	requested := slices.Collect(maps.Keys(frame.Digests))
	for _, file := range frame.Files {
		requested = append(requested, file.Name)
	}
	//nolint:errcheck // Only native source owners create input cache state.
	current, err := s.input.FS.(*gittree.FS).FreshInputs(ctx, requested, nil)
	if err != nil {
		return err
	}
	changed := frame.changes(current.Files, current.Types)
	for loadIndex := range frame.Loads {
		load := &frame.Loads[loadIndex]
		if !load.matches(changed) {
			return manifest.NewError(manifest.ErrInput, nil, "discovery inputs changed during construction")
		}
	}
	for walkIndex := range frame.Walks {
		walk := &frame.Walks[walkIndex]
		if !walk.matches(changed) {
			return manifest.NewError(manifest.ErrInput, nil, "discovery walk inputs changed during construction")
		}
	}
	for _, file := range frame.Files {
		mode, observed := current.Types[file.Name]
		if current.Files[file.Name] != file.Digest || !observed || !mode.IsRegular() {
			return manifest.NewError(manifest.ErrInput, nil, "discovery inputs changed during construction")
		}
	}
	return nil
}

type inputChanges struct {
	paths       map[string]bool
	directories map[string]bool
}

func (f *inputFrame) changes(files map[string]string, nodeTypes map[string]fs.FileMode) inputChanges {
	changed := map[string]bool{}
	for name, digest := range f.Digests {
		if files[name] != digest {
			changed[name] = true
		}
	}
	for name, digest := range files {
		if f.Digests[name] != digest {
			changed[name] = true
		}
	}
	for name, mode := range f.Types {
		if current, present := nodeTypes[name]; !present || current != mode {
			changed[name] = true
		}
	}
	for name, mode := range nodeTypes {
		if previous, present := f.Types[name]; !present || previous != mode {
			changed[name] = true
		}
	}
	directories := map[string]bool{}
	for name := range changed {
		directories[path.Dir(name)] = true
	}
	return inputChanges{paths: changed, directories: directories}
}

func (e *inputEvidence) matches(doc *document, digest string) bool {
	return e.digest == digest && covers(e.coverage, doc.coverage) && e.matchesProjection(doc.projection)
}

func (e *inputEvidence) matchesProjection(projection *projectionInputs) bool {
	if projection == nil {
		return e.projection == nil
	}
	return e.projection == projection.payload
}
