package artifactgenerator

import (
	"fmt"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
)

type archiveSelection struct{ Alias, Path, Destination string }

func (e *evaluation) composition(id manifest.NamedResource, req *request) (cache.Composition, error) {
	e.projectedInputs = slices.Grow(e.projectedInputs[:0], len(e.identity))[:len(e.identity)]
	e.projectedCopies = slices.Grow(e.projectedCopies[:0], len(req.Output.Copy))[:len(req.Output.Copy)]
	e.projectedContent.Namespace = "artifactgenerator/output/v1"
	e.projectedContent.Generator = id
	e.projectedContent.Request = *req
	e.projectedContent.Inputs = e.identity
	result := cache.Composition{
		Content: &e.projectedContent,
		Inputs:  e.projectedInputs,
		Copies:  e.projectedCopies,
		Policy:  "fixed",
	}
	for i := range e.identity {
		identity := &e.identity[i]
		result.Inputs[i] = cache.CompositionInput{FS: e.inputs[identity.Alias], Content: identity.Input.Content}
	}
	for i, op := range req.Output.Copy {
		selection := e.lastSelection
		if op.From != e.lastFrom || selection.alias == "" {
			var err error
			selection, err = sourcePath(op.From)
			if err != nil {
				return cache.Composition{}, err
			}
			if globErr := validateGlob(selection.pattern); globErr != nil {
				return cache.Composition{}, globErr
			}
			e.lastFrom, e.lastSelection = op.From, selection
		}
		position := e.inputIndex(selection.alias)
		if position == -1 {
			return cache.Composition{}, fmt.Errorf("unknown source alias %q", selection.alias)
		}
		to, err := destinationPath(op.To)
		if err != nil {
			return cache.Composition{}, err
		}
		lowered := cache.Copy{
			Input:   position,
			From:    selection.pattern,
			To:      to,
			Exclude: op.Exclude,
			Mode:    cache.CopyFiles,
		}
		switch op.Strategy {
		case "Merge":
			lowered.Mode = cache.MergeYAML
		case strategyExtract:
			lowered.Mode = cache.ExtractArchive
			lowered.ExtractContent = func(path, destination string) any {
				return struct {
					Namespace string
					Inputs    []inputIdentity
					Selection archiveSelection
				}{"artifactgenerator/extract/v1", e.identity, archiveSelection{selection.alias, path, destination}}
			}
		}
		result.Copies[i] = lowered
	}
	return result, nil
}

func (e *evaluation) inputIndex(alias string) int {
	for i := range e.identity {
		if e.identity[i].Alias == alias {
			return i
		}
	}
	return -1
}
