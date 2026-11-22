package artifactgenerator

import (
	"fmt"
	"slices"

	"github.com/home-operations/flate/internal/engine/source/cache"
)

func (e *evaluation) composition(req *request) (cache.Composition, error) {
	e.projectedInputs = slices.Grow(e.projectedInputs[:0], len(e.identity))[:len(e.identity)]
	e.projectedCopies = slices.Grow(e.projectedCopies[:0], len(req.Output.Copy))[:len(req.Output.Copy)]
	result := cache.Composition{
		Inputs: e.projectedInputs,
		Copies: e.projectedCopies,
		Policy: "fixed",
	}
	for i := range e.identity {
		identity := &e.identity[i]
		result.Inputs[i] = cache.CompositionInput{
			FS:      e.inputs[identity.Alias],
			Content: identity.Input.Content,
			Backing: identity.Input.LocalPath,
		}
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
