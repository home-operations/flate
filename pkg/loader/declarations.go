package loader

import (
	"context"
	"fmt"
	"io"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// LoadDocuments prepares required invocation declarations without exposing payloads in errors.
func LoadDocuments(ctx context.Context, r io.Reader, path string) ([]store.Declaration, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("supply %q: %w", path, err)
	}
	var children []store.Declaration
	var failure error
	err := (&documentReader{compact: true}).prepareStream(
		r,
		manifest.ParseDocOptions{RequiredIdentity: true},
		func(object parsedObject) {
			if failure != nil {
				return
			}
			if object.outcome.Err != nil || object.Obj == nil {
				failure = manifest.NewError(manifest.ErrInput,
					nil,
					fmt.Sprintf("supply %q document %d: invalid Kubernetes object",
						path,
						object.outcome.Ordinal+1))
				return
			}
			children = append(children,
				store.Declaration{
					Obj:       object.Obj,
					Doc:       object.Doc,
					Origin:    manifest.SuppliedDeclaration,
					InputPath: path,
					Ordinal:   object.outcome.Ordinal,
					Warnings:  object.Warnings,
				})
		},
	)
	if err != nil {
		return nil, fmt.Errorf("supply %q: %w", path, err)
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("supply %q: %w", path, err)
	}
	if failure != nil {
		return nil, failure
	}
	return children, nil
}

// ValidateDeclarations requires authored identity against all invocation definitions.
func ValidateDeclarations(children []store.Declaration) error {
	winners := make(map[manifest.NamedResource]int, len(children))
	for i := range children {
		winners[children[i].Obj.Named()] = i
	}
	documents := make([]map[string]any, 0, len(winners))
	for i := range children {
		if winners[children[i].Obj.Named()] == i {
			documents = append(documents, children[i].Doc)
		}
	}
	for i := range children {
		child := &children[i]
		if winners[child.Obj.Named()] != i {
			continue
		}
		if err := manifest.ValidateIdentity(child.Doc, documents); err != nil {
			return manifest.NewError(manifest.ErrInput,
				nil,
				fmt.Sprintf("supply %q document %d: invalid Kubernetes object",
					child.InputPath,
					child.Ordinal+1))
		}
	}
	return nil
}
