package kustomize

import (
	"fmt"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/engine/task"
	"github.com/home-operations/flate/internal/engine/values"
	"github.com/home-operations/flate/pkg/manifest"
)

// Prepare clones ks and resolves referenced and inline substitutions.
func Prepare(
	ks *manifest.Kustomization,
	inputs []values.Result,
) (*manifest.Kustomization, error) {
	ks = ks.Clone()
	if err := values.ExpandPostBuildSubstituteReference(ks, inputs); err != nil {
		return nil, err
	}
	return ks, nil
}

// RenderRequestOptions uses SourceReads only when the caller has no selected Source.
type RenderRequestOptions struct {
	Source              *store.SourceArtifact
	SourceReads         sourceReader
	HeldSlot            *task.Service
	SkipKinds           []string
	StrictSubstitutions bool
	TrackUnreadable     bool
}

// NewRenderRequest MUST retain unreadable names from the authored spec before expanding substitutions.
func NewRenderRequest(
	ks *manifest.Kustomization,
	inputs []values.Result,
	options *RenderRequestOptions,
) (RenderRequest, error) {
	var unreadable map[string]manifest.Reference
	if options.TrackUnreadable {
		unreadable = values.UnreadableSubstitutions(ks, inputs)
	}
	if ks.PostBuild != nil {
		prepared, err := Prepare(ks, inputs)
		if err != nil {
			return RenderRequest{}, err
		}
		ks = prepared
	}
	source := options.Source
	if source == nil {
		var err error
		source, err = SourceInput(options.SourceReads, ks)
		if err != nil {
			return RenderRequest{}, err
		}
	}
	return RenderRequest{
		Source:                      source,
		SubPath:                     ks.Path,
		Spec:                        ks,
		HeldSlot:                    options.HeldSlot,
		SkipKinds:                   options.SkipKinds,
		StrictSubstitutions:         options.StrictSubstitutions,
		UncertainSubstitutionInputs: values.UncertainSubstitutionInputs(inputs),
		UnreadableSubstitutions:     unreadable,
	}, nil
}

type sourceReader interface {
	GetObject(manifest.NamedResource) manifest.BaseManifest
	GetArtifact(manifest.NamedResource, store.ArtifactRead) store.Artifact
}

// SourceInput permits an omitted source only when a path identifies the local tree.
func SourceInput(reads sourceReader, ks *manifest.Kustomization) (*store.SourceArtifact, error) {
	if (ks.SourceRef.Kind == "" || ks.SourceRef.Name == "") && ks.Path == "" {
		return nil, manifest.NewError(manifest.ErrInput, nil, "has no path and no source")
	}
	srcID := ks.EffectiveSourceID()
	art := reads.GetArtifact(srcID, store.ArtifactContent)
	if sa, ok := art.(*store.SourceArtifact); ok {
		return sa, nil
	}
	return nil, manifest.NewError(
		manifest.ErrFlux,
		nil,
		fmt.Sprintf(
			"unsupported source artifact type %T for %s",
			art,
			manifest.DiagnosticFor(srcID, reads.GetObject(srcID)),
		),
	)
}
