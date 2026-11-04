package kustomize

import (
	"context"
	"encoding/json"
	"fmt"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	yaml "go.yaml.in/yaml/v3"
	sigsyaml "sigs.k8s.io/yaml"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
)

// Shape applies producer metadata and effective substitutions to private decoded documents.
func Shape(_ context.Context, ks *manifest.Kustomization, docs []manifest.DocOccurrence, options ShapeOptions) error {
	ks.EmissionPolicy().Stamp(docs)
	for i := range docs {
		docs[i], _ = manifest.SanitizeDocument(docs[i])
	}
	if (ks.PostBuild == nil || len(ks.PostBuild.Substitute) == 0) &&
		ks.SubstituteStrategy() != kustomizev1.SubstituteStrategyAlways {
		return nil
	}
	var vars map[string]string
	work := options.Work
	if ks.PostBuild != nil {
		vars = manifest.VarsMap(ks.PostBuild.Substitute)
	}
	for i, occurrence := range docs {
		doc := occurrence.Document
		if occurrence.Err != nil || manifest.HasSubstituteDisabled(doc) {
			continue
		}
		substituted, err := substituteDoc(doc, vars, options.Strict, work)
		if err != nil {
			return manifest.NewError(manifest.ErrInvalidSubstituteReference, err, "")
		}
		docs[i].Document = substituted
	}
	return nil
}

// Only expansion/escape text and scalars changed by JSON decoding need a
// round trip. Both map keys and values must be inspected.
func substituteDoc(
	doc map[string]any,
	vars map[string]string,
	strict bool,
	work *manifest.WorkRecord,
) (map[string]any, error) {
	if !values.NeedsSubstitution(doc) {
		return doc, nil
	}
	work.Add(&manifest.WorkCounts{Attempts: 1})
	raw, err := yaml.Marshal(doc)
	if err != nil {
		work.Finish(err)
		return nil, fmt.Errorf("substitute: marshal doc: %w", err)
	}
	out, err := Substitute(raw, vars, strict)
	if err != nil {
		work.Finish(err)
		return nil, err
	}
	// Flux kustomize.SubstituteVariables uses varSubstitution to preserve YAML-to-JSON scalar semantics.
	data, err := sigsyaml.YAMLToJSON(out)
	if err != nil {
		work.Finish(err)
		return nil, fmt.Errorf("substitute: convert doc to JSON: %w", err)
	}
	var next map[string]any
	if err := json.Unmarshal(data, &next); err != nil {
		work.Finish(err)
		return nil, fmt.Errorf("substitute: unmarshal doc: %w", err)
	}
	work.Finish(nil)
	return next, nil
}

type renderContent struct {
	Manifest     any                            `json:"Manifest"`
	Dependencies values.DependencySubstitutions `json:"Dependencies"`
}

// Content projects validated raw documents and effective variables into immutable final inputs.
//
//nolint:gocritic // Preserve the public value receiver and its render-input snapshot.
func (r RenderRequest) Content(docs []manifest.DocOccurrence) any {
	ks := r.Spec
	ks.EmissionPolicy().Stamp(docs)
	active := ks.PostBuild != nil && len(ks.PostBuild.Substitute) > 0 ||
		ks.SubstituteStrategy() == kustomizev1.SubstituteStrategyAlways
	names := values.SubstitutionNames(docs, active)
	if names == nil {
		names = make(map[string]struct{}, len(ks.PostBuild.Substitute))
		for name := range ks.PostBuild.Substitute {
			names[name] = struct{}{}
		}
	}
	source := struct {
		Input       store.ArtifactInput `json:"Input"`
		ApplyIgnore bool                `json:"ApplyIgnore"`
		Output      string              `json:"Output"`
		Strict      bool                `json:"Strict"`
		Skip        []string            `json:"Skip"`
	}{
		r.Source.Input(store.ArtifactContent), r.ApplyIgnore, manifest.Fingerprint(docs),
		r.StrictSubstitutions, r.SkipKinds,
	}
	return renderContent{ks.Content(source, names, active), r.DependencySubstitutions}
}

// ShapeOptions supplies substitution policy and its physical work counter.
type ShapeOptions struct {
	Strict bool
	Work   *manifest.WorkRecord
}
