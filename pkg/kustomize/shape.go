package kustomize

import (
	"encoding/json"
	"fmt"

	yaml "go.yaml.in/yaml/v3"
	sigsyaml "sigs.k8s.io/yaml"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
)

// Shape applies producer metadata and effective substitutions to private decoded documents.
func Shape(ks *manifest.Kustomization, docs []manifest.DocOccurrence, strict bool) error {
	ks.EmissionPolicy().Stamp(docs)
	if (ks.PostBuild == nil || len(ks.PostBuild.Substitute) == 0) &&
		ks.SubstituteStrategy() != kustomizev1.SubstituteStrategyAlways {
		return nil
	}
	var vars map[string]string
	if ks.PostBuild != nil {
		vars = manifest.VarsMap(ks.PostBuild.Substitute)
	}
	for i, occurrence := range docs {
		doc := occurrence.Document
		if occurrence.Err != nil || manifest.HasSubstituteDisabled(doc) {
			continue
		}
		substituted, err := substituteDoc(doc, vars, strict)
		if err != nil {
			return manifest.NewError(manifest.ErrInvalidSubstituteReference, err, "")
		}
		docs[i].Document = substituted
	}
	return nil
}

// Only expansion/escape text and scalars changed by JSON decoding need a
// round trip. Both map keys and values must be inspected.
func substituteDoc(doc map[string]any, vars map[string]string, strict bool) (map[string]any, error) {
	if !values.NeedsSubstitution(doc) {
		return doc, nil
	}
	raw, err := yaml.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("substitute: marshal doc: %w", err)
	}
	out, err := Substitute(raw, vars, strict)
	if err != nil {
		return nil, err
	}
	// Flux kustomize.SubstituteVariables uses varSubstitution to preserve YAML-to-JSON scalar semantics.
	data, err := sigsyaml.YAMLToJSON(out)
	if err != nil {
		return nil, fmt.Errorf("substitute: convert doc to JSON: %w", err)
	}
	var next map[string]any
	if err := json.Unmarshal(data, &next); err != nil {
		return nil, fmt.Errorf("substitute: unmarshal doc: %w", err)
	}
	return next, nil
}

type renderContent struct {
	Manifest     any                            `json:"Manifest"`
	Dependencies values.DependencySubstitutions `json:"Dependencies"`
}

// Content projects validated raw documents and effective variables into immutable final inputs.
//

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
		Wipe        bool                `json:"Wipe"`
		Strict      bool                `json:"Strict"`
		Skip        []string            `json:"Skip"`
	}{
		r.Source.Input(store.ArtifactContent), r.ApplyIgnore, manifest.Fingerprint(docs),
		r.WipeSecrets, r.StrictSubstitutions, r.SkipKinds,
	}
	return renderContent{ks.Content(source, names, active), r.DependencySubstitutions}
}
