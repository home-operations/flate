package kustomize

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"

	sigsyaml "sigs.k8s.io/yaml"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
)

// Shaping owns private documents and temporary YAML for one render attempt.
type Shaping struct {
	preparation *Preparation
	spec        *manifest.Kustomization
	docs        []manifest.DocOccurrence
	inputs      []substitutionDocument
	active      bool
	content     renderContent
}

type substitutionDocument struct {
	data []byte
	err  error
}

type preparedShaping struct {
	output string
	active bool
	inputs []substitutionDocument
	names  map[string]struct{}
}

// PrepareShape MUST retain raw identity separately from sanitized application content.
func PrepareShape(request *RenderRequest, docs []manifest.DocOccurrence) *Shaping {
	ks := request.Spec
	ks.EmissionPolicy().Stamp(docs)
	var vars map[string]string
	if ks.PostBuild != nil {
		vars = ks.PostBuild.Substitute
	}
	p := &Shaping{
		spec: ks, docs: docs, preparation: request.Preparation,
		active: values.SubstitutionActive(ks),
	}
	output := manifest.Fingerprint(docs)
	var prepared *preparedShaping
	if request.Preparation != nil {
		prepared = request.Preparation.shaping
	}
	var names map[string]struct{}
	if prepared != nil && output != "" && prepared.output == output && prepared.active == p.active {
		p.inputs, names = slices.Clone(prepared.inputs), prepared.names
	} else {
		names = p.prepareInputs()
		if request.Preparation != nil && output != "" {
			request.Preparation.shaping = &preparedShaping{
				output: output,
				active: p.active,
				inputs: p.inputs,
				names:  names,
			}
			p.inputs = slices.Clone(p.inputs)
		}
	}
	if names == nil {
		names = make(map[string]struct{})
		for name := range vars {
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
		request.Source.Input(store.ArtifactContent), request.ApplyIgnore, output,
		request.StrictSubstitutions, request.SkipKinds,
	}
	p.content = renderContent{Manifest: ks.Content(source, names, p.active)}
	return p
}

func (p *Shaping) prepareInputs() map[string]struct{} {
	names := map[string]struct{}{}
	if !p.active {
		return names
	}
	p.inputs = make([]substitutionDocument, len(p.docs))
	for i := range p.docs {
		input := &p.inputs[i]
		input.data, input.err = values.SubstitutionDocument(p.docs[i], true)
		if names == nil || input.data == nil && input.err == nil {
			continue
		}
		current := values.SubstitutionNames(input.data)
		if input.err != nil || current == nil {
			names = nil
		} else {
			maps.Copy(names, current)
		}
	}
	return names
}

// Content preserves the raw identity after Apply mutates the private documents.
func (p *Shaping) Content() any { return p.content }

// Retire releases prepared YAML after application or a deduplicated attempt.
func (p *Shaping) Retire() {
	if p.preparation != nil {
		p.preparation.shaping = nil
	}
	p.inputs, p.docs, p.spec, p.preparation = nil, nil, nil, nil
}

// Apply consumes the attempt's prepared YAML and releases it on every return.
func (p *Shaping) Apply(_ context.Context, options ShapeOptions) error {
	defer p.Retire()
	for i := range p.docs {
		var changed bool
		p.docs[i], changed = manifest.SanitizeDocument(p.docs[i])
		if p.active && changed {
			p.inputs[i].data, p.inputs[i].err = values.SubstitutionDocument(p.docs[i], true)
		}
	}
	if !p.active {
		return nil
	}
	var vars map[string]string
	if p.spec.PostBuild != nil {
		vars = manifest.VarsMap(p.spec.PostBuild.Substitute)
	}
	for i := range p.docs {
		input := &p.inputs[i]
		substituted, err := input.apply(p.docs[i].Document, vars, options.Strict, options.Work)
		if err != nil {
			return manifest.NewError(manifest.ErrInvalidSubstituteReference, err, "")
		}
		p.docs[i].Document = substituted
	}
	return nil
}

func (p *substitutionDocument) apply(
	doc map[string]any,
	vars map[string]string,
	strict bool,
	work *manifest.WorkRecord,
) (map[string]any, error) {
	if p.data == nil && p.err == nil {
		return doc, nil
	}
	work.Add(&manifest.WorkCounts{Attempts: 1})
	if p.err != nil {
		work.Finish(p.err)
		return nil, fmt.Errorf("substitute: marshal doc: %w", p.err)
	}
	out, err := Substitute(p.data, vars, strict)
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
	Manifest any `json:"Manifest"`
}

// ShapeOptions supplies substitution policy and its physical work counter.
type ShapeOptions struct {
	Strict bool
	Work   *manifest.WorkRecord
}
