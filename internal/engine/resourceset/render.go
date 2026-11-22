// Package resourceset renders flux-operator ResourceSet CRs offline.
//
// A ResourceSet is a templating CRD that emits a fixed set of Kubernetes
// resources across a matrix of input values. flate evaluates each
// spec.resources / spec.resourcesTemplate entry once per input set using
// slim-sprig with the `<<  >>` delimiter pair (so ResourceSet templates
// can sit inside Helm charts without delimiter conflicts).
//
// Supported:
//   - spec.inputs (in-YAML inline inputs)
//   - spec.inputsFrom with captured provider inputs evaluated before rendering.
//   - spec.inputStrategy: Flatten (default) AND Permute. Permute
//     Cartesian-products inputs across providers and scopes each
//     under its normalized name; templates access values via
//     `inputs.<provider>.foo`. ID generation matches upstream's
//     adler32 scheme so renders are byte-equivalent to flux-operator
//     output. Capped at 10000 permutations.
//   - spec.steps (ordered builds with cross-step duplicate rejection)
//   - spec.resources (typed JSON template list)
//   - spec.resourcesTemplate (multi-document YAML string)
//   - Deduplication by (API group, kind, namespace, name)
//
// Deferred:
//   - fluxcd.controlplane.io/copyFrom / convertKubeConfigFrom / checksumFrom
//     annotations — those need live cluster data flate does not have.
package resourceset

import (
	"errors"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"

	"github.com/home-operations/flate/internal/core/resourcesetinput"
	"github.com/home-operations/flate/pkg/manifest"
)

// ProviderResolver returns the ResourceSetInputProviders matching a
// single spec.inputsFrom reference at the canonical identity. Callers
// implement this against their object store (orchestrator) or pass nil
// to disable inputsFrom resolution (tests, simple cases).
type ProviderResolver func(ref fluxopv1.InputProviderReference, id manifest.NamedResource) ([]*manifest.ResourceSetInputProvider, error)

// Render evaluates the ResourceSet's resources or ordered steps across its
// input matrix and returns deduplicated, unstamped decoded documents.
// Controller metadata belongs to the shared emission operation.
// Referenced providers with no combined inputs produce no resources.
// admit receives borrowed canonical content before matrix/template work; true skips rendering.
// providers contains evaluated input records in canonical order.
func Render(
	rs *manifest.ResourceSet,
	providers []ProviderInputs,
	admit func(any) (bool, error),
	work *manifest.WorkRecord,
) ([]manifest.DocOccurrence, []manifest.Warning, error) {
	if rs == nil {
		return nil, nil, nil
	}
	if err := resourcesetinput.ValidateSteps(&rs.ResourceSetSpec); err != nil {
		return nil, nil, manifest.NewError(manifest.ErrInput, err, "")
	}

	var warnings []manifest.Warning
	for _, provider := range providers {
		if provider.State == ProviderInputUnsupported {
			warnings = append(warnings, manifest.Warning{
				Resource: rs.Named(),
				Category: manifest.WarnOfflineInputProvider,
				Message:  "dynamic input provider contributes no inputs offline",
				Detail:   []string{provider.ID.String(), provider.Type},
			})
		}
	}
	if admit != nil {
		handled, err := admit(struct {
			Name      string
			Namespace string
			Spec      *fluxopv1.ResourceSetSpec
			Providers []ProviderInputs
		}{rs.Name, rs.Namespace, &rs.ResourceSetSpec, providers})
		if handled || err != nil {
			return nil, warnings, err
		}
	}
	work.Add(&manifest.WorkCounts{Attempts: 1})
	inputs, err := buildInputSets(rs, providers)
	if err != nil {
		work.Finish(err)
		return nil, warnings, err
	}
	if len(rs.InputsFrom) > 0 && len(inputs) == 0 {
		work.Finish(nil)
		return nil, warnings, nil
	}

	documents, err := resourcesetinput.Build(&rs.ResourceSetSpec, inputs, false)
	if errors.Is(err, resourcesetinput.ErrDocument) {
		err = manifest.NewError(manifest.ErrInput, err, "")
	}
	work.Finish(err)
	if err != nil {
		return nil, warnings, err
	}
	var docs []manifest.DocOccurrence
	if len(documents) != 0 {
		docs = make([]manifest.DocOccurrence, len(documents))
		for i, document := range documents {
			docs[i] = manifest.DocOccurrence{Document: document, Ordinal: i}
		}
	}
	return docs, warnings, nil
}
