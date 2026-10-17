package resourceset

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// providerInputs is one per-provider list of input sets carrying its
// raw (un-normalized) name. Used to dispatch on spec.inputStrategy:
// Flatten concatenates; Permute scopes by name and Cartesian-products.
type providerInputs struct {
	name   string           // raw provider name (rs.Name for inline; RSIP.Name otherwise)
	inputs []map[string]any // each set already carries its `provider` block
}

// isPermute reports whether rs selects the Permute input strategy.
func isPermute(rs *manifest.ResourceSet) bool {
	return rs.InputStrategy != nil && rs.InputStrategy.Name == fluxopv1.InputStrategyPermute
}

// buildInputSets gathers per-provider input lists then dispatches on
// spec.inputStrategy. The ResourceSet's own inline spec.inputs are
// treated as "provider 0", followed by each referenced
// ResourceSetInputProvider in upstream's sorted order (matching
// flux-operator/internal/inputs/combine.go).
//
// Flatten (default) concatenates: each input set is rendered once
// against the templates, top-level access (`inputs.foo`).
//
// Permute Cartesian-products across providers: each result map has
// the providers' input sets nested under their normalized names
// (`inputs.<provider>.foo`) plus an `id` field — an adler32 hash of
// the provider=index path matching upstream's flux-operator/internal/
// inputs/permuter.go. Per-provider input lists are scoped before the
// product so authors can disambiguate "rset.x" from "rsip.x". A 10000
// permutation cap (also matching upstream) prevents pathological
// Cartesian blowups.
//
// inputs.id is left to the provider when it's a Static RSIP; inline-
// only inputs see no synthetic id under Flatten.
func buildInputSets(rs *manifest.ResourceSet, resolve ProviderResolver) ([]map[string]any, error) {
	groups, err := collectProviderInputs(rs, resolve)
	if err != nil {
		return nil, err
	}

	if isPermute(rs) {
		return permute(groups, rs.InputStrategy.IncludeEmptyProviders)
	}
	// Default: Flatten — pre-size out to total input count across all
	// providers to avoid incremental reallocations.
	total := 0
	for _, g := range groups {
		total += len(g.inputs)
	}
	out := make([]map[string]any, 0, total)
	for _, g := range groups {
		out = append(out, g.inputs...)
	}
	return out, nil
}

// collectProviderInputs assembles the per-provider input lists: rs's
// inline inputs as provider 0 (named after the ResourceSet itself),
// then every resolved ResourceSetInputProvider in (namespace, name)
// order. Each input set is stamped with a `provider` block so
// templates can recover which CR sourced the values.
func collectProviderInputs(rs *manifest.ResourceSet, resolve ProviderResolver) ([]providerInputs, error) {
	// Pre-size: 1 for inline + one slot per inputsFrom reference (upper
	// bound; deduplicated entries may yield fewer).
	groups := make([]providerInputs, 0, 1+len(rs.InputsFrom))

	// Provider 0: the ResourceSet itself with its inline spec.inputs.
	inlineProv := providerBlock(manifest.KindResourceSet, rs.Name, rs.Namespace)
	inline := make([]map[string]any, 0, len(rs.Inputs))
	for _, in := range rs.Inputs {
		decoded := decodeInputSet(in)
		decoded["provider"] = inlineProv
		inline = append(inline, decoded)
	}
	groups = append(groups, providerInputs{name: rs.Name, inputs: inline})

	if resolve == nil || len(rs.InputsFrom) == 0 {
		return groups, nil
	}

	seen := make(map[manifest.NamedResource]struct{})
	var providers []*manifest.ResourceSetInputProvider
	for edge := range manifest.References(rs) {
		if edge.Use != "inputsFrom" {
			continue
		}
		ref := rs.InputsFrom[edge.Position]
		matches, err := resolve(ref, edge.NamedResource)
		if err != nil {
			return nil, fmt.Errorf("inputsFrom %q: %w", ref.Name, err)
		}
		for _, p := range matches {
			if p == nil {
				continue
			}
			k := p.Named()
			if _, dup := seen[k]; dup {
				continue
			}
			seen[k] = struct{}{}
			providers = append(providers, p)
		}
	}
	// Sort providers by (namespace, name) for deterministic output,
	// matching upstream's Combine routine ordering. NamedResource.Compare
	// orders by (kind, group, namespace, name); every provider here shares the
	// same group and kind so the order reduces
	// to the desired (namespace, name).
	slices.SortFunc(providers, func(a, b *manifest.ResourceSetInputProvider) int {
		return a.Named().Compare(b.Named())
	})
	for _, p := range providers {
		exported, err := p.ExportedInputs()
		if err != nil {
			return nil, fmt.Errorf("ResourceSetInputProvider %s: %w", p.Named().NamespacedName(), err)
		}
		if exported == nil && p.Type != "" && p.Type != fluxopv1.InputProviderStatic {
			slog.Warn("resourceset: dynamic input provider contributes no inputs offline",
				"resource_set", rs.Named().NamespacedName(),
				"provider", p.Named().NamespacedName(),
				"type", p.Type)
		}
		provBlock := providerBlock(manifest.KindResourceSetInputProvider, p.Name, p.Namespace)
		pInputs := make([]map[string]any, 0, len(exported))
		for _, set := range exported {
			set["provider"] = provBlock
			pInputs = append(pInputs, set)
		}
		groups = append(groups, providerInputs{name: p.Name, inputs: pInputs})
	}
	return groups, nil
}

// providerBlock builds the `provider` block stamped onto each input set
// so templates can recover which CR sourced the values.
func providerBlock(kind, name, namespace string) map[string]any {
	return map[string]any{
		"apiVersion": fluxopv1.GroupVersion.String(),
		"kind":       kind,
		"name":       name,
		"namespace":  namespace,
	}
}

func decodeInputSet(in fluxopv1.ResourceSetInput) map[string]any {
	decoded := make(map[string]any, len(in))
	for k, v := range in {
		if v == nil {
			decoded[k] = nil
			continue
		}
		var raw any
		if err := json.Unmarshal(v.Raw, &raw); err != nil {
			// Malformed entries are skipped silently — the parser
			// already accepted the document, and there's no good
			// signaling channel beyond a controller log line.
			continue
		}
		decoded[k] = raw
	}
	return decoded
}

// StoreResolver returns a store-backed resolver for one sequential reconciliation.
// References and selectors MUST remain immutable; the resolver MUST NOT be shared
// across reconciliations or goroutines.
func StoreResolver(s *store.Store) ProviderResolver {
	type compilation struct {
		selector labels.Selector
		err      error
	}
	var firstSelector *metav1.LabelSelector
	var firstCompilation compilation
	var selectors map[*metav1.LabelSelector]compilation
	return func(ref fluxopv1.InputProviderReference, id manifest.NamedResource) ([]*manifest.ResourceSetInputProvider, error) {
		if id.Name != "" {
			obj, ok := s.Get[*manifest.ResourceSetInputProvider](id)
			if !ok {
				return nil, nil
			}
			return []*manifest.ResourceSetInputProvider{obj}, nil
		}
		if ref.Selector == nil {
			return nil, nil
		}
		var out []*manifest.ResourceSetInputProvider
		var matcher labels.Selector
		for _, p := range s.ListAs[*manifest.ResourceSetInputProvider](id.Group, id.Kind) {
			if p.Namespace != id.Namespace {
				continue
			}
			if matcher == nil {
				compiled := firstCompilation
				if ref.Selector != firstSelector {
					var ok bool
					compiled, ok = selectors[ref.Selector]
					if !ok {
						compiled.selector, compiled.err = metav1.LabelSelectorAsSelector(ref.Selector)
						if firstSelector == nil {
							firstSelector, firstCompilation = ref.Selector, compiled
						} else {
							if selectors == nil {
								selectors = make(map[*metav1.LabelSelector]compilation)
							}
							selectors[ref.Selector] = compiled
						}
					}
				}
				if compiled.err != nil {
					return nil, compiled.err
				}
				matcher = compiled.selector
			}
			if matcher.Matches(labels.Set(p.Labels)) {
				out = append(out, p)
			}
		}
		return out, nil
	}
}
