package loader

import (
	"io/fs"
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/internal/engine/kustomize/kustomizeinput"
	"github.com/home-operations/flate/internal/engine/values"
	"github.com/home-operations/flate/pkg/manifest"
)

func (l *Loader) qualifyOrderingOccurrences() {
	var checked map[int]bool
	for id, occurrence := range l.locations { //nolint:gocritic // Publish a copy of the occurrence with its qualified proof.
		if !occurrence.requiresBuild {
			continue
		}
		if !slices.ContainsFunc(
			occurrence.facts.relations(),
			func(ref manifest.Reference) bool { return ref.Ordering },
		) {
			continue
		}
		if checked == nil {
			checked = make(map[int]bool)
		}
		occurrence.orderingKnown = l.indexedOrderingKnown(&occurrence, checked)
		l.locations[id] = occurrence
	}
}

func (l *Loader) indexedOrderingKnown(occurrence *authoredOccurrence, checked map[int]bool) bool {
	file := filepath.Join(occurrence.Declaration.Source, filepath.FromSlash(occurrence.Declaration.File))
	matched := false
	for _, ref := range l.scopes.visits[file] {
		scope := &l.scopes.contributions[ref.contribution]
		if !scope.appliesToResources() {
			continue
		}
		allowed, known := checked[ref.contribution]
		if !known {
			allowed = l.scopePreservesOrdering(scope)
			checked[ref.contribution] = allowed
		}
		if !allowed {
			return false
		}
		matched = true
	}
	return matched
}

func (l *Loader) scopePreservesOrdering(scope *scopeContribution) bool {
	if scope.owner != (manifest.NamedResource{}) && !scope.owner.ReportHidden() {
		owner, _ := l.CatalogObject(scope.owner).(*manifest.Kustomization)
		if owner == nil || len(owner.Patches) != 0 || owner.PostBuild != nil {
			return false
		}
	}
	return scope.recipe&scopeOrderingConfiguration != 0 &&
		scope.recipe&(scopeUnknownRecipe|scopeOrderingOpaque) == 0
}

type scopeRecipe uint8

const (
	scopeUnknownRecipe scopeRecipe = 1 << iota
	scopeTransformingRecipe
	scopeOrderingOpaque
	scopeOrderingConfiguration
)

func (l *Loader) visitRecipe(visit *graphVisit) scopeRecipe {
	if visit.document == nil && visit.incomplete == "" {
		return 0
	}
	resource := visit.role == kustomizeinput.Resource || visit.role == kustomizeinput.Component
	if visit.incomplete != "" {
		if resource {
			return scopeUnknownRecipe
		}
		return scopeOrderingOpaque
	}
	dir := filepath.Join(visit.root, visit.dir)
	builder := l.documents.read(dir)
	if builder.body == nil {
		return scopeUnknownRecipe
	}
	var facts scopeRecipe
	if resource && builder.recipe&(recipeResourceOnly|recipeDataGeneratorsOnly) == 0 {
		facts |= scopeTransformingRecipe
	}
	switch {
	case builder.recipe&(recipeOpaquePluginInputs|recipeTransformers|recipeGenerators) != 0:
		facts |= scopeOrderingOpaque
	case builder.recipe&recipeConfigurations != 0:
		configuration := l.documents.classifyOrderingConfigurations(dir, visit, builder.body.Configurations)
		if configuration == orderingConfigurationSafe {
			facts |= scopeOrderingConfiguration
		} else {
			facts |= scopeOrderingOpaque
		}
	}
	return facts
}

type orderingConfiguration uint8

const (
	orderingConfigurationUnknown orderingConfiguration = iota
	orderingConfigurationOpaque
	orderingConfigurationSafe
)

func (r *documentReader) classifyOrderingConfigurations(
	dir string, visit *graphVisit, configurations []string,
) orderingConfiguration {
	doc := r.cache[dir]
	if doc.orderingConfig != orderingConfigurationUnknown {
		return doc.orderingConfig
	}
	doc.orderingConfig = orderingConfigurationSafe
	for _, configuration := range configurations {
		name, local := kustomizeinput.Local(visit.root, visit.dir, configuration)
		if !local {
			doc.orderingConfig = orderingConfigurationOpaque
			break
		}
		input := r.input(filepath.Join(visit.root, filepath.FromSlash(name)))
		data, err := fs.ReadFile(input.fs, input.name)
		if err != nil || !kustomizeinput.ConfigurationPreservesOrdering(data) {
			doc.orderingConfig = orderingConfigurationOpaque
			break
		}
	}
	r.cache[dir] = doc
	return doc.orderingConfig
}

func (l *Loader) indexTransformingProducers() {
	l.transformingProducers = nil
	if l.Options.Changes == nil || l.scopes == nil {
		return
	}
	for i := range l.scopes.contributions {
		scope := &l.scopes.contributions[i]
		if !scope.appliesToResources() || scope.owner == (manifest.NamedResource{}) {
			continue
		}
		owner, _ := l.CatalogObject(scope.owner).(*manifest.Kustomization)
		if owner == nil || owner.Suspended() {
			continue
		}
		if hasFluxBuildInputs(owner) || l.scopeCanTransform(scope, owner) {
			l.transformingProducers = append(l.transformingProducers, scope.owner)
		}
	}
	slices.SortFunc(l.transformingProducers, manifest.NamedResource.Compare)
	l.transformingProducers = slices.Compact(l.transformingProducers)
}

// These Flux spec fields feed generateManifest through RenderDocument.
func hasFluxBuildInputs(owner *manifest.Kustomization) bool {
	return owner.TargetNamespace != "" || owner.NamePrefix != "" || owner.NameSuffix != "" ||
		len(owner.Patches)+len(owner.Components)+len(owner.Images)+len(owner.BuildMetadata) != 0
}

func (l *Loader) scopeCanTransform(scope *scopeContribution, owner *manifest.Kustomization) bool {
	if scope.recipe&(scopeUnknownRecipe|scopeTransformingRecipe) != 0 {
		return true
	}
	substitution := owner.PostBuild != nil &&
		(len(owner.PostBuild.SubstituteFrom) != 0 || values.ProjectionSubstitutionActive(owner))
	if !substitution {
		return false
	}
	for i := range scope.visits {
		visit := &scope.visits[i]
		if (visit.role == kustomizeinput.Resource || visit.role == kustomizeinput.Component) &&
			l.substitutesIdentity(visit) {
			return true
		}
	}
	return false
}

func (l *Loader) substitutesIdentity(visit *graphVisit) bool {
	visitSubstituted := manifest.HasEnvsubstReference(visit.namespace) ||
		manifest.HasEnvsubstReference(visit.fallback) ||
		manifest.HasEnvsubstReference(visit.namePrefix) ||
		manifest.HasEnvsubstReference(visit.nameSuffix)
	objects := l.documents.cache[visit.file].objects
	for i := range objects {
		child := &objects[i]
		if child.outcome.Err != nil {
			continue
		}
		var id manifest.NamedResource
		var namespaced bool
		switch {
		case child.fact != nil:
			if child.fact.facts.substituteDisabled {
				continue
			}
			id, namespaced = child.fact.Authored, child.fact.facts.namespaced
		case child.Obj != nil && !child.substituteDisabled:
			id, namespaced = child.Obj.Named(), manifest.KindOf(child.Obj).Namespaced()
		default:
			continue
		}
		if !visitSubstituted && !identitySubstituted(id) {
			continue
		}
		projected, err := l.projectIdentity(id, visit, child.version, namespaced)
		if err != nil || identitySubstituted(projected) {
			return true
		}
	}
	return false
}
