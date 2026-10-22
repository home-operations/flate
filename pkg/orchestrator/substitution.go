package orchestrator

import (
	"cmp"
	"fmt"
	"maps"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/values"
)

func snapshotSubstitutions(cfg *Config) error {
	if len(cfg.Substitute) > 0 {
		for _, name := range slices.Sorted(maps.Keys(cfg.Substitute)) {
			if !values.ValidSubstitutionName(name) {
				return fmt.Errorf("%w: substitute variable name %q invalid", manifest.ErrInput, name)
			}
		}
		cfg.Substitute = maps.Clone(cfg.Substitute)
	} else {
		cfg.Substitute = nil
	}
	cfg.SubstituteFrom = slices.Clone(cfg.SubstituteFrom)
	for i, source := range cfg.SubstituteFrom {
		data, ok := source.Object.(manifest.Data)
		if !ok || source.Object == (*manifest.ConfigMap)(nil) || source.Object == (*manifest.Secret)(nil) {
			return fmt.Errorf("%w: substituteFrom input %d requires a ConfigMap or Secret with name and namespace", manifest.ErrInput, i+1)
		}
		doc := data.Snapshot()
		obj, _, _, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{WipeSecrets: cfg.WipeSecrets, DefaultNamespace: ""})
		if err != nil || obj.Named().Namespace == "" {
			return fmt.Errorf("%w: substituteFrom input %d requires a ConfigMap or Secret with name and namespace", manifest.ErrInput, i+1)
		}
		cfg.SubstituteFrom[i].Object = obj
	}
	return nil
}

// freezeSubstitutions runs once from Bootstrap before Run because the producer index gains render-time writers.
func (o *Orchestrator) freezeSubstitutions() {
	if len(o.cfg.SubstituteFrom) == 0 {
		return
	}
	o.substitutionSources = nil
	inputs := make(map[manifest.NamedResource]SubstitutionSource, len(o.cfg.SubstituteFrom))
	for _, source := range o.cfg.SubstituteFrom {
		inputs[source.Object.Named()] = source
	}
	ids := slices.Collect(maps.Keys(inputs))
	slices.SortFunc(ids, manifest.NamedResource.Compare)
	for _, id := range ids {
		source := inputs[id]
		record, indexed := o.index.Lookup(id)
		indexed = indexed && record.Path != ""
		produced := false
		for range o.store.Productions(id, o.index) {
			produced = true
			break
		}
		if o.store.GetObject(id) != nil || indexed || produced {
			o.store.AddWarning(id, manifest.Warning{Resource: id, Category: manifest.WarnSubstitutionPrecedence,
				Message: "repository substitution source takes precedence", Detail: []string{cmp.Or(source.Path, "SDK input")}})
			continue
		}
		if o.substitutionSources == nil {
			o.substitutionSources = make(map[manifest.NamedResource]manifest.BaseManifest, len(inputs))
		}
		o.substitutionSources[id] = source.Object
	}
}
