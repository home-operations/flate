package orchestrator

import (
	"cmp"
	"fmt"
	"log/slog"
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
		var obj manifest.BaseManifest
		switch input := source.Object.(type) {
		case *manifest.ConfigMap:
			if input != nil {
				owned := *input
				owned.Data = copySubstitutionData(input.Data)
				owned.BinaryData = copySubstitutionData(input.BinaryData)
				obj = &owned
			}
		case *manifest.Secret:
			if input != nil {
				owned := *input
				owned.Data = copySubstitutionData(input.Data)
				owned.StringData = copySubstitutionData(input.StringData)
				obj = &owned
			}
		}
		if obj == nil || obj.Named().Namespace == "" || obj.Named().Name == "" {
			return fmt.Errorf("%w: substituteFrom input %d requires a ConfigMap or Secret with name and namespace",
				manifest.ErrInput, i+1)
		}
		cfg.SubstituteFrom[i].Object = obj
	}
	return nil
}

func copySubstitutionData(data map[string]any) map[string]any {
	owned := manifest.DeepCopyMap(data)
	for name, value := range owned {
		if bytes, ok := value.([]byte); ok {
			owned[name] = slices.Clone(bytes)
		}
	}
	return owned
}

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
		_, indexed := o.existence.Get(id)
		_, produced := o.producers.Producer(id)
		if o.store.GetObject(id) != nil || indexed || produced || len(o.selfProduce.ProducedBy(id)) > 0 {
			slog.Warn("orchestrator: repository substitution source takes precedence",
				"file", cmp.Or(source.Path, "SDK input"), "id", id.String())
			continue
		}
		if o.substitutionSources == nil {
			o.substitutionSources = make(map[manifest.NamedResource]manifest.BaseManifest, len(inputs))
		}
		o.substitutionSources[id] = source.Object
	}
}
