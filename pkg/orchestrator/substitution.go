package orchestrator

import (
	"fmt"
	"maps"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
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
	var err error
	cfg.supplied, err = store.NewSupplied(cfg.Supply)
	cfg.Supply = nil
	return err
}
