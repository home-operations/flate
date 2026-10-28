package orchestrator

import (
	"slices"

	"github.com/home-operations/flate/pkg/store"
)

func snapshotSubstitutions(cfg *Config) error {
	cfg.AllowMissingKinds = slices.Clone(cfg.AllowMissingKinds)
	var err error
	cfg.supplied, err = store.NewSupplied(cfg.Supply)
	cfg.Supply = nil
	return err
}
