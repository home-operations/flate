package orchestrator

import (
	"fmt"
	"slices"

	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/pkg/manifest"
)

func clusterAdvisories(results []Rendered, stores []*store.Store) {
	for i := 0; i < len(results); {
		j := i + 1
		for j < len(results) && results[j].ID.Cluster == results[i].ID.Cluster {
			j++
		}
		clusterAdvisory(results[i:j], stores[i:j])
		i = j
	}
}

func clusterAdvisory(results []Rendered, stores []*store.Store) {
	var carrier *Rendered
	var overrides []string
	for k := range results {
		if !results[k].Absent && (carrier == nil || results[k].ID.Side == manifest.TreeHead) {
			carrier = &results[k]
		}
		if stores[k] == nil {
			continue
		}
		overrides = suppliedOverrides(stores[k], overrides)
	}
	var warnings []manifest.Warning
	if len(overrides) > 0 {
		slices.Sort(overrides)
		warnings = append(warnings, manifest.Warning{
			Category: manifest.WarnSuppliedPrecedence,
			Message:  "repository data takes precedence over supplied data",
			Detail:   slices.Compact(overrides),
			Count:    1,
		})
	}
	if carrier != nil && len(warnings) > 0 {
		if carrier.Result == nil {
			carrier.Result = &Result{}
		}
		slices.SortFunc(warnings, manifest.CompareWarning)
		carrier.Result.ClusterWarnings = warnings
	}
}

func suppliedOverrides(st *store.Store, overrides []string) []string {
	evidence := st.AdvisoryEvidence()
	for _, override := range evidence.Overrides {
		for _, key := range override.Keys {
			overrides = append(
				overrides,
				fmt.Sprintf("%s: %s (%s)", override.Resource, key, override.SupplyPath),
			)
		}
	}
	return overrides
}
