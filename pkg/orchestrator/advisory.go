package orchestrator

import (
	"fmt"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func clusterAdvisories(results []Rendered, stores []*store.Store) {
	for i := 0; i < len(results); {
		j := i + 1
		for j < len(results) && results[j].ID.Cluster == results[i].ID.Cluster {
			j++
		}
		var carrier *Rendered
		var overrides, agents []string
		for k := i; k < j; k++ {
			if !results[k].Absent && (carrier == nil || results[k].ID.Side == manifest.TreeHead) {
				carrier = &results[k]
			}
			if stores[k] == nil {
				continue
			}
			evidence := stores[k].AdvisoryEvidence()
			for _, override := range evidence.Overrides {
				for _, key := range override.Keys {
					overrides = append(overrides, fmt.Sprintf("%s: %s (%s)", override.Resource, key, override.SupplyPath))
				}
			}
			for _, source := range evidence.AgentSources {
				agents = append(agents, source.String())
			}
		}
		var warnings []manifest.Warning
		if len(overrides) > 0 {
			slices.Sort(overrides)
			warnings = append(warnings, manifest.Warning{Category: manifest.WarnSuppliedPrecedence,
				Message: "repository data takes precedence over supplied data", Detail: slices.Compact(overrides), Count: 1})
		}
		if len(agents) > 0 {
			slices.Sort(agents)
			warnings = append(warnings, manifest.Warning{Category: manifest.WarnLocalSSHAgent,
				Message: "authenticated with the local SSH agent", Detail: slices.Compact(agents), Count: 1})
		}
		if carrier != nil && len(warnings) > 0 {
			if carrier.Result == nil {
				carrier.Result = &Result{}
			}
			slices.SortFunc(warnings, manifest.CompareWarning)
			carrier.Result.ClusterWarnings = warnings
		}
		i = j
	}
}
