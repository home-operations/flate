package loader

import (
	"maps"
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
)

func (l *Loader) recordContributionRoots() {
	{
		contributionValues2 := l.scopes.contributions
		for contributionIndex2 := range contributionValues2 {
			contribution := contributionValues2[contributionIndex2]
			occurrence, located := l.locations[contribution.owner]
			if !located {
				continue
			}
			var roots []string
			complete := true
			{
				vValues2 := contribution.visits
				for vIndex2 := range vValues2 {
					v := vValues2[vIndex2]
					file, err := filepath.Rel(contribution.context.root, v.file)
					if v.incomplete != "" || err != nil {
						complete = false
						break
					}
					roots = append(roots, filepath.ToSlash(file))
				}
			}
			if complete && len(roots) != 0 {
				slices.Sort(roots)
				facts := *occurrence.facts
				facts.roots = slices.Compact(roots)
				occurrence.facts = &facts
				l.locations[contribution.owner] = occurrence
			}
		}
	}
}

func (l *Loader) warnIncompleteProductions(incomplete map[manifest.NamedResource][]string) {
	if len(incomplete) != 0 {
		for _, owner := range slices.SortedFunc(maps.Keys(incomplete), manifest.NamedResource.Compare) {
			details := incomplete[owner]
			slices.Sort(details)
			details = slices.Compact(details)
			l.Store.AddWarning(
				manifest.Warning{
					Resource: owner,
					Category: manifest.WarnIncompleteInputCoverage,
					Message:  "changed-only input coverage is incomplete; run flate build all without --base or --path-orig to validate the full tree",
					Detail:   details,
				},
			)
		}
	}
}
