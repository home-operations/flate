package output

import (
	"fmt"

	"github.com/home-operations/flate/pkg/manifest"
)

// Subject keeps canonical identity separate from its resolved human labels.
type Subject struct {
	ID         manifest.NamedResource
	Kind, Name string
}

// Names contains the prepared labels for one retained cluster.
type Names struct {
	Prefix   string
	Subjects map[manifest.NamedResource]Subject
}

// PrepareNames resolves cluster qualifiers and kind collisions across printed subjects.
func PrepareNames(clusters []string, inventory [][]manifest.NamedResource, subjects []map[manifest.NamedResource]manifest.DiagnosticIdentity) []Names {
	collisions, update := nameOccurrenceUpdater()
	collectNameOccurrences(inventory, subjects, update)
	return prepareNames(clusters, inventory, subjects, collisions)
}

func prepareNames(clusters []string, inventory [][]manifest.NamedResource, subjects []map[manifest.NamedResource]manifest.DiagnosticIdentity, collisions map[string]bool) []Names {
	out := make([]Names, len(clusters))
	for i, cluster := range clusters {
		prefix := ""
		if len(clusters) > 1 {
			prefix = cluster + ": "
		}
		var captured map[manifest.NamedResource]manifest.DiagnosticIdentity
		if i < len(subjects) {
			captured = subjects[i]
		}
		out[i] = prepareGroupNames(prefix, inventory[i], captured, collisions)
	}
	return out
}

func nameOccurrenceUpdater() (map[string]bool, func(string, string, int)) {
	indices := make(map[string]int)
	counts := make([]struct {
		group           string
		count, distinct int
		others          map[string]int
	}, 0, 8)
	collisions := make(map[string]bool)
	update := func(kind, group string, delta int) {
		index, exists := indices[kind]
		if !exists {
			index = len(counts)
			indices[kind] = index
			counts = append(counts, struct {
				group           string
				count, distinct int
				others          map[string]int
			}{group: group})
		}
		entry := &counts[index]
		before := entry.count
		if entry.group != group {
			if entry.others == nil {
				entry.others = make(map[string]int)
			}
			before = entry.others[group]
		}
		after := before + delta
		if entry.group == group {
			entry.count = after
		} else {
			entry.others[group] = after
		}
		distinct := entry.distinct
		if before == 0 && after > 0 {
			entry.distinct++
		} else if before > 0 && after == 0 {
			entry.distinct--
		}
		if entry.distinct > 1 != (distinct > 1) {
			if entry.distinct > 1 {
				collisions[kind] = true
			} else {
				delete(collisions, kind)
			}
		}
	}
	return collisions, update
}

func collectNameOccurrences(inventory [][]manifest.NamedResource, subjects []map[manifest.NamedResource]manifest.DiagnosticIdentity, update func(string, string, int)) {
	for i, ids := range inventory {
		for _, raw := range ids {
			id := raw
			if i < len(subjects) {
				if subject, ok := subjects[i][raw]; ok {
					id = subject.Resource
				}
			}
			update(id.Kind, id.Group, 1)
		}
	}
}

func adjustNameOccurrences(occurrences map[manifest.NamedResource]int, from, to map[manifest.NamedResource]manifest.DiagnosticIdentity, update func(string, string, int)) {
	for raw, count := range occurrences {
		before, after := raw, raw
		if subject, ok := from[raw]; ok {
			before = subject.Resource
		}
		if subject, ok := to[raw]; ok {
			after = subject.Resource
		}
		if before != after {
			update(before.Kind, before.Group, -count)
			update(after.Kind, after.Group, count)
		}
	}
}

func prepareGroupNames(prefix string, inventory []manifest.NamedResource, subjects map[manifest.NamedResource]manifest.DiagnosticIdentity, collisions map[string]bool) Names {
	names := Names{Prefix: prefix, Subjects: make(map[manifest.NamedResource]Subject, len(inventory))}
	for _, id := range inventory {
		if id == (manifest.NamedResource{}) {
			continue
		}
		subject, ok := subjects[id]
		if !ok {
			subject = manifest.DiagnosticFor(id, nil)
		}
		kind, name := subjectLabels(&subject.Resource, subject.Path, prefix, collisions)
		names.Subjects[id] = Subject{ID: id, Kind: kind, Name: name}
	}
	return names
}

func subjectLabels(resource *manifest.NamedResource, path, prefix string, collisions map[string]bool) (string, string) {
	kind, name := resource.Kind, resource.NamespacedName()
	if *resource == (manifest.NamedResource{}) {
		kind, name = "entry", fmt.Sprintf("path %q", path)
	} else if path != "" {
		name += fmt.Sprintf(" (entry path %q)", path)
	}
	if collisions[kind] {
		kind = resource.KindLabel()
	}
	return kind, prefix + name
}
