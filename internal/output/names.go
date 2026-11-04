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
func PrepareNames(
	clusters []string,
	inventory [][]manifest.NamedResource,
	subjects []map[manifest.NamedResource]manifest.DiagnosticIdentity,
) []Names {
	occurrences := nameOccurrenceUpdater()

	collectNameOccurrences(inventory, subjects, occurrences.update)
	return prepareNames(clusters, inventory, subjects, occurrences.collisions)
}

func prepareNames(
	clusters []string,
	inventory [][]manifest.NamedResource,
	subjects []map[manifest.NamedResource]manifest.DiagnosticIdentity,
	collisions map[string]bool,
) []Names {
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

func nameOccurrenceUpdater() nameUpdater {
	indices := make(map[string]int)
	counts := make([]nameCount, 0, initialKindCapacity)
	collisions := make(map[string]bool)
	update := func(kind, group string, delta int) {
		index, exists := indices[kind]
		if !exists {
			index = len(counts)
			indices[kind] = index
			counts = append(counts, nameCount{group: group})
		}
		entry := &counts[index]

		if entry.adjust(group, delta) {
			if entry.distinct > 1 {
				collisions[kind] = true
			} else {
				delete(collisions, kind)
			}
		}
	}
	return nameUpdater{collisions: collisions, update: update}
}

func collectNameOccurrences(
	inventory [][]manifest.NamedResource,
	subjects []map[manifest.NamedResource]manifest.DiagnosticIdentity,
	update func(string, string, int),
) {
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

func adjustNameOccurrences(
	occurrences map[manifest.NamedResource]int,
	from, to map[manifest.NamedResource]manifest.DiagnosticIdentity,
	update func(string, string, int),
) {
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

func prepareGroupNames(
	prefix string,
	inventory []manifest.NamedResource,
	subjects map[manifest.NamedResource]manifest.DiagnosticIdentity,
	collisions map[string]bool,
) Names {
	names := Names{Prefix: prefix, Subjects: make(map[manifest.NamedResource]Subject, len(inventory))}
	for _, id := range inventory {
		if id == (manifest.NamedResource{}) {
			continue
		}
		subject, ok := subjects[id]
		if !ok {
			subject = manifest.DiagnosticFor(id, nil)
		}
		labels := subjectLabels(&subject.Resource, subject.Path, prefix, collisions)

		names.Subjects[id] = Subject{ID: id, Kind: labels.kind, Name: labels.name}
	}
	return names
}

func subjectLabels(resource *manifest.NamedResource, path, prefix string, collisions map[string]bool) subjectName {
	kind, name := resource.Kind, resource.NamespacedName()
	if *resource == (manifest.NamedResource{}) {
		kind, name = "entry", fmt.Sprintf("path %q", path)
	} else if path != "" {
		name += fmt.Sprintf(" (entry path %q)", path)
	}
	if collisions[kind] {
		kind = resource.KindLabel()
	}
	return subjectName{kind: kind, name: prefix + name}
}

const initialKindCapacity = 8

type nameCount struct {
	group           string
	count, distinct int
	others          map[string]int
}

func (e *nameCount) adjust(group string, delta int) bool {
	before := e.count
	if e.group != group {
		if e.others == nil {
			e.others = make(map[string]int)
		}
		before = e.others[group]
	}
	after := before + delta
	if e.group == group {
		e.count = after
	} else {
		e.others[group] = after
	}
	distinct := e.distinct
	if before == 0 && after > 0 {
		e.distinct++
	} else if before > 0 && after == 0 {
		e.distinct--
	}
	return e.distinct > 1 != (distinct > 1)
}

type nameUpdater struct {
	collisions map[string]bool
	update     func(string, string, int)
}

type subjectName struct{ kind, name string }
