package diff

import (
	"fmt"

	"github.com/home-operations/flate/pkg/manifest"
)

// Subject keeps canonical identity separate from its resolved human labels.
type Subject struct {
	ID         manifest.NamedResource
	Kind, Name string
}

// Names labels only printed subjects in one retained cluster.
type Names struct {
	Prefix   string
	Subjects map[manifest.NamedResource]Subject
}

// PrepareNames limits collision counts to printed subjects across retained clusters.
func PrepareNames(
	clusters []string,
	inventory [][]manifest.NamedResource,
	subjects []map[manifest.NamedResource]manifest.DiagnosticIdentity,
) *Naming {
	occurrences := nameOccurrenceUpdater()
	collectNameOccurrences(inventory, subjects, occurrences.update)
	return &Naming{
		Groups:     prepareNames(clusters, inventory, subjects, occurrences.collisions),
		inventory:  inventory,
		subjects:   subjects,
		collisions: occurrences.collisions,
		update:     occurrences.update,
	}
}

// Naming must restore shared occurrence counts after each temporary context.
type Naming struct {
	Groups      []Names
	inventory   [][]manifest.NamedResource
	subjects    []map[manifest.NamedResource]manifest.DiagnosticIdentity
	occurrences []map[manifest.NamedResource]int
	collisions  map[string]bool
	update      func(string, string, int)
}

// Model inherits group subjects only when subjects is nil.
func (n *Naming) Model(group int, subjects map[manifest.NamedResource]manifest.DiagnosticIdentity) Names {
	if subjects == nil {
		return n.Groups[group]
	}
	var captured map[manifest.NamedResource]manifest.DiagnosticIdentity
	if group < len(n.subjects) {
		captured = n.subjects[group]
	}
	occurrences := n.groupOccurrences(group)
	adjustNameOccurrences(occurrences, captured, subjects, n.update)
	prepared := prepareGroupNames(n.Groups[group].Prefix, n.inventory[group], subjects, n.collisions)
	adjustNameOccurrences(occurrences, subjects, captured, n.update)
	return prepared
}

// Warning preserves captured subjects without changing the group context.
//
//nolint:gocritic // The naming API takes an immutable warning snapshot.
func (n *Naming) Warning(group int, warning manifest.Warning) Subject {
	if warning.Resource == (manifest.NamedResource{}) {
		return Subject{}
	}
	var captured map[manifest.NamedResource]manifest.DiagnosticIdentity
	if group < len(n.subjects) {
		captured = n.subjects[group]
	}
	prepared := manifest.DiagnosticFor(warning.Resource, nil)
	if subject, ok := captured[warning.Resource]; ok {
		prepared = subject
	}
	if warning.Subject.ID == (manifest.NamedResource{}) || warning.Subject == prepared {
		return n.Groups[group].Subjects[warning.Resource]
	}
	occurrences := n.groupOccurrences(group)
	adjustNameOccurrences(occurrences, captured, nil, n.update)
	count := occurrences[warning.Resource]
	n.update(warning.Resource.Kind, warning.Resource.Group, -count)
	n.update(warning.Subject.Resource.Kind, warning.Subject.Resource.Group, count)
	labels := subjectLabels(&warning.Subject.Resource, warning.Subject.Path, n.Groups[group].Prefix, n.collisions)
	n.update(warning.Subject.Resource.Kind, warning.Subject.Resource.Group, -count)
	n.update(warning.Resource.Kind, warning.Resource.Group, count)
	adjustNameOccurrences(occurrences, nil, captured, n.update)
	return Subject{ID: warning.Resource, Kind: labels.kind, Name: labels.name}
}

func (n *Naming) groupOccurrences(group int) map[manifest.NamedResource]int {
	if n.occurrences == nil {
		n.occurrences = make([]map[manifest.NamedResource]int, len(n.inventory))
	}
	if n.occurrences[group] == nil {
		n.occurrences[group] = make(map[manifest.NamedResource]int, len(n.inventory[group]))
		for _, raw := range n.inventory[group] {
			n.occurrences[group][raw]++
		}
	}
	return n.occurrences[group]
}

// Labels qualifies only collisions in ids, so hidden continuations cannot affect labels.
func (n Names) Labels(ids []manifest.NamedResource) []string {
	names := make([]string, 0, len(ids))
	counts := make(map[string]int, len(ids))
	for _, id := range ids {
		name := n.Subjects[id].Name
		names = append(names, name)
		counts[name]++
	}
	for i, name := range names {
		if counts[name] > 1 {
			names[i] = n.Subjects[ids[i]].Kind + " " + name
		}
	}
	return names
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
