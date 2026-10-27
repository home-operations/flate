package output

import "github.com/home-operations/flate/pkg/manifest"

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
func PrepareNames(clusters []string, inventory [][]manifest.NamedResource) []Names {
	groups := make(map[string]string)
	collisions := make(map[string]bool)
	for _, ids := range inventory {
		for _, id := range ids {
			if group, ok := groups[id.Kind]; ok && group != id.Group {
				collisions[id.Kind] = true
			}
			groups[id.Kind] = id.Group
		}
	}
	out := make([]Names, len(clusters))
	for i, cluster := range clusters {
		prefix := ""
		if len(clusters) > 1 {
			prefix = cluster + ": "
		}
		names := Names{Prefix: prefix, Subjects: make(map[manifest.NamedResource]Subject, len(inventory[i]))}
		for _, id := range inventory[i] {
			if id == (manifest.NamedResource{}) {
				continue
			}
			kind := id.Kind
			if collisions[kind] {
				kind = id.KindLabel()
			}
			names.Subjects[id] = Subject{ID: id, Kind: kind, Name: prefix + id.NamespacedName()}
		}
		out[i] = names
	}
	return out
}
