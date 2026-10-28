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
	groups := make(map[string]string)
	collisions := make(map[string]bool)
	for i, ids := range inventory {
		for _, raw := range ids {
			id := raw
			if i < len(subjects) {
				if subject, ok := subjects[i][raw]; ok {
					id = subject.Resource
				}
			}
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
			subject := manifest.DiagnosticFor(id, nil)
			if i < len(subjects) {
				if prepared, ok := subjects[i][id]; ok {
					subject = prepared
				}
			}
			resource := subject.Resource
			kind, name := resource.Kind, resource.NamespacedName()
			if resource == (manifest.NamedResource{}) {
				kind, name = "entry", fmt.Sprintf("path %q", subject.Path)
			} else if subject.Path != "" {
				name += fmt.Sprintf(" (entry path %q)", subject.Path)
			}
			if collisions[kind] {
				kind = resource.KindLabel()
			}
			names.Subjects[id] = Subject{ID: id, Kind: kind, Name: prefix + name}
		}
		out[i] = names
	}
	return out
}
