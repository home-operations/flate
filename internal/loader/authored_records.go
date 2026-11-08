package loader

import (
	"slices"
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
)

type authoredRecord struct {
	id, authored manifest.NamedResource
	path, file   string
}

func (l *Loader) indexAuthoredRecords() []authoredRecord {
	indexed := l.scopes.authored[:0]
	//nolint:gocritic // Discovery iterator values preserve traversal snapshots without another lookup.
	for id, r := range l.Index.Files() {
		if r.Authored != (manifest.NamedResource{}) && r.Path != "" {
			indexed = append(indexed, authoredRecord{id: id, authored: r.Authored, path: r.Path, file: r.File})
		}
	}
	l.scopes.authored = indexed
	slices.SortFunc(indexed, func(a, b authoredRecord) int {
		if a.path != b.path {
			return strings.Compare(a.path, b.path)
		}
		if c := a.authored.Compare(b.authored); c != 0 {
			return c
		}
		return a.id.Compare(b.id)
	})
	return indexed
}

func authoredGroupEnd(indexed []authoredRecord, start int, path string) int {
	end := start + 1
	for end < len(indexed) && indexed[end].path == path {
		end++
	}
	return end
}
