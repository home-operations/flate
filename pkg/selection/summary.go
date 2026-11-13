package selection

import (
	"iter"

	"github.com/home-operations/flate/pkg/manifest"
)

// ReportCounts requires unchanged-parent evidence before counting unpublished declarations.
func (s *Snapshot) ReportCounts(request Request) map[string]int {
	counts := map[string]int{}
	for info := range manifest.Kinds() {
		if !info.Render {
			continue
		}
		for _, object := range s.Store.ListObjects(info.Group, info.Kind) {
			if s.Store.Policy(object.Named()).Report {
				counts[info.Kind]++
			}
		}
	}
	for object := range s.UnchangedDeclarations(request) {
		counts[object.Named().Kind]++
	}
	return counts
}

// UnchangedDeclarations MUST retain current parent authority throughout iteration.
func (s *Snapshot) UnchangedDeclarations(request Request) iter.Seq[manifest.BaseManifest] {
	return func(yield func(manifest.BaseManifest) bool) {
		side := selectionSide{run: *s, request: request}
		if s.Decision != nil && s.Facts != nil && s.Store != nil {
			side.reads.Init(s.Store, manifest.BootstrapSourceID, nil)
			defer side.reads.Retire()
		}
		for id := range s.Index.Files() {
			if !id.KindInfo().Render || id.ReportHidden() {
				continue
			}
			record, _ := s.Index.Lookup(id)
			if object := side.unchangedDeclaration(id, &record); object != nil && !yield(object) {
				return
			}
		}
	}
}
