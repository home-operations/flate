package selection

import (
	"iter"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// Declarations excludes executable raw siblings from dependency acquisition.
func Declarations(documents []store.Child) iter.Seq[store.Child] {
	return func(yield func(store.Child) bool) {
		for i := range documents {
			child := &documents[i]
			info := manifest.KindOf(child.Obj)
			if info.Tracked && (info.IsSource() || !info.Runnable) && !yield(*child) {
				return
			}
		}
	}
}
