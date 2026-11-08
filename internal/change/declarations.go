package change

import (
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/pkg/manifest"
)

// DeclarationInputs carries the completed source pair and its preparation authority.
type DeclarationInputs struct {
	Stores   [2]*store.Store
	Prepared [2]map[manifest.NamedResource]store.Child
	Work     manifest.WorkScope
}
