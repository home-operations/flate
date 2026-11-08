package helmrelease

import (
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/pkg/manifest"
)

func captureEmission(st *store.Store, id manifest.NamedResource, output *store.Emission) *store.Emission {
	_, _, next := st.GetEmission(id)
	next.Finish()
	next.Children, next.Manifests = output.Children, output.Manifests
	next.Fingerprint, next.Unreadable = output.Fingerprint, output.Unreadable
	next.Complete = output.Complete
	return &next
}
