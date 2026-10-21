package store

import (
	"github.com/home-operations/flate/pkg/manifest"
)

func captureEmission(st *Store, id manifest.NamedResource, output *Emission) *Emission {
	_, next := st.GetEmission(id)
	next.Finish()
	next.Children, next.Manifests = output.Children, output.Manifests
	next.Fingerprint, next.Unreadable = output.Fingerprint, output.Unreadable
	next.Complete = output.Complete
	next.binding = output.binding
	return &next
}
