package kustomization

import (
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func captureEmission(st *store.Store, id manifest.NamedResource, output *store.Emission) *store.Emission {
	_, next := st.GetEmission(id)
	next.Finish()
	next.Children, next.Manifests = output.Children, output.Manifests
	next.Fingerprint, next.Unreadable = output.Fingerprint, output.Unreadable
	next.Complete = output.Complete
	return &next
}
