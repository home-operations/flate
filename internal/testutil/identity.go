package testutil

import "github.com/home-operations/flate/pkg/manifest"

// KustomizationID returns the identity of a Flux Kustomization.
func KustomizationID(namespace, name string) manifest.NamedResource {
	return manifest.NamedResource{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: namespace, Name: name}
}

// HelmReleaseID returns the identity of a Flux HelmRelease.
func HelmReleaseID(namespace, name string) manifest.NamedResource {
	return manifest.NamedResource{Group: manifest.HelmReleaseDomain, Kind: manifest.KindHelmRelease, Namespace: namespace, Name: name}
}
