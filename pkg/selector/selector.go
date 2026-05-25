// Package selector implements metadata filtering used by every flate
// command. Selectors are translated from CLI flags by the cli package
// and consumed by controllers and the orchestrator to decide which
// Kustomizations/HelmReleases to consider.
package selector

import "github.com/home-operations/flate/pkg/manifest"

// Metadata filters resources by name and Kubernetes labels.
type Metadata struct {
	Name   string
	Labels map[string]string
}

// Matches reports whether obj passes the metadata filter.
func (m Metadata) Matches(obj manifest.BaseManifest) bool {
	if obj == nil {
		return false
	}
	if m.Name != "" && obj.Named().Name != m.Name {
		return false
	}
	if len(m.Labels) > 0 {
		labels := labelsOf(obj)
		for k, v := range m.Labels {
			if labels[k] != v {
				return false
			}
		}
	}
	return true
}

func labelsOf(obj manifest.BaseManifest) map[string]string {
	switch o := obj.(type) {
	case *manifest.Kustomization:
		return o.Labels
	case *manifest.HelmRelease:
		return o.Labels
	case *manifest.ResourceSet:
		return o.Labels
	case *manifest.ResourceSetInputProvider:
		return o.Labels
	}
	return nil
}
