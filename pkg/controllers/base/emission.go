package base

import "github.com/home-operations/flate/pkg/manifest"

// Child retains a parsed render child and its source document in document order.
type Child struct {
	Obj manifest.BaseManifest
	Doc map[string]any
}

// Emit records ownership before extending selection or publishing children.
// Data precedes leaf reconcilables, whose batch must be visible to siblings.
// publish=false suppresses tracked publication during fingerprint replay;
// raw children retain AddObject's deduplication and arrival behavior.
func (c *Controller) Emit(parent manifest.NamedResource, docs []map[string]any, wipeSecrets, publish bool) []Child {
	// HelmRelease documents already carry the release namespace. KS/RS children
	// inherit their emitter namespace to dedup against file-loaded copies.
	helmRelease := parent.Kind == manifest.KindHelmRelease
	if !helmRelease {
		manifest.StampNamespaces(docs, parent.Namespace)
	}
	opts := manifest.ParseDocOptions{WipeSecrets: wipeSecrets}
	log := c.Logger()
	children := make([]Child, 0, len(docs))
	var emitted []manifest.NamedResource
	for _, doc := range docs {
		if manifest.IsEncryptedSecret(doc) {
			name, ns := manifest.DocMetadata(doc)
			log.Debug("SOPS-encrypted resource wiped to placeholder",
				"id", parent.String(), "ref", manifest.DocKind(doc)+" "+ns+"/"+name)
		}
		obj, err := manifest.ParseDoc(doc, opts)
		if err != nil {
			log.Debug("skipped doc", "id", parent.String(), "err", err)
			continue
		}
		if manifest.IsKustomizeBuildDirective(obj) {
			continue
		}
		children = append(children, Child{Obj: obj, Doc: doc})
		if isTrackedChild(obj, helmRelease) {
			if emitted == nil {
				emitted = make([]manifest.NamedResource, 0, len(docs))
			}
			emitted = append(emitted, obj.Named())
		}
	}
	c.Store.RecordEmitted(parent, emitted)
	for _, child := range children {
		obj := child.Obj
		id := obj.Named()
		switch {
		case !isTrackedChild(obj, helmRelease):
			c.Store.AddObject(obj)
		case id.Kind == manifest.KindKustomization || id.Kind == manifest.KindHelmRelease:
			continue
		default:
			c.KeepEmitted(parent, obj)
			if publish && id != parent {
				c.Store.AddObject(obj)
			}
		}
	}
	if helmRelease {
		return children
	}
	var leaves []manifest.BaseManifest
	for _, child := range children {
		obj := child.Obj
		switch obj.(type) {
		case *manifest.Kustomization, *manifest.HelmRelease:
		default:
			continue
		}
		id := obj.Named()
		c.KeepEmitted(parent, obj)
		// A parent's self-copy must never overwrite its substituted spec.
		if publish && id != parent {
			leaves = append(leaves, obj)
		}
	}
	if publish {
		c.Store.AddObjects(leaves)
	}
	return children
}

func isTrackedChild(obj manifest.BaseManifest, helmRelease bool) bool {
	switch obj.(type) {
	case *manifest.HelmRepository, *manifest.OCIRepository,
		*manifest.GitRepository, *manifest.Bucket,
		*manifest.HelmChartSource, *manifest.ExternalArtifact:
		return true
	case *manifest.Kustomization, *manifest.HelmRelease,
		*manifest.ConfigMap, *manifest.Secret,
		*manifest.ResourceSet, *manifest.ResourceSetInputProvider:
		return !helmRelease
	}
	return false
}
