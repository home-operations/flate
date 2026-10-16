package base

import (
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
)

// Child retains a parsed render child and its source document in document order.
type Child struct {
	Obj manifest.BaseManifest
	Doc map[string]any
}

// Emit records ownership before extending selection or publishing children.
// Data precedes leaf reconcilables, whose batch must be visible to siblings.
// publish=false suppresses tracked publication during fingerprint replay;
// raw children retain AddObject's deduplication and arrival behavior.
// Returned documents are authoritative even when a typed child is invalid.
func (c *Controller) Emit(parent manifest.NamedResource, docs []map[string]any, opts manifest.ParseDocOptions, publish bool) ([]Child, []map[string]any) {
	sourceOnly := parent.KindInfo().SourceOnly
	log := c.Logger()
	children := make([]Child, 0, len(docs))
	var emitted []manifest.NamedResource
	retained := false
	for i, doc := range docs {
		obj, authoritative, changed, err := manifest.ParseDoc(doc, opts)
		if changed {
			if !retained {
				docs = slices.Clone(docs)
				retained = true
			}
			docs[i] = authoritative
		}
		doc = authoritative
		if err != nil {
			log.Debug("skipped doc", "id", parent.String(), "err", err)
			continue
		}
		if obj == nil {
			continue
		}
		children = append(children, Child{Obj: obj, Doc: doc})
		info := manifest.KindOf(obj)
		if info.Tracked && (!sourceOnly || info.Group == manifest.SourceDomain) {
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
		info := manifest.KindOf(obj)
		switch {
		case !info.Tracked || sourceOnly && info.Group != manifest.SourceDomain:
			c.Store.AddObject(obj)
		case info.Render:
			continue
		default:
			c.KeepEmitted(parent, obj)
			if publish && id != parent {
				c.Store.AddObject(obj)
			}
		}
	}
	if sourceOnly {
		return children, docs
	}
	var leaves []manifest.BaseManifest
	for _, child := range children {
		obj := child.Obj
		info := manifest.KindOf(obj)
		if !info.Render {
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
	return children, docs
}
