package loader

import (
	"encoding/json"
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

type materializerMatch func(manifest.NamedResource, manifest.BaseManifest, *manifest.DiscoveryRecord) bool

func (c *admissionClosure) materializerMatcher(demand *materializerDemand) (manifest.NamedResource, materializerMatch) {
	var entry manifest.NamedResource
	var input *store.SourceArtifact
	if demand.application != "" {
		input, _ = c.objects.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
		relative, _ := filepath.Rel(c.loader.SourceRoot, demand.application)
		entry = manifest.NewEntryKustomization(manifest.BootstrapSourceID, filepath.ToSlash(relative), manifest.NamedResource{}).
			Named()
	}
	return entry, func(id manifest.NamedResource, object manifest.BaseManifest, record *manifest.DiscoveryRecord) bool {
		if demand.application == "" {
			return matchesMaterializerIdentity(demand.target, id)
		}
		if record != nil && record.Application != "" {
			var document map[string]any
			if err := json.Unmarshal([]byte(record.Application), &document); err != nil {
				return false
			}
			var err error
			parsed, parsedErr := manifest.ParseDoc(document, manifest.ParseDocOptions{})
			object, err = parsed.Object, parsedErr
			if err != nil || object == nil || object.Named() != id {
				return false
			}
		}
		if object == nil {
			return false
		}
		application, valid := manifest.ApplicationFor(object)
		if !valid && id == entry {
			application.Source, application.Path, valid = object.SourcePath()
		}
		return valid && AppliesToEntry(c.objects, &application, input, demand.application)
	}
}

func (c *admissionClosure) materializerApplicationKnown(entry manifest.NamedResource, matches materializerMatch) bool {
	if c.appKnown {
		return c.applied
	}
	c.appKnown = true
	if object := c.object(entry); object != nil && matches(entry, object, nil) {
		c.applied = true
		return true
	}
	for id := range c.loader.locations {
		object := c.object(id)
		if object == nil {
			object = c.loader.CatalogObject(id)
		}
		if c.loader.IsFileIndexed(id) && matches(id, object, nil) && c.independent(id, nil) {
			c.applied = true
			return true
		}
	}
	for owner, records := range c.accepted {
		if !c.replace[owner] {
			continue
		}
		for id := range records {
			record := records[id]
			if matches(id, nil, &record) {
				c.applied = true
				return true
			}
		}
	}
	for _, object := range c.loader.Store.Objects() {
		if c.materializerApplicationObjectKnown(object.Named(), matches) {
			c.applied = true
			return true
		}
	}
	return false
}

func (c *admissionClosure) materializerApplicationObjectKnown(
	id manifest.NamedResource,
	matches materializerMatch,
) bool {
	object := c.object(id)
	if object == nil {
		return false
	}
	if c.loader.IsFileIndexed(id) && matches(id, object, nil) && c.independent(id, nil) {
		return true
	}
	occurrence, known := c.loader.catalogOccurrence(id)
	if !known || occurrence.facts.policy == nil {
		return false
	}
	output, ok := c.objects.GetArtifact(id, store.ArtifactContent).(*store.Emission)
	status, _ := c.objects.GetStatus(id)
	accepted := ok && output.Complete && status.Outcome() == store.OutcomePassed
	return accepted && slices.ContainsFunc(output.Children, func(child store.Child) bool {
		return matches(child.Obj.Named(), child.Obj, nil)
	})
}
