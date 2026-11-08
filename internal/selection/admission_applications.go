package selection

import (
	"encoding/json"
	"path/filepath"

	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/pkg/manifest"
)

type materializerMatch func(manifest.NamedResource, manifest.BaseManifest, *manifest.DiscoveryRecord) bool

func (c *admissionClosure) materializerMatcher(demand *materializerDemand) (manifest.NamedResource, materializerMatch) {
	if demand.application == "" {
		return manifest.NamedResource{}, func(id manifest.NamedResource, _ manifest.BaseManifest, _ *manifest.DiscoveryRecord) bool {
			return matchesMaterializerIdentity(demand.target, id)
		}
	}
	input, _ := c.objects.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
	relative, _ := filepath.Rel(c.context.Root, demand.application)
	entry := manifest.NewEntryKustomization(manifest.BootstrapSourceID, filepath.ToSlash(relative), manifest.NamedResource{}).
		Named()
	return entry, func(id manifest.NamedResource, object manifest.BaseManifest, record *manifest.DiscoveryRecord) bool {
		if record != nil && record.Application != "" {
			var valid bool
			object, valid = parseApplication(record.Application)
			if !valid || object == nil || object.Named() != id {
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
		if !valid || input == nil {
			return false
		}
		source := c.boundSource(application.Source)
		if source.Input == nil || source.Pinned || source.External {
			return false
		}
		target := filepath.Join(source.Path, manifest.NormalizeClaimBase(application.Path))
		return containsPath(source.Path, target) &&
			input.OriginPath(demand.application) == source.Input.OriginPath(target)
	}
}

//nolint:ireturn // Retained applications MUST preserve their concrete manifest types.
func parseApplication(application string) (manifest.BaseManifest, bool) {
	var document map[string]any
	if json.Unmarshal([]byte(application), &document) != nil {
		return nil, false
	}
	parsed, err := manifest.ParseDoc(document, manifest.ParseDocOptions{})
	return parsed.Object, err == nil
}
