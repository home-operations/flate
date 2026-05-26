package discovery

import (
	"log/slog"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/resourceset"
)

// renderResourceSet evaluates rs.Spec across its inputs and AddObjects
// every resulting recognized Flux resource into the store. The rendered
// children are attributed to the ResourceSet's own source file so the
// change filter treats them as siblings of the ResourceSet definition
// (a ResourceSet change reruns its children's reconciles). Returns
// the count of new objects added so the caller can detect a fixed
// point in the expansion loop.
func (d *discoverer) renderResourceSet(rs *manifest.ResourceSet) (int, error) {
	docs, err := resourceset.Render(rs, resourceset.StoreResolver(d.cfg.Store))
	if err != nil {
		return 0, err
	}
	srcFile := d.sourceFiles[rs.Named()]
	opts := manifest.ParseDocOptions{WipeSecrets: d.cfg.WipeSecrets}
	added := 0
	for _, doc := range docs {
		obj, err := manifest.ParseDoc(doc, opts)
		if err != nil {
			// Warn rather than Debug: an RS template emitting a
			// malformed doc is a real authoring bug (silent at Debug
			// produces an RS that converges to zero docs and the
			// user sees no KSes with no explanation). The doc is
			// still skipped so other docs in the RS render proceed.
			slog.Warn("resourceset: skipped malformed doc",
				"rs", rs.Named().NamespacedName(),
				"docKind", manifest.DocKind(doc),
				"err", err)
			continue
		}
		if _, ok := obj.(*manifest.RawObject); ok {
			// Generic / unrecognized kinds: not something flate
			// reconciles further. Skipped here; the orchestrator's
			// post-Run RS expansion pass picks them up and attributes
			// them to the owning KS for `flate build` visibility.
			// That late pass sees RSIPs emitted from KS reconcile
			// (kustomize-substituted dragonfly-${APP} style) which
			// this discovery pass would miss.
			continue
		}
		id := obj.Named()
		if d.cfg.Store.GetObject(id) != nil {
			continue
		}
		d.cfg.Store.AddObject(obj)
		if srcFile != "" {
			d.sourceFiles[id] = srcFile
		}
		added++
	}
	return added, nil
}
