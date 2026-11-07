package selection

import (
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
)

func (c *admissionClosure) literalEntryChanged() bool {
	if len(c.session.request.Roots) != 0 {
		return false
	}
	delta, _, _ := c.delta(manifest.BootstrapSourceID)
	return slices.ContainsFunc(delta.Paths(), c.entryFileDemand)
}

func (c *admissionClosure) entryFileDemand(file string) bool {
	if file == "" || !containsPath(c.context.EntryPath, filepath.Join(c.context.Root, filepath.FromSlash(file))) {
		return false
	}
	index := *c.facts.SelectionIndex()
	index.Readers = nil
	localOwners := false
	for _, id := range index.OwnersOfFile(file).Owners {
		application, valid := manifest.ApplicationFor(c.catalogObject(id))
		if !valid {
			continue
		}
		source := c.boundSource(application.Source)
		if source.Pinned || source.External {
			continue
		}
		localOwners = true
		target := filepath.Join(source.Origin, manifest.NormalizeClaimBase(application.Path))
		if containsPath(source.Origin, target) && target == c.context.EntryPath {
			return true
		}
	}
	if localOwners {
		return false
	}
	if !c.context.NativeEntry {
		return !c.runnableDeclarationFile(file)
	}
	if len(c.facts.SelectionIndex().Readers[file]) != 0 {
		return true
	}
	for id := range c.facts.SelectionIndex().Records {
		record := c.facts.SelectionIndex().Records[id]
		if record.File == file && containsPath(c.context.EntryPath, record.Path) {
			return true
		}
	}
	return false
}

func (c *admissionClosure) runnableDeclarationFile(file string) bool {
	known := false
	for id := range c.facts.SelectionIndex().Records {
		record := c.facts.SelectionIndex().Records[id]
		if record.File != file || record.Path == "" {
			continue
		}
		if !id.KindInfo().Runnable {
			return false
		}
		known = true
	}
	return known
}
