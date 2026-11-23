package kustomize

import (
	"bytes"
	"context"
	"log/slog"
	"reflect"
	"slices"
	"strings"

	"sigs.k8s.io/kustomize/api/types"
	"sigs.k8s.io/kustomize/kyaml/yaml"

	"github.com/home-operations/flate/pkg/manifest"
)

type entryContributors struct {
	files      []string
	incomplete bool
	flat       bool
	fifo       bool
}

func (c *entryContributors) recipe(k *types.Kustomization, node *yaml.RNode) {
	c.incomplete = c.incomplete || len(k.ConfigMapGenerator)+len(k.SecretGenerator)+len(k.HelmCharts)+
		len(k.HelmChartInflationGenerator)+len(k.Generators)+len(k.Transformers)+len(k.Validators) != 0
	if !c.flat {
		return
	}
	fields, err := node.Fields()
	if err != nil {
		c.flat = false
		return
	}
	for _, field := range fields {
		switch strings.ToLower(field) {
		case "apiversion", "kind", "metadata", "resources", "namespace", "patches":
		case "sortoptions":
			c.fifo = k.SortOptions != nil && k.SortOptions.Order == types.FIFOSortOrder
			c.flat = c.fifo
		default:
			c.flat = false
		}
		if !c.flat {
			return
		}
	}
}

func (c *entryContributors) resource(overlay *treeFS, path *inputPath) {
	if path.directory {
		c.flat = false
	}
	if path.descriptor.URL != "" {
		c.incomplete = true
		return
	}
	if path.directory {
		return
	}
	relative, err := overlay.resolve(path.file)
	if err != nil {
		c.incomplete = true
		return
	}
	origin, local := overlay.origin(relative)
	if !local || overlay.modified(origin) {
		c.incomplete = true
		return
	}
	c.files = append(c.files, origin)
}

// EntryDocumentMembership ties raw output ordinals to physical contributors in the same full build.
func (p *Preparation) EntryDocumentMembership(
	ctx context.Context,
	receipt *manifest.RenderReceipt,
	project func([]string) []bool,
) map[int]bool {
	if p.source == nil || receipt == nil || !p.source.Reads.Valid() {
		return nil
	}
	//nolint:errcheck // The preparation and receipt come from the same Kustomize render.
	frame := receipt.Value().(renderFrame)
	reads := &frame.Header.Reads
	if len(reads.Remotes) != 0 {
		return nil
	}
	immutable := p.source.Content != ""
	valid, err := reads.localsValid(ctx, p.lower, immutable)
	if err != nil || !valid {
		return nil
	}
	raw, err := manifest.DecodeRender(frame.Output)
	if err != nil || len(raw) == 0 {
		return nil
	}

	rec := newReadSet(p.root)
	rec.immutable = immutable
	overlay := newTreeFS(p.lower, p.root, rec)
	req := RenderRequest{Source: p.source, Spec: p.spec, Label: p.label, SubPath: p.subPath}
	contributors := entryContributors{flat: true}
	overlay.contributors = &contributors
	files, err := prepareFilesAndPreflight(ctx, &req, overlay, nil)
	if err != nil || contributors.incomplete || len(contributors.files) == 0 {
		return nil
	}
	snapshot := rec.snapshot()
	if !includesEntryReads(reads, &snapshot) || !p.source.Reads.Valid() || ctx.Err() != nil {
		return nil
	}

	sides := project(contributors.files)
	first := sides[0]
	if !slices.Contains(sides, !first) {
		result := make(map[int]bool, len(raw))
		for _, doc := range raw {
			result[doc.Ordinal] = first
		}
		return result
	}
	if !contributors.flat || !contributors.singleDocuments(overlay) {
		return nil
	}

	observed := raw
	if !contributors.fifo {
		slog.Debug("kustomize: observe input order", "path", p.subPath)
		render := renderRequest{Preparation: *p, request: req}
		observed, err = render.buildDocuments(ctx, overlay, files, true)
		header := renderHeader{Reads: rec.snapshot()}
		if err != nil || !equalRenderHeader(frame.Header, header) || !p.source.Reads.Valid() {
			return nil
		}
	}
	return pairEntryDocuments(raw, observed, sides)
}

// The preflight reads a subset of what the full build reads.
func includesEntryReads(rs, preflight *readSetSnapshot) bool {
	if len(preflight.Remotes) != 0 {
		return false
	}
	for _, file := range preflight.Files {
		i, ok := slices.BinarySearchFunc(rs.Files, file, compareReadPath)
		if !ok || rs.Files[i].Hash != file.Hash {
			return false
		}
	}
	for _, dir := range preflight.Dirs {
		i, ok := slices.BinarySearchFunc(rs.Dirs, dir, compareReadPath)
		if !ok || rs.Dirs[i].Hash != dir.Hash {
			return false
		}
	}
	for _, node := range preflight.Nodes {
		i, ok := slices.BinarySearchFunc(rs.Nodes, node, compareReadPath)
		if ok && rs.Nodes[i].Kind == node.Kind {
			continue
		}
		if _, ok = slices.BinarySearchFunc(rs.Files, node, compareReadPath); ok && node.Kind == kindFile {
			continue
		}
		if _, ok = slices.BinarySearchFunc(rs.Dirs, node, compareReadPath); !ok || node.Kind != kindDir {
			return false
		}
	}
	return true
}

func compareReadPath(a, b localEntry) int {
	return strings.Compare(a.Path, b.Path)
}

func (c *entryContributors) singleDocuments(overlay *treeFS) bool {
	seen := make(map[string]struct{}, len(c.files))
	for _, file := range c.files {
		if _, repeated := seen[file]; repeated {
			return false
		}
		seen[file] = struct{}{}
		body, err := overlay.upper.ReadFile(file)
		if err != nil {
			return false
		}
		docs, err := manifest.DecodeDocs(bytes.NewReader(body), manifest.RequiredDocuments)
		if err != nil || len(docs) != 1 || docs[0].Err != nil ||
			strings.HasSuffix(manifest.DocIdentity(docs[0].Document).Kind, "List") {
			return false
		}
	}
	return true
}

func pairEntryDocuments(raw, observed []manifest.DocOccurrence, sides []bool) map[int]bool {
	if len(raw) != len(observed) || len(raw) != len(sides) {
		return nil
	}
	byID := make(map[manifest.NamedResource]manifest.DocOccurrence, len(raw))
	for _, doc := range raw {
		id := manifest.DocIdentity(doc.Document)
		if doc.Err != nil || id.Kind == "" || id.Name == "" {
			return nil
		}
		byID[id] = doc
	}
	result := make(map[int]bool, len(raw))
	for i, doc := range observed {
		id := manifest.DocIdentity(doc.Document)
		match, exists := byID[id]
		if !exists || doc.Err != nil || !reflect.DeepEqual(doc.Document, match.Document) {
			return nil
		}
		delete(byID, id)
		result[match.Ordinal] = sides[i]
	}
	return result
}
