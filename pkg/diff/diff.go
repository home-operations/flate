package diff

import (
	"bytes"
	"cmp"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"sigs.k8s.io/yaml"

	"github.com/home-operations/flate/pkg/manifest"
)

// Format selects the diff output flavor.
type Format string

// Format values understood by Render.
const (
	// FormatDiff is dyff's `--output github` mode: path-based diff
	// syntax (`@@`, `+`, `-`, `!`) that GitHub's diff lexer renders
	// natively as a colored diff block when wrapped in a ```diff
	// fence. K8s-aware: list entries are matched by identifier
	// (container name, env-var name, etc.), so reordering a list
	// produces no diff churn.
	FormatDiff     Format = "diff"
	FormatYAML     Format = "yaml"
	FormatJSON     Format = "json"
	FormatMarkdown Format = "markdown"
)

// Options tunes Run behavior.
type Options struct {
	// StripAttrs lists annotation/label keys removed from each
	// manifest's metadata (and pod-template metadata) before the diff
	// is computed. Cuts chart-bump noise — annotations like
	// `helm.sh/chart` or `checksum/config` whose values rotate on
	// every chart bump would otherwise produce a diff entry per
	// resource. dyff matches K8s lists by identifier but still
	// reports string-value changes verbatim, so this pre-filter still
	// pulls its weight after the dyff swap.
	StripAttrs []string
}

// Parent identifies the Flux Kustomization or HelmRelease that
// rendered a manifest. The diff header includes this so the reviewer
// can see which app the change belongs to.
type Parent struct {
	Kind      string `json:"kind,omitempty"      yaml:"kind,omitempty"`
	Namespace string `json:"namespace,omitempty" yaml:"namespace,omitempty"`
	Name      string `json:"name,omitempty"      yaml:"name,omitempty"`
	// Path is the Flux Kustomization spec.path (only set for KS
	// parents). Slash-normalized, with the conventional `./` prefix
	// stripped so headers stay tidy.
	Path string `json:"path,omitempty" yaml:"path,omitempty"`
}

// Doc pairs a rendered manifest with its parent. diff.Run consumes
// these so each ResourceDiff knows which Flux KS / HR produced it.
type Doc struct {
	Manifest map[string]any
	Parent   Parent
}

// ResourceDiff is the per-resource result of a Run.
type ResourceDiff struct {
	Parent    Parent `json:"parent,omitzero"     yaml:"parent,omitempty"`
	Kind      string `json:"kind"                yaml:"kind"`
	Namespace string `json:"namespace,omitempty" yaml:"namespace,omitempty"`
	Name      string `json:"name"                yaml:"name"`
	Diff      string `json:"diff"                yaml:"diff"`
}

// Header returns the "Parent: ns/name Child: ns/name" prefix used
// in diff output. The Kustomization source path is intentionally
// omitted so KS-owned and HR-owned resources render symmetrically
// — `Parent.Path` survives on the struct for JSON/YAML consumers
// but never appears in the human-facing header.
func (d ResourceDiff) Header() string {
	parts := make([]string, 0, 2)
	if d.Parent.Kind != "" {
		parts = append(parts, fmt.Sprintf("%s: %s", d.Parent.Kind, joinNS(d.Parent.Namespace, d.Parent.Name)))
	}
	parts = append(parts, fmt.Sprintf("%s: %s", d.Kind, joinNS(d.Namespace, d.Name)))
	return strings.Join(parts, " ")
}

func joinNS(ns, name string) string {
	if ns == "" {
		return name
	}
	return ns + "/" + name
}

// Run compares two manifest sets and returns the resources whose
// rendered form differs. Resources missing on either side are
// reported with the counterpart as an empty document, producing a
// wholesale addition/removal in the dyff output. Pairs are keyed by
// (parent, kind, namespace, name) so a Deployment rendered by
// HelmRelease A doesn't accidentally diff against the same-named
// Deployment from HelmRelease B.
func Run(left, right []Doc, opts Options) ([]ResourceDiff, error) {
	left = applyStrip(left, opts.StripAttrs)
	right = applyStrip(right, opts.StripAttrs)
	pairs := pair(left, right)
	out := make([]ResourceDiff, 0, len(pairs))
	for _, p := range pairs {
		body, err := dyffDiff(p.a, p.b)
		if err != nil {
			return nil, err
		}
		if body == "" {
			// Identical resources: dyff yields no diffs. Skip.
			continue
		}
		out = append(out, ResourceDiff{
			Parent: p.parent,
			Kind:   p.kind, Namespace: p.namespace, Name: p.name, Diff: body,
		})
	}
	return out, nil
}

// Render serializes a diff result set into the requested format.
func Render(diffs []ResourceDiff, format Format) ([]byte, error) {
	switch format {
	case "", FormatDiff:
		var b bytes.Buffer
		// Emit a `# <resource>` comment line above every body. dyff's
		// `@@ <path> @@` identifies the data path that changed but
		// not the owning resource (`spec.template.spec.containers
		// .app.image` is which Deployment from which HelmRelease?),
		// so the header is load-bearing even when there's only one
		// diff — a reviewer scanning a PR comment shouldn't have to
		// infer the resource from the body. `#`-prefixed lines are
		// dyff's own comment convention; GitHub's diff lexer renders
		// them magenta.
		for _, d := range diffs {
			fmt.Fprintf(&b, "# %s\n", d.Header())
			b.WriteString(d.Diff)
			if !strings.HasSuffix(d.Diff, "\n") {
				b.WriteByte('\n')
			}
		}
		return b.Bytes(), nil
	case FormatYAML:
		return yaml.Marshal(diffs)
	case FormatJSON:
		return json.MarshalIndent(diffs, "", "  ")
	case FormatMarkdown:
		return renderMarkdown(diffs), nil
	}
	return nil, fmt.Errorf("unknown diff format %q", format)
}

// renderMarkdown emits a PR-comment-friendly view of the diff set:
// a `# Diff` heading, a pipe-table summary by classification
// (added/modified/removed), and one H3 + ```diff fence per
// ResourceDiff wrapping the dyff body verbatim. Classification is
// inferred from the dyff body's root-level markers — `! + ` for
// wholesale additions, `! - ` for wholesale removals, anything else
// is treated as a modification. An empty diff set renders as the
// empty document so the markdown output can be dropped into a PR
// comment unconditionally without a "no changes" placeholder.
func renderMarkdown(diffs []ResourceDiff) []byte {
	if len(diffs) == 0 {
		return nil
	}
	var b bytes.Buffer
	b.WriteString("# Diff\n")
	var added, modified, removed int
	for _, d := range diffs {
		switch classifyDiff(d.Diff) {
		case "added":
			added++
		case "removed":
			removed++
		default:
			modified++
		}
	}
	fmt.Fprintf(&b, "\n| Added | Modified | Removed | Total |\n")
	b.WriteString("| --- | --- | --- | --- |\n")
	fmt.Fprintf(&b, "| %d | %d | %d | %d |\n\n", added, modified, removed, len(diffs))
	for _, d := range diffs {
		fmt.Fprintf(&b, "### %s\n\n", d.Header())
		b.WriteString("```diff\n")
		b.WriteString(d.Diff)
		if !strings.HasSuffix(d.Diff, "\n") {
			b.WriteByte('\n')
		}
		b.WriteString("```\n\n")
	}
	return b.Bytes()
}

// classifyDiff inspects a dyff body and returns one of "added",
// "removed", or "modified". Wholesale additions/removals from
// Run() emit a `(root level)` header followed by an `! + ` /
// `! - ` map-entries marker; anything else is a per-path
// modification.
func classifyDiff(body string) string {
	if !strings.Contains(body, "@@ (root level) @@") {
		return "modified"
	}
	switch {
	case strings.Contains(body, "\n! + "):
		return "added"
	case strings.Contains(body, "\n! - "):
		return "removed"
	default:
		return "modified"
	}
}

type pairedResource struct {
	parent                Parent
	kind, namespace, name string
	a, b                  map[string]any
}

type pairKey struct {
	// pPath disambiguates two KS parents with the same (kind, ns, name)
	// but different spec.path — a real-world collision in repos where
	// the same KS is rendered twice from different overlays.
	pKind, pNS, pName, pPath  string
	apiVersion                string
	kind, ns, name            string
}

func pair(left, right []Doc) []pairedResource {
	idx := make(map[pairKey]*pairedResource, len(left)+len(right))
	add := func(side int, d Doc) {
		kind := manifest.DocKind(d.Manifest)
		apiVersion := manifest.DocAPIVersion(d.Manifest)
		name, ns := manifest.DocMetadata(d.Manifest)
		k := pairKey{d.Parent.Kind, d.Parent.Namespace, d.Parent.Name, d.Parent.Path, apiVersion, kind, ns, name}
		p, ok := idx[k]
		if !ok {
			p = &pairedResource{parent: d.Parent, kind: kind, namespace: ns, name: name}
			idx[k] = p
		}
		if side == 0 {
			p.a = d.Manifest
		} else {
			p.b = d.Manifest
		}
	}
	for _, d := range left {
		add(0, d)
	}
	for _, d := range right {
		add(1, d)
	}
	out := make([]pairedResource, 0, len(idx))
	for _, p := range idx {
		out = append(out, *p)
	}
	slices.SortFunc(out, func(a, b pairedResource) int {
		return cmp.Or(
			cmp.Compare(a.parent.Kind, b.parent.Kind),
			cmp.Compare(a.parent.Namespace, b.parent.Namespace),
			cmp.Compare(a.parent.Name, b.parent.Name),
			cmp.Compare(a.parent.Path, b.parent.Path),
			cmp.Compare(a.kind, b.kind),
			cmp.Compare(a.namespace, b.namespace),
			cmp.Compare(a.name, b.name),
		)
	})
	return out
}

// applyStrip clones each Doc's manifest and removes the listed
// annotation/label keys before the diff runs. Deep-copies so the
// original tree (used by other consumers in the same orchestrator
// run) is untouched.
func applyStrip(docs []Doc, attrs []string) []Doc {
	if len(attrs) == 0 {
		return docs
	}
	out := make([]Doc, len(docs))
	for i, d := range docs {
		copyDoc := manifest.DeepCopyMap(d.Manifest)
		manifest.StripResourceAttributes(copyDoc, attrs)
		out[i] = Doc{Manifest: copyDoc, Parent: d.Parent}
	}
	return out
}
