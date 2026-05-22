package diff

import (
	"bytes"
	"cmp"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/pmezard/go-difflib/difflib"
	"sigs.k8s.io/yaml"

	"github.com/home-operations/flate/pkg/manifest"
)

// Format selects the diff output flavor.
type Format string

// Format values understood by Render.
const (
	FormatUnified Format = "diff"
	FormatObject  Format = "object"
	FormatYAML    Format = "yaml"
	FormatJSON    Format = "json"
)

// Options tunes Run behavior.
type Options struct {
	// Format selects the output flavor. Default FormatUnified.
	Format Format
	// Context lines around hunks. Default 3.
	Context int
	// LimitBytes truncates per-resource diff output. 0 = no limit.
	LimitBytes int
	// StripAttrs annotation/label keys to remove from manifests before
	// diffing (cuts kustomize-injected noise).
	StripAttrs []string
}

// Parent identifies the Flux Kustomization or HelmRelease that
// rendered a manifest. The unified-diff header includes this so the
// reviewer can see which app the change belongs to.
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
	Parent    Parent `json:"parent,omitempty" yaml:"parent,omitempty"`
	Kind      string `json:"kind"      yaml:"kind"`
	Namespace string `json:"namespace,omitempty" yaml:"namespace,omitempty"`
	Name      string `json:"name"      yaml:"name"`
	Diff      string `json:"diff"      yaml:"diff"`
}

// Header returns the flux-local-style "[path] Parent: ns/name
// Child: ns/name" prefix used in unified diff output.
func (d ResourceDiff) Header() string {
	var parts []string
	if d.Parent.Path != "" {
		parts = append(parts, d.Parent.Path)
	}
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
// serialized form differs. Resources missing on either side are
// reported with the counterpart side empty. Pairs are keyed by
// (parent, kind, namespace, name) so a Deployment rendered by
// HelmRelease A doesn't accidentally diff against the same-named
// Deployment from HelmRelease B.
func Run(left, right []Doc, opts Options) ([]ResourceDiff, error) {
	if opts.Context == 0 {
		opts.Context = 3
	}
	left = applyStrip(left, opts.StripAttrs)
	right = applyStrip(right, opts.StripAttrs)

	pairs := pair(left, right)
	out := make([]ResourceDiff, 0, len(pairs))
	for _, p := range pairs {
		a, err := marshalSide(p.a)
		if err != nil {
			return nil, err
		}
		b, err := marshalSide(p.b)
		if err != nil {
			return nil, err
		}
		if bytes.Equal(a, b) {
			continue
		}
		body, err := unified(string(a), string(b), opts.Context)
		if err != nil {
			return nil, err
		}
		if opts.LimitBytes > 0 && len(body) > opts.LimitBytes {
			body = body[:opts.LimitBytes] + "\n... [truncated]\n"
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
	case "", FormatUnified:
		var b bytes.Buffer
		for _, d := range diffs {
			h := d.Header()
			// Blank lines after each header (---, +++) and each hunk
			// marker (@@) — matches flux-local's reader-friendly
			// layout. difflib emits @@-lines as part of d.Diff, so we
			// post-process its output to insert the spacing.
			fmt.Fprintf(&b, "--- %s\n\n+++ %s\n\n", h, h)
			b.WriteString(spaceAfterHunks(d.Diff))
		}
		return b.Bytes(), nil
	case FormatObject:
		var b bytes.Buffer
		for _, d := range diffs {
			b.WriteString(d.Header())
			b.WriteByte('\n')
			b.WriteString(d.Diff)
			b.WriteByte('\n')
		}
		return b.Bytes(), nil
	case FormatYAML:
		return yaml.Marshal(diffs)
	case FormatJSON:
		return json.MarshalIndent(diffs, "", "  ")
	}
	return nil, fmt.Errorf("unknown diff format %q", format)
}

type pairedResource struct {
	parent                Parent
	kind, namespace, name string
	a, b                  map[string]any
}

type pairKey struct {
	pKind, pNS, pName string
	kind, ns, name    string
}

func pair(left, right []Doc) []pairedResource {
	idx := make(map[pairKey]*pairedResource, len(left)+len(right))
	add := func(side int, d Doc) {
		kind := manifest.DocKind(d.Manifest)
		name, ns := manifest.DocMetadata(d.Manifest)
		k := pairKey{d.Parent.Kind, d.Parent.Namespace, d.Parent.Name, kind, ns, name}
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
		if c := cmp.Compare(a.parent.Kind, b.parent.Kind); c != 0 {
			return c
		}
		if c := cmp.Compare(a.parent.Namespace, b.parent.Namespace); c != 0 {
			return c
		}
		if c := cmp.Compare(a.parent.Name, b.parent.Name); c != 0 {
			return c
		}
		if c := cmp.Compare(a.kind, b.kind); c != 0 {
			return c
		}
		if c := cmp.Compare(a.namespace, b.namespace); c != 0 {
			return c
		}
		return cmp.Compare(a.name, b.name)
	})
	return out
}

func applyStrip(docs []Doc, attrs []string) []Doc {
	if len(attrs) == 0 {
		return docs
	}
	out := make([]Doc, len(docs))
	for i, d := range docs {
		copyDoc := deepCopy(d.Manifest)
		manifest.StripResourceAttributes(copyDoc, attrs)
		out[i] = Doc{Manifest: copyDoc, Parent: d.Parent}
	}
	return out
}

// deepCopy clones a parsed YAML document. The shapes we see are
// JSON-equivalent (map[string]any / []any / scalars) so a structural
// walk is enough and avoids the marshal/unmarshal round-trip cost on
// the hot path.
func deepCopy(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = deepCopyValue(v)
	}
	return out
}

func deepCopyValue(v any) any {
	switch t := v.(type) {
	case map[string]any:
		return deepCopy(t)
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = deepCopyValue(e)
		}
		return out
	default:
		return v
	}
}

// marshalSide serializes one side of a paired manifest. A nil manifest
// (added or deleted between baseline and current) yields an empty
// byte slice, not "null\n", so the diff body never contains a
// "+null" / "-null" line. Non-nil manifests are prefixed with the YAML
// document separator to match flux-local's output.
func marshalSide(m map[string]any) ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	body, err := yaml.Marshal(m)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, len(body)+4)
	out = append(out, "---\n"...)
	out = append(out, body...)
	return out, nil
}

func unified(a, b string, context int) (string, error) {
	return difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
		A:       difflib.SplitLines(a),
		B:       difflib.SplitLines(b),
		Context: context,
	})
}

// spaceAfterHunks inserts a blank line after every "@@ ... @@" hunk
// header so the body of each hunk visually separates from the marker.
func spaceAfterHunks(s string) string {
	if !strings.Contains(s, "@@") {
		return s
	}
	var b strings.Builder
	b.Grow(len(s) + 32)
	for {
		nl := strings.IndexByte(s, '\n')
		if nl < 0 {
			b.WriteString(s)
			return b.String()
		}
		line := s[:nl+1]
		b.WriteString(line)
		if strings.HasPrefix(line, "@@") {
			b.WriteByte('\n')
		}
		s = s[nl+1:]
	}
}
