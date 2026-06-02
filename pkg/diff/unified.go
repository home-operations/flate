package diff

import (
	"bytes"
	"fmt"

	"github.com/pmezard/go-difflib/difflib"
	"sigs.k8s.io/yaml"
)

func joinNS(ns, name string) string {
	if ns == "" {
		return name
	}
	return ns + "/" + name
}

// renderUnified produces a plain unified diff (`diff -u`) of the two doc
// sets: it pairs resources by parent + identity (see pair), diffs each
// changed pair's YAML, and concatenates the bodies. Identical resources
// are dropped. Not Kubernetes-aware — see unifiedBody.
func renderUnified(left, right []Doc, opts Options) ([]byte, error) {
	left = normalizeDocs(left, opts.StripAttrs)
	right = normalizeDocs(right, opts.StripAttrs)
	var b bytes.Buffer
	first := true
	for _, p := range pair(left, right) {
		body, err := unifiedBody(p.a, p.b, p.kind+" "+joinNS(p.namespace, p.name))
		if err != nil {
			return nil, err
		}
		if body == "" {
			continue // identical resources
		}
		if !first {
			b.WriteByte('\n')
		}
		b.WriteString(body)
		first = false
	}
	return b.Bytes(), nil
}

// unifiedBody renders a standard unified diff (`diff -u` / `git diff`
// style) between a resource's two YAML serializations. label names both
// the `---` and `+++` sides identically — it's one resource's before and
// after, so the `-`/`+` line prefixes carry the distinction. A nil side
// (added or removed resource) serializes to the empty document, so the
// whole counterpart shows as an add/remove block.
//
// Unlike the dyff styles this is not Kubernetes-aware: a reordered list
// diffs line-by-line. It exists for users who want output any
// unified-diff tool can consume.
func unifiedBody(a, b map[string]any, label string) (string, error) {
	from, err := marshalForUnified(a)
	if err != nil {
		return "", err
	}
	to, err := marshalForUnified(b)
	if err != nil {
		return "", err
	}
	if from == to {
		return "", nil
	}
	out, err := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
		A:        difflib.SplitLines(from),
		B:        difflib.SplitLines(to),
		FromFile: label,
		ToFile:   label,
		Context:  3,
	})
	if err != nil {
		return "", fmt.Errorf("unified diff: %w", err)
	}
	return out, nil
}

// marshalForUnified serializes a manifest to YAML for line diffing. A
// nil map (added/removed side) yields the empty string so the diff
// reports a wholesale add/remove.
func marshalForUnified(m map[string]any) (string, error) {
	if m == nil {
		return "", nil
	}
	b, err := yaml.Marshal(m)
	if err != nil {
		return "", fmt.Errorf("marshal: %w", err)
	}
	return string(b), nil
}

