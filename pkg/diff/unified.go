package diff

import (
	"bytes"
	"fmt"

	"github.com/pmezard/go-difflib/difflib"
	"sigs.k8s.io/yaml"
)

const diffContextLines = 3

// renderUnified produces a plain unified diff (`diff -u`) of the two doc
// sets: it pairs resources by parent + identity (see pair), diffs each
// changed pair's YAML, and concatenates the bodies. Identical resources
// are dropped. Lists are diffed line by line, without Kubernetes-aware matching.
func renderUnified(left, right []Doc, opts Options) ([]byte, error) {
	var b bytes.Buffer
	first := true
	for p, err := range changedPairs(left, right, opts) {
		if err != nil {
			return nil, err
		}
		label := p.label()
		if p.parent.Path != "" {
			label += " (" + p.parent.Path + ")"
		}
		body, err := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
			A: difflib.SplitLines(p.from), B: difflib.SplitLines(p.to),
			FromFile: label, ToFile: label, Context: diffContextLines,
		})
		if err != nil {
			return nil, fmt.Errorf("unified diff: %w", err)
		}
		if !first {
			b.WriteByte('\n')
		}
		b.WriteString(body)
		first = false
	}
	return withHeader(suppressionHeader(opts.Suppressed, diffStyles[FormatDiff].note), b.Bytes()), nil
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
