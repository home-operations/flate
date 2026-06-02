package diff

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"sigs.k8s.io/yaml"
)

// Render serializes a per-resource diff result set (from Run) into one
// of the structured/aggregated formats: the unified diff, yaml, json, or
// markdown. The dyff text styles (github/human/brief/gitlab/gitea) are
// not rendered here — they go through renderNative (see RenderDocs).
func Render(diffs []ResourceDiff, format Format) ([]byte, error) {
	switch format {
	case FormatDiff:
		return renderUnified(diffs), nil
	case FormatYAML:
		return yaml.Marshal(diffs)
	case FormatJSON:
		return json.MarshalIndent(diffs, "", "  ")
	case FormatMarkdown:
		return renderMarkdown(diffs), nil
	}
	return nil, fmt.Errorf("unsupported structured diff format %q", format)
}

// renderUnified concatenates each resource's unified-diff body. The
// bodies carry their own `--- <res>` / `+++ <res>` labels, so no extra
// header is added — the result is a clean standard diff.
func renderUnified(diffs []ResourceDiff) []byte {
	var b bytes.Buffer
	for _, d := range diffs {
		writeBody(&b, d.Diff)
	}
	return b.Bytes()
}

// renderMarkdown emits a PR-comment-friendly view of the diff set:
// a `# Diff` heading, a pipe-table summary by classification
// (added/modified/removed), and one H3 + ```diff fence per
// ResourceDiff wrapping the github-style body verbatim. An empty diff
// set renders as the empty document so the markdown output can be
// dropped into a PR comment unconditionally without a "no changes"
// placeholder.
func renderMarkdown(diffs []ResourceDiff) []byte {
	if len(diffs) == 0 {
		return nil
	}
	var added, modified, removed int
	for _, d := range diffs {
		switch classify(d.Diff) {
		case changeAdded:
			added++
		case changeRemoved:
			removed++
		case changeModified:
			modified++
		}
	}
	var b bytes.Buffer
	b.WriteString("# Diff\n\n")
	b.WriteString("| Added | Modified | Removed | Total |\n")
	b.WriteString("| --- | --- | --- | --- |\n")
	fmt.Fprintf(&b, "| %d | %d | %d | %d |\n\n", added, modified, removed, len(diffs))
	for _, d := range diffs {
		fmt.Fprintf(&b, "### %s\n\n```diff\n", d.Header())
		writeBody(&b, d.Diff)
		b.WriteString("```\n\n")
	}
	return b.Bytes()
}

// writeBody appends a diff body to b, guaranteeing a single trailing
// newline so concatenated bodies and fence terminators stay aligned.
func writeBody(b *bytes.Buffer, body string) {
	b.WriteString(body)
	if !strings.HasSuffix(body, "\n") {
		b.WriteByte('\n')
	}
}

// changeKind classifies a resource diff as a wholesale add/remove or a
// modification, for the markdown summary table.
type changeKind int

const (
	changeModified changeKind = iota
	changeAdded
	changeRemoved
)

// classify inspects a github-style dyff body. Wholesale additions /
// removals from Run emit a `(root level)` header followed by an `! + `
// / `! - ` map-entries marker; anything else is a per-path
// modification.
func classify(body string) changeKind {
	if !strings.Contains(body, "@@ (root level) @@") {
		return changeModified
	}
	switch {
	case strings.Contains(body, "\n! + "):
		return changeAdded
	case strings.Contains(body, "\n! - "):
		return changeRemoved
	default:
		return changeModified
	}
}
