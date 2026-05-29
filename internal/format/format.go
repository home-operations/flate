// Package format provides the table, YAML, JSON, and "name" output
// modes used across flate's CLI surface.
package format

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

	"sigs.k8s.io/yaml"
)

// Output is the discriminator selected via -o on the CLI.
type Output string

// Output values understood by the -o flag.
const (
	OutputTable    Output = "table"
	OutputYAML     Output = "yaml"
	OutputJSON     Output = "json"
	OutputName     Output = "name"
	OutputMarkdown Output = "markdown"
	// OutputText is the implicit default for `flate test`. The constant
	// exists so test.go can dispatch on it explicitly rather than relying
	// on a string literal; the rendered output is the existing pytest-
	// style report from testrunner.Report.Write.
	OutputText Output = "text"
)

// TaskItem is one entry in a GitHub-flavored task list. Checked
// drives `- [x]` vs `- [ ]`. When Detail is non-empty, it's wrapped
// in a <details><summary>…</summary> block under the bullet so long
// failure bodies stay collapsed in PR comments.
type TaskItem struct {
	Label   string
	Checked bool
	// Summary is the short label shown in the <details>'s <summary>
	// tag — typically a one-line failure reason.
	Summary string
	// Detail is the long body shown when the <details> is expanded.
	// Empty Detail suppresses the <details> block entirely.
	Detail string
}

// Column describes a single table column.
type Column struct {
	Header string
	Key    string
}

// Table renders rows of map[string]string into a fixed-width table.
// Columns are sized to the widest cell + a 4-char gutter. Widths
// are measured in runes (not bytes) so cells with multi-byte UTF-8
// (paths with non-ASCII, chart names with unicode) align correctly.
// Doesn't account for double-width CJK glyphs — adding a runewidth
// dependency is out of scope; bring it in when CJK output matters.
func Table(w io.Writer, cols []Column, rows []map[string]string) error {
	widths := make([]int, len(cols))
	for i, c := range cols {
		widths[i] = utf8.RuneCountInString(c.Header)
	}
	for _, r := range rows {
		for i, c := range cols {
			if l := utf8.RuneCountInString(r[c.Key]); l > widths[i] {
				widths[i] = l
			}
		}
	}
	var b bytes.Buffer
	// +1 newline per row (header + data rows); each cell padded to width+gutter
	totalCols := 0
	for _, w := range widths {
		totalCols += w + 4
	}
	b.Grow((1 + len(rows)) * (totalCols + 1))
	last := len(cols) - 1
	for i, c := range cols {
		writeCol(&b, c.Header, widths[i], i == last)
	}
	b.WriteByte('\n')
	for _, r := range rows {
		for i, c := range cols {
			writeCol(&b, r[c.Key], widths[i], i == last)
		}
		b.WriteByte('\n')
	}
	_, err := w.Write(b.Bytes())
	return err
}

func writeCol(b *bytes.Buffer, value string, width int, last bool) {
	b.WriteString(value)
	if last {
		return
	}
	// Write padding directly to avoid the temporary string that strings.Repeat allocates.
	pad := max(width-utf8.RuneCountInString(value)+4, 1)
	for range pad {
		b.WriteByte(' ')
	}
}

// YAMLMulti emits a multi-document YAML stream.
func YAMLMulti(w io.Writer, docs []map[string]any) error {
	for _, d := range docs {
		out, err := yaml.Marshal(d)
		if err != nil {
			return err
		}
		if _, err := io.WriteString(w, "---\n"); err != nil {
			return err
		}
		if _, err := w.Write(out); err != nil {
			return err
		}
	}
	return nil
}

// YAML emits a single document.
func YAML(w io.Writer, value any) error {
	out, err := yaml.Marshal(value)
	if err != nil {
		return err
	}
	_, err = w.Write(out)
	return err
}

// JSON emits a 2-space-indented JSON document.
func JSON(w io.Writer, value any) error {
	out, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	if _, err := w.Write(out); err != nil {
		return err
	}
	_, err = io.WriteString(w, "\n")
	return err
}

// Name emits one resource name per line.
func Name(w io.Writer, items []map[string]string, key string) error {
	var b bytes.Buffer
	b.Grow(len(items) * 32) // rough estimate: 32 bytes per name
	for _, it := range items {
		b.WriteString(it[key])
		b.WriteByte('\n')
	}
	_, err := w.Write(b.Bytes())
	return err
}

// MarkdownTable renders rows as a GitHub-flavored Markdown pipe table.
// Pipes inside cell values are escaped to keep the table well-formed
// when the output is embedded in PR comments or release notes.
func MarkdownTable(w io.Writer, cols []Column, rows []map[string]string) error {
	var b bytes.Buffer
	// Rough sizing: header + separator + one line per row, ~16 bytes per cell.
	b.Grow((2 + len(rows)) * (len(cols)*16 + 4))
	// Header row.
	b.WriteByte('|')
	for _, c := range cols {
		b.WriteByte(' ')
		b.WriteString(escapeMarkdownCell(c.Header))
		b.WriteString(" |")
	}
	b.WriteByte('\n')
	// Separator row.
	b.WriteByte('|')
	for range cols {
		b.WriteString(" --- |")
	}
	b.WriteByte('\n')
	// Data rows.
	for _, r := range rows {
		b.WriteByte('|')
		for _, c := range cols {
			b.WriteByte(' ')
			b.WriteString(escapeMarkdownCell(r[c.Key]))
			b.WriteString(" |")
		}
		b.WriteByte('\n')
	}
	_, err := w.Write(b.Bytes())
	return err
}

// escapeMarkdownCell escapes characters that would break a GFM pipe
// table cell — currently just `|`. Newlines inside cells are replaced
// with `<br>` so multi-line values don't terminate the row.
func escapeMarkdownCell(s string) string {
	if !strings.ContainsAny(s, "|\n") {
		return s
	}
	r := strings.NewReplacer("|", `\|`, "\n", "<br>")
	return r.Replace(s)
}

// MarkdownDocs emits each doc as a fenced YAML block, preceded by an
// H3 header derived from kind/namespace/name. Cluster-scoped docs
// (empty namespace) render `### Kind/name`; namespaced docs render
// `### Kind/namespace/name`. Docs missing any of those fields fall
// back to a generic `### Document N` header.
func MarkdownDocs(w io.Writer, docs []map[string]any) error {
	var b bytes.Buffer
	for i, d := range docs {
		if i > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(markdownDocHeader(d, i))
		b.WriteString("\n\n")
		b.WriteString("```yaml\n")
		out, err := yaml.Marshal(d)
		if err != nil {
			return err
		}
		b.Write(out)
		// yaml.Marshal already terminates with '\n'; close the fence.
		b.WriteString("```\n")
	}
	_, err := w.Write(b.Bytes())
	return err
}

func markdownDocHeader(d map[string]any, idx int) string {
	kind, _ := d["kind"].(string)
	var ns, name string
	if md, ok := d["metadata"].(map[string]any); ok {
		name, _ = md["name"].(string)
		ns, _ = md["namespace"].(string)
	}
	if kind == "" || name == "" {
		return fmt.Sprintf("### Document %d", idx+1)
	}
	if ns == "" {
		return fmt.Sprintf("### %s/%s", kind, name)
	}
	return fmt.Sprintf("### %s/%s/%s", kind, ns, name)
}

// MarkdownTaskList emits a GitHub-flavored task list. Each item is a
// bullet with a checked/unchecked box. When Detail is non-empty, the
// item carries a collapsible <details> block (with Summary in the
// <summary> tag and Detail in a fenced code block). When Detail is
// empty but Summary is non-empty, Summary is appended inline after
// an em dash — the skip-case shorthand.
func MarkdownTaskList(w io.Writer, items []TaskItem) error {
	var b bytes.Buffer
	b.Grow(len(items) * 64)
	for _, it := range items {
		box := "[ ]"
		if it.Checked {
			box = "[x]"
		}
		b.WriteString("- ")
		b.WriteString(box)
		b.WriteByte(' ')
		b.WriteString(it.Label)
		switch {
		case it.Detail != "":
			b.WriteByte('\n')
			b.WriteString("  <details><summary>")
			b.WriteString(it.Summary)
			b.WriteString("</summary>\n\n")
			b.WriteString("  ```\n")
			b.WriteString(it.Detail)
			if !strings.HasSuffix(it.Detail, "\n") {
				b.WriteByte('\n')
			}
			b.WriteString("  ```\n")
			b.WriteString("  </details>\n")
		case it.Summary != "":
			b.WriteString(" — ")
			b.WriteString(it.Summary)
			b.WriteByte('\n')
		default:
			b.WriteByte('\n')
		}
	}
	_, err := w.Write(b.Bytes())
	return err
}
