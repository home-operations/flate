// Package format provides the table, YAML, JSON, and "name" output
// modes used across flate's CLI surface.
package format

import (
	"bytes"
	"encoding/json"
	"io"
	"unicode/utf8"

	"sigs.k8s.io/yaml"
)

// Output is the discriminator selected via -o on the CLI.
type Output string

// Output values understood by the -o flag.
const (
	OutputTable Output = "table"
	OutputYAML  Output = "yaml"
	OutputJSON  Output = "json"
	OutputName  Output = "name"
)

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
	for _, width := range widths {
		totalCols += width + 4
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
