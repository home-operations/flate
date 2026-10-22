// Package output owns serialization and terminal presentation of prepared values.
package output

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"slices"
	"unicode/utf8"

	"sigs.k8s.io/yaml"
)

// Format is the discriminator selected via -o on the CLI.
type Format string

// Format values understood by the -o flag.
const (
	FormatTable  Format = "table"
	FormatYAML   Format = "yaml"
	FormatJSON   Format = "json"
	FormatName   Format = "name"
	FormatHuman  Format = "human"
	FormatGitHub Format = "github"
	FormatDiff   Format = "diff"
	FormatHTML   Format = "html"
	FormatBrief  Format = "brief"
	FormatGitLab Format = "gitlab"
	FormatGitea  Format = "gitea"
)

// Encoder writes prepared values to one destination with resolved format and color.
type Encoder struct {
	Writer io.Writer
	Format Format
	Color  bool
}

// Documents writes a JSON array or a multi-document YAML stream.
func (e Encoder) Documents(docs []map[string]any) error {
	if e.Format == FormatJSON {
		return e.JSON(docs)
	}
	return e.yamlMulti(docs)
}

// Images writes the prepared image list in its supplied order.
func (e Encoder) Images(images []string) error {
	switch e.Format {
	case FormatJSON:
		return e.JSON(images)
	case FormatYAML:
		return e.YAML(images)
	}
	return e.lines(slices.Values(images))
}

// Cluster writes a single cluster summary.
func (e Encoder) Cluster(summary map[string]any) error {
	if e.Format == FormatJSON {
		return e.JSON(summary)
	}
	return e.YAML(summary)
}

// Resources writes the prepared rows or documents for the selected format.
func (e Encoder) Resources(cols []Column, rows []map[string]string, docs []map[string]any) error {
	switch e.Format {
	case FormatYAML:
		return e.yamlMulti(docs)
	case FormatJSON:
		return e.JSON(docs)
	case FormatName:
		return e.lines(func(yield func(string) bool) {
			for _, row := range rows {
				if !yield(row["name"]) {
					return
				}
			}
		})
	}
	return table(e.Writer, cols, rows)
}

// Column describes a single table column.
type Column struct {
	Header string
	Key    string
}

// gutter is the fixed-width padding appended after each column except the last.
const gutter = 4

// table renders rows of map[string]string into a fixed-width table.
// Columns are sized to the widest cell + a 4-char gutter. Widths
// are measured in runes (not bytes) so cells with multi-byte UTF-8
// (paths with non-ASCII, chart names with unicode) align correctly.
// Doesn't account for double-width CJK glyphs — adding a runewidth
// dependency is out of scope; bring it in when CJK output matters.
func table(w io.Writer, cols []Column, rows []map[string]string) error {
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
	// Each cell is padded to width+gutter; +1 for the per-row newline.
	rowWidth := 0
	for _, width := range widths {
		rowWidth += width + gutter
	}
	b.Grow((1 + len(rows)) * (rowWidth + 1))
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
	pad := max(width-utf8.RuneCountInString(value)+gutter, 1)
	for range pad {
		b.WriteByte(' ')
	}
}

// yamlMulti emits a multi-document YAML stream.
func (e Encoder) yamlMulti(docs []map[string]any) error {
	for _, d := range docs {
		out, err := yaml.Marshal(d)
		if err != nil {
			return err
		}
		if _, err := io.WriteString(e.Writer, "---\n"); err != nil {
			return err
		}
		if _, err := e.Writer.Write(out); err != nil {
			return err
		}
	}
	return nil
}

// YAML emits a single document.
func (e Encoder) YAML(value any) error {
	out, err := yaml.Marshal(value)
	if err != nil {
		return err
	}
	_, err = e.Writer.Write(out)
	return err
}

// JSON emits a 2-space-indented JSON document.
func (e Encoder) JSON(value any) error {
	out, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	if _, err := e.Writer.Write(out); err != nil {
		return err
	}
	_, err = io.WriteString(e.Writer, "\n")
	return err
}

func (e Encoder) lines(values iter.Seq[string]) error {
	for value := range values {
		if _, err := fmt.Fprintln(e.Writer, value); err != nil {
			return err
		}
	}
	return nil
}
