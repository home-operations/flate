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

// DocumentGroup holds one cluster's prepared documents.
type DocumentGroup struct {
	Cluster   string           `json:"cluster"`
	Documents []map[string]any `json:"documents"`
}

// ImageGroup holds one cluster's prepared images.
type ImageGroup struct {
	Cluster string   `json:"cluster"`
	Images  []string `json:"images"`
	Added   bool     `json:"added,omitempty"`
	Removed bool     `json:"removed,omitempty"`
	Names   []string `json:"-"`
}

// DocumentGroups writes cluster framing around multiple document groups.
func (e Encoder) DocumentGroups(groups []DocumentGroup) error {
	if e.Format == FormatJSON {
		if len(groups) == 1 {
			return e.JSON(groups[0].Documents)
		}
		return e.JSON(groups)
	}
	return e.documentGroups(groups)
}

// ImageGroups writes cluster framing around multiple image groups.
func (e Encoder) ImageGroups(groups []ImageGroup) error {
	if len(groups) == 1 && !groups[0].Added && !groups[0].Removed {
		if e.Format == FormatJSON || e.Format == FormatYAML {
			return e.Images(groups[0].Images)
		}
	}
	switch e.Format {
	case FormatJSON:
		return e.JSON(groups)
	case FormatYAML:
		return e.YAML(groups)
	}
	return e.Names(func(yield func(string) bool) {
		for _, g := range groups {
			for _, image := range g.Names {
				if !yield(image) {
					return
				}
			}
		}
	})
}

// Documents writes a JSON array or a multi-document YAML stream.
func (e Encoder) Documents(docs []map[string]any) error {
	if e.Format == FormatJSON {
		return e.JSON(docs)
	}
	return e.documentGroups([]DocumentGroup{{Documents: docs}})
}

// Images writes the prepared image list in its supplied order.
func (e Encoder) Images(images []string) error {
	switch e.Format {
	case FormatJSON:
		return e.JSON(images)
	case FormatYAML:
		return e.YAML(images)
	}
	return e.Names(slices.Values(images))
}

// Table holds final columns and rows for one prepared table section.
type Table struct {
	Columns []Column
	Rows    []map[string]string
}

// Tables writes ordered table sections with one blank line between them.
func (e Encoder) Tables(sections []Table) error {
	for i, section := range sections {
		if i > 0 {
			if _, err := io.WriteString(e.Writer, "\n"); err != nil {
				return err
			}
		}
		if err := table(e.Writer, section.Columns, section.Rows); err != nil {
			return err
		}
	}
	return nil
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

// Names writes prepared names in their supplied order.
func (e Encoder) Names(values iter.Seq[string]) error {
	for value := range values {
		if _, err := fmt.Fprintln(e.Writer, value); err != nil {
			return err
		}
	}
	return nil
}
