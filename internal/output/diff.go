package output

import (
	"bufio"
	"encoding/json"
	"fmt"
	"html"
	"io"
	"iter"
	"strings"

	"github.com/homeport/dyff/pkg/dyff"
	"github.com/pmezard/go-difflib/difflib"

	"github.com/home-operations/flate/pkg/manifest"
)

// DiffFormats lists accepted diff formats in help order, with the default first.
// Callers must not modify this list.
var DiffFormats = []Format{
	FormatHuman, FormatGitHub, FormatDiff, FormatHTML,
	FormatBrief, FormatGitLab, FormatGitea, FormatJSON,
}

const (
	diffContextLines         = 3
	dyffContextLines         = 4
	dyffMinorChangeThreshold = 0.1
)

// LineChange is a prepared resource difference with its producer and YAML sides.
type LineChange struct {
	Resource, Parent manifest.NamedResource
	ParentPath       string
	From, To         string
	Status           string
}

// DiffGroup holds one cluster's prepared diff and presence on each side.
type DiffGroup struct {
	Cluster        string
	Added, Removed bool
	Body           []byte
}

// DiffGroups writes cluster sections around prepared diff bodies.
func (e Encoder) DiffGroups(groups []DiffGroup) error {
	if len(groups) == 1 && !groups[0].Added && !groups[0].Removed {
		_, err := e.Writer.Write(groups[0].Body)
		return err
	}
	if e.Format == FormatJSON {
		values := make([]map[string]json.RawMessage, len(groups))
		for i, g := range groups {
			value := make(map[string]json.RawMessage)
			if len(g.Body) > 0 {
				if err := json.Unmarshal(g.Body, &value); err != nil {
					return fmt.Errorf("decode cluster diff: %w", err)
				}
			}
			value["cluster"], _ = json.Marshal(g.Cluster)
			if g.Added {
				value["added"] = json.RawMessage("true")
			}
			if g.Removed {
				value["removed"] = json.RawMessage("true")
			}
			values[i] = value
		}
		return e.JSON(values)
	}
	var b strings.Builder
	if e.Format == FormatHTML {
		b.WriteString("<!doctype html>\n<html lang=\"en\"><meta charset=\"utf-8\"><title>flate diff</title><body>\n")
	}
	for _, g := range groups {
		label := clusterLabel(g.Cluster, g.Added, g.Removed)
		switch e.Format {
		case FormatHTML:
			fmt.Fprintf(&b, "<section><h2>%s</h2>", html.EscapeString(label))
			if len(g.Body) > 0 {
				fmt.Fprintf(&b, "<iframe title=\"%s\" style=\"width:100%%;height:75vh;border:0\" srcdoc=\"%s\"></iframe>", html.EscapeString(label), html.EscapeString(string(g.Body)))
			}
			b.WriteString("</section>\n")
		case FormatGitHub, FormatGitLab, FormatGitea:
			fmt.Fprintf(&b, "## %s\n\n", label)
			b.Write(g.Body)
			b.WriteByte('\n')
		default:
			fmt.Fprintf(&b, "%s\n", label)
			b.Write(g.Body)
			b.WriteByte('\n')
		}
	}
	if e.Format == FormatHTML {
		b.WriteString("</body></html>\n")
	}
	_, err := io.WriteString(e.Writer, b.String())
	return err
}

// Lines consumes prepared resource differences once in unified or HTML format.
func (e Encoder) Lines(changes iter.Seq2[LineChange, error]) error {
	if e.Format == FormatHTML {
		return e.html(changes)
	}
	separate := false
	for p, err := range changes {
		if err != nil {
			return err
		}
		label := p.Resource.KindLabel() + " " + p.Resource.NamespacedName()
		if p.ParentPath != "" {
			label += " (" + p.ParentPath + ")"
		}
		body, err := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
			A: difflib.SplitLines(p.From), B: difflib.SplitLines(p.To),
			FromFile: label, ToFile: label, Context: diffContextLines,
		})
		if err != nil {
			return fmt.Errorf("unified diff: %w", err)
		}
		if separate {
			if _, err := io.WriteString(e.Writer, "\n"); err != nil {
				return err
			}
		}
		if _, err := io.WriteString(e.Writer, body); err != nil {
			return err
		}
		separate = true
	}
	return nil
}

var nativeStyles = map[Format]func(dyff.Report) dyff.ReportWriter{
	FormatGitHub: diffSyntax("@@", "#", "!"),
	FormatGitLab: diffSyntax("=", "=", "#"),
	FormatGitea:  diffSyntax("@@", "=", "!"),
	FormatHuman: func(report dyff.Report) dyff.ReportWriter {
		return &dyff.HumanReport{
			Report: report, Indent: 2, UseIndentLines: true, OmitHeader: true,
			MultilineContextLines: dyffContextLines, MinorChangeThreshold: dyffMinorChangeThreshold,
		}
	},
	FormatBrief: func(report dyff.Report) dyff.ReportWriter {
		return &dyff.BriefReport{Report: report}
	},
}

// Native writes a prepared dyff report.
func (e Encoder) Native(report dyff.Report) error {
	// dyff emits a stray newline for an empty report.
	if len(report.Diffs) == 0 {
		return nil
	}
	// dyff reuses this buffer but ignores its final Flush error.
	writer := bufio.NewWriter(e.Writer)
	err := nativeStyles[e.Format](report).WriteReport(writer)
	if err == nil {
		err = writer.Flush()
	}
	if err != nil {
		return fmt.Errorf("dyff render: %w", err)
	}
	return nil
}

func diffSyntax(path, root, change string) func(dyff.Report) dyff.ReportWriter {
	return func(report dyff.Report) dyff.ReportWriter {
		return &dyff.DiffSyntaxReport{
			PathPrefix: path, RootDescriptionPrefix: root, ChangeTypePrefix: change,
			Report: report, Indent: 0, UseIndentLines: true, NoTableStyle: true, OmitHeader: true,
			PrefixMultiline: true, MultilineContextLines: dyffContextLines, MinorChangeThreshold: dyffMinorChangeThreshold,
		}
	}
}
