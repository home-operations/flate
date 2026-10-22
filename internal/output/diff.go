package output

import (
	"bufio"
	"fmt"
	"io"
	"iter"

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
