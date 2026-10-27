package output

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"

	"github.com/homeport/dyff/pkg/dyff"
	"github.com/pmezard/go-difflib/difflib"
	"sigs.k8s.io/yaml"

	"github.com/home-operations/flate/internal/diffprep"
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

// DiffGroup holds one cluster's prepared diff and presence on each side.
type DiffGroup struct {
	Cluster        string
	Added, Removed bool
	Body           []byte
}

// DiffGroups writes canonical per-cluster JSON changes and suppressions.
func (e Encoder) DiffGroups(groups []DiffGroup) error {
	if len(groups) == 1 && !groups[0].Added && !groups[0].Removed {
		_, err := e.Writer.Write(groups[0].Body)
		return err
	}
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

// LineChange carries canonical diff values and resolved presentation labels.
type LineChange struct {
	Resource, Parent Subject
	ParentPath       string
	From, To         string
	Status, Label    string
}

// PrepareLines collects serialization errors before any report is written.
func PrepareLines(prepared diffprep.Prepared) ([]LineChange, error) {
	var out []LineChange
	for _, p := range prepared.Pairs {
		var sides [2]string
		for i, doc := range [2]map[string]any{p.Old, p.New} {
			if doc == nil {
				continue
			}
			raw, err := yaml.Marshal(doc)
			if err != nil {
				return nil, fmt.Errorf("marshal: %w", err)
			}
			sides[i] = string(raw)
		}
		if sides[0] == sides[1] {
			continue
		}
		out = append(out, LineChange{
			Resource: Subject{ID: p.Resource}, Parent: Subject{ID: p.Parent.NamedResource},
			ParentPath: p.Parent.Path, From: sides[0], To: sides[1], Status: p.Status(),
		})
	}
	return out, nil
}

// LineSubjects inventories only the resource and producer labels this format prints.
func LineSubjects(changes []LineChange, format Format) []manifest.NamedResource {
	ids := make([]manifest.NamedResource, 0, len(changes))
	for _, c := range changes {
		ids = append(ids, c.Resource.ID)
		if format == FormatHTML {
			ids = append(ids, c.Parent.ID)
		}
	}
	return ids
}

// NameLines binds the invocation's resolved subjects to its line changes.
func NameLines(changes []LineChange, names Names) {
	for i := range changes {
		c := &changes[i]
		c.Resource = names.Subjects[c.Resource.ID]
		c.Parent = names.Subjects[c.Parent.ID]
		c.Label = names.Prefix + c.Resource.Kind + " " + c.Resource.ID.NamespacedName()
	}
}

// Lines consumes prepared resource differences once in unified or HTML format.
func (e Encoder) Lines(changes []LineChange) error {
	if e.Format == FormatHTML {
		return e.html(changes)
	}
	separate := false
	for _, p := range changes {
		label := p.Label
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
