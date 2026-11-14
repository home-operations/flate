package diff

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/charmbracelet/colorprofile"
	"github.com/gonvenience/bunt"
	"github.com/homeport/dyff/pkg/dyff"
	"github.com/pmezard/go-difflib/difflib"
	"sigs.k8s.io/yaml"

	"github.com/home-operations/flate/pkg/manifest"
)

type encoder struct {
	Writer io.Writer
	Format Format
}

const (
	diffContextLines         = 3
	dyffContextLines         = 4
	nativeReportIndent       = 2
	dyffMinorChangeThreshold = 0.1
)

type diffGroup struct {
	Cluster        string
	Added, Removed bool
	Body           []byte
}

func (e encoder) diffGroups(groups []diffGroup) error {
	if len(groups) == 1 && !groups[0].Added && !groups[0].Removed {
		if _, err := e.Writer.Write(groups[0].Body); err != nil {
			return fmt.Errorf("write cluster diff: %w", err)
		}
		return nil
	}
	values := make([]map[string]json.RawMessage, len(groups))
	for i, g := range groups {
		value := make(map[string]json.RawMessage)
		if len(g.Body) > 0 {
			if err := json.Unmarshal(g.Body, &value); err != nil {
				return fmt.Errorf("decode cluster diff: %w", err)
			}
		}
		cluster, err := json.Marshal(g.Cluster)
		if err != nil {
			return fmt.Errorf("encode cluster name: %w", err)
		}
		value["cluster"] = cluster
		if g.Added {
			value["added"] = json.RawMessage("true")
		}
		if g.Removed {
			value["removed"] = json.RawMessage("true")
		}
		values[i] = value
	}
	return e.json(values)
}

type lineChange struct {
	Resource, Parent Subject
	Output           [2]manifest.NamedResource
	ParentPath       string
	From, To         string
	Status, Label    string
}

// prepareLines collects serialization errors before any report is written.
func prepareLines(prepared prepared) ([]lineChange, error) {
	var out []lineChange
	for pIndex := range prepared.Pairs {
		p := prepared.Pairs[pIndex]
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
		out = append(out, lineChange{
			Resource: Subject{ID: p.Resource}, Parent: Subject{ID: p.Parent.NamedResource},
			Output:     p.Output,
			ParentPath: p.Parent.Path, From: sides[0], To: sides[1], Status: p.status(),
		})
	}
	return out, nil
}

func lineInventory(changes []lineChange, format Format) GroupResult {
	inventory := GroupResult{Subjects: make([]manifest.NamedResource, 0, len(changes))}
	for i := range changes {
		c := &changes[i]
		inventory.Subjects = append(inventory.Subjects, c.Resource.ID)
		if format == FormatHTML {
			inventory.Subjects = append(inventory.Subjects, c.Parent.ID)
		}
		for side, content := range [2]string{c.From, c.To} {
			if content != "" {
				inventory.Output[side] = append(inventory.Output[side], c.Output[side])
			}
		}
	}
	return inventory
}

func nameLines(changes []lineChange, names Names) {
	for i := range changes {
		c := &changes[i]
		c.Resource = names.Subjects[c.Resource.ID]
		c.Parent = names.Subjects[c.Parent.ID]
		c.Label = names.Prefix + c.Resource.Kind + " " + c.Resource.ID.NamespacedName()
	}
}

func (e encoder) lines(changes []lineChange) error {
	if e.Format == FormatHTML {
		return e.html(changes)
	}
	separate := false
	for pIndex := range changes {
		p := changes[pIndex]
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
				return fmt.Errorf("write diff: %w", err)
			}
		}
		if _, err := io.WriteString(e.Writer, body); err != nil {
			return fmt.Errorf("write diff: %w", err)
		}
		separate = true
	}
	return nil
}

//nolint:ireturn // Dyff defines the common writer interface for its concrete report formats.
func nativeStyle(format Format, report *dyff.Report) dyff.ReportWriter {
	switch format {
	case FormatGitHub:
		return diffSyntax("@@", "#", "!")(*report)
	case FormatGitLab:
		return diffSyntax("=", "=", "#")(*report)
	case FormatGitea:
		return diffSyntax("@@", "=", "!")(*report)
	case FormatHuman:
		return &dyff.HumanReport{
			Report: *report, Indent: nativeReportIndent, UseIndentLines: true, OmitHeader: true,
			MultilineContextLines: dyffContextLines, MinorChangeThreshold: dyffMinorChangeThreshold,
		}
	case FormatBrief:
		return &dyff.BriefReport{Report: *report}
	case FormatJSON, FormatDiff, FormatHTML:
		return nil
	}
	return nil
}

func (e encoder) native(report *dyff.Report) error {
	// dyff emits a stray newline for an empty report.
	if len(report.Diffs) == 0 {
		return nil
	}
	color := bunt.OFF
	if colorprofile.Detect(e.Writer, os.Environ()) > colorprofile.Ascii {
		color = bunt.ON
	}
	bunt.SetColorSettings(color, bunt.AUTO)
	// dyff reuses this buffer but ignores its final Flush error.
	writer := bufio.NewWriter(e.Writer)
	err := nativeStyle(e.Format, report).WriteReport(writer)
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
			PathPrefix:            path,
			RootDescriptionPrefix: root,
			ChangeTypePrefix:      change,
			Report:                report,
			Indent:                0,
			UseIndentLines:        true,
			NoTableStyle:          true,
			OmitHeader:            true,
			PrefixMultiline:       true,
			MultilineContextLines: dyffContextLines,
			MinorChangeThreshold:  dyffMinorChangeThreshold,
		}
	}
}

func (e encoder) json(value any) error {
	out, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("encode output: %w", err)
	}
	if _, writeErr := e.Writer.Write(out); writeErr != nil {
		return fmt.Errorf("write output: %w", writeErr)
	}
	if _, err = io.WriteString(e.Writer, "\n"); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}
