package diff

import (
	"bytes"
	"fmt"

	"github.com/gonvenience/ytbx"
	"github.com/homeport/dyff/pkg/dyff"
	"sigs.k8s.io/yaml"
)

// dyffBody renders a single resource's diff via dyff in the given
// style (github/human/brief/gitlab/gitea). The diff-syntax styles
// (github/gitlab/gitea) emit path-based markers a forge's diff lexer
// renders as a colored block inside a ```diff fence; human is dyff's
// default colored report; brief is a one-line-per-change summary. The
// style configs mirror dyff's own CLI (internal/cmd/common.go).
//
// Either side can be nil to represent an added or removed resource.
// In that case the nil side becomes an empty YAML document so dyff's
// CompareInputFiles still sees two valid inputs; the report renders
// as a wholesale "addition" / "removal" against the empty root.
func dyffBody(a, b map[string]any, style Format) (string, error) {
	from, err := loadDyffInput("from", a)
	if err != nil {
		return "", err
	}
	to, err := loadDyffInput("to", b)
	if err != nil {
		return "", err
	}
	report, err := dyff.CompareInputFiles(from, to)
	if err != nil {
		return "", fmt.Errorf("dyff compare: %w", err)
	}
	if len(report.Diffs) == 0 {
		// Identical inputs. Returning early avoids dyff emitting a
		// single stray newline that the caller would mistake for a
		// non-empty diff body.
		return "", nil
	}
	writer, err := dyffWriter(report, style)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	if err := writer.WriteReport(&buf); err != nil {
		return "", fmt.Errorf("dyff render: %w", err)
	}
	return buf.String(), nil
}

// dyffWriter builds the dyff ReportWriter for a style. The diff-syntax
// styles differ only in their path/root/change prefixes; human and
// brief use their own report types. Configs mirror dyff's CLI so flate
// output matches `dyff between --output <style>`.
func dyffWriter(report dyff.Report, style Format) (dyff.ReportWriter, error) {
	switch style {
	case FormatGitHub:
		return diffSyntaxReport(report, "@@", "#", "!"), nil
	case FormatGitLab:
		return diffSyntaxReport(report, "=", "=", "#"), nil
	case FormatGitea:
		return diffSyntaxReport(report, "@@", "=", "!"), nil
	case FormatHuman:
		return &dyff.HumanReport{
			Report:                report,
			Indent:                2,
			UseIndentLines:        true,
			OmitHeader:            true,
			MultilineContextLines: 4,
			MinorChangeThreshold:  0.1,
		}, nil
	case FormatBrief:
		return &dyff.BriefReport{Report: report}, nil
	}
	return nil, fmt.Errorf("unsupported dyff style %q", style)
}

// diffSyntaxReport assembles a dyff DiffSyntaxReport with the given
// marker prefixes — the shape shared by the github/gitlab/gitea styles.
func diffSyntaxReport(report dyff.Report, pathPrefix, rootPrefix, changePrefix string) *dyff.DiffSyntaxReport {
	return &dyff.DiffSyntaxReport{
		PathPrefix:            pathPrefix,
		RootDescriptionPrefix: rootPrefix,
		ChangeTypePrefix:      changePrefix,
		HumanReport: dyff.HumanReport{
			Report:                report,
			Indent:                0,
			UseIndentLines:        true,
			NoTableStyle:          true,
			OmitHeader:            true,
			PrefixMultiline:       true,
			MultilineContextLines: 4,
			MinorChangeThreshold:  0.1,
		},
	}
}

// loadDyffInput marshals a manifest map (or nil — representing an
// added/removed resource) into a ytbx.InputFile that
// dyff.CompareInputFiles can consume. A nil map is encoded as the
// YAML empty mapping `{}` so both sides are valid documents.
func loadDyffInput(location string, m map[string]any) (ytbx.InputFile, error) {
	var raw []byte
	if m == nil {
		raw = []byte("{}\n")
	} else {
		b, err := yaml.Marshal(m)
		if err != nil {
			return ytbx.InputFile{}, fmt.Errorf("marshal %s: %w", location, err)
		}
		raw = b
	}
	docs, err := ytbx.LoadDocuments(raw)
	if err != nil {
		return ytbx.InputFile{}, fmt.Errorf("load %s: %w", location, err)
	}
	return ytbx.InputFile{Location: location, Documents: docs}, nil
}
