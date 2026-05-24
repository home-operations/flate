package diff

import (
	"bytes"
	"fmt"

	"github.com/gonvenience/ytbx"
	"github.com/homeport/dyff/pkg/dyff"
	"sigs.k8s.io/yaml"
)

// dyffDiff renders a single resource's diff via dyff in `--output
// github` mode. The output is path-based diff syntax that GitHub
// renders natively as a colored diff block when wrapped in a ```diff
// code fence — markers `@@`, `+`, `-`, `!` are exactly what the
// linguist diff lexer expects.
//
// Either side can be nil to represent an added or removed resource.
// In that case the nil side becomes an empty YAML document so dyff's
// CompareInputFiles still sees two valid inputs; the report renders
// as a wholesale "addition" / "removal" against the empty root.
func dyffDiff(a, b map[string]any) (string, error) {
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
	writer := &dyff.DiffSyntaxReport{
		PathPrefix:            "@@",
		RootDescriptionPrefix: "#",
		ChangeTypePrefix:      "!",
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
	var buf bytes.Buffer
	if err := writer.WriteReport(&buf); err != nil {
		return "", fmt.Errorf("dyff render: %w", err)
	}
	return buf.String(), nil
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
