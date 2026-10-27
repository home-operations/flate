package diffprep

import (
	"bytes"
	"cmp"
	"fmt"
	"slices"

	"github.com/gonvenience/ytbx"
	"github.com/homeport/dyff/pkg/dyff"
	yamlv3 "go.yaml.in/yaml/v3"
	"sigs.k8s.io/yaml"

	"github.com/home-operations/flate/pkg/manifest"
)

// Native prepares dyff inputs and compares each canonical pair exactly once.
func (p Prepared) Native() (dyff.Report, error) {
	paired := p.Pairs
	from, err := nativeInput("from", paired)
	if err != nil {
		return dyff.Report{}, err
	}
	to, err := nativeInput("to", paired)
	if err != nil {
		return dyff.Report{}, err
	}
	report := dyff.Report{From: from, To: to}
	rank := func(p Pair) int {
		if p.Old == nil {
			return 2
		}
		if p.New == nil {
			return 1
		}
		return 0
	}
	slices.SortFunc(paired, func(a, b Pair) int {
		return cmp.Or(cmp.Compare(rank(a), rank(b)), cmp.Compare(a.OldPos, b.OldPos), cmp.Compare(a.NewPos, b.NewPos))
	})
	for _, p := range paired {
		switch {
		case p.Old == nil && p.New == nil:
			continue
		case p.Old == nil:
			report.Diffs = append(report.Diffs, dyff.Diff{
				Path:    &ytbx.Path{Root: &report.To, DocumentIdx: p.NewPos},
				Details: []dyff.Detail{{Kind: dyff.ADDITION, To: to.Documents[p.NewPos]}},
			})
		case p.New == nil:
			report.Diffs = append(report.Diffs, dyff.Diff{
				Path:    &ytbx.Path{Root: &report.From, DocumentIdx: p.OldPos},
				Details: []dyff.Detail{{Kind: dyff.REMOVAL, From: from.Documents[p.OldPos]}},
			})
		default:
			if p.Status() == "" {
				continue
			}
			compared, err := dyff.CompareInputFiles(
				nativeWrapped(from.Documents[p.OldPos]), nativeWrapped(to.Documents[p.NewPos]),
			)
			if err != nil {
				return dyff.Report{}, fmt.Errorf("dyff compare: %w", err)
			}
			for _, d := range compared.Diffs {
				// The private outer map prevents dyff from matching documents again;
				// Kubernetes detection must remain enabled for nested named lists.
				d.Path.PathElements = d.Path.PathElements[1:]
				d.Path.Root, d.Path.DocumentIdx = &report.From, p.OldPos
				report.Diffs = append(report.Diffs, d)
			}
		}
	}
	return report, nil
}

func nativeWrapped(node *yamlv3.Node) ytbx.InputFile {
	return ytbx.InputFile{Documents: []*yamlv3.Node{{
		Kind: yamlv3.DocumentNode,
		Content: []*yamlv3.Node{{Kind: yamlv3.MappingNode, Tag: "!!map", Content: []*yamlv3.Node{
			{Kind: yamlv3.ScalarNode, Tag: "!!str", Value: "resource"}, node.Content[0],
		}}},
	}}}
}

func nativeInput(location string, paired []Pair) (ytbx.InputFile, error) {
	slices.SortFunc(paired, func(a, b Pair) int {
		if location == "from" {
			return cmp.Compare(a.OldPos, b.OldPos)
		}
		return cmp.Compare(a.NewPos, b.NewPos)
	})
	var buf bytes.Buffer
	input := ytbx.InputFile{Location: location}
	for i := range paired {
		p := &paired[i]
		m, pos := p.Old, &p.OldPos
		if location == "to" {
			m, pos = p.New, &p.NewPos
		}
		if m == nil {
			continue
		}
		raw, err := yaml.Marshal(m)
		if err != nil {
			return ytbx.InputFile{}, fmt.Errorf("marshal %s: %w", location, err)
		}
		buf.WriteString("---\n")
		buf.Write(raw)
		*pos = len(input.Names)
		id := manifest.DocIdentity(m)
		label := manifest.DocAPIVersion(m) + "/" + id.Kind + "/"
		meta, _ := m["metadata"].(map[string]any)
		if _, ok := meta["namespace"]; ok {
			label += id.Namespace + "/"
		}
		if input.Names == nil {
			input.Names = make([]string, 0, len(paired))
		}
		input.Names = append(input.Names, label+id.Name)
	}
	if buf.Len() != 0 {
		parsed, err := ytbx.LoadDocuments(buf.Bytes())
		if err != nil {
			return ytbx.InputFile{}, fmt.Errorf("load %s: %w", location, err)
		}
		input.Documents = parsed
	}
	return input, nil
}
