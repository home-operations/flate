package diff

import (
	"bytes"
	"cmp"
	"fmt"
	"slices"

	"github.com/gonvenience/ytbx"
	"github.com/homeport/dyff/pkg/dyff"
	yamlv3 "go.yaml.in/yaml/v3"
	"sigs.k8s.io/yaml"

	"github.com/home-operations/flate/internal/output"
	"github.com/home-operations/flate/pkg/manifest"
)

func renderNative(paired []pairedResource, format Format) ([]byte, error) {
	report, err := nativeReport(paired)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	e := output.Encoder{Writer: &buf, Format: output.Format(format)}
	if err := e.Native(report); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func nativeReport(paired []pairedResource) (dyff.Report, error) {
	from, err := nativeInput("from", paired)
	if err != nil {
		return dyff.Report{}, err
	}
	to, err := nativeInput("to", paired)
	if err != nil {
		return dyff.Report{}, err
	}
	report := dyff.Report{From: from, To: to}
	rank := func(p pairedResource) int {
		if p.a == nil {
			return 2
		}
		if p.b == nil {
			return 1
		}
		return 0
	}
	slices.SortFunc(paired, func(a, b pairedResource) int {
		return cmp.Or(cmp.Compare(rank(a), rank(b)), cmp.Compare(a.aPos, b.aPos), cmp.Compare(a.bPos, b.bPos))
	})
	for _, p := range paired {
		switch {
		case p.a == nil && p.b == nil:
			continue
		case p.a == nil:
			report.Diffs = append(report.Diffs, dyff.Diff{
				Path:    &ytbx.Path{Root: &report.To, DocumentIdx: p.bPos},
				Details: []dyff.Detail{{Kind: dyff.ADDITION, To: to.Documents[p.bPos]}},
			})
		case p.b == nil:
			report.Diffs = append(report.Diffs, dyff.Diff{
				Path:    &ytbx.Path{Root: &report.From, DocumentIdx: p.aPos},
				Details: []dyff.Detail{{Kind: dyff.REMOVAL, From: from.Documents[p.aPos]}},
			})
		default:
			if _, differs := classify(p.a, p.b); !differs {
				continue
			}
			compared, err := dyff.CompareInputFiles(
				nativeWrapped(from.Documents[p.aPos]), nativeWrapped(to.Documents[p.bPos]),
			)
			if err != nil {
				return dyff.Report{}, fmt.Errorf("dyff compare: %w", err)
			}
			for _, d := range compared.Diffs {
				// The private outer map prevents dyff from matching documents again;
				// Kubernetes detection must remain enabled for nested named lists.
				d.Path.PathElements = d.Path.PathElements[1:]
				d.Path.Root, d.Path.DocumentIdx = &report.From, p.aPos
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

func nativeInput(location string, paired []pairedResource) (ytbx.InputFile, error) {
	slices.SortFunc(paired, func(a, b pairedResource) int {
		if location == "from" {
			return cmp.Compare(a.aPos, b.aPos)
		}
		return cmp.Compare(a.bPos, b.bPos)
	})
	var buf bytes.Buffer
	input := ytbx.InputFile{Location: location}
	for i := range paired {
		p := &paired[i]
		m, pos := p.a, &p.aPos
		if location == "to" {
			m, pos = p.b, &p.bPos
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
