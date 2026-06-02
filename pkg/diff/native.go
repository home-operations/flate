package diff

import (
	"bytes"
	"fmt"

	"github.com/gonvenience/ytbx"
	"github.com/homeport/dyff/pkg/dyff"
	"sigs.k8s.io/yaml"
)

// renderNative diffs the two doc sets as whole multi-document inputs and
// renders dyff's report directly in the requested style. dyff pairs the
// documents by their Kubernetes identity (apiVersion/kind/namespace/name)
// and labels each diff with it natively — so we don't synthesize a
// per-resource header. This is the path for the dyff text styles
// (github/human/brief/gitlab/gitea); the structured/aggregated formats
// (diff/yaml/json/markdown) keep the per-resource Run+Render pipeline.
//
// Trade-offs accepted here (vs. per-resource diffing): pairing is by
// k8s identity rather than flate's parent, and a set change can draw a
// document-level "order changed" note. Both are rare in practice for
// content diffs.
func renderNative(left, right []Doc, opts Options) ([]byte, error) {
	from, err := multiDocInput("from", normalizeDocs(left, opts.StripAttrs))
	if err != nil {
		return nil, err
	}
	to, err := multiDocInput("to", normalizeDocs(right, opts.StripAttrs))
	if err != nil {
		return nil, err
	}
	// CompareInputFiles defaults to KubernetesEntityDetection=on (the
	// native labels + name-based pairing) and IgnoreOrderChanges=off (so
	// in-resource list reorders still surface as `⇆ order changed`).
	report, err := dyff.CompareInputFiles(from, to)
	if err != nil {
		return nil, fmt.Errorf("dyff compare: %w", err)
	}
	if len(report.Diffs) == 0 {
		return nil, nil
	}
	style := opts.Format
	if style == "" {
		style = FormatGitHub
	}
	writer, err := dyffWriter(report, style)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	if err := writer.WriteReport(&buf); err != nil {
		return nil, fmt.Errorf("dyff render: %w", err)
	}
	return buf.Bytes(), nil
}

// multiDocInput marshals a doc set into a single multi-document YAML
// stream and loads it as one ytbx input, so dyff sees every resource at
// once and can pair them by Kubernetes identity.
func multiDocInput(location string, docs []Doc) (ytbx.InputFile, error) {
	var buf bytes.Buffer
	for _, d := range docs {
		raw, err := yaml.Marshal(d.Manifest)
		if err != nil {
			return ytbx.InputFile{}, fmt.Errorf("marshal %s: %w", location, err)
		}
		buf.WriteString("---\n")
		buf.Write(raw)
	}
	parsed, err := ytbx.LoadDocuments(buf.Bytes())
	if err != nil {
		return ytbx.InputFile{}, fmt.Errorf("load %s: %w", location, err)
	}
	return ytbx.InputFile{Location: location, Documents: parsed}, nil
}
