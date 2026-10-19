package diff

import (
	"bytes"
	"fmt"
	"slices"

	"github.com/gonvenience/ytbx"
	"github.com/homeport/dyff/pkg/dyff"
	"sigs.k8s.io/yaml"

	"github.com/home-operations/flate/internal/output"
)

// Whole-document input preserves dyff's Kubernetes identity pairing and native labels.
func renderNative(left, right []Doc, opts Options) ([]byte, error) {
	normalizedLeft, _ := normalizeDocs(left, opts.StripAttrs, opts.StripFields, opts.Normalize)
	from, err := multiDocInput("from", normalizedLeft)
	if err != nil {
		return nil, err
	}
	normalizedRight, _ := normalizeDocs(right, opts.StripAttrs, opts.StripFields, opts.Normalize)
	to, err := multiDocInput("to", normalizedRight)
	if err != nil {
		return nil, err
	}
	// KubernetesEntityDetection stays enabled for name-based pairing, and
	// IgnoreOrderChanges stays disabled to retain in-resource list reorders.
	report, err := dyff.CompareInputFiles(from, to)
	if err != nil {
		return nil, fmt.Errorf("dyff compare: %w", err)
	}
	report.Diffs = slices.DeleteFunc(report.Diffs, isFileLevelOrderChange)
	var buf bytes.Buffer
	e := output.Encoder{Writer: &buf, Format: output.Format(opts.Format)}
	if err := e.Native(report, suppressionNotes(opts.Suppressed)); err != nil {
		return nil, err
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
