package resourceset

import (
	"bytes"
	"fmt"
	"strings"
	"text/template"

	sprig "github.com/go-task/slim-sprig/v3"
	"github.com/gosimple/slug"
	"sigs.k8s.io/yaml"
)

// init mirrors upstream flux-operator's slugify configuration
// (internal/builder/resourceset.go init): 63-char max with smart-
// truncate, matching Kubernetes label-value limits. Without this,
// slugify on inputs ≥64 chars renders the full slug in flate but a
// truncated 63-char prefix in cluster — different downstream resource
// names for the same ResourceSet input.
func init() {
	slug.MaxLength = 63
	slug.EnableSmartTruncate = true
}

func execute(tmplStr string, inputSet map[string]any) ([]byte, error) {
	tmpl, err := template.New("resourceset").
		Delims("<<", ">>").
		Funcs(sprig.HermeticTxtFuncMap()).
		Funcs(template.FuncMap{
			"slugify":    slug.Make,
			"toYaml":     toYaml,
			"mustToYaml": mustToYaml,
			"inputs":     func() any { return inputSet },
		}).
		Option("missingkey=error").
		Parse(tmplStr)
	if err != nil {
		return nil, fmt.Errorf("parse template: %w", err)
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, nil); err != nil {
		return nil, fmt.Errorf("execute template: %w", err)
	}
	return buf.Bytes(), nil
}

// toYaml mirrors upstream's silent-error variant — encodes v as YAML;
// any marshal error collapses to "". Templates expect this signature
// so authors don't have to wrap every call in {{ if }}…{{ end }}.
func toYaml(v any) string {
	s, err := mustToYaml(v)
	if err != nil {
		return ""
	}
	return s
}

// mustToYaml is the explicit error-returning variant — surfaces a
// marshal error to the template engine, which aborts execution. Use
// this when the template is willing to fail loudly on malformed inputs.
func mustToYaml(v any) (string, error) {
	out, err := yaml.Marshal(v)
	if err != nil {
		return "", err
	}
	return strings.TrimRight(string(out), "\n"), nil
}
