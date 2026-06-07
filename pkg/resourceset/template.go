package resourceset

import (
	"bytes"
	"fmt"
	"sync"
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

// bufPool reuses byte buffers across template executions to avoid a
// per-document allocation on the render hot path.
var bufPool = sync.Pool{New: func() any { return new(bytes.Buffer) }}

// parsedTemplate holds a compiled template together with an indirection
// cell for the `inputs` func so we can swap the active input set
// without re-parsing. The cell is NOT safe for concurrent use — callers
// must create one parsedTemplate per goroutine (via parseTemplate).
type parsedTemplate struct {
	tmpl *template.Template
	cell *map[string]any // `inputs` func closes over this pointer
}

// parseTemplate compiles tmplStr once. The returned parsedTemplate is
// NOT concurrency-safe; hot-path callers should parse once per resource
// entry, then call execute for each input set sequentially.
func parseTemplate(tmplStr string) (*parsedTemplate, error) {
	cell := new(map[string]any)
	tmpl, err := template.New("resourceset").
		Delims("<<", ">>").
		Funcs(sprig.HermeticTxtFuncMap()).
		Funcs(template.FuncMap{
			"slugify":    slug.Make,
			"toYaml":     toYaml,
			"mustToYaml": mustToYaml,
			// inputs returns the current input set via an indirection cell
			// swapped by executeTemplate without re-parsing.
			"inputs": func() any { return *cell },
		}).
		Option("missingkey=error").
		Parse(tmplStr)
	if err != nil {
		return nil, fmt.Errorf("parse template: %w", err)
	}
	return &parsedTemplate{tmpl: tmpl, cell: cell}, nil
}

// executeTemplate runs pt with the given input set, reusing a pooled
// buffer to reduce GC pressure. pt.cell is updated atomically before
// execution; callers must not share a parsedTemplate across goroutines.
func executeTemplate(pt *parsedTemplate, inputSet map[string]any) ([]byte, error) {
	*pt.cell = inputSet

	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufPool.Put(buf)

	if err := pt.tmpl.Execute(buf, nil); err != nil {
		return nil, fmt.Errorf("execute template: %w", err)
	}
	return bytes.Clone(buf.Bytes()), nil
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
	return string(out), err
}
