// Package resourcesetinput owns ResourceSet input composition and template evaluation.
package resourcesetinput

import (
	"bytes"
	"fmt"
	"sync"
	"text/template"

	sprig "github.com/go-task/slim-sprig/v3"
	"github.com/gosimple/slug"
	"sigs.k8s.io/yaml"
)

func init() {
	slug.MaxLength = 63
	slug.EnableSmartTruncate = true
}

var bufPool = sync.Pool{New: func() any { return new(bytes.Buffer) }}

// Template holds one sequential template execution context.
type Template struct {
	tmpl *template.Template
	cell *map[string]any // `inputs` func closes over this pointer
}

// Parse compiles one ResourceSet template with the Flux function set.
func Parse(tmplStr string) (*Template, error) {
	cell := new(map[string]any)
	tmpl, err := template.New("resourceset").
		Delims("<<", ">>").
		Funcs(sprig.HermeticTxtFuncMap()).
		Funcs(template.FuncMap{
			"slugify":    slug.Make,
			"toYaml":     toYaml,
			"mustToYaml": mustToYaml,
			"inputs":     func() any { return *cell },
		}).
		Option("missingkey=error").
		Parse(tmplStr)
	if err != nil {
		return nil, fmt.Errorf("parse template: %w", err)
	}
	return &Template{tmpl: tmpl, cell: cell}, nil
}

// Execute evaluates the template with one input set and returns owned bytes.
func (pt *Template) Execute(inputSet map[string]any) ([]byte, error) {
	*pt.cell = inputSet

	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufPool.Put(buf)

	if err := pt.tmpl.Execute(buf, nil); err != nil {
		return nil, fmt.Errorf("execute template: %w", err)
	}
	return bytes.Clone(buf.Bytes()), nil
}

func toYaml(v any) string {
	s, err := mustToYaml(v)
	if err != nil {
		return ""
	}
	return s
}

func mustToYaml(v any) (string, error) {
	out, err := yaml.Marshal(v)
	return string(out), err
}
