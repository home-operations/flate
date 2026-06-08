package deterministic

import (
	"maps"
	"text/template"
)

// Funcs returns deterministic overrides for the nondeterministic sprig
// functions Helm exposes to chart templates. Assign the result to a
// render's action.Configuration.CustomTemplateFuncs; the engine applies
// it after sprig (maps.Copy in engine.initFunMap), so these entries win —
// uniformly, including inside tpl/include and subcharts.
//
// Construct one FuncMap per render (per action.Configuration). The
// overrides are stateless today (a fixed clock); later tiers add a
// per-render seeded random stream, so the FuncMap must never be memoized
// or shared across goroutines.
func Funcs() template.FuncMap {
	fm := template.FuncMap{}
	maps.Copy(fm, clockFuncs())
	return fm
}
