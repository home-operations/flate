package deterministic

import (
	"maps"
	"text/template"
)

// Funcs returns deterministic overrides for the nondeterministic sprig
// functions Helm exposes to chart templates, all driven by a fixed clock and
// a single seeded stream derived from seed (see SeedFor). Assign the result
// to a render's action.Configuration.CustomTemplateFuncs; the engine applies
// it after sprig (maps.Copy in engine.initFunMap), so these entries win —
// uniformly, including inside tpl/include and subcharts.
//
// Construct one FuncMap per render (per action.Configuration). The random
// overrides share a stateful stream that advances on every draw, so the
// FuncMap is NOT safe for concurrent use and must never be memoized or shared
// across goroutines. Helm renders a chart's templates sequentially, so within
// one render the stream is consumed in a deterministic order.
func Funcs(seed []byte) template.FuncMap {
	s := newStream(seed)
	fm := template.FuncMap{}
	maps.Copy(fm, clockFuncs())
	maps.Copy(fm, randFuncs(s))
	return fm
}
