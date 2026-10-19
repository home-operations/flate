package deterministic

import (
	"maps"
	"slices"
	"text/template"
)

// Funcs returns deterministic overrides for the nondeterministic sprig
// functions Helm exposes to chart templates, using a fixed clock, a seeded
// random stream (see SeedFor), and sorted map traversal. Assign the result
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
	fm := make(template.FuncMap, len(staticFuncs)+streamFuncCount)
	maps.Copy(fm, staticFuncs)
	s := newStream(seed)
	randFuncs(s, fm)
	certFuncs(s, fm)
	return fm
}

// streamFuncCount MUST match the functions bound by randFuncs and certFuncs.
const streamFuncCount = 18

// staticFuncs MUST remain private and immutable after initialization.
var staticFuncs = func() template.FuncMap {
	fm := clockFuncs()
	fm["buildCustomCert"] = buildCustomCert
	fm["keys"] = func(dicts ...map[string]any) []string {
		keys := []string{}
		for _, dict := range dicts {
			keys = slices.AppendSeq(keys, maps.Keys(dict))
		}
		slices.Sort(keys)
		return keys
	}
	fm["values"] = func(dict map[string]any) []any {
		values := make([]any, len(dict))
		for i, key := range slices.Sorted(maps.Keys(dict)) {
			values[i] = dict[key]
		}
		return values
	}
	return fm
}()
