// Package deterministic provides drop-in replacements for the
// nondeterministic functions Helm exposes to chart templates through
// sprig — the time- and crypto/rand-backed ones (now, randAlphaNum,
// genCA, …) — so flate renders byte-identically run to run.
//
// Helm v4 applies action.Configuration.CustomTemplateFuncs LAST when it
// assembles the engine FuncMap (maps.Copy over sprig's defaults in
// engine.initFunMap), so an entry returned here OVERRIDES the sprig
// built-in of the same name, uniformly — including inside tpl/include and
// subcharts. pkg/helm assigns Funcs() to cfg.CustomTemplateFuncs once per
// render.
//
// The replacements preserve Helm's output SHAPE (a real timestamp string,
// a valid-length random string, a valid PEM certificate) while making the
// VALUE reproducible. flate renders for offline review and diff and never
// applies its output to a cluster, so a fixed clock — and, in later tiers,
// a seeded randomness stream — is a safe deterministic stand-in for the
// material the live controller draws freshly at apply time.
package deterministic
