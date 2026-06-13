// Package deterministic provides drop-in replacements for the
// nondeterministic functions Helm exposes to chart templates through
// sprig — the time-, crypto/rand-, and math/rand-backed ones (now,
// randAlphaNum, shuffle, genCA, …) — so flate renders byte-identically
// run to run.
//
// Helm v4 applies action.Configuration.CustomTemplateFuncs LAST when it
// assembles the engine FuncMap (maps.Copy over sprig's defaults in
// engine.initFunMap), so an entry returned here OVERRIDES the sprig
// built-in of the same name, uniformly — including inside tpl/include and
// subcharts. pkg/helm assigns Funcs() to cfg.CustomTemplateFuncs once per
// render.
//
// The replacements preserve Helm's output SHAPE (a real timestamp string, a
// valid-length random string, a valid PEM certificate) while making the VALUE
// reproducible: a fixed clock, a seeded ChaCha8 stream for the random funcs,
// and seed-derived ed25519 keys/certs (ed25519 is the only key type Go's crypto
// generates deterministically from a custom reader). flate renders for offline
// review and diff and never applies its output, so these are safe deterministic
// stand-ins for the material the live controller mints at apply time.
package deterministic
