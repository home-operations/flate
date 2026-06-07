// Package baseline resolves and materializes a git revision into a
// tempdir so `flate diff` can run without an explicit --path-orig.
//
// The user-facing flow: when --path-orig isn't supplied, the CLI
// calls AutoResolve, which picks the commit to diff against (either
// the explicit --base=<rev>, or auto-detected via merge-base with the
// branch's upstream / origin/HEAD / origin/{main,master}) and extracts
// that commit's tree into a fresh tempdir. The CLI sets --path-orig to
// that tempdir for the remainder of the diff run, then deletes it on
// exit.
//
// This package treats the user's checkout as read-only — it opens
// the existing repo via go-git's object store and does not mutate
// the working tree, the index, or refs. The materialized tempdir is
// the only side effect, and it lives only for the diff invocation.
package baseline
