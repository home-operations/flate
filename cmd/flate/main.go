// flate — local validator for Flux GitOps repositories.
package main

import (
	"os"
	"runtime/debug"

	"github.com/home-operations/flate/internal/cli"
)

// version and commit are stamped at build time via -ldflags: goreleaser
// sets version for release binaries, and the container Dockerfile sets both.
// With a plain `go build` / `go install` they keep their defaults and we
// fall back to the module BuildInfo for a useful version display.
var (
	version = "dev"
	commit  = "unknown"
)

func main() {
	tuneGC()
	os.Exit(cli.Execute(versionString()))
}

// versionString renders the value behind `flate --version`, appending a
// short commit when one was stamped in (the container build sets it).
func versionString() string {
	v := resolvedVersion()
	if commit == "unknown" {
		return v
	}
	return v + " (" + commit[:min(len(commit), 7)] + ")"
}

// tuneGC raises the GC target for flate's short-lived, allocation-heavy
// batch runs. A cold reconcile churns hundreds of GC cycles at the
// default GOGC=100; a higher target trades transient memory (bounded at
// ~4x the live set) for fewer collections, measurably cutting cold-start
// CPU. Skipped when the operator set GOGC or GOMEMLIMIT explicitly so
// their tuning always wins.
func tuneGC() {
	if os.Getenv("GOGC") == "" && os.Getenv("GOMEMLIMIT") == "" {
		debug.SetGCPercent(400)
	}
}

func resolvedVersion() string {
	if version != "dev" {
		return version
	}
	if info, ok := debug.ReadBuildInfo(); ok && info.Main.Version != "" && info.Main.Version != "(devel)" {
		return info.Main.Version
	}
	return version
}
