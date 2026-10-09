// flate — local validator for Flux GitOps repositories.
package main

import (
	"os"
	"runtime/debug"

	"github.com/KimMachineGun/automemlimit/memlimit"
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
	tuneGC(memlimit.Set)
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
// CPU. It also derives a soft memory limit from the cgroup limit.
// Skipped when the operator set GOGC or GOMEMLIMIT explicitly so
// their tuning always wins.
func tuneGC(setMemoryLimit func(...memlimit.Option) (int64, error)) {
	if os.Getenv("GOGC") == "" && os.Getenv("GOMEMLIMIT") == "" {
		debug.SetGCPercent(400)
		// Cgroup detection is best effort, including on unsupported platforms.
		_, _ = setMemoryLimit(memlimit.WithProvider(memlimit.FromCgroup), memlimit.WithRatio(0.8), memlimit.WithLogger(nil))
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
