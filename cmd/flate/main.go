// flate — local validator for Flux GitOps repositories.
package main

import (
	"os"
	"runtime/debug"

	"github.com/home-operations/flate/internal/cli"
)

// version is set at build time via -ldflags by goreleaser. When the
// binary is built with plain `go build` / `go install`, version stays
// "dev" and we fall back to the module BuildInfo for a useful display.
var version = "dev"

func main() {
	os.Exit(cli.Execute(resolvedVersion()))
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
