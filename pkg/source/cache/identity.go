package cache

import (
	"runtime/debug"

	"github.com/home-operations/flate/pkg/manifest"
)

const flateModule = "github.com/home-operations/flate"

//nolint:gochecknoglobals // The stamped process identity is computed once and shared by all cache owners.
var processIdentity, processIdentityReason = func() (string, string) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		info = nil
	}
	stamp := buildIdentity(info)
	return stamp.identity, stamp.reason
}()

// RenderPersistenceDisabledReason returns why this binary cannot persist renders, or an empty string when it can.
func RenderPersistenceDisabledReason() string {
	return processIdentityReason
}

// Empty identity disables render persistence; raw storage remains available.
func buildIdentity(info *debug.BuildInfo) buildStamp {
	if info == nil {
		return buildStamp{reason: "build metadata is unavailable"}
	}
	identified := info.Main.Path == flateModule && info.Main.Version != "" && info.Main.Version != "(devel)"
	for _, dep := range info.Deps {
		if dep.Replace != nil && dep.Replace.Version == "" {
			return buildStamp{reason: "a dependency replacement has no version"}
		}
		if dep.Path == flateModule && dep.Version != "" && dep.Version != "(devel)" {
			identified = true
		}
	}
	for _, setting := range info.Settings {
		if setting.Key == "vcs.modified" && setting.Value == "true" {
			return buildStamp{reason: "the binary was built from a dirty checkout"}
		}
		if setting.Key == "vcs.revision" && setting.Value != "" {
			identified = true
		}
	}
	if !identified {
		return buildStamp{reason: "the binary has no stamped build identity"}
	}
	return buildStamp{identity: manifest.Fingerprint(info.String())}
}

type renderIdentity struct {
	Format int
	Phase  RenderMode
	Build  string
	Render string
}

func blobID(identity, id string, phase RenderMode) string {
	return "render-" + manifest.Fingerprint(
		renderIdentity{Format: frameVersion, Build: identity, Render: id, Phase: phase},
	)
}

type buildStamp struct{ identity, reason string }
