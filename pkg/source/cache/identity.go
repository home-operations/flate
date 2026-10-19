package cache

import (
	"runtime/debug"

	"github.com/home-operations/flate/pkg/manifest"
)

const flateModule = "github.com/home-operations/flate"

var processIdentity, processIdentityReason = func() (string, string) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		info = nil
	}
	return buildIdentity(info)
}()

// RenderPersistenceDisabledReason returns why this binary cannot persist renders, or an empty string when it can.
func RenderPersistenceDisabledReason() string {
	return processIdentityReason
}

// Empty identity disables render persistence; raw storage remains available.
func buildIdentity(info *debug.BuildInfo) (string, string) {
	if info == nil {
		return "", "build metadata is unavailable"
	}
	identified := info.Main.Path == flateModule && info.Main.Version != "" && info.Main.Version != "(devel)"
	for _, dep := range info.Deps {
		if dep.Replace != nil && dep.Replace.Version == "" {
			return "", "a dependency replacement has no version"
		}
		if dep.Path == flateModule && dep.Version != "" && dep.Version != "(devel)" {
			identified = true
		}
	}
	for _, setting := range info.Settings {
		if setting.Key == "vcs.modified" && setting.Value == "true" {
			return "", "the binary was built from a dirty checkout"
		}
		if setting.Key == "vcs.revision" && setting.Value != "" {
			identified = true
		}
	}
	if !identified {
		return "", "the binary has no stamped build identity"
	}
	return manifest.Fingerprint(info.String()), ""
}

type renderIdentity struct {
	Build  string
	Render string
}

func blobID(identity, id string) string {
	return "render-" + manifest.Fingerprint(renderIdentity{Build: identity, Render: id})
}
