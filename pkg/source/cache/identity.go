package cache

import (
	"runtime/debug"

	"github.com/home-operations/flate/pkg/manifest"
)

const flateModule = "github.com/home-operations/flate"

var processIdentity = func() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return ""
	}
	return buildIdentity(info)
}()

// Empty identity disables render persistence; raw storage remains available.
func buildIdentity(info *debug.BuildInfo) string {
	if info == nil {
		return ""
	}
	identified := info.Main.Path == flateModule && info.Main.Version != "" && info.Main.Version != "(devel)"
	for _, dep := range info.Deps {
		if dep.Replace != nil && dep.Replace.Version == "" {
			return ""
		}
		if dep.Path == flateModule && dep.Version != "" && dep.Version != "(devel)" {
			identified = true
		}
	}
	for _, setting := range info.Settings {
		if setting.Key == "vcs.modified" && setting.Value == "true" {
			return ""
		}
		if setting.Key == "vcs.revision" && setting.Value != "" {
			identified = true
		}
	}
	if !identified {
		return ""
	}
	return manifest.Fingerprint(info.String())
}

type renderIdentity struct {
	Build  string
	Render string
}

func blobID(identity, id string) string {
	return "render-" + manifest.Fingerprint(renderIdentity{Build: identity, Render: id})
}
