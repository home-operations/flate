package cache

import (
	"crypto/sha256"
	"encoding/hex"
	"runtime/debug"
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
	digest := sha256.Sum256([]byte(info.String()))
	return hex.EncodeToString(digest[:])
}

func blobID(identity, id string) string {
	digest := sha256.Sum256([]byte("render-blob-v1\x00" + identity + "\x00" + id))
	return "render-v1-" + hex.EncodeToString(digest[:])
}
