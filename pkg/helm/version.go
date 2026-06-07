package helm

import (
	"runtime/debug"
	"strings"
	"sync"
)

// BundledKubeVersion is the Kubernetes minor version associated with
// the k8s.io/api module flate was built against. It mirrors what the
// upstream helm SDK would inject if running inside a cluster of that
// version, and is flate's default for the `--kube-version` flag.
//
// The mapping is well-defined: k8s.io/api v0.X.Y corresponds to
// Kubernetes v1.X.Y. We derive X.Y at startup from build-info; if
// build-info is unavailable, we fall back to a sensible default.
var BundledKubeVersion = sync.OnceValue(detectBundledKubeVersion)

const fallbackKubeVersion = "1.36.0"

func detectBundledKubeVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return fallbackKubeVersion
	}
	for _, dep := range info.Deps {
		if dep.Path != "k8s.io/api" {
			continue
		}
		// dep.Version is "v0.36.0" — strip the leading "v0." and
		// reassemble as "1.<minor>.<patch>".
		v := strings.TrimPrefix(dep.Version, "v0.")
		if v == dep.Version {
			return fallbackKubeVersion
		}
		// Optional pseudo-version suffix on master builds: trim.
		if i := strings.Index(v, "-"); i > 0 {
			v = v[:i]
		}
		return "1." + v
	}
	return fallbackKubeVersion
}
