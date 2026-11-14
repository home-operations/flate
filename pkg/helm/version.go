package helm

import (
	"runtime/debug"
	"strings"
	"sync"

	"helm.sh/helm/v4/pkg/chart/common"
)

// BundledKubeVersion returns the Kubernetes version from the k8s.io/api build
// dependency, or Helm's default when detection fails.
var BundledKubeVersion = sync.OnceValue(func() string {
	return detectBundledKubeVersion(debug.ReadBuildInfo())
})

func detectBundledKubeVersion(info *debug.BuildInfo, ok bool) string {
	if !ok {
		return common.DefaultCapabilities.KubeVersion.Version
	}
	for _, dep := range info.Deps {
		if dep.Path != "k8s.io/api" {
			continue
		}
		v, ok := strings.CutPrefix(dep.Version, "v0.")
		if !ok {
			return common.DefaultCapabilities.KubeVersion.Version
		}
		if version, _, _ := strings.Cut(v, "-"); version != "" {
			v = version
		}
		return "1." + v
	}
	return common.DefaultCapabilities.KubeVersion.Version
}
