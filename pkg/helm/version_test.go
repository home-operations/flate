package helm

import (
	"runtime/debug"
	"testing"

	"helm.sh/helm/v4/pkg/chart/common"
)

func TestDetectBundledKubeVersion_BuildInfo(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		info *debug.BuildInfo
		ok   bool
		want string
	}{
		{
			name: "reader failure",
			want: common.DefaultCapabilities.KubeVersion.Version,
		},
		{
			name: "unrelated dependency",
			info: &debug.BuildInfo{Deps: []*debug.Module{
				{Path: "example.com/unrelated", Version: "v0.38.0"},
			}},
			ok:   true,
			want: common.DefaultCapabilities.KubeVersion.Version,
		},
		{
			name: "supported dependency",
			info: &debug.BuildInfo{Deps: []*debug.Module{
				{Path: "example.com/unrelated", Version: "v0.38.0"},
				{Path: "k8s.io/api", Version: "v0.37.2"},
			}},
			ok:   true,
			want: "1.37.2",
		},
		{
			name: "pseudo-version suffix",
			info: &debug.BuildInfo{Deps: []*debug.Module{
				{Path: "k8s.io/api", Version: "v0.37.2-20261001000000-abcdef123456"},
			}},
			ok:   true,
			want: "1.37.2",
		},
		{
			name: "unsupported prefix",
			info: &debug.BuildInfo{Deps: []*debug.Module{
				{Path: "k8s.io/api", Version: "v1.37.2"},
			}},
			ok:   true,
			want: common.DefaultCapabilities.KubeVersion.Version,
		},
		{
			name: "first matching dependency",
			info: &debug.BuildInfo{Deps: []*debug.Module{
				{Path: "k8s.io/api", Version: "v0.37.2"},
				{Path: "k8s.io/api", Version: "v0.38.0"},
			}},
			ok:   true,
			want: "1.37.2",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := detectBundledKubeVersion(tt.info, tt.ok); got != tt.want {
				t.Errorf("detectBundledKubeVersion() = %q, want %q", got, tt.want)
			}
		})
	}
}
