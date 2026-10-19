package manifest

import (
	"testing"

	"github.com/fluxcd/pkg/envsubst"
)

func TestHasEnvsubstReference(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   string
		want bool
	}{
		{"bare reference", "${REDIS_APP_NAME}-redis", true},
		{"reference with :? error message", "${REQUIRED:?must be set}", true},
		{"default already resolved → no surviving reference", "biohazard", false},
		{"reference with default still survives the regex", "${VAR:=default}", true},
		{"no envsubst at all", "redis-server", false},
		{"plain dollar (no brace)", "$HOME", false},
		{"empty", "", false},
		{"escaped dollar", "$$HOME", false},
		{"reference in middle of string", "prefix-${VAR}-suffix", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := HasEnvsubstReference(tc.in); got != tc.want {
				t.Errorf("HasEnvsubstReference(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

func TestResolveEnvsubstDefaults(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name        string
		in          string
		want        string
		compareFlux bool
	}{
		{
			name:        "colon-dash empty default",
			in:          "${VAR:-}",
			want:        "",
			compareFlux: true,
		},
		{
			name:        "colon-equals empty default",
			in:          "${VAR:=}",
			want:        "",
			compareFlux: true,
		},
		{
			name:        "colon-dash empty prefix",
			in:          "${PREFIX:-}apps",
			want:        "apps",
			compareFlux: true,
		},
		{
			name:        "colon-equals empty prefix",
			in:          "${PREFIX:=}apps",
			want:        "apps",
			compareFlux: true,
		},
		{
			name:        "empty default between literals",
			in:          "pre${VAR:-}post",
			want:        "prepost",
			compareFlux: true,
		},
		{
			name: "bare reference with colon-dash empty default",
			in:   "${NS}/${PREFIX:-}apps",
			want: "${NS}/apps",
		},
		{
			name: "bare reference with colon-equals empty default",
			in:   "${NS}/${PREFIX:=}apps",
			want: "${NS}/apps",
		},
		{
			name: "colon-equals default → applied",
			in:   "./apps/${CLUSTER_NAME:=biohazard}",
			want: "./apps/biohazard",
		},
		{
			name: "colon-dash default → applied",
			in:   "tag: ${VERSION:-v1.0.0}",
			want: "tag: v1.0.0",
		},
		{
			name: "multiple patterns in one string",
			in:   "${A:=x}/${B:-y}",
			want: "x/y",
		},
		{
			name: "bare VAR (no default) → left as-is so postBuild can fill",
			in:   "${REDIS_APP_NAME}-redis",
			want: "${REDIS_APP_NAME}-redis",
		},
		{
			name: "error-on-unset (:?) → left as-is",
			in:   "${REQUIRED:?must be set}",
			want: "${REQUIRED:?must be set}",
		},
		{
			name: "mixed bare + default-bearing",
			in:   "${NS}/${APP:=foo}",
			want: "${NS}/foo",
		},
		{
			name: "no envsubst at all → no allocation",
			in:   "./apps/foo",
			want: "./apps/foo",
		},
		{
			name: "empty string",
			in:   "",
			want: "",
		},
		{
			name: "dollar without brace → left as-is",
			in:   "echo $HOME",
			want: "echo $HOME",
		},
		{
			name: "default with hyphen/slash characters",
			in:   "${IMG_TAG:=v1.2.3-alpha}",
			want: "v1.2.3-alpha",
		},
		{
			name: "real-world tholinka pattern",
			in:   "./kube/deploy/core/storage/rook-ceph/cluster/${CLUSTER_NAME:=biohazard}",
			want: "./kube/deploy/core/storage/rook-ceph/cluster/biohazard",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := resolveEnvsubstDefaults(tc.in)
			if got != tc.want {
				t.Errorf("resolveEnvsubstDefaults(%q) = %q, want %q", tc.in, got, tc.want)
			}
			if tc.compareFlux {
				upstream, err := envsubst.Eval(tc.in, func(string) (string, bool) { return "", false })
				if err != nil {
					t.Fatalf("envsubst.Eval(%q): %v", tc.in, err)
				}
				if got != upstream {
					t.Errorf("resolveEnvsubstDefaults(%q) = %q, Flux = %q", tc.in, got, upstream)
				}
			}
		})
	}
}
