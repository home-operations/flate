package manifest

import "testing"

func TestResolveEnvsubstDefaults(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
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
			if got := ResolveEnvsubstDefaults(tc.in); got != tc.want {
				t.Errorf("ResolveEnvsubstDefaults(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
