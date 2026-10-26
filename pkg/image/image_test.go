package image

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func TestExtract_Deployment(t *testing.T) {
	t.Parallel()
	doc := map[string]any{
		"kind": "Deployment",
		"spec": map[string]any{
			"template": map[string]any{
				"spec": map[string]any{
					"containers": []any{
						map[string]any{"image": "ghcr.io/library/nginx:1.27"},
						map[string]any{"image": "docker.io/library/redis:7"},
						map[string]any{"image": "nginx:latest"},
						map[string]any{"image": "redis:7"},
						map[string]any{"image": "a:b"},
					},
					"initContainers": []any{
						map[string]any{"image": "ghcr.io/owner/init:0.1"},
					},
				},
			},
		},
	}
	got := Extract(doc)
	want := []string{
		"a:b",
		"docker.io/library/redis:7",
		"ghcr.io/library/nginx:1.27",
		"ghcr.io/owner/init:0.1",
		"nginx:latest",
		"redis:7",
	}
	assert.Diff(t, got, want)
}

func TestExtract_FindsImageInArbitraryFieldName(t *testing.T) {
	t.Parallel()
	doc := map[string]any{
		"kind": "Cluster",
		"spec": map[string]any{"imageName": "ghcr.io/cloudnative-pg/postgresql:16.4"},
	}
	assert.Diff(t, Extract(doc), []string{"ghcr.io/cloudnative-pg/postgresql:16.4"})
}

func TestExtract_FindsImageInUnknownKind(t *testing.T) {
	t.Parallel()
	doc := map[string]any{
		"kind": "UnknownResource",
		"metadata": map[string]any{
			"annotations": map[string]any{
				"app.example.com/owning-image":   "ghcr.io/me/svc:v1@sha256:" + sha256(),
				"app.example.com/recording-rule": "count:up0",
			},
		},
	}
	assert.Diff(t, Extract(doc), []string{"count:up0", "ghcr.io/me/svc:v1@sha256:" + sha256()})
}

func TestExtract_DigestOnly(t *testing.T) {
	t.Parallel()
	doc := map[string]any{
		"spec": map[string]any{
			"image": "ghcr.io/owner/foo@sha256:" + sha256(),
		},
	}
	assert.Diff(t, Extract(doc), []string{"ghcr.io/owner/foo@sha256:" + sha256()})
}

func TestExtract_Nested(t *testing.T) {
	t.Parallel()
	doc := map[string]any{
		"spec": map[string]any{
			"containers": []any{
				map[string]any{"image": map[string]any{"reference": "ghcr.io/x:1.0"}},
				map[string]any{"image": "nginx:latest"},
				[]any{"ghcr.io/x:1.0", "docker.io/library/nginx:latest", "nginx:latest"},
			},
			"other": map[string]any{"duplicate": "docker.io/library/nginx:latest"},
		},
	}
	want := []string{"docker.io/library/nginx:latest", "ghcr.io/x:1.0", "nginx:latest"}
	assert.Diff(t, Extract(doc), want)
	assert.Diff(t, Extract(doc), want)
}

func TestExtract_RejectsNonImages(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		value any
	}{
		{"empty string", ""},
		{"version only", "v1.2.3"},
		{"url", "https://example.com"},
		{"oci url", "oci://ghcr.io/x/y"},
		{"absolute path", "/etc/config"},
		{"unsubstituted var", "${IMAGE_TAG}"},
		{"label-like", "app=demo"},
		{"non-string", 42},
		{"non-string map", map[string]any{"k": "v"}},
		{"nil", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			doc := map[string]any{"x": tc.value}
			assert.Diff(t, Extract(doc), []string(nil))
		})
	}
}

func TestIsImageRef_Classification(t *testing.T) {
	t.Parallel()
	yes := []string{
		"ghcr.io/owner/repo:v1",
		"ghcr.io/owner/repo@sha256:" + sha256(),
		"ghcr.io/owner/repo:v1@sha256:" + sha256(),
		"registry.k8s.io/ingress-nginx/controller:v1.10.1",
		"quay.io/jetstack/cert-manager-controller:v1.16.1",
		"docker.io/library/nginx:1.27",
		"localhost:5000/myimg:dev",
		"nginx:latest",
		"nginx:1.27",
		"redis:7",
		"a:b",
		"8080:8080",
		"nginx@sha256:" + sha256(),
		"nginx:latest@sha256:" + sha256(),
		"apiserver_request:burnrate1d",
		"cert-manager:leaderelection",
		"cert-manager-controller-approve:cert-manager-io",
		"count:up0",
		"system:auth-delegator",
		"system:metrics-server-aggregated-reader",
		"node_namespace_pod_container:container_memory_cache",
		"localhost:11220",
		"localhost:11221",
		"1.2.3:foo",
	}
	for _, s := range yes {
		t.Run("yes/"+s, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, isImageRef(s), true)
		})
	}

	no := []string{
		"", "x", "abc", "v1.2.3", "10.4.0",
		"hello world",
		"path/to/file",
		"foo bar:baz",
		"https://example.com/x:y",
		"nginx",
		"localhost:5000/myimg",
		"ghcr.io/owner/repo@sha256:bad",
		"nginx@sha256:bad",
		"/etc/config",
		"oci://ghcr.io/x/y",
		"${IMAGE_TAG}",
		"nginx:${IMAGE_TAG}",
		"ghcr.io/x:${TAG}",
	}
	for _, s := range no {
		t.Run("no/"+s, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, isImageRef(s), false)
		})
	}
}

func sha256() string {
	return "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
}
