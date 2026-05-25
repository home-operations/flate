package image

import (
	"slices"
	"testing"
)

func TestExtract_Deployment(t *testing.T) {
	doc := map[string]any{
		"kind": "Deployment",
		"spec": map[string]any{
			"template": map[string]any{
				"spec": map[string]any{
					"containers": []any{
						map[string]any{"image": "ghcr.io/library/nginx:1.27"},
						map[string]any{"image": "docker.io/library/redis:7"},
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
		"docker.io/library/redis:7",
		"ghcr.io/library/nginx:1.27",
		"ghcr.io/owner/init:0.1",
	}
	if !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestExtract_FindsImageInArbitraryFieldName(t *testing.T) {
	// CNPG Cluster uses "imageName"; a custom CRD might use anything.
	// We don't care — every string value is candidate.
	doc := map[string]any{
		"kind": "Cluster",
		"spec": map[string]any{"imageName": "ghcr.io/cloudnative-pg/postgresql:16.4"},
	}
	got := Extract(doc)
	if !slices.Equal(got, []string{"ghcr.io/cloudnative-pg/postgresql:16.4"}) {
		t.Errorf("got %v", got)
	}
}

func TestExtract_FindsImageInUnknownKind(t *testing.T) {
	// A Service with an annotation referencing an image should
	// still surface that image — value-based detection means there's
	// no "unknown kind" gap.
	doc := map[string]any{
		"kind": "Service",
		"metadata": map[string]any{
			"annotations": map[string]any{
				"app.example.com/owning-image": "ghcr.io/me/svc:v1@sha256:" + sha256(),
			},
		},
	}
	got := Extract(doc)
	if len(got) != 1 {
		t.Errorf("expected 1 image, got %v", got)
	}
}

func TestExtract_DigestOnly(t *testing.T) {
	doc := map[string]any{
		"spec": map[string]any{
			"image": "ghcr.io/owner/foo@sha256:" + sha256(),
		},
	}
	got := Extract(doc)
	if len(got) != 1 {
		t.Errorf("expected 1 image, got %v", got)
	}
}

func TestExtract_Nested(t *testing.T) {
	// Kubernetes 1.31 image volume: {reference: "..."}.
	doc := map[string]any{
		"spec": map[string]any{
			"containers": []any{
				map[string]any{"image": map[string]any{"reference": "ghcr.io/x:1.0"}},
			},
		},
	}
	got := Extract(doc)
	if !slices.Equal(got, []string{"ghcr.io/x:1.0"}) {
		t.Errorf("got %v", got)
	}
}

func TestExtract_RejectsNonImages(t *testing.T) {
	cases := []struct {
		name  string
		value any
	}{
		{"empty string", ""},
		{"short", "a:b"},
		{"version only", "v1.2.3"},
		{"url", "https://example.com"},
		{"oci url", "oci://ghcr.io/x/y"},
		{"absolute path", "/etc/config"},
		{"unsubstituted var", "${IMAGE_TAG}"},
		{"port mapping", "8080:8080"},
		{"label-like", "app=demo"},
		{"non-string", 42},
		{"non-string map", map[string]any{"k": "v"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			doc := map[string]any{"x": tc.value}
			got := Extract(doc)
			if len(got) != 0 {
				t.Errorf("expected nothing, got %v", got)
			}
		})
	}
}

func TestIsImageRef(t *testing.T) {
	yes := []string{
		"ghcr.io/owner/repo:v1",
		"ghcr.io/owner/repo@sha256:" + sha256(),
		"ghcr.io/owner/repo:v1@sha256:" + sha256(),
		"registry.k8s.io/ingress-nginx/controller:v1.10.1",
		"quay.io/jetstack/cert-manager-controller:v1.16.1",
		"docker.io/library/nginx:1.27",
		"localhost:5000/myimg:dev",
	}
	for _, s := range yes {
		t.Run("yes/"+s, func(t *testing.T) {
			if !IsImageRef(s) {
				t.Errorf("expected image ref")
			}
		})
	}

	no := []string{
		"", "x", "abc", "v1.2.3", "10.4.0",
		"hello world",
		"path/to/file",
		"foo bar:baz",
		"https://example.com/x:y",
		// real-world false positives surfaced on a live GitOps repo
		"apiserver_request:burnrate1d",
		"cert-manager:leaderelection",
		"cert-manager-controller-approve:cert-manager-io",
		"count:up0",
		"system:auth-delegator",
		"system:metrics-server-aggregated-reader",
		"node_namespace_pod_container:container_memory_cache",
		"localhost:11220",
		"localhost:11221",
		// bare-name references aren't seen in real GitOps manifests
		"nginx:1.27",
		"redis:7",
		"1.2.3:foo", // would pass naive heuristics, but no `/`
	}
	for _, s := range no {
		t.Run("no/"+s, func(t *testing.T) {
			if IsImageRef(s) {
				t.Errorf("expected NOT image ref")
			}
		})
	}
}

// sha256 returns a 64-hex stub used as a digest fixture.
func sha256() string {
	return "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
}
