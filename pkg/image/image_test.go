package image

import (
	"encoding/json"
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
						map[string]any{"image": "nginx"},
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
		"nginx",
		"nginx:latest",
		"redis:7",
	}
	assert.Diff(t, got, want)
}

func TestExtract_DeclaredAndFullReferences(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		doc  map[string]any
		want []string
	}{
		{
			name: "Prometheus and Alertmanager image",
			doc: map[string]any{"spec": map[string]any{
				"image": "quay.io/prometheus/prometheus:v3.0",
			}},
			want: []string{"quay.io/prometheus/prometheus:v3.0"},
		},
		{
			name: "CNPG cluster and extension",
			doc: map[string]any{"kind": "Cluster", "spec": map[string]any{
				"imageName": "ghcr.io/cloudnative-pg/postgresql:16.4",
				"postgresql": map[string]any{"extensions": []any{map[string]any{
					"image": map[string]any{"reference": "postgres", "tag": "v1.2.3"},
				}}},
			}},
			want: []string{"ghcr.io/cloudnative-pg/postgresql:16.4", "postgres"},
		},
		{
			name: "EnvoyProxy nested container",
			doc: map[string]any{"kind": "EnvoyProxy", "spec": map[string]any{"provider": map[string]any{
				"kubernetes": map[string]any{"envoyDeployment": map[string]any{
					"container": map[string]any{"image": "envoyproxy/envoy:v1.32"},
				}},
			}}},
			want: []string{"envoyproxy/envoy:v1.32"},
		},
		{
			name: "CephCluster",
			doc: map[string]any{"spec": map[string]any{
				"cephVersion": map[string]any{"image": "quay.io/ceph/ceph:v19"},
			}},
			want: []string{"quay.io/ceph/ceph:v19"},
		},
		{
			name: "CatalogSource and ClusterVersion",
			doc: map[string]any{"spec": map[string]any{
				"image":         "quay.io/example/catalog:v1",
				"desiredUpdate": map[string]any{"image": "quay.io/example/release@sha256:" + sha256()},
			}},
			want: []string{"quay.io/example/catalog:v1", "quay.io/example/release@sha256:" + sha256()},
		},
		{
			name: "HelmRelease values and unknown resource keys",
			doc: map[string]any{"kind": "UnknownResource", "spec": map[string]any{"values": map[string]any{
				"runner":       map[string]any{"tools": []any{map[string]any{"image": "a:b"}}},
				"sidecarImage": "busybox", "initImage": map[string]any{"reference": "alpine"},
				"imageName": "nginx", "image": "v1.2.3",
			}}},
			want: []string{"a:b", "alpine", "busybox", "nginx", "v1.2.3"},
		},
		{
			name: "declared context boundaries",
			doc: map[string]any{"spec": map[string]any{
				"image": map[string]any{
					"reference": "nginx", "tag": "v1.2.3", "repository": "library/nginx",
					"nested": map[string]any{"reference": "redis", "sidecarImage": "busybox"},
					"list":   []any{"alpine", map[string]any{"reference": "postgres", "imageName": "memcached"}},
				},
				"initImage":    map[string]any{"reference": map[string]any{"reference": "rabbitmq"}},
				"sidecarImage": []any{"caddy", map[string]any{"reference": "httpd", "image": "traefik"}},
				"images":       []any{"grafana/grafana", "ghcr.io/demo/array:v1"},
				"sidecarimage": "prometheus", "IMAGE": "envoy", "imagename": "redis",
				"other": "ghcr.io/demo/untagged", "portOnly": "localhost:5000/untagged",
			}},
			want: []string{"busybox", "ghcr.io/demo/array:v1", "memcached", "nginx", "traefik"},
		},
		{
			name: "full references and captured junk in arbitrary fields",
			doc: map[string]any{
				"metadata": map[string]any{"annotations": map[string]any{
					"app.example.com/owning-image": "ghcr.io/me/svc:v1@sha256:" + sha256(),
				}},
				"data": map[string]any{"tool": "ghcr.io/demo/config:v1", "tag": "v1.2.3"},
				"spec": map[string]any{
					"env":       []any{map[string]any{"value": "ghcr.io/demo/env:v1"}},
					"args":      []any{"ghcr.io/demo/arg@sha256:" + sha256(), "nginx:latest"},
					"records":   []any{"apiserver_request:burnrate1d", "count:up0"},
					"addresses": []any{"0.0.0.0:80", "127.0.0.1:9234"},
					"tag":       "0.10.12@sha256:5338b99662e2e15048b4e14b49e17279d384970fdcb3c29169f509f806f49cbe",
				},
			},
			want: []string{"ghcr.io/demo/arg@sha256:" + sha256(), "ghcr.io/demo/config:v1", "ghcr.io/demo/env:v1", "ghcr.io/me/svc:v1@sha256:" + sha256()},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Diff(t, Extract(tc.doc), tc.want)
		})
	}
}

func TestExtract_EmbeddedPodSpec(t *testing.T) {
	t.Parallel()
	doc := map[string]any{
		"kind": "UnknownResource",
		"metadata": map[string]any{
			"annotations": map[string]any{
				"app.example.com/owning-image":   "ghcr.io/me/svc:v1@sha256:" + sha256(),
				"app.example.com/recording-rule": "count:up0",
			},
		},
		"spec": map[string]any{"workers": []any{map[string]any{
			"template": map[string]any{"spec": map[string]any{
				"containers": []any{map[string]any{"image": "ghcr.io/demo/worker:v1@sha256:" + sha256()}},
			}},
		}}},
	}
	assert.Diff(t, Extract(doc), []string{"ghcr.io/demo/worker:v1@sha256:" + sha256(), "ghcr.io/me/svc:v1@sha256:" + sha256()})
}

func TestExtract_CronJob(t *testing.T) {
	t.Parallel()
	doc := map[string]any{
		"kind": "CronJob",
		"spec": map[string]any{"jobTemplate": map[string]any{"spec": map[string]any{
			"template": map[string]any{"spec": map[string]any{
				"containers":     []any{map[string]any{"image": "ghcr.io/demo/job:v1"}},
				"initContainers": []any{map[string]any{"image": "ghcr.io/demo/init:v1"}},
			}},
		}}},
	}
	assert.Diff(t, Extract(doc), []string{"ghcr.io/demo/init:v1", "ghcr.io/demo/job:v1"})
}

func TestExtract_PodEphemeralContainers(t *testing.T) {
	t.Parallel()
	doc := map[string]any{
		"kind": "Pod",
		"spec": map[string]any{
			"ephemeralContainers": []any{map[string]any{"image": "busybox:1.36"}},
		},
	}
	assert.Diff(t, Extract(doc), []string{"busybox:1.36"})
}

func TestExtract_DigestOnly(t *testing.T) {
	t.Parallel()
	doc := map[string]any{
		"spec": map[string]any{
			"containers": []any{map[string]any{"image": "ghcr.io/owner/foo@sha256:" + sha256()}},
		},
	}
	assert.Diff(t, Extract(doc), []string{"ghcr.io/owner/foo@sha256:" + sha256()})
}

func TestExtract_Nested(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		doc  map[string]any
		want []string
	}{
		{name: "nil document"},
		{name: "empty document", doc: map[string]any{}},
		{
			name: "deduplicated lists",
			doc: map[string]any{"spec": map[string]any{
				"containers": []any{
					map[string]any{"image": "nginx:latest"},
					map[string]any{"image": "docker.io/library/nginx:latest"},
					map[string]any{"image": "nginx:latest"},
				},
				"other": []any{map[string]any{
					"initContainers":      []any{map[string]any{"image": "ghcr.io/x:1.0"}},
					"ephemeralContainers": []any{map[string]any{"image": "nginx:latest"}},
				}},
			}},
			want: []string{"docker.io/library/nginx:latest", "ghcr.io/x:1.0", "nginx:latest"},
		},
		{
			name: "recognized scalar",
			doc:  map[string]any{"containers": "nginx:latest"},
		},
		{
			name: "container wrapper map",
			doc:  map[string]any{"initContainers": map[string]any{"image": "nginx:latest"}},
			want: []string{"nginx:latest"},
		},
		{
			name: "non-map entries",
			doc: map[string]any{"containers": []any{
				nil, 42, "nginx:latest", []any{"redis:7", map[string]any{"image": "ghcr.io/demo/bare:v1"}},
			}},
			want: []string{"ghcr.io/demo/bare:v1"},
		},
		{
			name: "missing or non-string image",
			doc: map[string]any{"containers": []any{
				map[string]any{}, map[string]any{"image": nil}, map[string]any{"image": 42},
				map[string]any{"image": map[string]any{"reference": "ghcr.io/x:1.0"}},
				map[string]any{"image": []any{"redis:7"}},
			}},
			want: []string{"ghcr.io/x:1.0"},
		},
		{
			name: "unrelated entry fields",
			doc: map[string]any{"containers": []any{map[string]any{
				"image": "nginx:latest", "name": "redis:7", "command": []any{"ghcr.io/demo/command:v1"},
			}}},
			want: []string{"ghcr.io/demo/command:v1", "nginx:latest"},
		},
		{
			name: "nested list below malformed map",
			doc: map[string]any{"containers": map[string]any{
				"image":   "ghcr.io/demo/direct:v1",
				"wrapper": map[string]any{"initContainers": []any{map[string]any{"image": "ghcr.io/demo/init:v1"}}},
			}},
			want: []string{"ghcr.io/demo/direct:v1", "ghcr.io/demo/init:v1"},
		},
		{
			name: "nested lists below malformed entries",
			doc: map[string]any{"ephemeralContainers": []any{
				[]any{map[string]any{
					"image":      "ghcr.io/demo/direct:v1",
					"containers": []any{map[string]any{"image": "ghcr.io/demo/nested:v1"}},
				}},
				map[string]any{"image": map[string]any{
					"initContainers": []any{map[string]any{"image": "ghcr.io/demo/init:v1"}},
				}},
			}},
			want: []string{"ghcr.io/demo/direct:v1", "ghcr.io/demo/init:v1", "ghcr.io/demo/nested:v1"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			before, err := json.Marshal(tc.doc)
			assert.NoError(t, err)
			assert.Diff(t, Extract(tc.doc), tc.want)
			assert.Diff(t, Extract(tc.doc), tc.want)
			after, err := json.Marshal(tc.doc)
			assert.NoError(t, err)
			assert.Equal(t, string(after), string(before))
		})
	}
}

func TestExtract_RejectsNonImages(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		value any
	}{
		{"empty string", ""},
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
			doc := map[string]any{"containers": []any{map[string]any{"image": tc.value}}}
			assert.Diff(t, Extract(doc), []string(nil))
		})
	}
}

func TestIsImageRef_Classification(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		value      string
		declared   bool
		undeclared bool
	}{
		{name: "tagged", value: "ghcr.io/owner/repo:v1", declared: true, undeclared: true},
		{name: "digested", value: "ghcr.io/owner/repo@sha256:" + sha256(), declared: true, undeclared: true},
		{name: "tagged and digested", value: "ghcr.io/owner/repo:v1@sha256:" + sha256(), declared: true, undeclared: true},
		{name: "registry port", value: "localhost:5000/myimg:dev", declared: true, undeclared: true},
		{name: "short full reference", value: "a/b:c", declared: true, undeclared: true},
		{name: "short declared reference", value: "a:b", declared: true},
		{name: "untagged declared reference", value: "nginx", declared: true},
		{name: "slashless tagged", value: "nginx:latest", declared: true},
		{name: "slashless digested", value: "nginx@sha256:" + sha256(), declared: true},
		{name: "untagged full reference", value: "ghcr.io/owner/repo", declared: true},
		{name: "port without tag", value: "localhost:5000/myimg", declared: true},
		{name: "version", value: "v1.2.3", declared: true},
		{name: "recording rule", value: "apiserver_request:burnrate1d", declared: true},
		{name: "count rule", value: "count:up0", declared: true},
		{name: "listen address", value: "127.0.0.1:9234", declared: true},
		{name: "standalone tag and digest", value: "0.10.12@sha256:" + sha256(), declared: true},
		{name: "empty"},
		{name: "spaces", value: "foo bar:baz"},
		{name: "url", value: "https://example.com/x:y"},
		{name: "oci url", value: "oci://ghcr.io/x/y:v1"},
		{name: "absolute path", value: "/etc/config:v1"},
		{name: "placeholder", value: "ghcr.io/x:${TAG}"},
		{name: "invalid digest", value: "ghcr.io/owner/repo@sha256:bad"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, isImageRef(tc.value, true), tc.declared)
			assert.Equal(t, isImageRef(tc.value, false), tc.undeclared)
		})
	}
}

func sha256() string {
	return "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
}
