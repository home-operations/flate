package image

import "testing"

func BenchmarkExtract_MixedFields(b *testing.B) {
	doc := map[string]any{
		"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "app", "namespace": "apps", "annotations": map[string]any{
			"record": "apiserver_request:burnrate1d", "address": "0.0.0.0:80", "other": "ghcr.io/demo/config:v1",
		}},
		"spec": map[string]any{"sidecarImage": "busybox", "imageName": "ghcr.io/demo/declared:v1", "template": map[string]any{"spec": map[string]any{
			"containers": []any{
				map[string]any{"name": "app", "image": "ghcr.io/demo/app:v1", "env": []any{
					map[string]any{"name": "ADDRESS", "value": "127.0.0.1:9234"},
					map[string]any{"name": "TAG", "value": "0.10.12@sha256:5338b99662e2e15048b4e14b49e17279d384970fdcb3c29169f509f806f49cbe"},
				}},
				map[string]any{"name": "sidecar", "image": "nginx"},
				map[string]any{"name": "duplicate", "image": "ghcr.io/demo/app:v1"},
			},
			"initContainers":      []any{map[string]any{"name": "init", "image": "ghcr.io/demo/init:v1"}},
			"ephemeralContainers": []any{map[string]any{"name": "debug", "image": "busybox:1.36"}},
		}}},
	}
	b.ReportAllocs()
	for b.Loop() {
		Extract(doc)
	}
}
