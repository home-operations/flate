package helm

import (
	"bytes"
	"strings"
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	"github.com/fluxcd/pkg/apis/kustomize"
)

func TestNewPostRenderer_NilWhenNoKustomize(t *testing.T) {
	if pr := newPostRenderer(nil); pr != nil {
		t.Errorf("expected nil for empty list, got %T", pr)
	}
	if pr := newPostRenderer([]helmv2.PostRenderer{{}}); pr != nil {
		t.Errorf("expected nil for postRenderer without Kustomize, got %T", pr)
	}
}

func TestKustomizePostRenderer_PatchAppliesToHelmOutput(t *testing.T) {
	in := bytes.NewBufferString(`apiVersion: v1
kind: ConfigMap
metadata:
  name: app
  namespace: default
data:
  greeting: hello
`)
	pr := newPostRenderer([]helmv2.PostRenderer{{
		Kustomize: &helmv2.Kustomize{
			Patches: []kustomize.Patch{{
				Patch: `- op: replace
  path: /data/greeting
  value: bonjour`,
				Target: &kustomize.Selector{
					Kind: "ConfigMap",
					Name: "app",
				},
			}},
		},
	}})
	if pr == nil {
		t.Fatal("expected non-nil post renderer")
	}
	out, err := pr.Run(in)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	got := out.String()
	if !strings.Contains(got, "greeting: bonjour") {
		t.Errorf("patch not applied:\n%s", got)
	}
}

func TestKustomizePostRenderer_ImageOverride(t *testing.T) {
	in := bytes.NewBufferString(`apiVersion: apps/v1
kind: Deployment
metadata:
  name: app
  namespace: default
spec:
  template:
    spec:
      containers:
      - name: c
        image: nginx:1.19
`)
	pr := newPostRenderer([]helmv2.PostRenderer{{
		Kustomize: &helmv2.Kustomize{
			Images: []kustomize.Image{{
				Name:   "nginx",
				NewTag: "1.25",
			}},
		},
	}})
	out, err := pr.Run(in)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !strings.Contains(out.String(), "nginx:1.25") {
		t.Errorf("image not overridden:\n%s", out.String())
	}
}
