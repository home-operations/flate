package helm

import (
	"bytes"
	"strings"
	"testing"
	"time"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	"github.com/fluxcd/pkg/apis/kustomize"

	flatekustomize "github.com/home-operations/flate/pkg/kustomize"
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

// TestKustomizePostRenderer_HoldsBuildMutex pins the serialization contract
// behind the non-determinism fix: the helm kustomize post-render runs krusty,
// which mutates kustomize's process-global state, so it MUST hold the shared
// kustomize.BuildMutex — otherwise it races a Kustomization's SecureBuild and
// corrupts renders run-to-run. With the mutex held, Run blocks; once released
// it completes. A regression (missing lock) lets a trivial post-render finish
// while the mutex is held, tripping the first select.
func TestKustomizePostRenderer_HoldsBuildMutex(t *testing.T) {
	pr := newPostRenderer([]helmv2.PostRenderer{{
		Kustomize: &helmv2.Kustomize{
			Patches: []kustomize.Patch{{
				Patch: `- op: replace
  path: /data/greeting
  value: bonjour`,
				Target: &kustomize.Selector{Kind: "ConfigMap", Name: "app"},
			}},
		},
	}})
	if pr == nil {
		t.Fatal("expected non-nil post renderer")
	}
	in := bytes.NewBufferString("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: app, namespace: default}\ndata: {greeting: hello}\n")

	flatekustomize.BuildMutex.Lock()
	done := make(chan struct{})
	go func() {
		_, _ = pr.Run(in)
		close(done)
	}()
	// Mutex held → the krusty build must block (pre-lock work is fast; a
	// no-lock regression would let this trivial post-render complete here).
	select {
	case <-done:
		flatekustomize.BuildMutex.Unlock()
		t.Fatal("post-render completed while BuildMutex was held — it does not serialize on the shared krusty lock")
	case <-time.After(300 * time.Millisecond):
	}
	flatekustomize.BuildMutex.Unlock()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("post-render did not complete after releasing BuildMutex")
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
