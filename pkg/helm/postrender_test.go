package helm

import (
	"bytes"
	"strings"
	"sync"
	"testing"
	"time"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	"github.com/fluxcd/pkg/apis/kustomize"
	"sigs.k8s.io/kustomize/kyaml/filesys"
	"sigs.k8s.io/kustomize/kyaml/openapi"
	"sigs.k8s.io/kustomize/kyaml/yaml"

	"github.com/home-operations/flate/internal/assert"

	kustomizestate "github.com/home-operations/flate/internal/kustomize"
)

func TestNewPostRenderer_NilWhenNoKustomize(t *testing.T) {
	t.Parallel()
	if pr := newPostRenderer(nil); pr != nil {
		t.Errorf("expected nil for empty list, got %T", pr)
	}
	if pr := newPostRenderer([]helmv2.PostRenderer{{}}); pr != nil {
		t.Errorf("expected nil for postRenderer without Kustomize, got %T", pr)
	}
}

func TestKustomizePostRenderer_PatchAppliesToHelmOutput(t *testing.T) {
	t.Parallel()
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

	kustomizestate.BuildMutex.Lock()
	done := make(chan struct{})
	go func() {
		_, _ = pr.Run(in)
		close(done)
	}()
	select {
	case <-done:
		kustomizestate.BuildMutex.Unlock()
		t.Fatal("post-render completed while BuildMutex was held: it does not serialize on the shared krusty lock")
	case <-time.After(300 * time.Millisecond):
	}
	kustomizestate.BuildMutex.Unlock()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("post-render did not complete after releasing BuildMutex")
	}
}

func TestKustomizePostRenderer_ImageOverride(t *testing.T) {
	const input = `apiVersion: apps/v1
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
`
	fs := filesys.MakeFsInMemory()
	assert.NoError(t, fs.WriteFile("/kustomization.yaml", []byte("resources: [deployment.yaml]\n")))
	assert.NoError(t, fs.WriteFile("/deployment.yaml", []byte(input)))
	kustomizestate.BuildMutex.Lock()
	openapi.ResetOpenAPI()
	definition := openapi.SchemaForResourceType(yaml.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"}).Schema
	kustomizestate.BuildMutex.Unlock()
	t.Cleanup(func() {
		kustomizestate.BuildMutex.Lock()
		defer kustomizestate.BuildMutex.Unlock()
		openapi.ResetOpenAPI()
	})
	pr := newPostRenderer([]helmv2.PostRenderer{{
		Kustomize: &helmv2.Kustomize{
			Images: []kustomize.Image{{Name: "nginx", NewTag: "1.25"}},
		},
	}})
	start := make(chan struct{})
	var wg sync.WaitGroup
	for worker := range 2 {
		wg.Go(func() {
			<-start
			for range 4 {
				if worker == 0 {
					kustomizestate.BuildMutex.Lock()
					rm, err := kustomizestate.Build(fs, "/")
					if err != nil {
						t.Errorf("Build: %v", err)
					} else {
						out, err := rm.AsYaml()
						if err != nil || !strings.Contains(string(out), "nginx:1.19") {
							t.Errorf("Build output: %s; error: %v", out, err)
						}
					}
					kustomizestate.BuildMutex.Unlock()
				} else {
					out, err := pr.Run(bytes.NewBufferString(input))
					if err != nil {
						t.Errorf("Run: %v", err)
					} else if !strings.Contains(out.String(), "nginx:1.25") {
						t.Errorf("image not overridden:\n%s", out.String())
					}
				}
				kustomizestate.BuildMutex.Lock()
				assert.Equal(t, openapi.SchemaForResourceType(yaml.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"}).Schema, definition)
				kustomizestate.BuildMutex.Unlock()
			}
		})
	}
	close(start)
	wg.Wait()
}
