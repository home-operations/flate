package manifest

import (
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/kustomize/api/resmap"
	"sigs.k8s.io/kustomize/kyaml/filesys"
	"sigs.k8s.io/kustomize/kyaml/openapi"
	"sigs.k8s.io/kustomize/kyaml/yaml"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/kustomize"
)

type scopeCase struct{ version, kind, namespace string }

var scopeCases = []scopeCase{
	{"apps/v1", "Deployment", "apps"},
	{"v1", "ConfigMap", "apps"},
	{"rbac.authorization.k8s.io/v1", "ClusterRole", ""},
	{"v1", "Namespace", ""},
	{"apiextensions.k8s.io/v1", "CustomResourceDefinition", ""},
	{"rbac.authorization.k8s.io/v999", "ClusterRole", "apps"},
	{"scope.test.example/v1", "ScopeCluster", "apps"},
	{"v1", "NodeProxyOptions", ""},
	{"admissionregistration.k8s.io/v1", "ValidatingAdmissionPolicy", ""},
	{"admissionregistration.k8s.io/v1", "ValidatingAdmissionPolicyBinding", ""},
}

func resetScope() {
	kustomize.BuildMutex.Lock()
	openapi.ResetOpenAPI()
	kustomize.BuildMutex.Unlock()
	for _, tc := range scopeCases {
		namespaceScopes.Delete(yaml.TypeMeta{APIVersion: tc.version, Kind: tc.kind})
	}
}

func assertScope(t *testing.T, tc scopeCase, after func()) {
	t.Helper()
	meta := yaml.TypeMeta{APIVersion: tc.version, Kind: tc.kind}
	namespaceScopes.Delete(meta)
	doc := map[string]any{"apiVersion": tc.version, "kind": tc.kind, "metadata": map[string]any{"name": "scope"}}
	obj, authoritative, _, err := ParseDoc(doc, ParseDocOptions{DefaultNamespace: "apps"})
	assert.NoError(t, err)
	assert.Equal(t, obj.Named().Namespace, tc.namespace)
	_, ns := DocMetadata(authoritative)
	assert.Equal(t, ns, tc.namespace)
	if after != nil {
		after()
	}
	namespaceScopes.Delete(meta)
	rs := &ResourceSet{Namespace: "apps"}
	rs.DependsOn = []fluxopv1.Dependency{{APIVersion: tc.version, Kind: tc.kind, Name: "scope"}}
	gv, err := schema.ParseGroupVersion(tc.version)
	assert.NoError(t, err)
	assert.Diff(t, slices.Collect(References(rs)), []Reference{{
		Group: gv.Group, Kind: tc.kind, Namespace: tc.namespace, Name: "scope",
		Use: "dependsOn",
	}})
	if after != nil {
		after()
	}
}

func TestParseDoc_ScopeMemo(t *testing.T) {
	resetScope()
	t.Cleanup(resetScope)
	t.Run("no_load", func(t *testing.T) {
		kustomize.BuildMutex.Lock()
		root := openapi.Schema()
		loaded := len(root.Definitions)
		openapi.ResetOpenAPI()
		empty := len(root.Definitions)
		kustomize.BuildMutex.Unlock()
		assert.Equal(t, empty, 0)
		if loaded == 0 {
			t.Fatal("builtin schema setup has no definitions")
		}
		for _, tc := range scopeCases {
			t.Run(tc.kind+"/"+tc.version, func(t *testing.T) {
				assertScope(t, tc, func() {
					kustomize.BuildMutex.Lock()
					defer kustomize.BuildMutex.Unlock()
					if len(root.Definitions) != 0 {
						t.Error("cold scope lookup loaded builtin definitions")
					}
				})
			})
		}
	})
	meta := yaml.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"}
	for _, workers := range []int{2, 4} {
		for _, state := range []string{"cold", "warm"} {
			t.Run(fmt.Sprintf("%s/workers%d", state, workers), func(t *testing.T) {
				namespaceScopes.Delete(meta)
				if state == "warm" {
					assert.Equal(t, isNamespaceScoped(meta), true)
				}
				start, done := make(chan struct{}), make(chan struct{})
				started := make(chan struct{}, workers)
				var wg sync.WaitGroup
				kustomize.BuildMutex.Lock()
				for range workers {
					wg.Go(func() {
						<-start
						started <- struct{}{}
						assert.Equal(t, isNamespaceScoped(meta), true)
					})
				}
				close(start)
				go func() { wg.Wait(); close(done) }()
				for range workers {
					<-started
				}
				if state == "cold" {
					select {
					case <-done:
						t.Error("cold scope lookup completed while the build mutex was held")
					case <-time.After(300 * time.Millisecond):
					}
				} else {
					select {
					case <-done:
					case <-time.After(5 * time.Second):
						t.Error("warm scope lookup blocked on the build mutex")
					}
				}
				kustomize.BuildMutex.Unlock()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Fatal("scope workers did not finish after releasing the build mutex")
				}
			})
		}
	}
}

func TestParseDoc_ScopeBuildIsolation(t *testing.T) {
	fs := filesys.MakeFsInMemory()
	for _, dir := range []string{"/success", "/error"} {
		assert.NoError(t, fs.MkdirAll(dir))
		resource := "resource.yaml"
		if dir == "/error" {
			resource = "missing.yaml"
		}
		assert.NoError(t, fs.WriteFile(dir+"/kustomization.yaml", []byte("namespace: build\nresources: ["+resource+"]\nopenapi:\n  path: schema.json\n")))
		assert.NoError(t, fs.WriteFile(dir+"/resource.yaml", []byte("apiVersion: scope.test.example/v1\nkind: ScopeCluster\nmetadata:\n  name: custom\n")))
		assert.NoError(t, fs.WriteFile(dir+"/schema.json", []byte(`{"definitions":{},"paths":{
			"/apis/scope.test.example/v1/scopeclusters":{"get":{
				"x-kubernetes-action":"get",
				"x-kubernetes-group-version-kind":{"group":"scope.test.example","version":"v1","kind":"ScopeCluster"}
			}}
		}}`)))
	}
	for _, state := range []string{"success", "error", "concurrent"} {
		t.Run(state, func(t *testing.T) {
			resetScope()
			t.Cleanup(resetScope)
			start, done := make(chan struct{}), make(chan struct{})
			started := make(chan struct{}, 2)
			var wg sync.WaitGroup
			kustomize.BuildMutex.Lock()
			if state == "concurrent" {
				for worker := range 2 {
					wg.Go(func() {
						<-start
						started <- struct{}{}
						for i := worker; i < len(scopeCases); i += 2 {
							assertScope(t, scopeCases[i], nil)
						}
					})
				}
				close(start)
				for range 2 {
					<-started
				}
			}
			go func() { wg.Wait(); close(done) }()
			dir := "/success"
			if state == "error" {
				dir = "/error"
			}
			rm, err := func() (resmap.ResMap, error) {
				defer kustomize.BuildMutex.Unlock()
				return kustomize.Build(fs, dir)
			}()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("scope workers did not finish after the custom build")
			}
			if state == "error" {
				if err == nil || !strings.Contains(err.Error(), "missing.yaml") {
					t.Fatalf("expected missing-resource error, got %v", err)
				}
			} else {
				assert.NoError(t, err)
				assert.Equal(t, len(rm.Resources()), 1)
				assert.Equal(t, rm.Resources()[0].GetNamespace(), "")
			}
			for _, tc := range scopeCases {
				assertScope(t, tc, nil)
			}
		})
	}
}
