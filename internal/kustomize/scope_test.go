package kustomize

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	"sigs.k8s.io/kustomize/api/resmap"
	"sigs.k8s.io/kustomize/kyaml/filesys"
	"sigs.k8s.io/kustomize/kyaml/openapi"
	"sigs.k8s.io/kustomize/kyaml/yaml"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestParseDoc_ScopeMemo(t *testing.T) {
	t.Run("no_load", func(t *testing.T) {
		BuildMutex.Lock()
		root := openapi.Schema()
		loaded := len(root.Definitions)
		openapi.ResetOpenAPI()
		BuildMutex.Unlock()
		if loaded == 0 {
			t.Fatal("builtin schema setup has no definitions")
		}
		for _, tc := range []struct{ version, kind string }{
			{"apps/v1", "Deployment"},
			{"v1", "ConfigMap"},
			{"rbac.authorization.k8s.io/v1", "ClusterRole"},
			{"v1", "Namespace"},
			{"apiextensions.k8s.io/v1", "CustomResourceDefinition"},
		} {
			key := yaml.TypeMeta{APIVersion: tc.version, Kind: tc.kind}
			namespaceScopes.Delete(key)
			(scopeResolver{}).ResolveScope(key)
			BuildMutex.Lock()
			if len(root.Definitions) != 0 {
				BuildMutex.Unlock()
				t.Errorf("cold scope lookup for %s loaded builtin definitions", key.Kind)
				return
			}
			BuildMutex.Unlock()
		}
	})

	t.Run("dependency_preparation", func(t *testing.T) {
		for _, workers := range []int{2, 4} {
			for _, namespace := range []string{"", "authored"} {
				for _, fallback := range []string{"", "default"} {
					t.Run(fmt.Sprintf("workers%d/namespace=%s/default=%s", workers, namespace, fallback), func(t *testing.T) {
						meta := yaml.TypeMeta{APIVersion: "scope-preparation.example/v1", Kind: "FreshDependency"}
						cluster := yaml.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole"}
						namespaceScopes.Delete(meta)
						namespaceScopes.Delete(cluster)
						doc := map[string]any{"apiVersion": manifest.FluxOperatorDomain + "/v1", "kind": manifest.KindResourceSet,
							"metadata": map[string]any{"name": "set", "namespace": namespace}, "spec": map[string]any{"dependsOn": []any{
								map[string]any{"apiVersion": meta.APIVersion, "kind": meta.Kind, "name": "fresh", "namespace": "explicit"},
								map[string]any{"apiVersion": cluster.APIVersion, "kind": cluster.Kind, "name": "cluster", "namespace": "explicit"},
							}}}
						obj, _, _, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{DefaultNamespace: fallback})
						assert.NoError(t, err)
						wantNamespace := namespace
						if wantNamespace == "" {
							wantNamespace = fallback
						}
						assert.Equal(t, obj.Named().Namespace, wantNamespace)
						for _, key := range []yaml.TypeMeta{meta, cluster} {
							_, prepared := (scopeResolver{}).LookupScope(key)
							assert.Equal(t, prepared, true)
						}
						BuildMutex.Lock()
						done := make(chan struct{})
						var wg sync.WaitGroup
						for range workers {
							wg.Go(func() {
								for ref := range manifest.References(obj, manifest.ReferenceRelations) {
									want := ""
									if ref.Kind == meta.Kind {
										want = "explicit"
									}
									assert.Equal(t, ref.Namespace, want)
								}
							})
						}
						go func() { wg.Wait(); close(done) }()
						select {
						case <-done:
							BuildMutex.Unlock()
						case <-time.After(5 * time.Second):
							BuildMutex.Unlock()
							<-done
							t.Fatal("prepared dependency walk acquired the build mutex")
						}
					})
				}
			}
		}
	})
	t.Run("memo_miss", func(t *testing.T) {
		meta := yaml.TypeMeta{APIVersion: "unprepared.scope.example/v1", Kind: "NeverPrepared"}
		namespaceScopes.Delete(meta)
		rs := &manifest.ResourceSet{Namespace: "apps", DependsOn: []fluxopv1.Dependency{{APIVersion: meta.APIVersion, Kind: meta.Kind, Name: "missing"}}}
		BuildMutex.Lock()
		done := make(chan any, 1)
		go func() {
			defer func() { done <- recover() }()
			for range manifest.References(rs, manifest.ReferenceRelations) {
				t.Error("unprepared dependency yielded a reference")
			}
		}()
		select {
		case failure := <-done:
			BuildMutex.Unlock()
			assert.Equal(t, failure, any("manifest: ResourceSet dependency scope was not prepared"))
		case <-time.After(5 * time.Second):
			BuildMutex.Unlock()
			<-done
			t.Fatal("memo miss attempted cold scope resolution")
		}
		_, prepared := (scopeResolver{}).LookupScope(meta)
		assert.Equal(t, prepared, false)
	})

	for _, workers := range []int{2, 4} {
		for _, tc := range []struct{ version, kind, namespace string }{
			{"v1", "ConfigMap", "apps"},
			{"apps/v1", "Deployment", "apps"},
			{"admissionregistration.k8s.io/v1", "ValidatingAdmissionPolicy", ""},
			{"admissionregistration.k8s.io/v1", "ValidatingAdmissionPolicyBinding", ""},
			{"v1", "NodeProxyOptions", ""},
			{"foreign.example/v1", "ClusterRole", "apps"},
			{"rbac.authorization.k8s.io/v1", "ClusterRole", ""},
			{"rbac.authorization.k8s.io/v999", "ClusterRole", "apps"},
			{"scope.example/v1", "ClusterCustom", "apps"},
		} {
			t.Run(fmt.Sprintf("%s/%s/workers%d", tc.version, tc.kind, workers), func(t *testing.T) {
				meta := yaml.TypeMeta{APIVersion: tc.version, Kind: tc.kind}
				namespaceScopes.Delete(meta)
				for _, state := range []string{"cold", "warm"} {
					t.Run(state, func(t *testing.T) {
						start, done := make(chan struct{}), make(chan struct{})
						var wg sync.WaitGroup
						for range workers {
							wg.Go(func() {
								<-start
								doc := map[string]any{"apiVersion": tc.version, "kind": tc.kind, "metadata": map[string]any{"name": "scope"}}
								obj, authoritative, _, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{DefaultNamespace: "apps"})
								if err != nil {
									t.Errorf("manifest.ParseDoc: %v", err)
									return
								}
								assert.Equal(t, obj.Named().Namespace, tc.namespace)
								_, ns := manifest.DocMetadata(authoritative)
								assert.Equal(t, ns, tc.namespace)
								rs := &manifest.ResourceSet{Namespace: "apps"}
								rs.DependsOn = []fluxopv1.Dependency{{APIVersion: tc.version, Kind: tc.kind, Name: "scope"}}
								for ref := range manifest.References(rs, manifest.ReferenceRelations) {
									assert.Equal(t, ref.Namespace, tc.namespace)
								}
								for _, namespace := range []string{"", "authored"} {
									doc := map[string]any{"apiVersion": tc.version, "kind": tc.kind, "metadata": map[string]any{"name": "authored", "namespace": "authored"}}
									obj, _, changed, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{DefaultNamespace: namespace})
									if err != nil {
										t.Error(err)
										return
									}
									assert.Equal(t, obj.Named().Namespace, "authored")
									assert.Equal(t, changed, false)
								}
							})
						}
						BuildMutex.Lock()
						if state == "cold" {
							err := openapi.SetSchema(nil, []byte(`{"definitions":{},"paths":{
					"/apis/scope.example/v1/clustercustoms":{"get":{
						"x-kubernetes-action":"get",
						"x-kubernetes-group-version-kind":{"group":"scope.example","version":"v1","kind":"ClusterCustom"}
					}}
				}}`), true)
							assert.NoError(t, err)
						}
						close(start)
						go func() { wg.Wait(); close(done) }()
						if state == "cold" {
							select {
							case <-done:
								openapi.ResetOpenAPI()
								BuildMutex.Unlock()
								t.Fatal("cold scope lookup completed while the build mutex was held")
							case <-time.After(time.Second):
							}
							openapi.ResetOpenAPI()
							BuildMutex.Unlock()
						}
						select {
						case <-done:
						case <-time.After(5 * time.Second):
							if state == "warm" {
								BuildMutex.Unlock()
							}
							t.Fatal("scope lookup did not complete while the build mutex was held")
						}
						if state == "warm" {
							BuildMutex.Unlock()
						}
					})
				}
			})
		}
	}
}

func TestScopeBuildIsolation(t *testing.T) {
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
			key := yaml.TypeMeta{APIVersion: "scope.test.example/v1", Kind: "ScopeCluster"}
			namespaceScopes.Delete(key)
			start, done := make(chan struct{}), make(chan struct{})
			started := make(chan struct{}, 2)
			var wg sync.WaitGroup
			BuildMutex.Lock()
			if state == "concurrent" {
				for range 2 {
					wg.Go(func() {
						<-start
						started <- struct{}{}
						(scopeResolver{}).ResolveScope(key)
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
				defer BuildMutex.Unlock()
				return Build(fs, dir)
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
			assert.Equal(t, (scopeResolver{}).ResolveScope(key), true)
		})
	}
}

func TestScopeResolver_MissDoesNotResetSchema(t *testing.T) {
	meta := yaml.TypeMeta{APIVersion: "noreset.example/v1", Kind: "ClusterWidget"}
	namespaceScopes.Delete(meta)
	t.Cleanup(func() {
		openapi.ResetOpenAPI()
		namespaceScopes.Delete(meta)
	})

	BuildMutex.Lock()
	err := openapi.SetSchema(nil, []byte(`{"definitions":{},"paths":{
		"/apis/noreset.example/v1/clusterwidgets":{"get":{
			"x-kubernetes-action":"get",
			"x-kubernetes-group-version-kind":{"group":"noreset.example","version":"v1","kind":"ClusterWidget"}
		}}
	}}`), true)
	BuildMutex.Unlock()

	assert.NoError(t, err)
	assert.Equal(t, (scopeResolver{}).ResolveScope(meta), false)
}
