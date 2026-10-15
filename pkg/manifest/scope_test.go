package manifest

import (
	"fmt"
	"sync"
	"testing"
	"time"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	"sigs.k8s.io/kustomize/kyaml/openapi"
	"sigs.k8s.io/kustomize/kyaml/yaml"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/kustomize"
)

func TestParseDoc_ScopeMemo(t *testing.T) {
	for _, workers := range []int{2, 4} {
		for _, tc := range []struct{ version, kind, namespace string }{
			{"apps/v1", "Deployment", "apps"},
			{"rbac.authorization.k8s.io/v1", "ClusterRole", ""},
			{"rbac.authorization.k8s.io/v999", "ClusterRole", "apps"},
			{"scope.example/v1", "ClusterCustom", "apps"},
		} {
			t.Run(fmt.Sprintf("%s/%s/workers%d", tc.version, tc.kind, workers), func(t *testing.T) {
				meta := yaml.TypeMeta{APIVersion: tc.version, Kind: tc.kind}
				namespaceScopes.Delete(meta)
				kustomize.BuildMutex.Lock()
				err := openapi.SetSchema(nil, []byte(`{"definitions":{},"paths":{
					"/apis/scope.example/v1/clustercustoms":{"get":{
						"x-kubernetes-action":"get",
						"x-kubernetes-group-version-kind":{"group":"scope.example","version":"v1","kind":"ClusterCustom"}
					}}
				}}`), true)
				kustomize.BuildMutex.Unlock()
				assert.NoError(t, err)
				for _, state := range []string{"cold", "warm"} {
					t.Run(state, func(t *testing.T) {
						start, done := make(chan struct{}), make(chan struct{})
						var wg sync.WaitGroup
						for range workers {
							wg.Go(func() {
								<-start
								doc := map[string]any{"apiVersion": tc.version, "kind": tc.kind, "metadata": map[string]any{"name": "scope"}}
								obj, authoritative, _, err := ParseDoc(doc, ParseDocOptions{DefaultNamespace: "apps"})
								if err != nil {
									t.Errorf("ParseDoc: %v", err)
									return
								}
								assert.Equal(t, obj.Named().Namespace, tc.namespace)
								_, ns := DocMetadata(authoritative)
								assert.Equal(t, ns, tc.namespace)
								rs := &ResourceSet{Namespace: "apps"}
								rs.DependsOn = []fluxopv1.Dependency{{APIVersion: tc.version, Kind: tc.kind, Name: "scope"}}
								for ref := range References(rs) {
									assert.Equal(t, ref.Namespace, tc.namespace)
								}
							})
						}
						kustomize.BuildMutex.Lock()
						close(start)
						go func() { wg.Wait(); close(done) }()
						if state == "cold" {
							select {
							case <-done:
								kustomize.BuildMutex.Unlock()
								t.Fatal("cold scope lookup completed while the build mutex was held")
							case <-time.After(time.Second):
							}
							kustomize.BuildMutex.Unlock()
						}
						select {
						case <-done:
						case <-time.After(5 * time.Second):
							if state == "warm" {
								kustomize.BuildMutex.Unlock()
							}
							t.Fatal("scope lookup did not complete while the build mutex was held")
						}
						if state == "warm" {
							kustomize.BuildMutex.Unlock()
						}
					})
				}
			})
		}
	}
}
