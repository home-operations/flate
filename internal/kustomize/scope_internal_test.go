package kustomize

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	filepath "path/filepath"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	"sigs.k8s.io/kustomize/api/resmap"
	"sigs.k8s.io/kustomize/kyaml/filesys"
	"sigs.k8s.io/kustomize/kyaml/openapi"
	"sigs.k8s.io/kustomize/kyaml/yaml"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

//nolint:paralleltest // Tests mutate the process-wide OpenAPI schema and scope memo.
func TestParseDoc_ScopeMemo(t *testing.T) {
	t.Run("no_load", func(t *testing.T) { parseDocScopeMemoNoLoad(t) })

	t.Run("dependency_preparation", func(t *testing.T) { parseDocScopeMemoDependencyPreparation(t) })
	t.Run("memo_miss", func(t *testing.T) { parseDocScopeMemoMemoMiss(t) })

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
			t.Run(
				fmt.Sprintf("%s/%s/workers%d", tc.version, tc.kind, workers),
				func(t *testing.T) { parseDocScopeMemoCase(t, &tc, &workers) },
			)
		}
	}
}

//nolint:paralleltest // Tests mutate the process-wide OpenAPI schema and scope memo.
func TestScopeBuildIsolation(t *testing.T) {
	fs := filesys.MakeFsInMemory()
	for _, dir := range []string{"/success", "/error"} {
		assert.NoError(t, fs.MkdirAll(dir))
		resource := "resource.yaml"
		if dir == "/error" {
			resource = "missing.yaml"
		}
		assert.NoError(
			t,
			fs.WriteFile(
				dir+"/kustomization.yaml",
				[]byte("namespace: build\nresources: ["+resource+"]\nopenapi:\n  path: schema.json\n"),
			),
		)
		assert.NoError(
			t,
			fs.WriteFile(
				dir+"/resource.yaml",
				[]byte("apiVersion: scope.test.example/v1\nkind: ScopeCluster\nmetadata:\n  name: custom\n"),
			),
		)
		assert.NoError(t, fs.WriteFile(dir+"/schema.json", []byte(`{"definitions":{},"paths":{
			"/apis/scope.test.example/v1/scopeclusters":{"get":{
				"x-kubernetes-action":"get",
				"x-kubernetes-group-version-kind":{"group":"scope.test.example","version":"v1","kind":"ScopeCluster"}
			}}
		}}`)))
	}
	for _, state := range []string{"success", "error", "concurrent"} {
		t.Run(state, func(t *testing.T) {
			scopeBuildIsolationState(t, &state, (*interface {
				CleanedAbs(path string) (filesys.ConfirmedDir, string, error)
				Create(path string) (filesys.File, error)
				Exists(path string) bool
				Glob(pattern string) ([]string, error)
				IsDir(path string) bool
				Mkdir(path string) error
				MkdirAll(path string) error
				Open(path string) (filesys.File, error)
				ReadDir(path string) ([]string, error)
				ReadFile(path string) ([]byte, error)
				RemoveAll(path string) error
				Walk(path string, walkFn filepath.WalkFunc) error
				WriteFile(path string, data []byte) error
			})(&fs))
		})
	}
}

//nolint:paralleltest // Tests mutate the process-wide OpenAPI schema and scope memo.
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

func parseDocScopeMemoNoLoad(t *testing.T) {
	t.Helper()

	BuildMutex.Lock()
	root := openapi.Schema()
	loaded := len(root.Definitions)
	openapi.ResetOpenAPI()
	BuildMutex.Unlock()
	assert.FatalIf(t, loaded == 0, "%v", "builtin schema setup has no definitions")
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
}

func parseDocScopeMemoDependencyPreparation(t *testing.T) {
	t.Helper()

	for _, workers := range []int{2, 4} {
		for _, namespace := range []string{"", "authored"} {
			for _, fallback := range []string{"", "default"} {
				t.Run(
					fmt.Sprintf("workers%d/namespace=%s/default=%s", workers, namespace, fallback),
					func(t *testing.T) {
						parseDocScopeMemoDependencyPreparationCase(t, &namespace, &fallback, &workers)
					},
				)
			}
		}
	}
}

func parseDocScopeMemoMemoMiss(t *testing.T) {
	t.Helper()

	meta := yaml.TypeMeta{APIVersion: "unprepared.scope.example/v1", Kind: "NeverPrepared"}
	namespaceScopes.Delete(meta)
	rs := &manifest.ResourceSet{
		Namespace: "apps",
		DependsOn: []fluxopv1.Dependency{{APIVersion: meta.APIVersion, Kind: meta.Kind, Name: "missing"}},
	}
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
}

func parseDocScopeMemoCase(t *testing.T, tc *struct {
	version   string
	kind      string
	namespace string
}, workers *int,
) {
	t.Helper()

	meta := yaml.TypeMeta{APIVersion: tc.version, Kind: tc.kind}
	namespaceScopes.Delete(meta)
	for _, state := range []string{"cold", "warm"} {
		t.Run(state, func(t *testing.T) { parseDocScopeMemoCaseState(t, &workers, &tc, &state) })
	}
}

func parseDocScopeMemoDependencyPreparationCase(t *testing.T, namespace *string, fallback *string, workers *int) {
	t.Helper()

	meta := yaml.TypeMeta{APIVersion: "scope-preparation.example/v1", Kind: "FreshDependency"}
	cluster := yaml.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole"}
	namespaceScopes.Delete(meta)
	namespaceScopes.Delete(cluster)
	doc := map[string]any{
		"apiVersion": manifest.FluxOperatorDomain + "/v1",
		"kind":       manifest.KindResourceSet,
		"metadata": map[string]any{
			"name":      "set",
			"namespace": (*namespace),
		},
		"spec": map[string]any{"dependsOn": []any{
			map[string]any{
				"apiVersion": meta.APIVersion,
				"kind":       meta.Kind,
				"name":       "fresh",
				"namespace":  "explicit",
			},
			map[string]any{
				"apiVersion": cluster.APIVersion,
				"kind":       cluster.Kind,
				"name":       "cluster",
				"namespace":  "explicit",
			},
		}},
	}
	obj, _, _, err := manifest.ParseDoc(
		doc,
		manifest.ParseDocOptions{DefaultNamespace: (*fallback)},
	)
	assert.NoError(t, err)
	wantNamespace := (*namespace)
	if wantNamespace == "" {
		wantNamespace = (*fallback)
	}
	assert.Equal(t, obj.Named().Namespace, wantNamespace)
	for _, key := range []yaml.TypeMeta{meta, cluster} {
		_, prepared := (scopeResolver{}).LookupScope(key)
		assert.Equal(t, prepared, true)
	}
	BuildMutex.Lock()
	done := make(chan struct{})
	var wg sync.WaitGroup
	for range *workers {
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
}

func parseDocScopeMemoCaseState(t *testing.T, workers **int, tc **struct {
	version   string
	kind      string
	namespace string
}, state *string,
) {
	t.Helper()

	start, done := make(chan struct{}), make(chan struct{})
	var wg sync.WaitGroup
	for range *(*workers) {
		wg.Go(func() { parseDocScopeMemoCaseStateBody(&start, &tc, &t) })
	}
	BuildMutex.Lock()
	if (*state) == "cold" {
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
	parseDocScopeMemoCaseStateLoop(t, struct {
		state **string
		done  *chan struct{}
	}{state: &state, done: &done})
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		if (*state) == "warm" {
			BuildMutex.Unlock()
		}
		t.Fatal("scope lookup did not complete while the build mutex was held")
	}
	if (*state) == "warm" {
		BuildMutex.Unlock()
	}
}

func scopeBuildIsolationState(t *testing.T, state *string, fs *interface {
	CleanedAbs(path string) (filesys.ConfirmedDir, string, error)
	Create(path string) (filesys.File, error)
	Exists(path string) bool
	Glob(pattern string) ([]string, error)
	IsDir(path string) bool
	Mkdir(path string) error
	MkdirAll(path string) error
	Open(path string) (filesys.File, error)
	ReadDir(path string) ([]string, error)
	ReadFile(path string) ([]byte, error)
	RemoveAll(path string) error
	Walk(path string, walkFn filepath.WalkFunc) error
	WriteFile(path string, data []byte) error
},
) {
	t.Helper()

	key := yaml.TypeMeta{APIVersion: "scope.test.example/v1", Kind: "ScopeCluster"}
	namespaceScopes.Delete(key)
	start, done := make(chan struct{}), make(chan struct{})
	started := make(chan struct{}, 2)
	var wg sync.WaitGroup
	BuildMutex.Lock()
	if (*state) == "concurrent" {
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
	if (*state) == "error" {
		dir = "/error"
	}
	rm, err := func() (resmap.ResMap, error) {
		defer BuildMutex.Unlock()
		return Build((*fs), dir)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("scope workers did not finish after the custom build")
	}
	if (*state) == "error" {
		assert.FatalIf(
			t,
			err == nil || !strings.Contains(err.Error(), "missing.yaml"),
			"expected missing-resource error, got %v",
			err,
		)
	} else {
		assert.NoError(t, err)
		assert.Equal(t, len(rm.Resources()), 1)
		assert.Equal(t, rm.Resources()[0].GetNamespace(), "")
	}
	assert.Equal(t, (scopeResolver{}).ResolveScope(key), true)
}

func parseDocScopeMemoCaseStateBody(start *chan struct{}, tc ***struct {
	version   string
	kind      string
	namespace string
}, t **testing.T,
) {
	(*t).Helper()

	<-(*start)
	doc := map[string]any{
		"apiVersion": (*(*tc)).version,
		"kind":       (*(*tc)).kind,
		"metadata":   map[string]any{"name": "scope"},
	}
	obj, authoritative, _, err := manifest.ParseDoc(
		doc,
		manifest.ParseDocOptions{DefaultNamespace: "apps"},
	)
	if err != nil {
		(*t).Errorf("manifest.ParseDoc: %v", err)
		return
	}
	assert.Equal((*t), obj.Named().Namespace, (*(*tc)).namespace)
	_, ns := manifest.DocMetadata(authoritative)
	assert.Equal((*t), ns, (*(*tc)).namespace)
	rs := &manifest.ResourceSet{Namespace: "apps"}
	rs.DependsOn = []fluxopv1.Dependency{
		{APIVersion: (*(*tc)).version, Kind: (*(*tc)).kind, Name: "scope"},
	}
	for ref := range manifest.References(rs, manifest.ReferenceRelations) {
		assert.Equal((*t), ref.Namespace, (*(*tc)).namespace)
	}
	for _, namespace := range []string{"", "authored"} {
		doc := map[string]any{
			"apiVersion": (*(*tc)).version,
			"kind":       (*(*tc)).kind,
			"metadata":   map[string]any{"name": "authored", "namespace": "authored"},
		}
		obj, _, changed, err := manifest.ParseDoc(
			doc,
			manifest.ParseDocOptions{DefaultNamespace: namespace},
		)
		if err != nil {
			(*t).Error(err)
			return
		}
		assert.Equal((*t), obj.Named().Namespace, "authored")
		assert.Equal((*t), changed, false)
	}
}

func parseDocScopeMemoCaseStateLoop(t *testing.T, loopInputs struct {
	state **string
	done  *chan struct{}
},
) {
	t.Helper()

	if (*(*loopInputs.state)) == "cold" {
		select {
		case <-(*loopInputs.done):
			openapi.ResetOpenAPI()
			BuildMutex.Unlock()
			t.Fatal("cold scope lookup completed while the build mutex was held")
		case <-time.After(time.Second):
		}
		openapi.ResetOpenAPI()
		BuildMutex.Unlock()
	}
}
