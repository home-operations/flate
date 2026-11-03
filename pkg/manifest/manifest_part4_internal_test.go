package manifest

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"

	"github.com/home-operations/flate/internal/assert"
)

func TestSplitDocs(t *testing.T) {
	t.Parallel()
	data := []byte(`
apiVersion: v1
kind: ConfigMap
metadata: {name: a, namespace: ns}
---
apiVersion: v1
kind: ConfigMap
metadata: {name: b, namespace: ns}
`)
	docs, err := splitDocMaps(data)
	{
		assert.FatalIf(t, err != nil, "SplitDocs: %v", err)
	}
	{
		assert.FatalIf(t, len(docs) != 2, "expected 2 docs, got %d", len(docs))
	}
	names := []string{
		docs[0]["metadata"].(map[string]any)["name"].(string),
		docs[1]["metadata"].(map[string]any)["name"].(string),
	}
	assert.Diff(t, names, []string{"a", "b"})
}

func TestParseDoc_PoolReuseConsistency(t *testing.T) {
	t.Parallel()
	hr := []byte(`
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: app
  namespace: default
spec:
  chart:
    spec:
      chart: app-template
      version: "3.5.1"
      sourceRef:
        kind: HelmRepository
        name: bjw-s
        namespace: flux-system
  values:
    replicas: 2
    env:
      TZ: UTC
`)
	ks := []byte(`
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: apps
  namespace: flux-system
spec:
  path: ./apps
  sourceRef: {kind: GitRepository, name: flux-system}
  postBuild:
    substitute:
      X: y
`)
	cm := []byte(`
apiVersion: v1
kind: ConfigMap
metadata:
  name: cm
  namespace: ns
data:
  key1: value1
  key2: value2
`)
	secret := "apiVersion: v1\nkind: Secret\nmetadata: {name: credentials, namespace: ns}\ndata: {key: dmFsdWU=}\n"
	raw := "apiVersion: apps/v1\nkind: Deployment\nmetadata: {name: app}\nspec: {value: keep}\n"
	for _, input := range []string{string(hr), string(ks), string(cm), secret, raw} {
		t.Run(DocKind(mustYAML(t, input)), func(t *testing.T) {
			t.Parallel()
			testParseDocPoolReuseConsistencyCase(t, &input)
		},
		)
	}

	for _, input := range []string{string(hr), string(ks), string(cm), secret, raw} {
		docs, err := decodeDocMaps(strings.NewReader(input))
		assert.NoError(t, err)
		doc := docs[0]
		delete(doc["metadata"].(map[string]any), "namespace")
		before := DeepCopyMap(doc)
		opts := ParseDocOptions{WipeSecrets: true, DefaultNamespace: "apps"}
		obj, authoritative, changed, err := ParseDoc(doc, opts)
		assert.NoError(t, err)
		assert.Equal(t, obj.Named().Namespace, "apps")
		assert.Equal(t, changed, true)
		replay, retained, changed, err := ParseDoc(authoritative, opts)
		assert.NoError(t, err)
		assert.Equal(t, replay.Named(), obj.Named())
		assert.Equal(t, changed, false)
		{
			next, err := decodeDocMaps(
				strings.NewReader(
					"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: next, namespace: other}\ndata: {key: next}\n",
				),
			)
			assert.NoError(t, err)
			_, _, _, err = ParseDoc(next[0], opts)
			assert.NoError(t, err)
			ReleaseDoc(next[0])
		}
		assert.Diff(t, doc, before)
		expected := DeepCopyMap(before)
		expected["metadata"].(map[string]any)["namespace"] = "apps"
		assert.Diff(t, authoritative, expected)
		assert.Diff(t, retained, expected)
		switch obj := obj.(type) {
		case *RawObject:
			assert.Diff(t, obj.Spec, before["spec"])
		}
	}
}

// TestSplitDocs_AliasAsMapKey pins the home-ops pattern of using a
// YAML anchor as a mapping key (e.g. `&app sonarr` in metadata.name,
// referenced later as `*app :` to share the literal across the doc).
// Sigs/Flux YAML parsers resolve the alias to its underlying scalar
// before assigning into the map; flate's manual node walk used to
// reject any non-scalar key kind. See m00nwtchr/homelab-cluster
// apps/media/sonarr/app/helmrelease.yaml.
func TestSplitDocs_AliasAsMapKey(t *testing.T) {
	t.Parallel()
	data := []byte(`
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: &app sonarr
spec:
  values:
    controllers:
      *app :
        replicas: 2
`)
	docs, err := splitDocMaps(data)
	{
		assert.FatalIf(t, err != nil, "SplitDocs: %v", err)
	}
	{
		assert.FatalIf(t, len(docs) != 1, "expected 1 doc, got %d", len(docs))
	}
	controllers, _ := docs[0]["spec"].(map[string]any)["values"].(map[string]any)["controllers"].(map[string]any)
	{
		_, ok := controllers["sonarr"]
		assert.ErrorIf(t, !ok, "alias key not resolved to scalar; controllers=%v", controllers)
	}
}

func TestParseDoc_BuildDirectives(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"Kustomization", "Component"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			obj, _, _, err := ParseDoc(
				map[string]any{"kind": kind, "apiVersion": "kustomize.config.k8s.io/v1beta1"},
				ParseDocOptions{DefaultNamespace: "apps"},
			)
			{
				assert.FatalIf(t, err != nil || obj != nil, "ParseDoc = %T, %v; want nil, nil", obj, err)
			}
		})
	}
}

func TestParseHelmRelease_DisableValidationCombinations(t *testing.T) {
	t.Parallel()
	for _, flags := range []struct {
		name            string
		schema, openAPI bool
	}{
		{name: "neither"},
		{name: "schema", schema: true},
		{name: "openapi", openAPI: true},
		{name: "both", schema: true, openAPI: true},
	} {
		for _, stage := range []string{"neither", "install", "upgrade", "both"} {
			t.Run(flags.name+"/"+stage, func(t *testing.T) {
				t.Parallel()
				enabled := fmt.Sprintf(
					"{disableSchemaValidation: %t, disableOpenAPIValidation: %t}",
					flags.schema,
					flags.openAPI,
				)
				var spec string
				if stage == "install" || stage == "both" {
					spec += "  install: " + enabled + "\n"
				}
				if stage == "upgrade" || stage == "both" {
					spec += "  upgrade: " + enabled + "\n"
				}
				hr, err := parseHelmRelease(helmReleaseDoc(t, spec))
				{
					assert.NoError(t, err)
				}
				wantSchema := (stage == "install" || stage == "both") && flags.schema
				assert.Equal(t, hr.DisableSchemaValidation, wantSchema)
				release := helmv2.HelmRelease{Spec: hr.HelmReleaseSpec}
				assert.Equal(
					t,
					release.GetInstall().DisableOpenAPIValidation,
					(stage == "install" || stage == "both") && flags.openAPI,
				)
				assert.Equal(
					t,
					release.GetUpgrade().DisableOpenAPIValidation,
					(stage == "upgrade" || stage == "both") && flags.openAPI,
				)
			})
		}
	}
}

func TestCheckAPIVersion_ExactGroup(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		version string
		wantErr error
		invalid bool
	}{
		{version: SourceDomain + "/v1"},
		{version: SourceDomain + ".example.com/v1", wantErr: ErrInput},
		{version: "", wantErr: ErrInput},
		{version: SourceDomain + "/", wantErr: ErrInput, invalid: true},
		{version: SourceDomain + "/v1/extra", wantErr: ErrInput, invalid: true},
	} {
		t.Run(tc.version, func(t *testing.T) {
			t.Parallel()
			err := checkAPIVersion(map[string]any{"apiVersion": tc.version}, SourceDomain)
			{
				assert.ErrorIf(t, !errors.Is(err, tc.wantErr) || (tc.wantErr != nil && !errors.Is(err, ErrFlux)), "error = %v, want %v", err, tc.wantErr)
			}
			_, invalid := errors.AsType[*field.Error](err)
			assert.Equal(t, invalid, tc.invalid)
			if tc.invalid {
				for _, kind := range []string{KindGitRepository, "Widget"} {
					doc := map[string]any{"apiVersion": tc.version, "kind": kind, "metadata": 7, "spec": "bad"}
					_, authoritative, changed, err := ParseDoc(doc, ParseDocOptions{DefaultNamespace: "apps"})
					invalid, ok := errors.AsType[*field.Error](err)
					{
						assert.FatalIf(
							t,
							!ok || invalid.Field != "apiVersion" || !errors.Is(err, ErrInput) ||
								!errors.Is(err, ErrFlux),
							"version error lost precedence or classification: %v",
							err,
						)
					}
					assert.Equal(t, changed, false)
					assert.Diff(t, authoritative, doc)
				}
			}
		})
	}
}

func TestResolve_NamespacePrecedence(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, forced, authored, fallback, want string }{
		{"forced", "forced", "authored", "fallback", "forced"},
		{"authored", "", "authored", "fallback", "authored"},
		{"fallback", "", "", "fallback", "fallback"},
		{"empty", "", "", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			id := NamedResource{
				Group:     FluxKustomizeDomain,
				Kind:      KindKustomization,
				Namespace: tc.authored,
				Name:      "app",
			}
			want := NamedResource{Group: FluxKustomizeDomain, Kind: KindKustomization, Namespace: tc.want, Name: "app"}
			assert.Equal(t, Resolve(id, tc.forced, tc.fallback), want)
			assert.Equal(
				t,
				id,
				NamedResource{Group: FluxKustomizeDomain, Kind: KindKustomization, Namespace: tc.authored, Name: "app"},
			)
		})
	}
}

func TestNamedResource_BindLocal(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{KindGitRepository, KindOCIRepository, KindBucket} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			testNamedResourceBindLocalCase(t, &kind)
		},
		)
	}
	for _, id := range []NamedResource{
		{Group: "wrong.group", Kind: KindGitRepository, Namespace: "other", Name: "entry"},
		{Group: SourceDomain, Kind: "Unknown", Namespace: "other", Name: "entry"},
		{Group: SourceDomain, Kind: KindBucket, Name: "entry"},
		{Group: SourceDomain, Kind: KindOCIRepository, Namespace: "other"},
		{Kind: KindConfigMap, Namespace: "other", Name: "entry"},
	} {
		assert.Equal(t, id.CanBindLocal(), false)
		{
			obj, err := id.BindLocal(
				"/chosen/root",
			)
			assert.FatalIf(t, obj != nil || !errors.Is(err, ErrInput) ||
				!errors.Is(err, ErrFlux), "invalid identity %s: %v, %v", id, obj, err)
		}
	}
}

func TestBootstrapAlias_SourceKinds(t *testing.T) {
	t.Parallel()
	ignore := "excluded/"
	ref := &sourcev1.GitRepositoryRef{Tag: "v1"}
	git := &GitRepository{
		Name:              "entry",
		Namespace:         "other",
		URL:               "https://fixture.invalid/repo",
		Reference:         ref,
		Ignore:            &ignore,
		SparseCheckout:    []string{"apps"},
		RecurseSubmodules: true,
	}
	for _, obj := range []BaseManifest{git, &OCIRepository{}, &Bucket{}, &HelmRepository{}} {
		t.Run(obj.Named().Kind, func(t *testing.T) {
			t.Parallel()
			behavior, ok := obj.(interface {
				BootstrapAlias(string) (BaseManifest, *GitRepository)
			})
			{
				assert.FatalIf(t, ok != (obj == git), "bootstrap capability on %T", obj)
			}
			if !ok {
				return
			}
			before := Fingerprint(obj)
			alias, descriptor := behavior.BootstrapAlias("/repo")
			assert.Equal(t, alias.Named(), obj.Named())
			{
				assert.FatalIf(
					t,
					alias == obj || Fingerprint(obj) != before,
					"%s",
					"alias mutated or retained receiver",
				)
			}
			a := alias.(*GitRepository)
			assert.Equal(t, descriptor, git)
			assert.Equal(t, a.URL, "file:///repo")
			assert.Equal(t, a.Reference, ref)
			assert.Equal(t, a.Ignore, &ignore)
			assert.Equal(t, a.RecurseSubmodules, true)
			assert.Diff(t, a.SparseCheckout, []string{"apps"})
		})
	}
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
//nolint:paralleltest // This test changes process-global state.
func TestDocIdentity_ReadOnly(t *testing.T) {
	for _, tc := range []struct {
		name string
		doc  map[string]any
		want NamedResource
	}{
		{name: "nil"},
		{name: "empty", doc: map[string]any{}},
		{name: "wrong fields", doc: map[string]any{"apiVersion": 1, "kind": false, "metadata": "wrong"}},
		{name: "null metadata", doc: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": nil}, want: NamedResource{Kind: "ConfigMap"}},
		{name: "wrong metadata fields", doc: map[string]any{"apiVersion": "example.test/v1", "kind": "Widget", "metadata": map[string]any{"name": 1, "namespace": false}}, want: NamedResource{Group: "example.test", Kind: "Widget"}},
		{name: "core", doc: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "config", "namespace": "apps"}}, want: NamedResource{Kind: "ConfigMap", Namespace: "apps", Name: "config"}},
		{name: "group", doc: map[string]any{"apiVersion": "example.test/v1", "kind": "Widget", "metadata": map[string]any{"name": "widget", "namespace": "apps"}}, want: NamedResource{Group: "example.test", Kind: "Widget", Namespace: "apps", Name: "widget"}},
		{name: "version", doc: map[string]any{"apiVersion": "example.test/v2", "kind": "Widget", "metadata": map[string]any{"name": "widget", "namespace": "apps"}}, want: NamedResource{Group: "example.test", Kind: "Widget", Namespace: "apps", Name: "widget"}},
		{name: "permissive group", doc: map[string]any{"apiVersion": "example.test/v1/extra", "kind": "Widget", "metadata": map[string]any{"name": "widget"}}, want: NamedResource{Group: "example.test", Kind: "Widget", Name: "widget"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := DeepCopyMap(tc.doc)
			for range 2 {
				assert.Equal(t, DocIdentity(tc.doc), tc.want)
			}
			assert.Diff(t, tc.doc, before)
			assert.Equal(t, testing.AllocsPerRun(100, func() { DocIdentity(tc.doc) }), float64(0))
		})
	}
}

func TestNamedResource_Labels(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		id         NamedResource
		kind, full string
	}{
		{"core", NamedResource{Kind: "ConfigMap", Namespace: "ns", Name: "shared"}, "ConfigMap", "ConfigMap/ns/shared"},
		{"foreign group", NamedResource{Group: "example.test", Kind: "ConfigMap", Namespace: "ns", Name: "shared"}, "ConfigMap.example.test", "ConfigMap.example.test/ns/shared"},
		{"cluster", NamedResource{Group: "example.test", Kind: "Widget", Name: "shared"}, "Widget.example.test", "Widget.example.test/shared"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.id.KindLabel(), tc.kind)
			assert.Equal(t, tc.id.String(), tc.full)
		})
	}
}

func testParseDocPoolReuseConsistencyCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	docs, err := decodeDocMaps(strings.NewReader((*testCaseInput)))
	assert.NoError(t, err)
	obj, _, _, err := ParseDoc(docs[0], defaultParseDocOptions())
	assert.NoError(t, err)
	before := Fingerprint(obj)
	var projected map[string]any
	if k, ok := obj.(*Kustomization); ok {
		projected, err = k.RenderDocument()
		assert.NoError(t, err)
	}
	ReleaseDoc(docs[0])
	next, err := decodeDocMaps(
		strings.NewReader("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: next}\ndata: {key: next}\n"),
	)
	assert.NoError(t, err)
	ReleaseDoc(next[0])
	assert.Equal(t, Fingerprint(obj), before)
	switch v := obj.(type) {
	case *Kustomization:
		doc, err := v.RenderDocument()
		assert.NoError(t, err)
		assert.Diff(t, doc, projected)
		start := make(chan struct{})
		done := make(chan error, 2)
		for i := range 2 {
			changed := v.Clone()
			changed.Path = fmt.Sprintf("typed-%d", i)
			changed.PostBuild.Substitute["X"] = changed.Path
			go func() {
				<-start
				edited, renderDocumentErr := changed.RenderDocument()
				if renderDocumentErr == nil && edited["spec"].(map[string]any)["path"] != changed.Path {
					renderDocumentErr = fmt.Errorf("projection did not reflect typed path %q", changed.Path)
				}
				done <- renderDocumentErr
			}()
		}
		close(start)
		for range 2 {
			assert.NoError(t, <-done)
		}
		unchanged, err := v.RenderDocument()
		assert.NoError(t, err)
		assert.Diff(t, unchanged, projected)
		assert.Diff(t, doc, projected)
		assert.Equal(t, v.PostBuild.Substitute["X"], "y")
	case *ConfigMap:
		assert.Equal(t, v.Data["key1"], any("value1"))
	case *Secret:
		assert.Equal(t, v.Data["key"], any("dmFsdWU="))
	case *RawObject:
		assert.Equal(t, v.Spec["value"], any("keep"))
	}
}

func testNamedResourceBindLocalCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	id := NamedResource{Group: SourceDomain, Kind: (*testCaseInput), Namespace: "other", Name: "entry"}
	assert.Equal(t, id.CanBindLocal(), true)
	obj, err := id.BindLocal("/chosen/root")
	assert.NoError(t, err)
	assert.Equal(t, obj.Named(), id)
	assert.Equal(t, KindOf(obj).APIVersion(), sourcev1.GroupVersion.String())
	switch *testCaseInput {
	case KindGitRepository:
		{
			g, ok := obj.(*GitRepository)
			assert.FatalIf(t, !ok || g.URL != "file:///chosen/root" ||
				g.Provider != sourcev1.GitProviderGeneric, "decoded Git identity: %#v", obj)
		}
	case KindOCIRepository:
		{
			o, ok := obj.(*OCIRepository)
			assert.FatalIf(t, !ok || o.URL != "file:///chosen/root" ||
				o.Provider != sourcev1.GenericOCIProvider, "decoded OCI identity: %#v", obj)
		}
	case KindBucket:
		{
			b, ok := obj.(*Bucket)
			assert.FatalIf(t, !ok || b.Endpoint != "file:///chosen/root" || b.BucketName != id.Name ||
				b.Provider != sourcev1.BucketProviderGeneric, "decoded bucket identity: %#v", obj)
		}
	}
	doc := map[string]any{
		"apiVersion": sourcev1.GroupVersion.String(),
		"kind":       (*testCaseInput),
		"metadata":   map[string]any{"name": id.Name, "namespace": id.Namespace},
	}
	_, _, _, err = ParseDoc(doc, defaultParseDocOptions())
	{
		assert.FatalIf(
			t,
			!errors.Is(err, ErrInput) || !errors.Is(err, ErrFlux),
			"authored missing spec: %v",
			err,
		)
	}
}
