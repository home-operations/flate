package resourceset_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	apix "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/resourceset"
	"github.com/home-operations/flate/pkg/store"
)

func TestRender_InputsFrom_StaticProvider(t *testing.T) {
	t.Parallel()
	rsip := &manifest.ResourceSetInputProvider{
		Name: "apps", Namespace: "kube-system", Labels: map[string]string{"set": "apps"},
		Type: fluxopv1.InputProviderStatic,
		DefaultValues: fluxopv1.ResourceSetInput{
			"defaults": jsonTmpl(t, `{"capacity": "1Gi"}`),
			"apps":     jsonTmpl(t, `[{"app": "alpha"}, {"app": "bravo", "capacity": "5Gi"}]`),
		},
	}
	rs := &manifest.ResourceSet{
		Name: "volsync", Namespace: "kube-system",
		InputsFrom: []fluxopv1.InputProviderReference{
			{Name: "apps"},
		},
		ResourcesTemplate: `<<- range $app := inputs.apps >>
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: << $app.app >>
  namespace: default
data:
  capacity: << get $app "capacity" | default inputs.defaults.capacity >>
<<- end >>
`,
	}
	st := store.New()
	st.AddObject(rsip)
	other := *rsip
	other.Namespace = "other"
	st.AddObject(&other)
	st.AddObject(
		&manifest.RawObject{
			APIVersion: "example.test/v1",
			Kind:       manifest.KindResourceSetInputProvider,
			Name:       rsip.Name,
			Namespace:  rsip.Namespace,
		},
	)
	docs, _, err := renderProviders(rs, resourceset.StoreResolver(st), nil)
	{
		assert.FatalIf(t, err != nil, "Render: %v", err)
	}
	{
		assert.FatalIf(t, len(docs) != 2, "expected 2 ConfigMaps, got %d", len(docs))
	}
	caps := map[string]string{}
	for _, doc := range docs {
		md := doc.Document["metadata"].(map[string]any)
		data := doc.Document["data"].(map[string]any)
		caps[md["name"].(string)] = data["capacity"].(string)
	}
	assert.Diff(t, caps, map[string]string{"alpha": "1Gi", "bravo": "5Gi"})
	for _, selector := range []bool{false, true} {
		for _, tc := range []struct {
			name, apiVersion, kind string
			want                   int
		}{
			{"omitted", "", "", 2},
			{"explicit", "fluxcd.controlplane.io/v1", manifest.KindResourceSetInputProvider, 2},
			{"kind only", "", manifest.KindResourceSetInputProvider, 2},
			{"version only", "fluxcd.controlplane.io/v1", "", 2},
			{"foreign", "example.test/v1", "", 0},
			{"core", "v1", "", 0},
			{"other kind", "", manifest.KindResourceSet, 0},
		} {
			t.Run(fmt.Sprintf("%s/selector=%t", tc.name, selector), func(t *testing.T) {
				t.Parallel()
				testRenderInputsFromStaticProviderCase(
					t,
					&testRenderInputsFromStaticProviderCaseInputs{
						rsip:     &rsip,
						rs:       &rs,
						st:       &st,
						selector: &selector,
						tc:       &tc,
					},
				)
			},
			)
		}
	}
	for _, tc := range []struct {
		name string
		refs []fluxopv1.InputProviderReference
		want int
	}{
		{"expressions", []fluxopv1.InputProviderReference{{Selector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "set", Operator: metav1.LabelSelectorOpIn, Values: []string{"apps"}}}}}}, 2},
		{"empty selector", []fluxopv1.InputProviderReference{{Selector: &metav1.LabelSelector{}}}, 2},
		{"named invalid selector", []fluxopv1.InputProviderReference{{Name: rsip.Name, Selector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "set", Operator: "invalid"}}}}}, 2},
		{"nil selector", []fluxopv1.InputProviderReference{{}}, 0},
		{"overlapping references", []fluxopv1.InputProviderReference{{Name: rsip.Name}, {Selector: &metav1.LabelSelector{}}, {Selector: &metav1.LabelSelector{MatchLabels: rsip.Labels}}}, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testRenderInputsFromStaticProviderCase2(
				t,
				&testRenderInputsFromStaticProviderCase2Inputs{rs: &rs, st: &st, docs: &docs, tc: &tc},
			)
		},
		)
	}
}

func TestStoreResolver_CollectionAssessmentTotal(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		provider  bool
		selector  *metav1.LabelSelector
		wantError bool
		wantDocs  int
	}{
		{name: "empty", selector: &metav1.LabelSelector{}},
		{name: "null values", provider: true, selector: &metav1.LabelSelector{}, wantDocs: 1},
		{name: "selector error", selector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "role", Operator: "invalid"}}}, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			if tc.provider {
				st.AddObject(
					&manifest.ResourceSetInputProvider{
						Name:          "provider",
						Namespace:     "apps",
						DefaultValues: fluxopv1.ResourceSetInput{"value": nil},
					},
				)
			}
			rs := &manifest.ResourceSet{
				Name:              "reader",
				Namespace:         "apps",
				InputsFrom:        []fluxopv1.InputProviderReference{{Selector: tc.selector}},
				ResourcesTemplate: "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output}\n",
			}
			var reads store.ReadSet
			reads.Init(st, rs.Named(), nil)
			defer reads.Retire()
			docs, _, err := renderProviders(rs, resourceset.StoreResolver(&reads), nil)
			{
				assert.FatalIf(t, (err != nil) != tc.wantError, "prepare error=%v, want error=%t", err, tc.wantError)
			}
			if tc.wantError {
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, len(docs), tc.wantDocs)
			assert.Equal(t, reads.Valid(), true)
		})
	}
}

func TestStoreResolver_SelectorInputs(t *testing.T) {
	t.Parallel()
	db := &manifest.ResourceSetInputProvider{
		Name:          "db",
		Namespace:     "apps",
		Labels:        map[string]string{"role": "db"},
		DefaultValues: fluxopv1.ResourceSetInput{"value": jsonTmpl(t, `"first"`)},
	}
	web := *db
	web.Name, web.Labels = "web", map[string]string{"role": "web"}
	other := *db
	other.Namespace = "other"
	invalid := &metav1.LabelSelector{
		MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "role", Operator: "invalid"}},
	}
	selected := &metav1.LabelSelector{MatchLabels: map[string]string{"role": "db"}}
	scope := manifest.NamedResource{
		Group:     manifest.FluxOperatorDomain,
		Kind:      manifest.KindResourceSetInputProvider,
		Namespace: "apps",
	}
	for _, tc := range []struct {
		name      string
		providers []manifest.BaseManifest
		ref       fluxopv1.InputProviderReference
		id        manifest.NamedResource
		want      []string
		wantError bool
	}{
		{"success", []manifest.BaseManifest{db, &web}, fluxopv1.InputProviderReference{Selector: selected}, scope, []string{"db"}, false},
		{"empty selector", []manifest.BaseManifest{db, &web}, fluxopv1.InputProviderReference{Selector: &metav1.LabelSelector{}}, scope, []string{"db", "web"}, false},
		{"invalid eligible", []manifest.BaseManifest{db}, fluxopv1.InputProviderReference{Selector: invalid}, scope, nil, true},
		{"invalid empty", nil, fluxopv1.InputProviderReference{Selector: invalid}, scope, nil, true},
		{"invalid other namespace", []manifest.BaseManifest{&other}, fluxopv1.InputProviderReference{Selector: invalid}, scope, nil, true},
		{"invalid foreign group", []manifest.BaseManifest{db, &manifest.RawObject{APIVersion: "other.example/v1", Kind: scope.Kind, Namespace: scope.Namespace, Name: db.Name}}, fluxopv1.InputProviderReference{Selector: invalid}, manifest.NamedResource{Group: "other.example", Kind: scope.Kind, Namespace: scope.Namespace}, nil, true},
		{"invalid foreign kind", []manifest.BaseManifest{db, &manifest.ResourceSet{Name: db.Name, Namespace: scope.Namespace}}, fluxopv1.InputProviderReference{Selector: invalid}, manifest.NamedResource{Group: scope.Group, Kind: manifest.KindResourceSet, Namespace: scope.Namespace}, nil, true},
		{"named invalid selector", []manifest.BaseManifest{db}, fluxopv1.InputProviderReference{Selector: invalid}, db.Named(), []string{"db"}, false},
		{"unnamed nil selector", []manifest.BaseManifest{db}, fluxopv1.InputProviderReference{}, scope, nil, false},
		{"omitted group and kind", []manifest.BaseManifest{db, &web}, fluxopv1.InputProviderReference{Selector: selected}, manifest.NamedResource{Namespace: scope.Namespace}, []string{"db"}, false},
		{"omitted group", []manifest.BaseManifest{db}, fluxopv1.InputProviderReference{Selector: invalid}, manifest.NamedResource{Kind: scope.Kind, Namespace: scope.Namespace}, nil, true},
		{"omitted kind", []manifest.BaseManifest{db}, fluxopv1.InputProviderReference{Selector: invalid}, manifest.NamedResource{Group: scope.Group, Namespace: scope.Namespace}, nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testStoreResolverSelectorInputsCase(t, &tc)
		},
		)
	}
	for _, tc := range []struct {
		name  string
		prior *metav1.LabelSelector
	}{
		{"first cached error in empty scope", nil},
		{"additional cached error in empty scope", selected},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testStoreResolverSelectorInputsCase2(
				t,
				&testStoreResolverSelectorInputsCase2Inputs{db: &db, invalid: &invalid, scope: &scope, tc: &tc},
			)
		},
		)
	}
	t.Run("distinct selectors and live providers", func(t *testing.T) {
		t.Parallel()
		testStoreResolverSelectorInputsDistinctSelectorsAndLiveProviders(
			t,
			&testStoreResolverSelectorInputsDistinctSelectorsAndLiveProvidersInputs{
				db:       &db,
				web:      &web,
				selected: &selected,
				scope:    &scope,
			},
		)
	},
	)
}

func TestRender_InputsFrom_DynamicProviderEmptySkip(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{fluxopv1.InputProviderACRArtifactTag, fluxopv1.InputProviderECRArtifactTag, fluxopv1.InputProviderGARArtifactTag, fluxopv1.InputProviderGitHubBranch} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			provider := &manifest.ResourceSetInputProvider{Name: "remote", Namespace: "apps", Type: kind}
			rs := &manifest.ResourceSet{
				Name: "matrix", Namespace: "apps",
				Inputs:            []fluxopv1.ResourceSetInput{{"tenant": jsonTmpl(t, `"inline-only"`)}},
				InputsFrom:        []fluxopv1.InputProviderReference{{Name: provider.Name}},
				ResourcesTemplate: "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: '<< inputs.tenant >>'}\n",
			}
			docs, warnings, err := renderProviders(
				rs,
				func(fluxopv1.InputProviderReference, manifest.NamedResource) ([]*manifest.ResourceSetInputProvider, error) {
					return []*manifest.ResourceSetInputProvider{provider}, nil
				},
				nil,
			)
			assert.NoError(t, err)
			assert.Equal(t, len(docs), 1)
			assert.Equal(t, docs[0].Document["metadata"].(map[string]any)["name"], any("inline-only"))
			{
				assert.FatalIf(t, len(warnings) != 1 || warnings[0].Category != manifest.WarnOfflineInputProvider ||
					warnings[0].Resource != rs.Named(), "unsupported provider warning = %+v", warnings)
			}
			assert.Diff(t, warnings[0].Detail, []string{provider.Named().String(), kind})
		})
	}
}

func TestRender_ResourcesTemplate(t *testing.T) {
	t.Parallel()
	tmpl := `---
apiVersion: v1
kind: ConfigMap
metadata:
  name: << inputs.name >>
  namespace: flux-system
---
apiVersion: v1
kind: Namespace
metadata:
  name: << inputs.name >>
`
	rs := &manifest.ResourceSet{
		Name: "test", Namespace: "flux-system",
		Inputs: []fluxopv1.ResourceSetInput{
			{"name": jsonTmpl(t, `"a"`)},
			{"name": jsonTmpl(t, `"b"`)},
		},
		ResourcesTemplate: tmpl,
	}
	docs, _, err := renderProviders(rs, nil, nil)
	{
		assert.FatalIf(t, err != nil, "Render: %v", err)
	}
	{
		assert.ErrorIf(t, len(docs) != 4, "expected 4 docs (2 inputs × 2 docs each), got %d", len(docs))
	}
	t.Run("invalid stream separator", func(t *testing.T) {
		t.Parallel()
		candidate := &manifest.ResourceSet{
			Name:              "stream",
			ResourcesTemplate: "\n---oops: kept\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: rejected}",
		}
		_, _, err := renderProviders(candidate, nil, nil)
		{
			assert.FatalIf(
				t,
				!errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux),
				"error = %v, want classified invalid stream separator",
				err,
			)
		}
	})
	t.Run("duplicate mapping keys", func(t *testing.T) {
		t.Parallel()
		candidate := &manifest.ResourceSet{Name: "duplicate-keys", ResourcesTemplate: `--- # comment
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata: {name: db}
spec:
  postgresql: {value: first}
  postgresql: {value: last}
`}
		docs, _, err := renderProviders(candidate, nil, nil)
		{
			assert.NoError(t, err)
		}
		{
			assert.FatalIf(t, len(docs) != 1, "documents = %d, want 1", len(docs))
		}
		assert.Diff(t, docs[0].Document["spec"], map[string]any{"postgresql": map[string]any{"value": "last"}})
	})
}

func TestRender_NullInput(t *testing.T) {
	t.Parallel()
	rs := &manifest.ResourceSet{
		Name: "null", Namespace: "apps",
		Inputs: []fluxopv1.ResourceSetInput{{"value": nil, "malformed": {Raw: []byte("invalid")}}},
		ResourcesTemplate: `apiVersion: v1
kind: ConfigMap
metadata: {name: null-input}
data:
  value: << if eq inputs.value nil >>"null"<< end >>
  malformed: << if hasKey inputs "malformed" >>present<< else >>omitted<< end >>`,
	}
	docs, _, err := renderProviders(rs, nil, nil)
	{
		assert.NoError(t, err)
	}
	{
		assert.FatalIf(t, len(docs) != 1, "documents = %d, want 1", len(docs))
	}
	assert.Diff(t, docs[0].Document["data"], map[string]any{"value": "null", "malformed": "omitted"})
}

func TestRender_SlugifyLength(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, input, want string }{
		{"truncate", strings.Repeat("a", 64), strings.Repeat("a", 63)},
		{"smart truncate", strings.Repeat("a", 32) + " " + strings.Repeat("b", 32), strings.Repeat("a", 32)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rs := &manifest.ResourceSet{
				Name: "slug", Namespace: "apps",
				Inputs: []fluxopv1.ResourceSetInput{{"name": jsonTmpl(t, `"`+tc.input+`"`)}},
				ResourcesTemplate: `apiVersion: v1
kind: ConfigMap
metadata: {name: << inputs.name | slugify >>}`,
			}
			docs, _, err := renderProviders(rs, nil, nil)
			{
				assert.NoError(t, err)
			}
			{
				assert.FatalIf(t, len(docs) != 1, "documents = %d, want 1", len(docs))
			}
			assert.Equal(t, docs[0].Document["metadata"].(map[string]any)["name"].(string), tc.want)
		})
	}
}

func TestRender_Steps(t *testing.T) {
	t.Parallel()
	resource := jsonTmpl(t, `{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"first"}}`)
	template := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: second}"
	for _, tt := range []struct {
		name    string
		spec    fluxopv1.ResourceSetSpec
		names   []string
		wantErr string
	}{
		{name: "ordered steps", spec: fluxopv1.ResourceSetSpec{Steps: []fluxopv1.ResourceSetStep{
			{Name: "deploy", Resources: []*apix.JSON{resource}},
			{Name: "config", ResourcesTemplate: template},
		}}, names: []string{"first", "second"}},
		{name: "dedup within step", spec: fluxopv1.ResourceSetSpec{Inputs: []fluxopv1.ResourceSetInput{{}}, Steps: []fluxopv1.ResourceSetStep{
			{Name: "deploy", Resources: []*apix.JSON{resource, resource}},
		}}, names: []string{"first"}},
		{name: "no-input duplicate inside a step keeps both", spec: fluxopv1.ResourceSetSpec{Steps: []fluxopv1.ResourceSetStep{
			{Name: "deploy", Resources: []*apix.JSON{resource, resource}},
		}}, names: []string{"first", "first"}},
		{name: "duplicate across steps", spec: fluxopv1.ResourceSetSpec{Steps: []fluxopv1.ResourceSetStep{
			{Name: "first", Resources: []*apix.JSON{resource}},
			{Name: "second", Resources: []*apix.JSON{jsonTmpl(t, `{"apiVersion":"apps/v1beta2","kind":"Deployment","metadata":{"name":"first"}}`)}},
		}}, wantErr: "duplicate resource"},
		{name: "empty identity across steps", spec: fluxopv1.ResourceSetSpec{Steps: []fluxopv1.ResourceSetStep{
			{Name: "first", Resources: []*apix.JSON{jsonTmpl(t, `{}`)}},
			{Name: "second", Resources: []*apix.JSON{jsonTmpl(t, `{}`)}},
		}}, wantErr: "duplicate resource"},
		{name: "steps and resources", spec: fluxopv1.ResourceSetSpec{Resources: []*apix.JSON{resource}, Steps: []fluxopv1.ResourceSetStep{{Name: "deploy", Resources: []*apix.JSON{resource}}}}, wantErr: "mutually exclusive"},
		{name: "steps and template", spec: fluxopv1.ResourceSetSpec{ResourcesTemplate: template, Steps: []fluxopv1.ResourceSetStep{{Name: "deploy", Resources: []*apix.JSON{resource}}}}, wantErr: "mutually exclusive"},
		{name: "empty step with empty providers", spec: fluxopv1.ResourceSetSpec{InputsFrom: []fluxopv1.InputProviderReference{{Name: "empty"}}, Steps: []fluxopv1.ResourceSetStep{{Name: "empty"}}}, wantErr: "at least one"},
		{name: "step template error", spec: fluxopv1.ResourceSetSpec{Steps: []fluxopv1.ResourceSetStep{{Name: "bad", ResourcesTemplate: "<< inputs.missing >>"}}}, wantErr: `step "bad"`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rs := &manifest.ResourceSet{Name: "steps", Namespace: "default", ResourceSetSpec: tt.spec}
			docs, _, err := renderProviders(rs, nil, nil)
			if tt.wantErr != "" {
				{
					assert.FatalIf(
						t,
						err == nil || !strings.Contains(err.Error(), tt.wantErr),
						"error = %v, want %q",
						err,
						tt.wantErr,
					)
				}
				{
					assert.FatalIf(t, strings.Contains(err.Error(), "ResourceSet default/steps") ||
						strings.Contains(err.Error(), "flux error") ||
						strings.Contains(
							err.Error(),
							"input error",
						), "step diagnostic repeated owner or classification: %v", err)
				}
				return
			}
			{
				assert.NoError(t, err)
			}
			names := make([]string, 0, len(docs))
			rs.EmissionPolicy().Stamp(docs)
			for _, doc := range docs {
				name, ns := manifest.DocMetadata(doc.Document)
				{
					assert.ErrorIf(t, ns != "", "namespace = %q", ns)
				}
				labels := doc.Document["metadata"].(map[string]any)["labels"].(map[string]any)
				{
					assert.ErrorIf(t, labels[fluxopv1.OwnerLabelResourceSetName] != "steps", "labels = %v", labels)
				}
				names = append(names, name)
			}
			assert.Diff(t, names, tt.names)
		})
	}
}

func TestRender_EmptyInputRule(t *testing.T) {
	t.Parallel()
	for _, strategy := range []string{fluxopv1.InputStrategyFlatten, fluxopv1.InputStrategyPermute} {
		for _, referenced := range []bool{false, true} {
			name := strategy + "/inline-only"
			if referenced {
				name = strategy + "/empty-provider"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				rs := &manifest.ResourceSet{
					Name:              "empty",
					Namespace:         "default",
					InputStrategy:     &fluxopv1.InputStrategySpec{Name: strategy},
					ResourcesTemplate: "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: static}",
				}
				want := 1
				if referenced {
					rs.InputsFrom = []fluxopv1.InputProviderReference{{Name: "empty"}}
					want = 0
				}
				resolve := func(fluxopv1.InputProviderReference, manifest.NamedResource) ([]*manifest.ResourceSetInputProvider, error) {
					return []*manifest.ResourceSetInputProvider{
						{Name: "empty", Type: fluxopv1.InputProviderGitHubBranch},
					}, nil
				}
				docs, _, err := renderProviders(rs, resolve, nil)
				{
					assert.NoError(t, err)
				}
				{
					assert.FatalIf(t, len(docs) != want, "docs = %d, want %d", len(docs), want)
				}
			})
		}
	}
}

func TestRender_GroupDedup(t *testing.T) {
	t.Parallel()
	rs := &manifest.ResourceSet{
		Name:      "group",
		Namespace: "default",
		Inputs:    []fluxopv1.ResourceSetInput{{}},
		Resources: []*apix.JSON{
			jsonTmpl(t, `{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"shared"}}`),
			jsonTmpl(t, `{"apiVersion":"apps/v1","kind":"StatefulSet","metadata":{"name":"shared"}}`),
			jsonTmpl(t, `{"apiVersion":"example.test/v1","kind":"Widget"}`),
			jsonTmpl(t, `{"apiVersion":"other.test/v1","kind":"Widget"}`),
			jsonTmpl(t, `{"apiVersion":"v1","metadata":{"name":"kindless"}}`),
			jsonTmpl(t, `{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"shared","namespace":"a"}}`),
			jsonTmpl(t, `{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"shared","namespace":"b"}}`),
			jsonTmpl(t, `{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"other"}}`),
			jsonTmpl(t, `{"data":{"value":"zero"}}`),
			jsonTmpl(t, `{}`),
		},
		ResourcesTemplate: `apiVersion: apps/v1beta2
kind: Deployment
metadata: {name: shared}
---
apiVersion: other.example/v1
kind: Deployment
metadata: {name: shared}
---
apiVersion: example.test/v1
kind: Deployment
metadata: {}
---
apiVersion: example.test/v1
metadata: {name: incomplete}`,
	}
	docs, _, err := renderProviders(rs, nil, nil)
	{
		assert.NoError(t, err)
	}
	{
		assert.FatalIf(t, len(docs) != 10, "docs = %d, want 10", len(docs))
	}
	assert.Diff(
		t,
		[]string{manifest.DocAPIVersion(docs[0].Document), manifest.DocAPIVersion(docs[len(docs)-1].Document)},
		[]string{"apps/v1", "other.example/v1"},
	)
	malformed := map[string]any{
		"apiVersion": "example.io/v1/extra",
		"kind":       "Deployment",
		"metadata":   map[string]any{"name": "shared", "namespace": "default"},
	}
	assert.Equal(t, manifest.DocIdentity(malformed).Group, "example.io")
	assert.Equal(
		t,
		resourceset.DedupKey(malformed),
		manifest.NamedResource{Kind: "Deployment", Namespace: "default", Name: "shared"},
	)
}

func TestRender_NoInputJSONBypassesExclusionAndDedup(t *testing.T) {
	t.Parallel()
	raw := jsonTmpl(
		t,
		`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"static","annotations":{"fluxcd.controlplane.io/reconcile":"disabled"}}}`,
	)
	rs := &manifest.ResourceSet{
		Name:      "static",
		Namespace: "default",
		Resources: []*apix.JSON{raw, raw},
		ResourcesTemplate: `apiVersion: v1
kind: ConfigMap
metadata: {name: static}
---
apiVersion: v1
kind: ConfigMap
metadata:
 name: excluded
 annotations: {fluxcd.controlplane.io/reconcile: disabled}`,
	}
	docs, _, err := renderProviders(rs, nil, nil)
	{
		assert.NoError(t, err)
	}
	{
		assert.FatalIf(t, len(docs) != 2, "docs = %d, want 2 JSON duplicates", len(docs))
	}
	for _, doc := range docs {
		{
			assert.FatalIf(
				t,
				doc.Document["metadata"].(map[string]any)["annotations"].(map[string]any)[fluxopv1.ReconcileAnnotation] != fluxopv1.DisabledValue,
				"wrong resource: %v",
				doc,
			)
		}
	}
}

type testRenderInputsFromStaticProviderCaseInputs struct {
	rsip     **manifest.ResourceSetInputProvider
	rs       **manifest.ResourceSet
	st       **store.Store
	selector *bool
	tc       *struct {
		name       string
		apiVersion string
		kind       string
		want       int
	}
}

func testRenderInputsFromStaticProviderCase(t *testing.T, testCaseInput *testRenderInputsFromStaticProviderCaseInputs) {
	t.Helper()

	candidate := *(*testCaseInput.rs)
	ref := fluxopv1.InputProviderReference{
		APIVersion: testCaseInput.tc.apiVersion,
		Kind:       testCaseInput.tc.kind,
		Name:       (*testCaseInput.rsip).Name,
	}
	if *testCaseInput.selector {
		ref.Name = ""
		ref.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{"set": "apps"}}
	}
	candidate.InputsFrom = []fluxopv1.InputProviderReference{ref}
	renderProvidersDocs, _, err := renderProviders(&candidate, resourceset.StoreResolver((*testCaseInput.st)), nil)
	if err != nil || len(renderProvidersDocs) != testCaseInput.tc.want {
		t.Fatalf("docs=%d, error=%v, want %d", len(renderProvidersDocs), err, testCaseInput.tc.want)
	}
}

type testRenderInputsFromStaticProviderCase2Inputs struct {
	rs   **manifest.ResourceSet
	st   **store.Store
	docs *[]manifest.DocOccurrence
	tc   *struct {
		name string
		refs []fluxopv1.InputProviderReference
		want int
	}
}

func testRenderInputsFromStaticProviderCase2(
	t *testing.T,
	testCaseInput *testRenderInputsFromStaticProviderCase2Inputs,
) {
	t.Helper()

	candidate := *(*testCaseInput.rs)
	candidate.InputsFrom = testCaseInput.tc.refs
	got, _, err := renderProviders(&candidate, resourceset.StoreResolver((*testCaseInput.st)), nil)
	assert.NoError(t, err)
	if len(got) != testCaseInput.tc.want {
		t.Fatalf("docs=%d, want %d", len(got), testCaseInput.tc.want)
	}
	if testCaseInput.tc.want != 0 {
		assert.Diff(t, got, (*testCaseInput.docs))
	}
}

func testStoreResolverSelectorInputsCase(t *testing.T, testCaseInput *struct {
	name      string
	providers []manifest.BaseManifest
	ref       fluxopv1.InputProviderReference
	id        manifest.NamedResource
	want      []string
	wantError bool
},
) {
	t.Helper()

	st := store.New()
	for _, p := range testCaseInput.providers {
		st.AddObject(p)
	}
	resolve := resourceset.StoreResolver(st)
	for range 2 {
		got, err := resolve(testCaseInput.ref, testCaseInput.id)
		if (err != nil) != testCaseInput.wantError {
			t.Fatalf("resolve error=%v, want error=%t", err, testCaseInput.wantError)
		}
		names := make([]string, 0, len(got))
		if len(got) == 0 {
			names = nil
		}
		for _, p := range got {
			names = append(names, p.Name)
		}
		assert.Diff(t, names, testCaseInput.want)
	}
}
