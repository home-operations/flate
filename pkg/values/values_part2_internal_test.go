package values

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strconv"
	"strings"
	"sync"
	"testing"

	"helm.sh/helm/v4/pkg/chart/v2/loader"
	"sigs.k8s.io/yaml"

	"github.com/home-operations/flate/internal/assert"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestReplaceValueAtPath_TypeCoercion(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		path string
		val  string
		want any
	}{
		{"int", "replicaCount", "3", float64(3)},
		{"bool", "enabled", "true", true},
		{"null", "extra", "null", nil},
		{"nested map", "image.repository", "nginx", "nginx"},
		{"quoted string forces string", "tag", `"123"`, "123"},
		{"list index", "ports[0]", "8080", float64(8080)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := map[string]any{}
			err := replaceValueAtPath(got, tc.path, tc.val)
			{
				assert.FatalIf(t, err != nil, "replaceValueAtPath: %v", err)
			}
			leaf := walkPath(t, got, tc.path)
			{
				assert.ErrorIf(
					t,
					!equalish(leaf, tc.want),
					"path %q: got %v (%T), want %v (%T)",
					tc.path,
					leaf,
					leaf,
					tc.want,
					tc.want,
				)
			}
		})
	}
}

func walkPath(t *testing.T, m map[string]any, path string) any {
	t.Helper()
	cur := any(m)
	parts := strings.SplitSeq(path, ".")
	for p := range parts {
		if key, _, ok := strings.Cut(p, "["); ok {
			if cm, ok := cur.(map[string]any); ok {
				cur = cm[key]
			}
			// Strip "[0]" → 0 and index.
			cur = cur.([]any)[0]
			continue
		}
		if cm, ok := cur.(map[string]any); ok {
			cur = cm[p]
		}
	}
	return cur
}

func equalish(a, b any) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	// strvals.ParseInto returns int64 for integers; allow comparison
	// against float64 literals in test cases.
	switch av := a.(type) {
	case int64:
		if bv, ok := b.(float64); ok {
			return float64(av) == bv
		}
	case int:
		if bv, ok := b.(float64); ok {
			return float64(av) == bv
		}
	}
	return a == b
}

func TestDeepMergeInto_MatchesHelmMerge(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name           string
		base, override map[string]any
	}{
		{"nil-inputs", nil, nil},
		{"nil-base", nil, map[string]any{"m": map[string]any{"x": 1.0}}},
		{"empty-override", map[string]any{"m": map[string]any{"x": 1.0}}, map[string]any{}},
		{"disjoint", map[string]any{"a": 1.0}, map[string]any{"b": 2.0}},
		{"scalar-over-scalar", map[string]any{"a": 1.0}, map[string]any{"a": 2.0}},
		{
			"nested-collision",
			map[string]any{"m": map[string]any{"x": 1.0, "y": 2.0}},
			map[string]any{"m": map[string]any{"y": 9.0, "z": 3.0}},
		},
		{
			"deep-collision",
			map[string]any{"m": map[string]any{"n": map[string]any{"x": 1.0}}},
			map[string]any{"m": map[string]any{"n": map[string]any{"y": 2.0}}},
		},
		{
			"list-map-replace",
			map[string]any{"l": []any{map[string]any{"x": 1.0}}},
			map[string]any{"l": []any{map[string]any{"y": 2.0}}},
		},
		{"list-replace", map[string]any{"l": []any{1.0, 2.0}}, map[string]any{"l": []any{3.0}}},
		{"scalar-over-map", map[string]any{"k": map[string]any{"x": 1.0}}, map[string]any{"k": "scalar"}},
		{"map-over-scalar", map[string]any{"k": "scalar"}, map[string]any{"k": map[string]any{"x": 1.0}}},
		{"nil-override", map[string]any{"k": map[string]any{"x": 1.0}}, map[string]any{"k": nil}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			baseBefore := manifest.DeepCopyMap(tc.base)
			overrideBefore := manifest.DeepCopyMap(tc.override)
			want := loader.MergeMaps(tc.base, tc.override)
			got := DeepMergeInto(maps.Clone(tc.base), tc.override)
			assert.Diff(t, got, want)
			assert.Diff(t, tc.base, baseBefore)
			assert.Diff(t, tc.override, overrideBefore)
		})
	}
}

// nestedValuesCM returns a ConfigMap whose values.yaml is a nested map +
// list document — the shape a platform-wide values CM takes.
func nestedValuesCM() *manifest.ConfigMap {
	return &manifest.ConfigMap{
		Name:      "platform",
		Namespace: "default",
		Data: map[string]any{
			"values.yaml": "image:\n  repository: nginx\n  tag: v1\nenv:\n  - name: A\n    value: \"1\"\n",
		},
	}
}

func wholeDocHR() *manifest.HelmRelease {
	return &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		ValuesFrom: []manifest.ValuesReference{{Kind: "ConfigMap", Name: "platform"}},
	}
}

func TestExpandValueReferences_ConcurrentSharedCache(t *testing.T) {
	t.Parallel()
	cm := nestedValuesCM()
	provider := &sliceProvider{ConfigMaps: []*manifest.ConfigMap{cm, {
		Name: "inject", Namespace: "default", Data: map[string]any{"v": "Always"},
	}}}
	cache := &Cache{}
	prior := wholeDocHR()
	expandValuesForTest(t, prior, provider, cache)
	before := manifest.DeepCopyMap(prior.Values)
	canonical, ok := cache.lookup(cm.Data["values.yaml"].(string))
	{
		assert.FatalIf(t, !ok, "%s", "missing cached document")
	}
	cases := []struct {
		name    string
		refs    []manifest.ValuesReference
		inline  map[string]any
		want    map[string]any
		wantErr bool
	}{
		{
			name:   "ordinary",
			inline: map[string]any{"image": map[string]any{"tag": "inline"}},
			want: map[string]any{
				"image": map[string]any{"repository": "nginx", "tag": "inline"},
				"env":   []any{map[string]any{"name": "A", "value": "1"}},
			},
		},
		{
			name: "target map",
			refs: []manifest.ValuesReference{
				{Kind: "ConfigMap", Name: "inject", ValuesKey: "v", TargetPath: "image.tag"},
			},
			want: map[string]any{
				"image": map[string]any{"repository": "nginx", "tag": "Always"},
				"env":   []any{map[string]any{"name": "A", "value": "1"}},
			},
		},
		{
			name: "target list map",
			refs: []manifest.ValuesReference{
				{Kind: "ConfigMap", Name: "inject", ValuesKey: "v", TargetPath: "env[0].value"},
			},
			want: map[string]any{
				"image": map[string]any{"repository": "nginx", "tag": "v1"},
				"env":   []any{map[string]any{"name": "A", "value": "Always"}},
			},
		},
		{
			name: "target list element",
			refs: []manifest.ValuesReference{{Kind: "ConfigMap", Name: "inject", ValuesKey: "v", TargetPath: "env[0]"}},
			want: map[string]any{"image": map[string]any{"repository": "nginx", "tag": "v1"}, "env": []any{"Always"}},
		},
		{name: "yaml between writes", refs: []manifest.ValuesReference{
			{Kind: "ConfigMap", Name: "inject", ValuesKey: "v", TargetPath: "env[0].name", Literal: true},
			{Kind: "ConfigMap", Name: "platform"},
			{Kind: "ConfigMap", Name: "inject", ValuesKey: "v", TargetPath: "env[0].value"},
		}, want: map[string]any{"image": map[string]any{"repository": "nginx", "tag": "v1"}, "env": []any{map[string]any{"name": "A", "value": "Always"}}}},
		{
			name: "partial ordinary failure",
			refs: []manifest.ValuesReference{
				{Kind: "ConfigMap", Name: "inject", ValuesKey: "v", TargetPath: "env[0].value=changed,broken[bad]"},
			},
			inline:  map[string]any{"untouched": true},
			want:    map[string]any{"untouched": true},
			wantErr: true,
		},
		{
			name: "partial literal failure",
			refs: []manifest.ValuesReference{
				{
					Kind:       "ConfigMap",
					Name:       "inject",
					ValuesKey:  "v",
					TargetPath: "env[0].value=changed,broken[bad]",
					Literal:    true,
				},
			},
			inline:  map[string]any{"untouched": true},
			want:    map[string]any{"untouched": true},
			wantErr: true,
		},
	}
	start := make(chan struct{})
	var ready, done sync.WaitGroup
	ready.Add(len(cases))
	for _, tc := range cases {
		done.Go(func() {
			hr := wholeDocHR()
			hr.ValuesFrom = append(hr.ValuesFrom, tc.refs...)
			hr.Values = tc.inline
			inlineBefore := manifest.DeepCopyMap(tc.inline)
			ready.Done()
			<-start
			_, err := ExpandValueReferences(hr, provider, cache)
			if (err != nil) != tc.wantErr {
				t.Errorf("%s: error = %v, want error %v", tc.name, err, tc.wantErr)
				return
			}
			assert.Diff(t, hr.Values, tc.want)
			assert.Diff(t, tc.inline, inlineBefore)
		})
	}
	ready.Wait()
	close(start)
	done.Wait()
	assert.Diff(t, canonical, before)
	assert.Diff(t, prior.Values, before)
	after := wholeDocHR()
	expandValuesForTest(t, after, provider, cache)
	assert.Diff(t, after.Values, before)
}

func TestExpandValueReferences_TargetPathDoesNotCorruptSharedCache(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		literal, cached bool
	}{{false, false}, {false, true}, {true, false}, {true, true}} {
		t.Run(fmt.Sprintf("literal=%t/cache=%t", tc.literal, tc.cached), func(t *testing.T) {
			t.Parallel()
			var cache *Cache
			if tc.cached {
				cache = &Cache{}
			}
			hr := wholeDocHR()
			cm := nestedValuesCM()
			provider := &sliceProvider{ConfigMaps: []*manifest.ConfigMap{cm, {
				Name: "inject", Namespace: "default", Data: map[string]any{"v": "Always"},
			}}}
			expandValuesForTest(t, hr, provider, cache)
			before := manifest.DeepCopyMap(hr.Values)
			hr.ValuesFrom = append(hr.ValuesFrom, manifest.ValuesReference{
				Kind: "ConfigMap", Name: "inject", ValuesKey: "v",
				TargetPath: "env[0].value=changed,broken[bad]", Literal: tc.literal,
			})
			_, err := ExpandValueReferences(hr, provider, cache)
			{
				assert.FatalIf(t, err == nil, "%s", "malformed target path succeeded")
			}
			assert.Diff(t, hr.Values, before)
			after := wholeDocHR()
			expandValuesForTest(t, after, provider, cache)
			assert.Diff(t, after.Values, before)
			if tc.cached {
				canonical, _ := cache.lookup(cm.Data["values.yaml"].(string))
				assert.Diff(t, canonical, before)
			}
		})
	}
}

func TestExpandValueReferences_InterleavedTargetPaths(t *testing.T) {
	t.Parallel()
	for _, literal := range []bool{false, true} {
		t.Run(fmt.Sprintf("literal=%t", literal), func(t *testing.T) {
			t.Parallel()
			first := nestedValuesCM()
			second := &manifest.ConfigMap{Name: "later", Namespace: "default", Data: map[string]any{
				"values.yaml": "later:\n  image:\n    tag: v2\n  env:\n    - name: B\n      value: two\n",
			}}
			provider := &sliceProvider{ConfigMaps: []*manifest.ConfigMap{first, second, {
				Name: "inject", Namespace: "default", Data: map[string]any{"v": "Always"},
			}}}
			cache := &Cache{}
			priorFirst := wholeDocHR()
			priorSecond := &manifest.HelmRelease{
				Name:       "prior",
				Namespace:  "default",
				ValuesFrom: []manifest.ValuesReference{{Kind: "ConfigMap", Name: second.Name}},
			}
			expandValuesForTest(t, priorFirst, provider, cache)
			expandValuesForTest(t, priorSecond, provider, cache)
			firstBefore := manifest.DeepCopyMap(priorFirst.Values)
			secondBefore := manifest.DeepCopyMap(priorSecond.Values)
			hr := wholeDocHR()
			hr.Values = map[string]any{"image": map[string]any{"tag": "inline"}}
			hr.ValuesFrom = append(
				hr.ValuesFrom,
				manifest.ValuesReference{
					Kind:       "ConfigMap",
					Name:       "inject",
					ValuesKey:  "v",
					TargetPath: "image.pullPolicy",
				},
				manifest.ValuesReference{Kind: "ConfigMap", Name: "inject", ValuesKey: "v", TargetPath: "env[0].value"},
				manifest.ValuesReference{Kind: "ConfigMap", Name: "missing", Optional: true},
				manifest.ValuesReference{Kind: "ConfigMap", Name: second.Name},
				manifest.ValuesReference{
					Kind:       "ConfigMap",
					Name:       "inject",
					ValuesKey:  "v",
					TargetPath: "later.image.tag",
					Literal:    literal,
				},
				manifest.ValuesReference{
					Kind:       "ConfigMap",
					Name:       "inject",
					ValuesKey:  "v",
					TargetPath: "later.env[0].value",
					Literal:    literal,
				},
			)
			expandValuesForTest(t, hr, provider, cache)
			assert.Diff(t, hr.Values, map[string]any{
				"image": map[string]any{"repository": "nginx", "tag": "inline", "pullPolicy": "Always"},
				"env":   []any{map[string]any{"name": "A", "value": "Always"}},
				"later": map[string]any{
					"image": map[string]any{"tag": "Always"},
					"env":   []any{map[string]any{"name": "B", "value": "Always"}},
				},
			})
			assert.Diff(t, priorFirst.Values, firstBefore)
			assert.Diff(t, priorSecond.Values, secondBefore)
			for _, document := range []struct {
				cm   *manifest.ConfigMap
				want map[string]any
			}{{first, firstBefore}, {second, secondBefore}} {
				canonical, ok := cache.lookup(document.cm.Data["values.yaml"].(string))
				{
					assert.FatalIf(t, !ok, "%s", "missing cached document")
				}
				assert.Diff(t, canonical, document.want)
			}
		})
	}
}

// TestExpandValueReferences_YAMLContainingPlaceholderIsMerged verifies that
// a ConfigMap whose values.yaml contains placeholder tokens inside valid YAML
// values (e.g. from postBuild substitution on a configMapGenerator) is correctly
// parsed and merged into hr.Values instead of being discarded.
func TestExpandValueReferences_YAMLContainingPlaceholderIsMerged(t *testing.T) {
	t.Parallel()
	cm := &manifest.ConfigMap{
		Name:      "generator-cm",
		Namespace: "default",
		Data: map[string]any{
			"values.yaml": "domain: ..PLACEHOLDER_DOMAIN..\nport: 8080\n",
		},
	}
	provider := &sliceProvider{ConfigMaps: []*manifest.ConfigMap{cm}}
	hr := &manifest.HelmRelease{
		Name:      "demo",
		Namespace: "default",
		ValuesFrom: []manifest.ValuesReference{
			{Kind: "ConfigMap", Name: "generator-cm"},
		},
	}
	{
		_, err := ExpandValueReferences(hr, provider, nil)
		assert.FatalIf(t, err != nil, "ExpandValueReferences failed: %v", err)
	}
	if hr.Values["domain"] != "..PLACEHOLDER_DOMAIN.." {
		t.Errorf("domain = %v, want ..PLACEHOLDER_DOMAIN..", hr.Values["domain"])
	}
	if fmt.Sprint(hr.Values["port"]) != "8080" {
		t.Errorf("port = %v, want 8080", hr.Values["port"])
	}
}

func TestExpandValueReferences_WipedScalarPlaceholderTolerated(t *testing.T) {
	t.Parallel()
	sec := &manifest.Secret{
		Name:      "sops-secret",
		Namespace: "default",
		StringData: map[string]any{
			"values.yaml": "..PLACEHOLDER_values.yaml..",
		},
	}
	provider := &sliceProvider{Secrets: []*manifest.Secret{sec}}
	hr := &manifest.HelmRelease{
		Name:      "demo",
		Namespace: "default",
		ValuesFrom: []manifest.ValuesReference{
			{Kind: "Secret", Name: "sops-secret"},
		},
	}
	omitted, err := ExpandValueReferences(hr, provider, nil)
	{
		assert.FatalIf(t, err != nil, "ExpandValueReferences failed for wiped scalar placeholder: %v", err)
	}
	assert.Diff(t, omitted, []manifest.NamedResource{sec.Named()})
	if len(hr.Values) != 0 {
		t.Errorf("expected empty values, got %v", hr.Values)
	}
}

func TestExpandValueReferences_MalformedYAMLClassified(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, document string
		typeError      bool
	}{
		{"syntax", "a: [", false},
		{"document type", "- name: demo", true},
	} {
		for _, optional := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/optional=%t", tc.name, optional), func(t *testing.T) {
				t.Parallel()
				st := store.New()
				st.AddObject(
					&manifest.ConfigMap{
						Name:      "extra",
						Namespace: "default",
						Data:      map[string]any{"requested.yaml": tc.document},
					},
				)
				hr := &manifest.HelmRelease{
					Name:      "demo",
					Namespace: "default",
					ValuesFrom: []manifest.ValuesReference{
						{Kind: manifest.KindConfigMap, Name: "extra", ValuesKey: "requested.yaml", Optional: optional},
					},
				}
				_, err := ExpandValueReferences(hr, NewStoreProvider(st, true), &Cache{})
				{
					assert.FatalIf(
						t,
						!errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux),
						"malformed values must retain input/domain identity: %v",
						err,
					)
				}
				for _, detail := range []string{"extra", "requested.yaml"} {
					{
						assert.ErrorIf(t, !strings.Contains(err.Error(), detail), "error lacks %q: %v", detail, err)
					}
				}
				wrapped, ok := errors.AsType[interface {
					error
					Unwrap() []error
				}](err)
				{
					assert.FatalIf(t, !ok, "parser cause is not wrapped beside ErrInput: %v", err)
				}
				causes := wrapped.Unwrap()
				var decoded map[string]any
				parseErr := yaml.Unmarshal([]byte(tc.document), &decoded)
				{
					assert.FatalIf(
						t,
						len(causes) != 2 || parseErr == nil || errors.Unwrap(causes[1]).Error() != parseErr.Error(),
						"parser cause changed: %v",
						err,
					)
				}
				if tc.typeError {
					{
						_, ok := errors.AsType[*json.UnmarshalTypeError](err)
						assert.FatalIf(t, !ok, "typed parser cause lost: %v", err)
					}
				}
			})
		}
	}
}

func TestExpandValueReferences_MalformedTargetPathClassified(t *testing.T) {
	t.Parallel()
	for _, literal := range []bool{false, true} {
		t.Run(fmt.Sprintf("literal=%t", literal), func(t *testing.T) {
			t.Parallel()
			const path = "env[bad].value"
			cm := &manifest.ConfigMap{Name: "extra", Namespace: "default", Data: map[string]any{"value": "replacement"}}
			hr := &manifest.HelmRelease{
				Name:      "demo",
				Namespace: "default",
				Values:    map[string]any{"retained": "yes"},
				ValuesFrom: []manifest.ValuesReference{
					{
						Kind:       manifest.KindConfigMap,
						Name:       cm.Name,
						ValuesKey:  "value",
						TargetPath: path,
						Literal:    literal,
						Optional:   true,
					},
				},
			}
			_, err := ExpandValueReferences(hr, &sliceProvider{ConfigMaps: []*manifest.ConfigMap{cm}}, nil)
			{
				assert.FatalIf(t, !errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) ||
					!errors.Is(err, strconv.ErrSyntax), "target parser identities lost: %v", err)
			}
			cause, ok := errors.AsType[*strconv.NumError](err)
			{
				assert.FatalIf(t, !ok || cause.Num != "bad", "original index parser cause lost: %v", err)
			}
			for _, detail := range []string{`targetPath "` + path + `"`} {
				{
					assert.ErrorIf(t, !strings.Contains(err.Error(), detail), "error lacks %q: %v", detail, err)
				}
			}
			assert.Diff(t, hr.Values, map[string]any{"retained": "yes"})
		})
	}
}

type countedDataReader struct {
	*store.ReadSet

	objects, productions int
}

func (r *countedDataReader) ReferenceFacts(
	id manifest.NamedResource,
	index *manifest.DiscoveryIndex,
) store.ReferenceFacts {
	r.objects++
	r.productions++
	return r.ReadSet.ReferenceFacts(id, index)
}

func TestStoreProvider_LookupRetainsOneSource(t *testing.T) {
	t.Parallel()
	s := store.New()
	supplied := &manifest.Secret{Name: "supplied", Namespace: "apps", StringData: map[string]any{"VALUE": "supplied"}}
	snapshot, err := store.NewSupplied([]store.SuppliedObject{{Object: supplied}})
	assert.NoError(t, err)
	s.SetSupplied(snapshot)
	first := &manifest.ConfigMap{Name: "settings", Namespace: "apps", Data: map[string]any{"VALUE": "first"}}
	s.AddObject(first)
	owner := manifest.NamedResource{
		Group:     manifest.FluxKustomizeDomain,
		Kind:      manifest.KindKustomization,
		Namespace: "apps",
		Name:      "producer",
	}
	generatedID := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "generated"}
	materializer := manifest.NamedResource{
		Group:     manifest.FluxKustomizeDomain,
		Kind:      manifest.KindKustomization,
		Namespace: "apps",
		Name:      "materializer",
	}
	declaration := manifest.NamedResource{
		Group:     "external-secrets.io",
		Kind:      "ExternalSecret",
		Namespace: "apps",
		Name:      "declared",
	}
	s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		first.Named(): {
			Productions: []manifest.Production{{Owner: owner, Path: "settings.yaml", Authored: first.Named()}},
		},
		generatedID: {Productions: []manifest.Production{{Owner: materializer, Declaration: declaration}}},
	}})
	s.AddObject(&manifest.Kustomization{Name: owner.Name, Namespace: owner.Namespace})
	s.RecordEmitted(owner, &store.Emission{Children: []store.Child{{Obj: first}}, Complete: true}, nil)
	var reads store.ReadSet
	reader := manifest.NamedResource{
		Group:     manifest.FluxKustomizeDomain,
		Kind:      manifest.KindKustomization,
		Namespace: "apps",
		Name:      "reader",
	}
	reads.Init(s, reader, nil)
	defer reads.Close()
	counted := &countedDataReader{ReadSet: &reads}
	provider := NewStoreProvider(counted, true)
	input := provider.Lookup(first.Named())
	{
		assert.FatalIf(
			t,
			input.Data != first || !input.Offline || len(input.Materializers) != 0,
			"captured input=%+v",
			input,
		)
	}
	absent := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: first.Namespace, Name: first.Name}
	other := &manifest.ConfigMap{Name: first.Name, Namespace: "other", Data: map[string]any{"VALUE": "other"}}
	s.AddObject(other)
	missing := provider.Lookup(absent)
	{
		assert.FatalIf(t, missing.Data != nil || missing.Generated || missing.Offline || missing.Private ||
			len(missing.Materializers) != 0, "missing input=%+v", missing)
	}
	generated := provider.Lookup(generatedID)
	{
		assert.FatalIf(t, generated.Data != nil || !generated.Generated || generated.Offline || generated.Private ||
			len(generated.Materializers) != 1 ||
			generated.Materializers[0] != materializer, "generated input=%+v", generated)
	}
	otherInput := provider.Lookup(other.Named())
	private := provider.Lookup(supplied.Named())
	{
		assert.FatalIf(
			t,
			otherInput.Data != other || private.Data == nil || private.Private,
			"distinct inputs=%+v %+v",
			otherInput,
			private,
		)
	}
	s.AddObject(
		&manifest.Secret{
			Name:       absent.Name,
			Namespace:  absent.Namespace,
			StringData: map[string]any{"VALUE": "inserted"},
		},
	)
	for _, captured := range []struct {
		id    manifest.NamedResource
		input Input
	}{{first.Named(), input}, {absent, missing}, {other.Named(), otherInput}, {supplied.Named(), private}, {generatedID, generated}} {
		assert.Diff(t, captured.input, provider.Lookup(captured.id))
		{
			assert.FatalIf(
				t,
				provider.Data(captured.id) != captured.input.Data,
				"%s",
				"cached data differs from the captured input",
			)
		}
	}
	next := *first
	next.Data = map[string]any{"VALUE": "next"}
	s.RecordEmitted(owner, &store.Emission{Children: []store.Child{{Obj: &next}}, Complete: true}, nil)
	if provider.Data(first.Named()) != first || provider.Lookup(first.Named()).Data != first || counted.objects != 5 ||
		counted.productions != 5 {
		t.Fatalf("expansion rescanned captured input: object=%d productions=%d", counted.objects, counted.productions)
	}
	{
		assert.FatalIf(t, reads.Valid(), "%s", "changed consumed data did not invalidate execution")
	}
	var fresh store.ReadSet
	fresh.Init(s, reader, nil)
	defer fresh.Close()
	{
		got := NewStoreProvider(&fresh, true).Data(first.Named())
		assert.FatalIf(t, got != &next, "next execution retained preceding value: %v", got)
	}
	{
		got := NewStoreProvider(&fresh, true).Data(absent)
		assert.FatalIf(t, got == nil, "%s", "fresh execution retained frozen absence")
	}
}
