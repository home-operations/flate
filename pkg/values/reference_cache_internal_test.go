package values

import (
	"fmt"
	"sync"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

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
	assert.FatalIf(t, !ok, "%s", "missing cached document")
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
			assert.FatalIf(t, err == nil, "%s", "malformed target path succeeded")
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
				assert.FatalIf(t, !ok, "%s", "missing cached document")
				assert.Diff(t, canonical, document.want)
			}
		})
	}
}
