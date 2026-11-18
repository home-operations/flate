package values

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

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
			err := ExpandValueReferences(
				hr,
				resolveInputsForTest(hr, &sliceProvider{ConfigMaps: []*manifest.ConfigMap{cm}}, nil),
				nil,
			)
			assert.FatalIf(t, !errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) ||
				!errors.Is(err, strconv.ErrSyntax), "target parser identities lost: %v", err)
			cause, ok := errors.AsType[*strconv.NumError](err)
			assert.FatalIf(t, !ok || cause.Num != "bad", "original index parser cause lost: %v", err)
			for _, detail := range []string{`targetPath "` + path + `"`} {
				assert.ErrorIf(t, !strings.Contains(err.Error(), detail), "error lacks %q: %v", detail, err)
			}
			assert.Diff(t, hr.Values, map[string]any{"retained": "yes"})
		})
	}
}

func TestExpandValueReferences_TargetPathOrdering(t *testing.T) {
	t.Parallel()

	type reference struct {
		path, body string
		literal    bool
	}
	for _, tc := range []struct {
		name    string
		inline  map[string]any
		refs    []reference
		want    map[string]any
		wantErr bool
	}{
		{
			name: "with target path",
			refs: []reference{{path: "merge.at.specific.path", body: "value"}},
			want: map[string]any{"merge": map[string]any{"at": map[string]any{"specific": map[string]any{"path": "value"}}}},
		},
		{
			name:   "root merges remain ordered",
			inline: map[string]any{"other": "values"},
			refs:   []reference{{body: "flat: value\nnested:\n  configuration: value\n"}, {body: "flat:\n  nested: value\nnested: value\n"}},
			want:   map[string]any{"flat": map[string]any{"nested": "value"}, "nested": "value", "other": "values"},
		},
		{
			name:   "target path precedence over all",
			inline: map[string]any{"nested": map[string]any{"configuration": []any{"list", "item", "option"}}},
			refs:   []reference{{path: "nested.configuration[0]", body: "value"}, {body: "flat: value\nnested:\n  configuration: [one, two, three]\n"}},
			want:   map[string]any{"flat": "value", "nested": map[string]any{"configuration": []any{"value", "item", "option"}}},
		},
		{
			name:   "target path for string type array item",
			inline: map[string]any{"other": "values"},
			refs:   []reference{{body: "flat: value\nnested:\n  configuration: [list, item, option]\n"}, {path: "nested.configuration[1]", body: "foo"}},
			want:   map[string]any{"flat": "value", "nested": map[string]any{"configuration": []any{"list", "foo", "option"}}, "other": "values"},
		},
		{
			name: "reported bucket",
			inline: map[string]any{"configuration": map[string]any{"backupStorageLocation": []any{
				map[string]any{"name": "primary", "bucket": "primary-bucket"},
				map[string]any{"name": "secondary", "provider": "aws"},
			}}},
			refs: []reference{{path: "configuration.backupStorageLocation[1].bucket", body: "archive"}},
			want: map[string]any{"configuration": map[string]any{"backupStorageLocation": []any{
				map[string]any{"name": "primary", "bucket": "primary-bucket"},
				map[string]any{"name": "secondary", "provider": "aws", "bucket": "archive"},
			}}},
		},
		{
			name:   "reported scalar",
			inline: map[string]any{"image": map[string]any{"tag": "inline"}},
			refs:   []reference{{path: "image.tag", body: "referenced"}},
			want:   map[string]any{"image": map[string]any{"tag": "referenced"}},
		},
		{
			name:   "scalar with preceding root map",
			inline: map[string]any{"image": map[string]any{"tag": "inline"}},
			refs:   []reference{{body: "image:\n  repository: nginx\n  tag: root\n"}, {path: "image.tag", body: "referenced"}},
			want:   map[string]any{"image": map[string]any{"repository": "nginx", "tag": "inline"}},
		},
		{
			name:   "inline top-level replacement",
			inline: map[string]any{"image": "inline", "keep": true},
			refs:   []reference{{path: "image", body: "referenced"}},
			want:   map[string]any{"image": "inline", "keep": true},
		},
		{
			name:   "inline list growth",
			inline: map[string]any{"items": []any{"inline"}},
			refs:   []reference{{path: "items[1]", body: "grown"}},
			want:   map[string]any{"items": []any{"inline"}},
		},
		{
			name:   "inline map root target interleaving",
			inline: map[string]any{"image": map[string]any{"tag": "inline"}},
			refs:   []reference{{path: "image.tag", body: "first"}, {body: "image:\n  repository: nginx\n  tag: root\n"}, {path: "image.tag", body: "second"}},
			want:   map[string]any{"image": map[string]any{"repository": "nginx", "tag": "first"}},
		},
		{
			name:    "non-literal comma input",
			refs:    []reference{{path: `externalConfig.application\.yml.content`, body: "endpoints: [a,b,c]"}},
			wantErr: true,
		},
		{
			name: "literal metacharacters",
			refs: []reference{{path: `externalConfig.application\.yml.content`, body: "server:\n  port: 8080\nmanagement:\n  endpoints:\n    web:\n      exposure:\n        include: [ \"prometheus\", \"health\" ]\n", literal: true}},
			want: map[string]any{"externalConfig": map[string]any{"application.yml": map[string]any{"content": "server:\n  port: 8080\nmanagement:\n  endpoints:\n    web:\n      exposure:\n        include: [ \"prometheus\", \"health\" ]\n"}}},
		},
		{
			name: "literal multiline HOCON",
			refs: []reference{{path: `externalConfig.application\.conf.content`, body: "kafka {\n  bootstrap = \"b-1:9098,b-2:9098\"\n  group = \"my.service\"\n}\n", literal: true}},
			want: map[string]any{"externalConfig": map[string]any{"application.conf": map[string]any{"content": "kafka {\n  bootstrap = \"b-1:9098,b-2:9098\"\n  group = \"my.service\"\n}\n"}}},
		},
		{
			name: "literal without target",
			refs: []reference{{body: "flat: value\n", literal: true}},
			want: map[string]any{"flat": "value"},
		},
		{
			name:    "ordinary brokers fail",
			refs:    []reference{{path: "kafka.brokers", body: "kafka01.net:9092,kafka02.net:9092,kafka03.net:9092"}},
			wantErr: true,
		},
		{
			name: "literal brokers",
			refs: []reference{{path: "kafka.brokers", body: "kafka01.net:9092,kafka02.net:9092,kafka03.net:9092", literal: true}},
			want: map[string]any{"kafka": map[string]any{"brokers": "kafka01.net:9092,kafka02.net:9092,kafka03.net:9092"}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			hr := &manifest.HelmRelease{Name: "consumer", Namespace: "default", Values: tc.inline}
			provider := &sliceProvider{}
			for i, ref := range tc.refs {
				name := fmt.Sprintf("reference-%d", i)
				provider.ConfigMaps = append(provider.ConfigMaps, &manifest.ConfigMap{
					Name: name, Namespace: "default", Data: map[string]any{"v": ref.body},
				})
				hr.ValuesFrom = append(hr.ValuesFrom, manifest.ValuesReference{
					Kind:       manifest.KindConfigMap,
					Name:       name,
					ValuesKey:  "v",
					TargetPath: ref.path,
					Literal:    ref.literal,
				})
			}
			before := hr.Clone()
			inlineBefore := manifest.DeepCopyMap(tc.inline)
			refsBefore := slices.Clone(hr.ValuesFrom)
			err := ExpandValueReferences(hr, resolveInputsForTest(hr, provider, nil), nil)
			assert.Diff(t, tc.inline, inlineBefore)
			assert.Diff(t, hr.ValuesFrom, refsBefore)
			if tc.wantErr {
				assert.Equal(t, errors.Is(err, manifest.ErrInput), true)
				assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
				assert.Equal(t, manifest.Equal(hr, before), true)
				return
			}
			assert.NoError(t, err)
			assert.Diff(t, hr.Values, tc.want)
		})
	}
}
