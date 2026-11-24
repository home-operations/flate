package manifest

import (
	"encoding/json"
	"slices"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/testutil/assert"
)

func TestReferences_ArtifactCopiedInputs(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, from, pattern, alias, inputs string
		want                               []string
	}{
		{name: "literal file", from: "@repo/payload/one/cm.yaml", want: []string{"payload/one/cm.yaml"}},
		{name: "literal directory", from: "@repo/payload/one", want: []string{"payload/one"}},
		{name: "recursive glob", from: "@repo/payload/one/**", want: []string{"payload/one"}},
		{name: "capture", from: "@repo/payload/{app}/**", want: []string{"payload"}},
		{name: "component glob", from: "@repo/payload/cm-?.yaml", want: []string{"payload"}},
		{name: "character class", from: "@repo/payload/[ab]/**", want: []string{"payload"}},
		{name: "root glob", from: "@repo/**", want: []string{"."}},
		{name: "root copy", from: "@repo/", want: []string{"."}},
		{name: "explicit root", from: "@repo/.", want: []string{"."}},
		{name: "root pattern", pattern: "@repo/", want: []string{"."}},
		{name: "root duplicate", from: "@repo/", pattern: "@repo/", want: []string{"."}},
		{name: "path pattern", pattern: "@repo/apps/{app}", want: []string{"apps"}},
		{name: "combined", from: "@repo/payload/one/**", pattern: "@repo/apps/{app}", want: []string{"apps", "payload/one"}},
		{name: "foreign alias", from: "@other/payload/**"},
		{name: "alias prefix", from: "@repository/payload/**"},
		{name: "destination alias", from: "@artifact/payload/**"},
		{name: "missing alias", from: "payload/**"},
		{name: "bare alias", from: "@repo"},
		{name: "empty file path", inputs: "@repo/", want: []string{"."}},
		{
			name: "declared artifact alias", alias: "artifact", from: "@artifact/payload/**",
			pattern: "@artifact/apps/{app}", inputs: "@artifact/staged.yaml", want: []string{"apps", "payload"},
		},
		{name: "revision only"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			alias := tc.alias
			if alias == "" {
				alias = "repo"
			}
			raw := &RawObject{
				APIVersion: "source.extensions.fluxcd.io/v1beta1", Kind: "ArtifactGenerator", Namespace: "apps",
				Spec: map[string]any{
					"sources":     []any{map[string]any{"alias": alias, "kind": KindGitRepository, "name": "input"}},
					"pathPattern": tc.pattern,
					"artifacts": []any{map[string]any{
						"name": "bundle", "revision": "@" + alias, "originRevision": "@" + alias,
						"inputsFrom": tc.inputs,
						"copy": []any{
							map[string]any{
								"from":    tc.from,
								"to":      "@artifact/different",
								"exclude": []any{"**/ignored"},
							},
							map[string]any{"from": tc.from, "to": "@artifact/another"},
						},
					}},
				},
			}
			before := Fingerprint(raw)
			refs := slices.Collect(References(raw, ReferenceAll))
			want := []Reference{
				{
					Group:            SourceDomain,
					Kind:             KindGitRepository,
					Namespace:        "apps",
					Name:             "input",
					Use:              "artifact",
					Content:          true,
					Gate:             GateArtifacts,
					Artifact:         true,
					ArtifactOnly:     true,
					ArtifactStages:   GateArtifacts,
					NamespaceDefault: true,
					Files:            tc.want,
					Subtree:          len(tc.want) != 0,
				},
			}
			assert.Equal(t, cmp.Diff(want, refs, cmp.AllowUnexported(Reference{})), "")
			data, err := json.Marshal(refs) //nolint:musttag // Cache references use the exported Go field names.
			assert.NoError(t, err)
			var cached []Reference
			//nolint:musttag // Cache references use the exported Go field names.
			assert.NoError(t, json.Unmarshal(data, &cached))
			assert.Equal(t, len(cached), len(want))
			assert.Equal(t, cached[0].ReadsSourceTree(), true)
			assert.Equal(t, cached[0].BindsSource(), true)
			assert.Equal(t, Fingerprint(raw), before)
			calls := 0
			References(raw, ReferenceAll)(func(Reference) bool { calls++; return false })
			assert.Equal(t, calls, 1)
		})
	}
}
