package artifactgenerator

import (
	"errors"
	"io/fs"
	"strings"
	"testing"
	"testing/fstest"

	sourcewatcherv1 "github.com/fluxcd/source-watcher/api/v2/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/core/artifactsyntax"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestNamespaceTargets_ValidationOrder(t *testing.T) {
	t.Parallel()
	paths := &sourcewatcherv1.NamespaceMetadataFromSource{Path: "@repo/{Module}.yaml"}
	makeRequest := func(name, path string) request {
		return request{
			Namespace:         manifest.NamedResource{Name: name},
			namespaceMetadata: artifactsyntax.ParseSelector(path),
		}
	}
	first := makeRequest("tenant-a", "@repo/First.yaml")
	next := makeRequest("tenant-a", "@repo/Second.yaml")
	undefined := makeRequest("tenant-b", "")
	undefined.NamespaceMetadataPathError = errors.New("undefined capture")
	for _, tc := range []struct {
		name     string
		requests []request
		want     string
	}{
		{name: "no_targets", requests: []request{makeRequest("", ""), makeRequest("", "@repo/Other.yaml")}},
		{name: "shared_path_and_distinct_targets", requests: []request{first, first, makeRequest("tenant-b", next.namespaceMetadata.Text)}},
		{
			name: "names_before_path_errors", requests: []request{first, next, makeRequest("default", "")},
			want: `namespace "default" cannot be managed by an ArtifactGenerator`,
		},
		{
			name: "conflict_before_later_template_error", requests: []request{first, next, undefined},
			want: `namespace reconciliation failed: namespace metadata path "@repo/{Module}.yaml" resolves to "@repo/First.yaml" and "@repo/Second.yaml" for different artifacts targeting namespace "tenant-a", it must only use namespace-level captures`,
		},
		{
			name: "undefined_capture", requests: []request{first, undefined, next},
			want: `namespace reconciliation failed: invalid namespace metadata path "@repo/{Module}.yaml": undefined capture`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			policies, err := compileNamespacePolicies(
				&sourcewatcherv1.ManagedNamespaces{AllowedNames: []string{"true"}},
			)
			assert.NoError(t, err)
			targets := collectNamespaceTargets(tc.requests)
			err = targets.validate(t.Context(), policies, tc.requests, paths)
			if tc.want != "" {
				assert.FatalIf(t, err == nil, "expected namespace target error")
				assert.Equal(t, err.Error(), tc.want)
				return
			}
			assert.NoError(t, err)
			if len(targets.ordered) > 0 {
				assert.Equal(t, len(targets.ordered), 2)
				assert.Equal(t, targets.ordered[0], &tc.requests[0])
				assert.Equal(t, targets.ordered[1], &tc.requests[2])
			}
		})
	}
}

func TestNamespaceMetadata_ReadSource(t *testing.T) {
	t.Parallel()
	const metadata = "metadata: {labels: {team: platform}}"
	for _, tc := range []struct {
		name, path, want, cause string
		err                     error
	}{
		{name: "original_case", path: "@repo/Team.yaml", want: metadata},
		{name: "staged_source", path: "@artifact/Team.yaml", want: "{}"},
		{
			name: "unknown_alias", path: "@other/Team.yaml",
			cause: `invalid namespace metadata path "@other/Team.yaml": source alias "other" not found`,
		},
		{
			name: "escape", path: "@repo/team/../../Team.yaml",
			cause: `invalid namespace metadata path "@repo/team/../../Team.yaml": path "team/../../Team.yaml" must stay within the source root`,
		},
		{name: "missing", path: "@repo/team.yaml", cause: `failed to read namespace metadata file "@repo/team.yaml":`, err: fs.ErrNotExist},
		{name: "limit", path: "@repo/large.yaml", cause: `failed to read namespace metadata file "@repo/large.yaml": file exceeds the maximum size of 1048576 bytes`, err: errSourceFileTooLarge},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := evaluation{inputs: map[string]fs.FS{"repo": fstest.MapFS{
				"Team.yaml": {Data: []byte(metadata)}, "large.yaml": {Data: make([]byte, maxInputsFileSize+1)},
			}}}
			staged := fstest.MapFS{"Team.yaml": {Data: []byte("{}")}}
			data, err := e.readSourceFile(t.Context(), artifactsyntax.ParseSelector(tc.path), staged, namespaceFile)
			if tc.cause != "" {
				assert.FatalIf(t, err == nil, "expected metadata read error")
				assert.Equal(t, strings.HasPrefix(err.Error(), tc.cause), true)
				if tc.err != nil {
					assert.Equal(t, errors.Is(err, tc.err), true)
				}
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, string(data), tc.want)
		})
	}
}

func TestNamespaceMetadata_DecodeAndFilter(t *testing.T) {
	t.Parallel()
	const path = "@repo/metadata.yaml"
	policies, err := compileNamespacePolicies(&sourcewatcherv1.ManagedNamespaces{
		AllowedNames: []string{"true"},
		Metadata: &sourcewatcherv1.ManagedNamespacesMetadata{
			FromSource: &sourcewatcherv1.NamespaceMetadataFromSource{
				AllowedLabels:      map[string]string{"team": "value == 'platform'", "rank": "int(value) > 0"},
				AllowedAnnotations: map[string]string{"owner": "value.startsWith('platform')"},
			},
		},
	})
	assert.NoError(t, err)
	for _, tc := range []struct {
		name, doc, cause string
		want             namespaceMetadataValues
	}{
		{
			name: "yaml_ignores_unlisted_keys", doc: "metadata: {labels: {team: platform, ignored: no}, annotations: {owner: platform-a, ignored: no}}",
			want: namespaceMetadataValues{
				Labels: map[string]string{"team": "platform"}, Annotations: map[string]string{"owner": "platform-a"},
			},
		},
		{
			name: "json_with_type_fields",
			doc:  `{"apiVersion":"source.extensions.fluxcd.io/v1beta1","kind":"NamespaceMetadata","metadata":{"labels":{"team":"platform"}}}`,
			want: namespaceMetadataValues{Labels: map[string]string{"team": "platform"}},
		},
		{
			name: "parse_first", doc: "apiVersion: v1\nkind: Secret\nmetadata: [",
			cause: `failed to parse namespace metadata file "@repo/metadata.yaml": error converting YAML to JSON: yaml: line 3: did not find expected node content`,
		},
		{
			name: "version_before_kind", doc: "apiVersion: v1\nkind: Secret",
			cause: `namespace metadata file "@repo/metadata.yaml" has unsupported apiVersion "v1"`,
		},
		{
			name: "wrong_kind", doc: "kind: Secret",
			cause: `namespace metadata file "@repo/metadata.yaml" has unsupported kind "Secret"`,
		},
		{
			name: "label_before_annotation", doc: "metadata: {labels: {team: other}, annotations: {owner: other}}",
			cause: `namespace metadata file "@repo/metadata.yaml": label "team" with value "other" is rejected by the allowed expression "value == 'platform'"`,
		},
		{
			name: "annotation_rejected", doc: "metadata: {labels: {team: platform}, annotations: {owner: other}}",
			cause: `namespace metadata file "@repo/metadata.yaml": annotation "owner" with value "other" is rejected by the allowed expression "value.startsWith('platform')"`,
		},
		{
			name: "eval_error_in_key_order", doc: "metadata: {labels: {team: other, rank: invalid}, annotations: {owner: other}}",
			cause: `namespace metadata file "@repo/metadata.yaml": failed to evaluate the allowed label expression for key "rank": failed to evaluate the CEL expression 'int(value) > 0': type conversion error from 'string' to 'int'`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := policies.decodeMetadata(t.Context(), path, []byte(tc.doc))
			if tc.cause != "" {
				assert.FatalIf(t, err == nil, "expected metadata decode error")
				assert.Equal(t, err.Error(), tc.cause)
				return
			}
			assert.NoError(t, err)
			assert.Diff(t, got, tc.want)
		})
	}
}

func TestNamespaceMetadata_OrderedStrategiesAndOwnership(t *testing.T) {
	t.Parallel()
	spec := manifest.ArtifactGeneratorSpec{
		ManagedNamespaces: &sourcewatcherv1.ManagedNamespaces{
			Metadata: &sourcewatcherv1.ManagedNamespacesMetadata{
				From: []sourcewatcherv1.NamespaceMetadataFrom{
					{
						Namespace: "reset",
						Strategy:  sourcewatcherv1.NamespaceMetadataResetStrategy,
						Labels:    map[string]string{"team": "reset"},
					},
					{
						Namespace: "*",
						Strategy:  sourcewatcherv1.MergeStrategy,
						Labels:    map[string]string{"team": "merge", "added": "merge"},
					},
					{
						Namespace: "*",
						Strategy:  sourcewatcherv1.OverrideStrategy,
						Labels:    map[string]string{"added": "override"},
					},
					{
						Namespace: "*",
						Strategy:  sourcewatcherv1.MergeStrategy,
						Labels:    map[string]string{"added": "discarded"},
					},
					{Namespace: "reset", Strategy: sourcewatcherv1.OverrideStrategy, Labels: map[string]string{
						"source.extensions.fluxcd.io/generator": "entry", "app.kubernetes.io/managed-by": "entry",
					}},
				},
			},
		},

		CommonMetadata: &sourcewatcherv1.CommonMetadata{
			Labels:      map[string]string{"team": "common", "kept": "common"},
			Annotations: map[string]string{"owner": "common"},
		},
	}
	sourced := namespaceMetadataValues{
		Labels: map[string]string{"team": "sourced"}, Annotations: map[string]string{"owner": "sourced"},
	}
	for _, tc := range []struct {
		name                string
		labels, annotations map[string]string
	}{
		{name: "reset", labels: map[string]string{"team": "reset", "added": "override"}},
		{
			name: "tenant-a", labels: map[string]string{"team": "sourced", "added": "override", "kept": "common"},
			annotations: map[string]string{"owner": "sourced"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rules, err := resolveNamespaceMetadataRules(spec.ManagedNamespaces.Metadata)
			assert.NoError(t, err)
			publication := generatorPublication{
				spec: &generatorSpec{declaration: spec, namespaceRules: rules},
				uid:  "generator",
			}
			got, err := publication.namespaceMetadataFor(tc.name, sourced)
			assert.NoError(t, err)
			tc.labels["app.kubernetes.io/managed-by"] = "source-watcher"
			tc.labels["source.extensions.fluxcd.io/generator"] = "generator"
			assert.Diff(t, got, metav1.ObjectMeta{Name: tc.name, Labels: tc.labels, Annotations: tc.annotations})
			assert.Equal(t, spec.CommonMetadata.Annotations["owner"], "common")
			assert.Equal(t, sourced.Annotations["owner"], "sourced")
		})
	}
}

func TestNamespaceMetadata_KubernetesValidationOrder(t *testing.T) {
	t.Parallel()
	const finalizer = "source.extensions.fluxcd.io/externalFinalizer"
	meta := metav1.ObjectMeta{
		Name: "tenant-a", Labels: map[string]string{"z": "bad z", "a": "bad a"},
		Annotations: map[string]string{finalizer: "reserved", "team/": "bad"},
	}
	err := validateResolvedNamespaceMetadata(&meta)
	assert.FatalIf(t, err == nil, "expected reserved annotation error")
	assert.Equal(
		t,
		err.Error(),
		`metadata for namespace "tenant-a" must not set the reserved "source.extensions.fluxcd.io/externalFinalizer" annotation, it is reserved for external finalizers`,
	)
	meta.Annotations[finalizer] = ""
	err = validateResolvedNamespaceMetadata(&meta)
	assert.FatalIf(t, err == nil, "expected invalid metadata")
	text := err.Error()
	first := strings.Index(text, `metadata.labels: Invalid value: "bad a"`)
	second := strings.Index(text, `metadata.labels: Invalid value: "bad z"`)
	last := strings.Index(text, `metadata.annotations: Invalid value: "team/"`)
	assert.Equal(t, first >= 0 && first < second && second < last, true)
}
