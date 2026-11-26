package artifactgenerator

import (
	"fmt"
	"strings"
	"testing"

	sourcewatcherv1 "github.com/fluxcd/source-watcher/api/v2/v1beta1"

	"github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/engine/task"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
)

func TestCompileNamespacePolicies_DiagnosticOrder(t *testing.T) {
	t.Parallel()
	const (
		nameError       = `invalid .spec.managedNamespaces.allowedNames expression "ns": CEL expression output type mismatch: expected bool, got string`
		labelError      = `invalid allowed label expression for key "a": CEL expression output type mismatch: expected bool, got string`
		annotationError = `invalid allowed annotation expression for key "a": CEL expression output type mismatch: expected bool, got string`
	)
	for _, tc := range []struct {
		name                string
		names               []string
		labels, annotations map[string]string
		want                string
	}{
		{name: "empty", want: ".spec.managedNamespaces.allowedNames must contain at least one expression"},
		{name: "names_first", names: []string{"ns", "123"}, labels: map[string]string{"a": "value"}, want: nameError},
		{
			name: "labels_next_in_key_order", names: []string{"true"},
			labels: map[string]string{"b": "123", "a": "value"}, annotations: map[string]string{"a": "value"},
			want: labelError,
		},
		{
			name: "annotations_last_in_key_order", names: []string{"true"},
			labels: map[string]string{"a": "value == 'ok'"}, annotations: map[string]string{"b": "123", "a": "value"},
			want: annotationError,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := compileNamespacePolicies(&sourcewatcherv1.ManagedNamespaces{
				AllowedNames: tc.names,
				Metadata: &sourcewatcherv1.ManagedNamespacesMetadata{
					FromSource: &sourcewatcherv1.NamespaceMetadataFromSource{
						AllowedLabels: tc.labels, AllowedAnnotations: tc.annotations,
					},
				},
			})
			assert.FatalIf(t, err == nil, "expected compilation error")
			assert.Equal(t, err.Error(), tc.want)
		})
	}
	t.Run("malformed", func(t *testing.T) {
		t.Parallel()
		_, err := compileNamespacePolicies(&sourcewatcherv1.ManagedNamespaces{AllowedNames: []string{"ns."}})
		assert.FatalIf(t, err == nil, "expected compilation error")
		assert.Equal(t, strings.HasPrefix(
			err.Error(),
			`invalid .spec.managedNamespaces.allowedNames expression "ns.": failed to parse the CEL expression 'ns.': ERROR:`,
		), true)
	})
	t.Run("absent", func(t *testing.T) {
		t.Parallel()
		policies, err := compileNamespacePolicies(nil)
		assert.NoError(t, err)
		assert.Equal(t, policies, nil)
	})
}

func TestNamespacePolicies_NameEvaluation(t *testing.T) {
	t.Parallel()
	const broken = "int(ns) > 0"
	for _, tc := range []struct {
		name, namespace string
		expressions     []string
		want            string
	}{
		{
			name: "extensions_and_short_circuit", namespace: "tenant-a",
			expressions: []string{
				"false",
				"sets.contains(['tenant'], [ns.split('-')[0]]) && optional.of(ns).hasValue() && base64.decode('YQ==') == b'a'",
				broken,
			},
		},
		{
			name: "none_match", namespace: "other", expressions: []string{"ns.startsWith('tenant-')", "false"},
			want: `namespace "other" is not allowed by .spec.managedNamespaces.allowedNames`,
		},
		{
			name: "evaluation_error_before_true", namespace: "tenant-a", expressions: []string{broken, "true"},
			want: `failed to evaluate .spec.managedNamespaces.allowedNames expression "int(ns) > 0": failed to evaluate the CEL expression 'int(ns) > 0': type conversion error from 'string' to 'int'`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			policies, err := compileNamespacePolicies(
				&sourcewatcherv1.ManagedNamespaces{AllowedNames: tc.expressions},
			)
			assert.NoError(t, err)
			err = policies.validateName(t.Context(), tc.namespace)
			if tc.want == "" {
				assert.NoError(t, err)
				return
			}
			assert.FatalIf(t, err == nil, "expected name evaluation error")
			assert.Equal(t, err.Error(), tc.want)
		})
	}
	for _, reserved := range []string{"default", "kube-system", "kube-public", "flux-system"} {
		t.Run(reserved, func(t *testing.T) {
			t.Parallel()
			policies, err := compileNamespacePolicies(&sourcewatcherv1.ManagedNamespaces{
				AllowedNames: []string{broken, "true"},
			})
			assert.NoError(t, err)
			err = policies.validateName(t.Context(), reserved)
			assert.FatalIf(t, err == nil, "expected reserved name error")
			assert.Equal(
				t,
				err.Error(),
				fmt.Sprintf("namespace %q cannot be managed by an ArtifactGenerator", reserved),
			)
		})
	}
}

func TestArtifactGenerator_NamespacePolicyBeforeSpecValidation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, allowedName   string
		labels, annotations map[string]any
		want                string
	}{
		{
			name: "allowed_names", allowedName: "ns",
			want: `invalid .spec.managedNamespaces.allowedNames expression "ns": CEL expression output type mismatch: expected bool, got string`,
		},
		{
			name: "allowed_labels", allowedName: "true", labels: map[string]any{"a": "value"},
			want: `invalid allowed label expression for key "a": CEL expression output type mismatch: expected bool, got string`,
		},
		{
			name: "allowed_annotations", allowedName: "true", annotations: map[string]any{"a": "value"},
			want: `invalid allowed annotation expression for key "a": CEL expression output type mismatch: expected bool, got string`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			id := addManagedGenerator(t, st, managedGeneratorFixture{
				name: "generator",
				managed: map[string]any{
					"allowedNames": []any{tc.allowedName},
					"metadata": map[string]any{"fromSource": map[string]any{
						"path": "@repo/meta.yaml", "allowedLabels": tc.labels, "allowedAnnotations": tc.annotations,
					}},
				},
			})
			c := New(st, task.NewBounded(2), cachetest.New(t, "", cache.New))
			runForTest(t, c, id)
			status, _ := st.GetStatus(id)
			assert.Equal(t, status.Status, store.StatusFailed)
			assert.Equal(t, status.Message, tc.want)
		})
	}
}
