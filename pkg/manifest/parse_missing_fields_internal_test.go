package manifest

import (
	"errors"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func TestParseDoc_MissingFields(t *testing.T) {
	t.Parallel()
	{
		_, _, _, err := ParseDoc(
			map[string]any{"kind": "Foo"},
			ParseDocOptions{DefaultNamespace: "apps"},
		)
		assert.ErrorIf(t, err == nil ||
			!strings.Contains(err.Error(), "apiVersion"), "expected apiVersion error, got %v", err)
	}
	// Bare data files (no kind:) are not Kubernetes resources.
	obj, _, _, err := ParseDoc(map[string]any{"apiVersion": "v1"}, ParseDocOptions{DefaultNamespace: "apps"})
	{
		assert.ErrorIf(t, err != nil, "missing-kind docs should not error, got %v", err)
	}
	{
		assert.ErrorIf(t, obj != nil, "expected nil for missing-kind doc, got %T", obj)
	}
	for _, tc := range []struct {
		name             string
		metadata         any
		absent           bool
		raw, core, typed string
	}{
		{name: "absent", absent: true, raw: "missing metadata for Widget", core: "ConfigMap missing metadata", typed: "HelmRelease missing metadata.name"},
		{name: "null", raw: "missing metadata for Widget", core: "ConfigMap missing metadata", typed: "HelmRelease missing metadata.name"},
		{name: "scalar", metadata: "bad", raw: "missing metadata for Widget", core: "ConfigMap missing metadata", typed: "HelmRelease decode: json: cannot unmarshal string"},
		{name: "numeric metadata", metadata: 7, raw: "missing metadata for Widget", core: "ConfigMap missing metadata", typed: "HelmRelease decode: json: cannot unmarshal number"},
		{name: "empty map", metadata: map[string]any{}, raw: "missing metadata.name for Widget", core: "ConfigMap missing metadata.name", typed: "HelmRelease missing metadata.name"},
		{name: "numeric name", metadata: map[string]any{"name": 7}, raw: "missing metadata.name for Widget", core: "ConfigMap missing metadata.name", typed: "HelmRelease decode: json: cannot unmarshal number"},
		{name: "numeric namespace", metadata: map[string]any{"name": "app", "namespace": 7}, typed: "HelmRelease decode: json: cannot unmarshal number"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testParseDocMissingFieldsCase(t, &tc)
		},
		)
	}
	t.Run(
		"typed spec error precedes missing metadata",
		func(t *testing.T) { t.Parallel(); testParseDocMissingFieldsTypedSpecErrorPrecedesMissingMetadata(t) },
	)
}

func testParseDocMissingFieldsCase(t *testing.T, testCaseInput *struct {
	name     string
	metadata any
	absent   bool
	raw      string
	core     string
	typed    string
},
) {
	t.Helper()

	for _, admission := range []struct{ kind, version, want string }{
		{"Widget", "example.test/v1", testCaseInput.raw},
		{KindConfigMap, "v1", testCaseInput.core},
		{KindHelmRelease, HelmReleaseDomain + "/v2", testCaseInput.typed},
	} {
		t.Run(admission.kind, func(t *testing.T) {
			t.Parallel()
			doc := map[string]any{"apiVersion": admission.version, "kind": admission.kind}
			if !testCaseInput.absent {
				doc["metadata"] = testCaseInput.metadata
			}
			before := DeepCopyMap(doc)
			obj, authoritative, changed, err := ParseDoc(doc, ParseDocOptions{DefaultNamespace: "apps"})
			if admission.want == "" {
				assert.NoError(t, err)
				assert.Equal(t, obj.Named().Namespace, "apps")
				assert.Equal(t, changed, true)
			} else {
				{
					assert.FatalIf(t, !errors.Is(err, ErrInput) || !errors.Is(err, ErrFlux) ||
						!strings.Contains(
							err.Error(),
							admission.want,
						), "error = %v, want %q with ErrInput/ErrFlux", err, admission.want)
				}
				assert.Equal(t, changed, false)
				assert.Diff(t, authoritative, before)
			}
			assert.Diff(t, doc, before)
		})
	}
}

func testParseDocMissingFieldsTypedSpecErrorPrecedesMissingMetadata(t *testing.T) {
	t.Helper()

	doc := map[string]any{"apiVersion": HelmReleaseDomain + "/v2", "kind": KindHelmRelease, "spec": "bad"}
	_, authoritative, changed, err := ParseDoc(doc, ParseDocOptions{DefaultNamespace: "apps"})
	{
		assert.FatalIf(t, !errors.Is(err, ErrInput) || !errors.Is(err, ErrFlux) ||
			!strings.Contains(
				err.Error(),
				"HelmRelease decode: json: cannot unmarshal string",
			), "malformed spec lost decode precedence: %v", err)
	}
	assert.Equal(t, changed, false)
	assert.Diff(t, authoritative, doc)
}
