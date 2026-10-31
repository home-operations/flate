package manifest

import (
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func TestOutputEqual_FieldPolicy(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		kind, field string
		equal       bool
	}{
		{KindKustomization, "interval", true}, {KindKustomization, "timeout", true},
		{KindKustomization, "retryInterval", true}, {KindKustomization, "prune", true},
		{KindKustomization, "decryption.provider", false}, {KindKustomization, "suspend", false},
		{KindKustomization, "futureOutputOption", false}, {KindKustomization, "postBuild.substitute.IMAGE", false},
		{KindKustomization, "healthChecks.name", true}, {KindKustomization, "healthChecks.future", false},
		{KindHelmRelease, "interval", true}, {KindHelmRelease, "install.remediation.retries", true},
		{KindHelmRelease, "install.remediation.future", false}, {KindHelmRelease, "install.replace", false},
		{KindHelmRelease, "install.disableHooks", false}, {KindHelmRelease, "install.skipCRDs", false},
		{KindHelmRelease, "install.disableSchemaValidation", false}, {KindHelmRelease, "suspend", false},
		{KindHelmRelease, "values.image", false}, {KindHelmRelease, "chart.spec.version", false},
		{KindHelmRelease, "upgrade.remediation.strategy", true}, {KindHelmRelease, "rollback.force", true},
	} {
		t.Run(tc.kind+"/"+tc.field, func(t *testing.T) {
			t.Parallel()
			var object BaseManifest = &Kustomization{Name: "app", Namespace: "apps"}
			if tc.kind == KindHelmRelease {
				object = &HelmRelease{Name: "app", Namespace: "apps"}
			}
			makeDoc := func(value string) map[string]any {
				spec := map[string]any{}
				current := spec
				fields := strings.Split(tc.field, ".")
				for _, field := range fields[:len(fields)-1] {
					child := map[string]any{}
					current[field] = child
					current = child
				}
				current[fields[len(fields)-1]] = value
				return map[string]any{"spec": spec}
			}
			docs := [2]map[string]any{makeDoc("before"), makeDoc("after")}
			original := [2]map[string]any{DeepCopyMap(docs[0]), DeepCopyMap(docs[1])}
			equal, err := OutputEqual(object, object, docs)
			assert.NoError(t, err)
			assert.Equal(t, equal, tc.equal)
			assert.Diff(t, docs, original)
		})
	}
}

func TestReference_CloneAndContentInput(t *testing.T) {
	t.Parallel()
	owner := &HelmRelease{Name: "app", Namespace: "apps", ValuesFrom: []ValuesReference{{Kind: KindConfigMap, Name: "values", ValuesKey: "chosen"}}}
	refs := []Reference{}
	for ref := range References(owner, ReferenceRelations) {
		refs = append(refs, ref.Clone())
	}
	assert.Equal(t, len(refs), 1)
	assert.Equal(t, refs[0].Key, "chosen")
	assert.Equal(t, refs[0].NamespaceDefault, true)
	assert.Equal(t, refs[0].ContentInput(), true)
}
