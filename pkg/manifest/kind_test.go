package manifest

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func TestKindOf_GroupAndScope(t *testing.T) {
	for _, tc := range []struct {
		api, kind  string
		namespaced bool
	}{
		{"rbac.authorization.k8s.io/v1", "ClusterRole", false},
		{"foreign.example/v1", "ClusterRole", true},
		{"foreign.example/v1", KindBucket, true},
	} {
		t.Run(tc.api+"/"+tc.kind, func(t *testing.T) {
			obj, _, _, err := ParseDoc(map[string]any{"apiVersion": tc.api, "kind": tc.kind, "metadata": map[string]any{"name": "object", "namespace": "authored"}}, ParseDocOptions{})
			if err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, obj.Named().Namespace, "authored")
			assert.Diff(t, KindOf(obj), KindInfo{Group: apiGroup(tc.api), Kind: tc.kind, Namespaced: tc.namespaced})
			assert.Equal(t, testing.AllocsPerRun(100, func() { KindOf(obj) }), float64(0))
		})
	}
}
