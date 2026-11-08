package loader

import (
	"path/filepath"
	"testing"

	"sigs.k8s.io/kustomize/api/types"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestNativeOperand_PhysicalSupport(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		owner      manifest.NamedResource
		membership bool
		role       kustomizeinput.Role
		want       bool
	}{
		{name: "native", role: kustomizeinput.Resource, want: true},
		{name: "owned", owner: testutil.KustomizationID("apps", "owner"), role: kustomizeinput.Resource},
		{name: "membership", membership: true, role: kustomizeinput.Resource},
		{name: "file", role: kustomizeinput.File},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			l := New(store.New())
			l.SourceRoot = t.TempDir()
			l.ensureScopes()
			root := graphVisit{root: l.SourceRoot, dir: "native", owner: tc.owner}
			file := filepath.Join(l.SourceRoot, "mounted", "input.yaml")
			origin := filepath.Join(l.SourceRoot, "original", "input.yaml")
			l.recordScopeVisits(&root, []graphVisit{{file: file, origin: origin, role: tc.role}}, tc.membership, false)
			assert.Equal(t, l.nativeOperand(file), false)
			assert.Equal(t, l.nativeOperand(origin), false)
			l.documents.cache[filepath.Join(root.root, root.dir)] = document{body: &types.Kustomization{}}
			assert.Equal(t, l.nativeOperand(file), tc.want)
			assert.Equal(t, l.nativeOperand(origin), tc.want)
			assert.Equal(t, l.nativeOperand(filepath.Join(l.SourceRoot, "unrelated.yaml")), false)
			delete(l.documents.cache, filepath.Join(root.root, root.dir))
			assert.Equal(t, l.nativeOperand(file), false)
			assert.Equal(t, l.nativeOperand(origin), false)
		})
	}
}
