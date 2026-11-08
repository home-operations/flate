package loader

import (
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/internal/selection"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestReduceStatic_PublishesBorrowedConstruction(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	l := New(store.New())
	l.SourceRoot = root
	l.ensureScopes()
	owner := testutil.KustomizationID("apps", "owner")
	file := filepath.Join(root, "owner.yaml")
	child := filepath.Join(root, "child", "object.yaml")
	terminal := filepath.Join(root, "patch.yaml")
	l.Index.Records[owner] = manifest.DiscoveryRecord{Path: file}
	l.documents.cache[file] = document{objects: []parsedObject{{}}, rootKnown: true, rootAdmitted: true, terminal: true}
	l.documents.cache[child] = document{objects: []parsedObject{{}}}
	l.documents.cache[terminal] = document{objects: []parsedObject{{}}}
	graph := graphContext{root: root, dir: "child"}
	visits := []graphVisit{
		{role: kustomizeinput.Resource, file: child},
		{role: kustomizeinput.File, file: terminal},
	}
	l.scopes.walks[graph] = scopeTraversal{visits: visits}
	l.scopes.contributions = []scopeContribution{{owner: owner, context: graph, visits: visits, input: 1}}
	l.scopes.inputs = []scopeInput{{}, {}}
	var reads []string
	for dir := range selection.ReduceStatic(staticScope{loader: l}) {
		reads = append(reads, dir)
		l.documents.cache[dir] = document{searched: true}
	}
	assert.Equal(t, 1, len(reads))
	assert.Equal(t, root, reads[0])
	assert.Equal(t, selection.RootEligible, l.documents.cache[file].eligible)
	assert.Equal(t, selection.RootEligible, l.documents.cache[child].eligible)
	assert.Equal(t, selection.RootInactive, l.documents.cache[terminal].eligible)
	assert.Equal(t, false, l.documents.cache[file].rootKnown)
	assert.Equal(t, false, l.documents.cache[file].rootAdmitted)
	assert.Equal(t, false, l.documents.cache[file].terminal)
	assert.Equal(t, true, l.scopes.walks[graph].controlled)
	assert.Equal(t, selection.RootEligible, l.scopes.walks[graph].reached)
	assert.Equal(t, true, l.scopes.contributions[0].active)
	assert.Equal(t, true, l.scopes.inputs[0].active)
	assert.Equal(t, true, l.scopes.inactive)
	assert.Equal(t, true, &l.scopes.walks[graph].visits[0] == &visits[0])
}
