package loader

import (
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func testRegisterBuiltStandaloneRootsInactivePackageBatch(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: []\n")
	testutil.WriteFile(
		t,
		root,
		"apps/disabled.yaml",
		testutil.KustomizationYAML("disabled", "apps", "  path: ./target\n"),
	)
	testutil.WriteFile(t, root, "target/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, root, "target/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: raw}\n")
	l := New(store.New())
	l.SourceRoot = root
	_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	obj := &manifest.ConfigMap{Name: "injected", Namespace: "apps"}
	l.RegisterBuilt(
		supportedBuiltInputForTest(l, &BuiltInput{
			Root:   filepath.Join(root, "target"),
			Source: manifest.BootstrapSourceID,
			Owner:  testutil.KustomizationID("apps", "entry"),
		}),
		[]store.Child{{Obj: obj}},
	)
	{
		_, indexed := l.Index.Lookup(obj.Named())
		assert.FatalIf(
			t,
			indexed || l.Store.GetObject(obj.Named()) != nil,
			"%s",
			"inactive package published authoritative output",
		)
	}
}

func testRegisterBuiltStandaloneRootsCase(t *testing.T, testCaseInput *struct {
	name      string
	ownerPath string
	reader    bool
	scanned   bool
	eligible  bool
},
) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, "entry/kustomization.yaml", "resources: [cm.yaml, nested]\n")
	testutil.WriteFile(t, root, "entry/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: outer}\n")
	nested := "resources: []\n"
	if testCaseInput.reader {
		nested += "configMapGenerator: [{name: read, files: [../kustomization.yaml]}]\n"
	}
	testutil.WriteFile(t, root, "entry/nested/kustomization.yaml", nested)
	st := store.New()
	admitForTest(st, root)
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: testCaseInput.ownerPath}
	st.AddObject(owner)
	l := New(st)
	l.SourceRoot = root
	entry := filepath.Join(root, "entry")
	if testCaseInput.scanned {
		entry = root
	}
	{
		_, err := l.Load(t.Context(), entry, manifest.NamedResource{}, nil)
		assert.NoError(t, err)
	}
	var want []string
	if testCaseInput.eligible {
		want = []string{"entry"}
	}
	{
		roots := standaloneRootsForTest(t, l, root)
		assert.FatalIf(t, !slices.Equal(roots, want), "standalone roots = %v, want %v", roots, want)
	}
	{
		err := l.FinalizeIndex(t.Context(), root, true)
		assert.NoError(t, err)
	}
	if testCaseInput.reader {
		if !slices.ContainsFunc(
			l.Index.Readers["entry/kustomization.yaml"],
			func(r manifest.FileReader) bool { return r.Reader == owner.Named() },
		) {
			t.Fatalf("nested generator lost its read edge: %v", l.Index.Readers)
		}
		const entryPath = "entry/"
		for prefix, owners := range l.Index.Claims {
			assert.FatalIf(
				t,
				strings.HasPrefix(entryPath, prefix) && slices.Contains(owners, owner.Named()),
				"nested reader acquired a parent claim: %s",
				prefix,
			)
		}
	}
}
