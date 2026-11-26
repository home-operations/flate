package loader

import (
	"maps"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestReplaceBuiltRecords_ChangedDeclarationEvidence(t *testing.T) {
	t.Parallel()
	for _, change := range []string{"document", "warning payload", "warning subject", "warning count"} {
		t.Run(change, func(t *testing.T) {
			t.Parallel()
			root := testutil.WriteTree(t, map[string]string{
				"parent/kustomization.yaml": "resources: [child.yaml]\n",
				"parent/child.yaml":         testutil.KustomizationYAML("child", "apps", "  path: ./authored\n"),
			})
			st := store.New()
			admitForTest(st, root)
			owner := &manifest.Kustomization{Name: "parent", Namespace: "apps", Path: "./parent"}
			st.AddObject(owner)
			l := New(st)
			t.Cleanup(l.Close)
			l.SourceRoot = root
			packageRoot := filepath.Join(root, "parent")
			_, err := l.Load(t.Context(), packageRoot, manifest.NamedResource{}, nil)
			assert.NoError(t, err)
			docs := builtObjectsForTest(t, testutil.KustomizationYAML("child", "apps", "  path: ./built\n"))
			child := &docs[0]
			id := child.Obj.Named()
			child.Warnings = []manifest.Warning{{Resource: id, Category: manifest.WarnMissingCRD, Count: 1}}
			input := supportedBuiltInputForTest(l, &BuiltInput{
				Root: packageRoot, Owner: owner.Named(), Source: manifest.BootstrapSourceID,
			})
			defer input.Support.Close()
			publication, err := l.RegisterBuilt(t.Context(), input, docs)
			assert.NoError(t, err)
			assert.Equal(t, publication, BuiltRegistered)
			record, ok := l.Index.Lookup(id)
			assert.FatalIf(t, !ok || record.Authored != id, "built child must retain its authored candidate")
			l.admitted[id] = child.Obj
			st.AddObject(child.Obj, child.Doc)
			var reader store.ReadSet
			reader.Init(st, owner.Named(), nil)
			defer reader.Close()
			assert.Equal(t, reader.GetDeclaration(id), child.Obj)

			next := *child
			next.Doc, next.Warnings = maps.Clone(child.Doc), slices.Clone(child.Warnings)
			switch change {
			case "document":
				next.Doc["review"] = "updated"
			case "warning payload":
				next.Warnings[0].Message = "updated"
			case "warning subject":
				next.Warnings[0].Subject = manifest.DiagnosticFor(owner.Named(), owner)
			case "warning count":
				next.Warnings[0].Count++
			}
			replacement := supportedBuiltInputForTest(l, &input)
			inputs := l.selectBuiltInputs(root, "parent", &replacement, map[string]bool{})
			l.replaceBuiltRecords(&replacement, []store.Child{next}, &inputs, record.Builder)
			assert.Equal(t, reader.Valid(), false)
			assert.Equal(t, st.GetDeclaration(id), nil)
			after, _ := l.Index.Lookup(id)
			assert.Equal(t, after.Declaration, child.Obj)
			assert.Equal(t, after.Builder, record.Builder)
			var currentReads store.ReadSet
			currentReads.Init(st, owner.Named(), nil)
			defer currentReads.Close()
			current, complete := l.BuiltChildren(packageRoot, owner.Named(), &currentReads)
			assert.FatalIf(t, !complete || len(current) != 1, "replacement is missing: %v", current)
			assert.Diff(t, current[0].Doc, next.Doc)
			assert.Diff(t, current[0].Warnings, next.Warnings)
		})
	}
}

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
	t.Cleanup(l.Close)
	l.SourceRoot = root
	_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	obj := &manifest.ConfigMap{Name: "injected", Namespace: "apps"}
	input := supportedBuiltInputForTest(l, &BuiltInput{
		Root:   filepath.Join(root, "target"),
		Source: manifest.BootstrapSourceID,
		Owner:  testutil.KustomizationID("apps", "entry"),
	})
	defer l.Store.Retire(input.Support)
	publication, registrationErr := l.RegisterBuilt(t.Context(), input, []store.Child{{Obj: obj}})
	assert.NoError(t, registrationErr)
	assert.Equal(t, publication, BuiltInventory)
	{
		_, indexed := l.Index.Lookup(obj.Named())
		assert.FatalIf(
			t,
			indexed || l.Store.GetObject(obj.Named()) != nil,
			"%s",
			"inactive package published authoritative output",
		)
	}

	// The same capture remains current when only the package's static eligibility changes.
	file := l.documents.read(input.Root).file
	scope := staticScope{loader: l}
	scope.SetEligibility(file, selection.RootEligible)
	publication, err = l.RegisterBuilt(t.Context(), input, []store.Child{{Obj: obj}})
	assert.NoError(t, err)
	assert.Equal(t, publication, BuiltRegistered)
	batch := l.builtBatches[input.Owner]
	assert.FatalIf(t, batch == nil, "eligible package did not install a batch")

	scope.SetEligibility(file, selection.RootInactive)
	publication, err = l.RegisterBuilt(t.Context(), input, []store.Child{{Obj: obj}})
	assert.NoError(t, err)
	assert.Equal(t, publication, BuiltRegistered)
	assert.Equal(t, l.builtBatches[input.Owner], batch)
	assert.Equal(t, input.Support.Valid(), true)
	var reads store.ReadSet
	reads.Init(l.Store, input.Owner, nil)
	t.Cleanup(reads.Close)
	children, current := l.BuiltChildren(input.Root, input.Owner, &reads)
	assert.FatalIf(t, !current || len(children) != 1, "installed batch lost current children: %v", children)
	assert.Equal(t, children[0].Obj, manifest.BaseManifest(obj))
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
