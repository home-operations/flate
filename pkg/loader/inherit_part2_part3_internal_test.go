package loader

import (
	"io/fs"
	"path/filepath"
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func testAdmitGraphProjectionSubtreeAvailability(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(
		t,
		root,
		"flux/ks.yaml",
		testutil.KustomizationYAML(
			"parent",
			"flux-system",
			"  {path: ./apps, targetNamespace: media, sourceRef: {kind: OCIRepository, name: source}}\n",
		),
	)
	testutil.WriteFile(t, root, "kubernetes/apps/kustomization.yaml", "resources: [child.yaml, explicit.yaml]\n")
	for _, name := range []string{"child", "explicit"} {
		namespace := ""
		if name == "explicit" {
			namespace = "control"
		}
		testutil.WriteFile(
			t,
			root,
			"kubernetes/apps/"+name+".yaml",
			testutil.KustomizationYAML(
				name,
				namespace,
				"  {path: ./output, sourceRef: {kind: OCIRepository, name: source, namespace: flux-system}}\n",
			),
		)
	}
	st := store.New()
	l := loaderFixture(st, nil)
	l.SourceRoot = root
	admitForTest(st, root)
	for _, dir := range []string{"flux", "kubernetes/apps"} {
		{
			_, err := loadForTest(t.Context(), t, l, filepath.Join(root, dir))
			assert.NoError(t, err)
		}
	}
	owner, _ := st.Get[*manifest.Kustomization](testutil.KustomizationID("flux-system", "parent"))
	{
		assert.FatalIf(t, owner == nil, "%s", "parent missing")
	}
	l.Admit(root)
	{
		_, _, external := SourceView(st, owner.SourceID(), root)
		assert.FatalIf(t, !external, "%s", "unavailable source borrowed bootstrap coordinates")
	}
	for _, id := range []manifest.NamedResource{
		testutil.KustomizationID("", "child"),
		testutil.KustomizationID("control", "explicit"),
	} {
		{
			_, ok := l.Index.Lookup(id)
			assert.FatalIf(t, !ok, "unavailable source changed raw identity %s", id)
		}
	}
	input := st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
	view, err := fs.Sub(input.FS, "kubernetes")
	assert.NoError(t, err)
	source, err := owner.SourceID().BindLocal(root)
	assert.NoError(t, err)
	st.AddObject(source)
	mount := filepath.Join(filepath.Dir(root), ".flate-sources", "subtree", "flux-system", "source")
	st.SetArtifact(
		source.Named(),
		&store.SourceArtifact{
			Kind:       manifest.KindOCIRepository,
			LocalRoot:  filepath.Join(root, "kubernetes"),
			LocalPath:  mount,
			InputAlias: true,
			FS:         view,
		},
	)
	{
		got, _, external := SourceView(st, owner.SourceID(), root)
		assert.FatalIf(t, external || got != mount, "available source = %s, external=%t", got, external)
	}
	{
		_, err := l.Load(t.Context(), filepath.Join(mount, "apps"))
		assert.NoError(t, err)
	}
	raw := map[string]manifest.DiscoveryRecord{}
	for _, name := range []string{"child", "explicit"} {
		id := testutil.KustomizationID("", name)
		if name == "explicit" {
			id.Namespace = "control"
		}
		record, ok := l.Index.Lookup(id)
		file, err := filepath.Rel(root, filepath.Join(mount, "apps", name+".yaml"))
		assert.NoError(t, err)
		assert.FatalIf(
			t,
			!ok || record.File != filepath.ToSlash(file) || record.Authored != id,
			"raw %s record = %+v",
			id,
			record,
		)
		raw[name] = record
	}
	l.Admit(root)
	for _, name := range []string{"child", "explicit"} {
		id := testutil.KustomizationID("media", name)
		record, ok := l.Index.Lookup(id)
		if !ok || candidateForTest(t, l, id) == nil || st.GetObject(id) == nil || record.File != raw[name].File ||
			record.Path != filepath.Join(mount, "apps", name+".yaml") ||
			record.Path != raw[name].Path {
			t.Fatalf("projected %s record=%+v, raw=%+v", id, record, raw[name])
		}
	}
	{
		err := l.FinalizeIndex(t.Context(), root, true)
		assert.NoError(t, err)
	}
	for _, name := range []string{"child", "explicit"} {
		id := testutil.KustomizationID("media", name)
		record, ok := l.Index.Lookup(id)
		assert.FatalIf(t, !ok || record.Parent != owner.Named() || record.File != "kubernetes/apps/"+name+".yaml" ||
			record.Path != raw[name].Path, "finalized %s record=%+v", id, record)
		{
			readers := slices.Collect(
				l.Index.ReadersOfFile(record.File),
			)
			assert.FatalIf(t, !slices.ContainsFunc(
				readers,
				func(r manifest.FileReader) bool { return r.Reader == owner.Named() },
			), "subtree readers = %v", readers)
		}
	}
}

func testAdmitGraphProjectionStaleDeclaration(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "namespace: immich\nresources: [ks.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"apps/ks.yaml",
		testutil.KustomizationYAML("immich-db", "pg", "  {path: ./output, targetNamespace: pg}\n"),
	)
	l := loaderFixture(store.New(), nil)
	l.SourceRoot = root
	{
		_, err := loadForTest(t.Context(), t, l, filepath.Join(root, "apps"))
		assert.NoError(t, err)
	}
	l.Admit(root)
	{
		err := l.FinalizeIndex(t.Context(), root, true)
		assert.NoError(t, err)
	}
	canonical := testutil.KustomizationID("immich", "immich-db")
	ks, ok := l.Store.Get[*manifest.Kustomization](canonical)
	assert.FatalIf(t, !ok || ks.TargetNamespace != "pg", "canonical declaration = %+v, want targetNamespace pg", ks)
	{
		_, ok := l.Index.Lookup(canonical)
		assert.FatalIf(t, !ok, "%s", "canonical declaration missing from finalized index")
	}
	stale := testutil.KustomizationID("pg", "immich-db")
	_, indexed := l.Index.Lookup(stale)
	{
		assert.FatalIf(
			t,
			l.Store.GetObject(stale) != nil || indexed,
			"%s",
			"stale pg/immich-db declaration survived projection",
		)
	}
}
