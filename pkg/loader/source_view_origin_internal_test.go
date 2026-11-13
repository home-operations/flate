package loader

import (
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestSourceView_InputOrigin(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		identity      bool
		alias         bool
		pinned        bool
		external      bool
		physicalInput string
	}{
		{name: "mutable_subtree_alias", alias: true, physicalInput: "subtree/apps"},
		{name: "mutable_identity", identity: true, physicalInput: "apps"},
		{name: "mutable_family", physicalInput: "apps"},
		{name: "committed_at_same_mount", pinned: true, physicalInput: ".inputs/source/apps"},
		{name: "external_at_same_mount", external: true, physicalInput: ".inputs/source/apps"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testSourceViewInputOriginCase(t, &tc)
		},
		)
	}
}

func testSourceViewInputOriginCase(t *testing.T, testCaseInput *struct {
	name          string
	identity      bool
	alias         bool
	pinned        bool
	external      bool
	physicalInput string
},
) {
	t.Helper()

	repoRoot := t.TempDir()
	mount := filepath.Join(repoRoot, ".inputs", "source")
	art := &store.SourceArtifact{
		LocalPath:  mount,
		LocalRoot:  repoRoot,
		Key:        "derived",
		InputAlias: testCaseInput.alias,
		FS:         fstest.MapFS{},
	}
	if testCaseInput.pinned {
		art.Content = art.Key
	}
	if testCaseInput.identity {
		mount, art.LocalPath, art.Key = repoRoot, repoRoot, ""
	}
	if testCaseInput.alias {
		art.LocalRoot = filepath.Join(repoRoot, "subtree")
	}
	if testCaseInput.external {
		art.Content = art.Key
		art.LocalRoot = t.TempDir()
	}
	st := store.New()
	source := &manifest.GitRepository{Name: "source", Namespace: "flux-system"}
	st.AddObject(source)
	st.SetArtifact(source.Named(), art)
	location := SourceView(st, source.Named(), repoRoot)
	root, pinned, external := location.Path, location.Pinned, location.External
	wantRoot := mount
	if testCaseInput.external {
		wantRoot = repoRoot
	}
	if root != wantRoot || pinned != testCaseInput.pinned || external != testCaseInput.external {
		t.Fatalf(
			"source view = (%q, %t, %t), want (%q, %t, %t)",
			root,
			pinned,
			external,
			wantRoot, testCaseInput.pinned, testCaseInput.external,
		)
	}
	file := filepath.Join(mount, "apps")
	origin := filepath.Join(repoRoot, filepath.FromSlash(testCaseInput.physicalInput))
	assert.Equal(t, art.OriginPath(file), origin)
	if testCaseInput.identity {
		assert.Equal(
			t,
			art.OriginPath(mount+string(filepath.Separator)+filepath.FromSlash("discard/../apps")),
			origin,
		)
	}
	reader := documentReader{}
	reader.mountSources(st, nil)
	assert.Equal(t, new(reader).localPath(file), origin)
	base := "."
	if testCaseInput.alias {
		base = "subtree"
	}
	assert.Equal(t, new(reader).localBase(mount, repoRoot), base)
}
