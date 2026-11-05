package orchestrator

import (
	"encoding/json"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gitlib "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

type entrySourceFixture struct{ root, want string }

func writeEntrySourceReplacement(t *testing.T, field, url string) entrySourceFixture {
	t.Helper()
	root, remote := t.TempDir(), t.TempDir()
	repo := testutil.GitInit(t, root)
	assert.NoError(t,
		repo.Storer.SetReference(plumbing.NewSymbolicReference(plumbing.HEAD,
			plumbing.NewBranchReferenceName("main"))))
	op := map[string]any{"op": "replace", "path": "/spec/url", "value": "file://" + remote}
	want := "remote"
	switch field {
	case "ref":
		op["path"], op["value"] = "/spec/ref/name", "refs/tags/pinned"
		want = "pinned"
	case "ignore":
		op["op"], op["path"], op["value"] = "add", "/spec/ignore", "entry/omitted.yaml"
		want = "current"
	}
	patch, err := json.Marshal([]any{op})
	assert.NoError(t, err)
	declaration := "apiVersion: fluxcd.controlplane.io/v1\n" +
		"kind: FluxInstance\n" +
		"metadata: {name: flux, namespace: apps}\n" +
		"spec:\n" +
		"  sync: {name: self, kind: GitRepository, url: " + url + ", ref: refs/heads/main, path: ./entry}\n" +
		"  kustomize:\n" +
		"    patches:\n" +
		"    - target: {kind: GitRepository}\n" +
		"      patch: '" + string(patch) + "'\n"
	testutil.WriteFile(t, root, "entry/instance.yaml", declaration)
	testutil.WriteFile(t, root, "entry/kustomization.yaml", "resources: [instance.yaml, cm.yaml]\n")
	testutil.WriteFile(t, root, "entry/cm.yaml", "apiVersion: v1\n"+
		"kind: ConfigMap\n"+
		"metadata: {name: workload, namespace: apps}\n"+
		"data: {value: pinned}\n")
	testutil.WriteFile(t,
		root,
		"entry/omitted.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: omitted, namespace: apps}\n")
	options := &gitlib.CommitOptions{
		Author: &object.Signature{Name: "test", Email: "test@example.test", When: time.Unix(0, 0)},
	}
	pin := testutil.GitCommitAll(t, repo, "pinned", options)
	assert.NoError(t, repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewTagReferenceName("pinned"), pin)))
	testutil.WriteFile(t, root, "entry/cm.yaml", "apiVersion: v1\n"+
		"kind: ConfigMap\n"+
		"metadata: {name: workload, namespace: apps}\n"+
		"data: {value: current}\n")
	testutil.GitCommitAll(t, repo, "current", &gitlib.CommitOptions{Author: options.Author})
	remoteRepo := testutil.GitInit(t, remote)
	assert.NoError(t,
		remoteRepo.Storer.SetReference(plumbing.NewSymbolicReference(plumbing.HEAD,
			plumbing.NewBranchReferenceName("main"))))
	testutil.WriteFile(t, remote, "entry/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, remote, "entry/cm.yaml", "apiVersion: v1\n"+
		"kind: ConfigMap\n"+
		"metadata: {name: workload, namespace: apps}\n"+
		"data: {value: remote}\n")
	testutil.GitCommitAll(t, remoteRepo, "remote", &gitlib.CommitOptions{Author: options.Author})
	return entrySourceFixture{root: root, want: want}
}

func TestRun_EntrySourceReplacement(t *testing.T) {
	t.Parallel()
	for _, field := range []string{"url", "ref", "ignore"} {
		t.Run(field, func(t *testing.T) {
			t.Parallel()
			testRunEntrySourceReplacement(t, field)
		})
	}
}

func testRunEntrySourceReplacement(t *testing.T, field string) {
	t.Helper()
	const url = "https://example.test/entry-source"
	fixture := writeEntrySourceReplacement(t, field, url)
	root, want := fixture.root, fixture.want
	o, err := newTestOrchestrator(t,
		Tree{
			Path: filepath.Join(root,
				"entry"),
			RepoRoot: root,
			SelfURLs: []string{url},
		},
		Config{Concurrency: 2})
	assert.NoError(t, err)
	t.Cleanup(o.stop)
	assert.NoError(t, o.bootstrap(t.Context()))
	source := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "apps",
		Name:      "self",
	}
	before := o.store.GetArtifact(source, store.ArtifactContent)
	result, err := o.collectResult(o.run(t.Context()))
	assert.NoError(t, err)
	artifact, ok := o.store.GetArtifact(source, store.ArtifactContent).(*store.SourceArtifact)
	if !ok || before == artifact {
		t.Fatalf("source patch reused stale bootstrap material: %+v", artifact)
	}
	data, err := fs.ReadFile(artifact.FS, "entry/cm.yaml")
	assert.NoError(t, err)
	if !strings.Contains(string(data), "value: "+want) {
		t.Fatalf("wrong source-side content: %s", data)
	}
	if field == "ignore" {
		_, err := fs.Stat(artifact.FS, "entry/omitted.yaml")
		if err == nil {
			t.Fatal("source ignore retained excluded input")
		}
	}
	// Pass 8 Mayor ruling: literal entry and application receipts replace generated-sync ownership, identity, and publication assertions.
	rootID := o.entryBinding.Producer
	assert.Equal(t, rootID.ReportHidden(), true)
	assert.Equal(t, o.entryBinding.Source, source)
	assert.Equal(t, o.entryBinding.Path, "entry")
	found := false
	for _, doc := range outputByParent(result.Manifests)[rootID] {
		if doc["kind"] == "ConfigMap" && manifest.DocIdentity(doc).Name == "workload" {
			found = true
			assert.Equal(t, doc["data"].(map[string]any)["value"], any(want))
		}
	}
	assert.Equal(t, found, true)
}
