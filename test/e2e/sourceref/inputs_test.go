package sourceref

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

func TestE2E_PinnedDiff_EscapedPatchInput(t *testing.T) {
	root, repo, _, _ := sourceRefFixture(t)
	for _, file := range []string{"apps/kustomization.yaml", "apps/cm.yaml"} {
		if err := os.Remove(filepath.Join(root, file)); err != nil {
			t.Fatal(err)
		}
	}
	testutil.WriteFile(t, root, "apps/nested/kustomization.yaml", "resources: [../../shared]\n")
	testutil.WriteFile(t, root, "shared/kustomization.yaml", "resources: [cm.yaml]\npatches: [{path: ../inputs/patch}]\n")
	testutil.WriteFile(t, root, "shared/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello, namespace: apps}\ndata: {value: plain}\n")
	setSourceRef(t, root, "tag: input-before")
	var original string
	for _, version := range []string{"input-before", "input-after"} {
		testutil.WriteFile(t, root, "inputs/patch", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello, namespace: apps}\ndata: {value: "+version+"}\n")
		testutil.GitCommitAll(t, repo, version, &git.CommitOptions{Author: &object.Signature{Name: "fixture", Email: "fixture@example.test", When: time.Unix(0, 0)}})
		head, err := repo.Head()
		if err != nil {
			t.Fatal(err)
		}
		if err := repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewTagReferenceName(version), head.Hash())); err != nil {
			t.Fatal(err)
		}
		if version == "input-before" {
			original = t.TempDir()
			testutil.CopyTree(t, root, original)
		}
	}
	testutil.WriteFile(t, root, "unrelated.txt", "advance HEAD beyond both source pins")
	testutil.GitCommitAll(t, repo, "advance HEAD", &git.CommitOptions{Author: &object.Signature{Name: "fixture", Email: "fixture@example.test", When: time.Unix(0, 0)}})
	harness.MutateFile(t, filepath.Join(root, "flux/entry.yaml"), "tag: input-before", "tag: input-after")
	harness.MutateFile(t, filepath.Join(root, "inputs/patch"), "value: input-after", "value: dirty")
	loader := installSourceRefTransport(t, repo.Storer)
	for _, side := range []struct{ root, value string }{{original, "input-before"}, {root, "input-after"}} {
		out, _ := harness.RequireCLIOK(t, "build", "all", "--path", filepath.Join(side.root, "flux"), "--no-progress", "--concurrency", "2", "--cache-dir", t.TempDir())
		if !strings.Contains(out, "value: "+side.value) || strings.Contains(out, "value: dirty") {
			t.Fatalf("pinned input read a different source view: %s", out)
		}
	}
	for _, baseline := range [][]string{{"--base", "HEAD"}, {"--path-orig", filepath.Join(original, "flux")}} {
		t.Run(baseline[0], func(t *testing.T) {
			args := append([]string{"diff", "all", "-o", "diff", "--path", filepath.Join(root, "flux"), "--no-progress", "--concurrency", "2", "--cache-dir", t.TempDir()}, baseline...)
			out, _ := harness.RequireCLIOK(t, args...)
			assertSourceValueDiff(t, out, "input-before", "input-after")
		})
	}
	if loader.calls.Load() != 0 {
		t.Fatalf("local pinned input fetched remotely %d times", loader.calls.Load())
	}
}
