package sourceref_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/assert"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_PinnedDiff_EscapedPatchInput(t *testing.T) {
	root, repo, _, _ := sourceRefFixture(t)
	for _, file := range []string{"apps/kustomization.yaml", "apps/cm.yaml"} {
		assert.NoError(t, os.Remove(filepath.Join(root, file)))
	}
	testutil.WriteFile(t, root, "apps/nested/kustomization.yaml", "resources: [../../shared]\n")
	testutil.WriteFile(
		t,
		root,
		"shared/kustomization.yaml",
		"resources: [cm.yaml]\npatches: [{path: ../inputs/patch}]\n",
	)
	testutil.WriteFile(
		t,
		root,
		"shared/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello, namespace: apps}\ndata: {value: plain}\n",
	)
	setSourceRef(t, root, "tag: input-before")
	var original string
	for _, version := range []string{"input-before", "input-after"} {
		testutil.WriteFile(
			t,
			root,
			"inputs/patch",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello, namespace: apps}\ndata: {value: "+version+"}\n",
		)
		testutil.GitCommitAll(
			t,
			repo,
			version,
			&git.CommitOptions{
				Author: &object.Signature{Name: "fixture", Email: "fixture@example.test", When: time.Unix(0, 0)},
			},
		)
		head, err := repo.Head()
		assert.NoError(t, err)
		assert.NoError(t, repo.Storer.SetReference(
			plumbing.NewHashReference(plumbing.NewTagReferenceName(version), head.Hash()),
		))
		if version == "input-before" {
			original = t.TempDir()
			testutil.CopyTree(t, root, original)
		}
	}
	testutil.WriteFile(t, root, "unrelated.txt", "advance HEAD beyond both source pins")
	testutil.GitCommitAll(
		t,
		repo,
		"advance HEAD",
		&git.CommitOptions{
			Author: &object.Signature{Name: "fixture", Email: "fixture@example.test", When: time.Unix(0, 0)},
		},
	)
	harness.MutateFile(t, filepath.Join(root, "flux/entry.yaml"), "tag: input-before", "tag: input-after")
	harness.MutateFile(t, filepath.Join(root, "inputs/patch"), "value: input-after", "value: dirty")
	loader := installSourceRefTransport(t, repo.Storer)
	for _, side := range []struct{ root, value string }{{original, "input-before"}, {root, "input-after"}} {
		out, _ := harness.RequireCLIOK(
			t,
			"build",
			"all",
			"--path",
			filepath.Join(side.root, "flux"),
			"--no-progress",
			"--concurrency",
			"2",
			"--cache-dir",
			t.TempDir(),
		)
		assert.FatalIf(
			t,
			!strings.Contains(out, "value: "+side.value) || strings.Contains(out, "value: dirty"),
			"pinned input read a different source view: %s",
			out,
		)
	}
	for _, baseline := range [][]string{{"--base", "HEAD"}, {"--path-orig", filepath.Join(original, "flux")}} {
		t.Run(baseline[0], func(t *testing.T) {
			args := append(
				[]string{
					"diff",
					"all",
					"-o",
					"diff",
					"--path",
					filepath.Join(root, "flux"),
					"--no-progress",
					"--concurrency",
					"2",
					"--cache-dir",
					t.TempDir(),
				},
				baseline...)
			out, _ := harness.RequireCLIOK(t, args...)
			assertSourceValueDiff(t, out, "input-before", "input-after")
		})
	}
	if loader.calls.Load() != 0 {
		t.Fatalf("local pinned input fetched remotely %d times", loader.calls.Load())
	}

	t.Run("supplied GitRepository", testSourceRefSuppliedGitRepository)
}

func testSourceRefSuppliedGitRepository(t *testing.T) {
	t.Helper()
	remote := t.TempDir()
	repo := testutil.GitInit(t, remote)
	testutil.WriteFile(t, remote, "app/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t,
		remote,
		"app/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: supplied-source-output, namespace: "+
			"apps}\ndata: {value: committed}\n")
	testutil.GitCommitAll(t,
		repo,
		"source",
		&git.CommitOptions{Author: &object.Signature{
			Name:  "fixture",
			Email: "fixture@example.test",
			When: time.Unix(0,
				0),
		}})
	root, inputs := t.TempDir(), t.TempDir()
	testutil.GitInit(t, root)
	testutil.WriteFile(t,
		root,
		"consumer.yaml",
		testutil.KustomizationYAML("consumer",
			"apps",
			"  path: ./app\n  sourceRef: {kind: GitRepository, name: remote}\n"))
	testutil.WriteFile(t,
		inputs,
		"source.yaml",
		"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: remote, namespace: apps}\n"+
			"spec: {url: 'file://"+filepath.ToSlash(remote)+"'}\n"+
			"---\napiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\n"+
			"metadata: {name: unused, namespace: apps}\nspec: {url: 'file:///absent-supplied-source'}\n")
	var previous string
	for range 2 {
		out, _ := harness.RequireCLIOK(t,
			"build",
			"all",
			"--path",
			root,
			"--supply",
			filepath.Join(inputs,
				"source.yaml"),
			"--concurrency=2",
			"--no-progress")
		if !strings.Contains(out,
			"name: supplied-source-output") ||
			strings.Contains(out,
				"kind: GitRepository") ||
			strings.Contains(out,
				"unused") {
			t.Fatalf("supplied source output: %s", out)
		}
		if previous != "" && previous != out {
			t.Fatal("nondeterministic supplied source output")
		}
		previous = out
	}
}
