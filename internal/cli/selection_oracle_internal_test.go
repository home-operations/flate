package cli

import (
	"encoding/json"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-git/v6"
	gitconfig "github.com/go-git/go-git/v6/config"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/testutil"
)

//nolint:paralleltest // CLI invocations replace process-wide logging.
func TestE2E_Selection_FullBuildOracle(t *testing.T) {
	for _, scope := range []struct {
		name, producer, namespace string
		command                   []string
	}{
		{name: "all", command: []string{"diff", "all"}},
		{name: "named", producer: "app", command: []string{"diff", "ks", "app"}},
		{name: "namespace", namespace: "apps", command: []string{"diff", "all", "--namespace=apps"}},
	} {
		t.Run(scope.name, func(t *testing.T) {
			before, after := t.TempDir(), t.TempDir()
			selectionOracleFixture(t, before, "before")
			selectionOracleFixture(t, after, "after")
			old, current := membershipBuild(t, before), membershipBuild(t, after)
			if old.code != 0 || current.code != 0 {
				t.Fatalf("full builds exit=%d/%d, want 0/0", old.code, current.code)
			}
			for _, snapshot := range []membershipSnapshot{old, current} {
				selectionOracleAttribution(t, snapshot.documents)
			}
			want := membershipDelta(
				selectionOracleProject(old.documents, scope.producer, scope.namespace),
				selectionOracleProject(current.documents, scope.producer, scope.namespace),
			)
			if len(want) == 0 {
				t.Fatal("fixture must change projected full output")
			}
			first := selectionOracleDiff(
				t,
				after,
				before,
				slices.Concat(scope.command, []string{"--cache-dir", t.TempDir()}),
			)
			if first.Code != 0 || len(first.Suppressed) != 0 {
				t.Errorf("narrow diff exit=%d suppressions=%+v", first.Code, first.Suppressed)
			}
			membershipCompare(t, want, first.Changes)
			second := selectionOracleDiff(
				t,
				after,
				before,
				slices.Concat(scope.command, []string{"--cache-dir", t.TempDir()}),
			)
			if difference := cmp.Diff(first, second); difference != "" {
				t.Errorf("independent cold replays differ (-want +got):\n%s", difference)
			}
		})
	}
}

//nolint:paralleltest // CLI invocations replace process-wide logging.
func TestE2E_Selection_WarmNamedOracle(t *testing.T) {
	before, after, cache := t.TempDir(), t.TempDir(), t.TempDir()
	selectionOracleFixture(t, before, "before")
	selectionOracleFixture(t, after, "after")
	old, current := membershipBuild(t, before), membershipBuild(t, after)
	if old.code != 0 || current.code != 0 {
		t.Fatalf("full builds exit=%d/%d, want 0/0", old.code, current.code)
	}
	want := membershipDelta(
		selectionOracleProject(old.documents, "app", ""),
		selectionOracleProject(current.documents, "app", ""),
	)
	selectionOracleDiff(t, after, before, []string{"diff", "all", "--cache-dir", cache})
	result := selectionOracleDiff(t, after, before, []string{"diff", "ks", "app", "--cache-dir", cache})
	if result.Code != 0 || len(result.Suppressed) != 0 {
		t.Errorf("warm named diff exit=%d suppressions=%+v", result.Code, result.Suppressed)
	}
	membershipCompare(t, want, result.Changes)
	args := []string{
		"get", "ks", "app", "--path", after, "--path-orig", before, "--cache-dir", cache,
		"--concurrency=2", "--no-progress", "-o", "json",
	}
	first := runCLI(t, args...)
	if first.code != 0 {
		t.Fatalf("warm named listing exit=%d stderr=%s", first.code, first.stderr)
	}
	var rows []struct {
		Name      string `json:"name"`
		Namespace string `json:"namespace"`
	}
	if err := json.Unmarshal([]byte(first.stdout), &rows); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Name != "app" || rows[0].Namespace != "apps" {
		t.Errorf("warm named listing rows=%+v, want apps/app only", rows)
	}
	second := runCLI(t, args...)
	if first != second {
		t.Errorf("warm named listing replays differ: first=%+v second=%+v", first, second)
	}
}

//nolint:paralleltest // CLI invocations replace process-wide logging.
func TestE2E_Selection_ResolvedRevisionOracle(t *testing.T) {
	before, after := t.TempDir(), t.TempDir()
	for _, side := range []struct{ root, value string }{{before, "before"}, {after, "after"}} {
		repo := testutil.GitInit(t, side.root)
		if _, err := repo.CreateRemote(
			&gitconfig.RemoteConfig{Name: "origin", URLs: []string{"git://oracle.invalid/repo"}},
		); err != nil {
			t.Fatal(err)
		}
		testutil.WriteFile(
			t,
			side.root,
			"flux/source.yaml",
			"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: pinned, namespace: apps}\nspec: {url: 'git://oracle.invalid/repo', ref: {tag: shared}, interval: 10m}\n",
		)
		testutil.WriteFile(
			t,
			side.root,
			"flux/app.yaml",
			testutil.KustomizationYAML(
				"app",
				"apps",
				"  path: ./payload\n  sourceRef: {kind: GitRepository, name: pinned}\n",
			),
		)
		testutil.WriteFile(t, side.root, "payload/kustomization.yaml", "namespace: apps\nresources: [output.yaml]\n")
		selectionOracleValue(t, side.root, side.value)
		hash := testutil.GitCommitAll(t, repo, "pinned", &git.CommitOptions{
			Author: &object.Signature{Name: "fixture", Email: "fixture@example.test", When: time.Unix(0, 0)},
		})
		if err := repo.Storer.SetReference(plumbing.NewHashReference("refs/tags/shared", hash)); err != nil {
			t.Fatal(err)
		}
		selectionOracleValue(t, side.root, "working-tree")
		testutil.GitCommitAll(t, repo, "checkout", &git.CommitOptions{
			Author: &object.Signature{Name: "fixture", Email: "fixture@example.test", When: time.Unix(1, 0)},
		})
	}
	old := membershipBuild(t, filepath.Join(before, "flux"))
	current := membershipBuild(t, filepath.Join(after, "flux"))
	if old.code != 0 || current.code != 0 {
		t.Fatalf("independent pinned full builds exit=%d/%d, want 0/0", old.code, current.code)
	}
	if len(old.documents) != 1 || len(current.documents) != 1 {
		t.Fatalf("independent pinned full output counts=%d/%d, want 1/1", len(old.documents), len(current.documents))
	}
	want := membershipDelta(old.documents, current.documents)
	if len(want) != 2 || !strings.Contains(old.documents[0].Body, `"value":"before"`) ||
		!strings.Contains(current.documents[0].Body, `"value":"after"`) {
		t.Fatalf(
			"separate object stores did not produce distinct full values: old=%+v current=%+v",
			old.documents,
			current.documents,
		)
	}
	result := selectionOracleDiff(
		t,
		filepath.Join(after, "flux"),
		filepath.Join(before, "flux"),
		[]string{"diff", "all", "--cache-dir", t.TempDir()},
	)
	if result.Code != 0 || len(result.Suppressed) != 0 {
		t.Errorf("resolved revision diff exit=%d suppressions=%+v", result.Code, result.Suppressed)
	}
	membershipCompare(t, want, result.Changes)
}

func selectionOracleFixture(t *testing.T, root, value string) {
	t.Helper()
	for _, producer := range []struct{ name, namespace string }{{"app", "apps"}, {"unrelated", "other"}} {
		testutil.WriteFile(t, root, "flux/"+producer.name+".yaml", testutil.KustomizationYAML(
			producer.name, producer.namespace, "  path: ./"+producer.name+"\n"))
		testutil.WriteFile(
			t,
			root,
			producer.name+"/kustomization.yaml",
			"namespace: "+producer.namespace+"\nresources: [output.yaml]\n",
		)
		testutil.WriteFile(
			t,
			root,
			producer.name+"/output.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: shared, uid: retained, annotations: {'checksum/oracle': "+value+"}}\ndata: {value: unchanged}\n",
		)
	}
}

func selectionOracleAttribution(t *testing.T, documents []membershipDocument) {
	t.Helper()
	want := map[membershipParent]int{
		{Group: "kustomize.toolkit.fluxcd.io", Kind: "Kustomization", Namespace: "apps", Name: "app"}:        1,
		{Group: "kustomize.toolkit.fluxcd.io", Kind: "Kustomization", Namespace: "other", Name: "unrelated"}: 1,
	}
	got := make(map[membershipParent]int)
	for _, doc := range documents {
		got[doc.Parent]++
	}
	if difference := cmp.Diff(want, got); difference != "" {
		t.Fatalf("fixture producer attribution (-want +got):\n%s", difference)
	}
}

func selectionOracleProject(documents []membershipDocument, name, namespace string) []membershipDocument {
	var result []membershipDocument
	for _, doc := range documents {
		if (name == "" || doc.Parent.Name == name) && (namespace == "" || doc.Parent.Namespace == namespace) {
			result = append(result, doc)
		}
	}
	return result
}

func selectionOracleDiff(t *testing.T, current, before string, command []string) membershipResult {
	t.Helper()
	args := slices.Concat(command, []string{
		"--path", current, "--path-orig", before,
		"--concurrency=2", "--no-progress", "--strip-attr=flate.test/oracle-unused", "-o", "json",
	})
	result := runCLI(t, args...)
	if strings.Contains(result.stderr, "no changes detected") {
		t.Errorf("changed full output produced a no-changes warning: %s", result.stderr)
	}
	var output membershipResult
	if err := json.Unmarshal([]byte(result.stdout), &output); err != nil {
		t.Fatalf("diff JSON: %v, stdout=%s stderr=%s", err, result.stdout, result.stderr)
	}
	output.Code = result.code
	return output
}

func selectionOracleValue(t *testing.T, root, value string) {
	t.Helper()
	testutil.WriteFile(
		t,
		root,
		"payload/output.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output}\ndata: {value: "+value+"}\n",
	)
}
