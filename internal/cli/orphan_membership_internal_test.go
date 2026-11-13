package cli

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/testutil"
)

type membershipParent struct {
	Group     string `json:"Group"`
	Kind      string `json:"Kind"`
	Namespace string `json:"Namespace"`
	Name      string `json:"Name"`
}

type membershipDocument struct {
	Parent membershipParent
	Body   string
}

type membershipChange struct {
	Parent     membershipParent `json:"Parent"`
	APIVersion string           `json:"APIVersion"`
	Kind       string           `json:"Kind"`
	Namespace  string           `json:"Namespace"`
	Name       string           `json:"Name"`
	Status     string           `json:"Status"`
	Old        map[string]any   `json:"Old"`
	New        map[string]any   `json:"New"`
}

type membershipSuppression struct {
	Parent membershipParent `json:"Parent"`
	Side   string           `json:"Side"`
	Count  int              `json:"Count"`
	Reason string           `json:"Reason"`
}

type membershipSnapshot struct {
	documents []membershipDocument
	code      int
}

type membershipResult struct {
	Code       int                     `json:"-"`
	Changes    []membershipChange      `json:"Changes"`
	Suppressed []membershipSuppression `json:"Suppressed"`
}

//nolint:paralleltest // CLI invocations replace process-wide logging.
func TestE2E_OrphanMembership_FullBuildDelta(t *testing.T) {
	for _, scenario := range []string{
		"indexed winner", "indexed loser", "indexed document", "namespace siblings", "standalone Secret", "Secret siblings",
		"graph-owned Secret", "bare encrypted input", "omitted encrypted input",
		"graph directory", "graph direct file", "graph explicit namespace", "omitted mixed file",
		"addition", "deletion", "file move", "namespace move", "same identity different producers",
	} {
		t.Run(scenario, func(t *testing.T) {
			testMembershipFullBuildDelta(t, scenario)
		})
	}
}

//nolint:paralleltest // CLI invocations replace process-wide logging.
func TestE2E_OrphanMembership_SecretProducerPrecedence(t *testing.T) {
	before, after := t.TempDir(), t.TempDir()
	membershipFixture(t, before, "preexisting producer", false)
	membershipFixture(t, after, "preexisting producer", true)
	oldDocsSnapshot := membershipBuild(t, before)
	oldDocs, oldCode := oldDocsSnapshot.documents, oldDocsSnapshot.code
	newDocsSnapshot := membershipBuild(t, after)
	newDocs, newCode := newDocsSnapshot.documents, newDocsSnapshot.code
	if oldCode != 0 || newCode != 0 {
		t.Fatalf("full builds: orig=%d current=%d", oldCode, newCode)
	}
	membershipConsumerValue(t, oldDocs, "fallback")
	membershipConsumerValue(t, newDocs, "fallback")
	result := membershipDiff(t, after, []string{"--path-orig", before})
	code := result.Code
	if code != 0 || len(result.Suppressed) != 0 {
		t.Errorf("diff exit=%d suppressions=%+v", code, result.Suppressed)
	}
	membershipCompare(t, membershipDelta(oldDocs, newDocs), result.Changes)
}

//nolint:paralleltest // CLI invocations replace process-wide logging.
func TestE2E_OrphanMembership_UnreadableInput(t *testing.T) {
	before, after := t.TempDir(), t.TempDir()
	membershipFixture(t, before, "bare encrypted input", false)
	membershipFixture(t, after, "bare encrypted input", true)
	oldDocsSnapshot := membershipBuild(t, before)
	oldDocs, oldCode := oldDocsSnapshot.documents, oldDocsSnapshot.code
	newDocsSnapshot := membershipBuild(t, after)
	newDocs, newCode := newDocsSnapshot.documents, newDocsSnapshot.code
	if oldCode != 0 || newCode != 0 {
		t.Fatalf("full builds: orig=%d current=%d", oldCode, newCode)
	}
	membershipConsumerValue(t, oldDocs, "fallback")
	membershipConsumerValue(t, newDocs, "fallback")
	result := membershipDiff(t, after, []string{"--path-orig", before})
	code := result.Code
	if code != 1 || len(result.Suppressed) != 1 ||
		!strings.Contains(result.Suppressed[0].Reason, "Secret/apps/settings") {
		t.Fatalf("unreadable required input: exit=%d suppressions=%+v", code, result.Suppressed)
	}
}

//nolint:paralleltest // CLI invocations replace process-wide logging.
func TestE2E_OrphanMembership_UnavailableProducer(t *testing.T) {
	for _, side := range []string{"orig", "current"} {
		t.Run(side, func(t *testing.T) {
			testMembershipUnavailableProducer(t, side)
		})
	}
}

//nolint:paralleltest // CLI invocations replace process-wide logging.
func TestE2E_OrphanMembership_RequiredInputAcrossScope(t *testing.T) {
	before, after, invalid := t.TempDir(), t.TempDir(), t.TempDir()
	for _, side := range []struct{ root, value string }{{before, "before"}, {after, "after"}, {invalid, "after, bad-key: invalid"}} {
		testutil.WriteFile(
			t,
			side.root,
			"flux/producer.yaml",
			testutil.KustomizationYAML(
				"producer",
				"infra",
				"  path: ./release\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n  postBuild: {substituteFrom: [{kind: ConfigMap, name: settings}]}\n",
			),
		)
		testutil.WriteFile(
			t,
			side.root,
			"settings.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: infra}\ndata: {VALUE: "+side.value+"}\n",
		)
		testutil.WriteFile(t, side.root, "release/kustomization.yaml", "resources: [release.yaml]\n")
		testutil.WriteFile(
			t,
			side.root,
			"release/release.yaml",
			"apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: consumer, namespace: apps}\nspec:\n  chart:\n    spec:\n      chart: charts/demo\n      sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n  values: {value: '${VALUE}'}\n",
		)
		testutil.WriteFile(t, side.root, "charts/demo/Chart.yaml", "apiVersion: v2\nname: demo\nversion: 1.0.0\n")
		testutil.WriteFile(
			t,
			side.root,
			"charts/demo/templates/output.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output, namespace: apps}\ndata: {value: {{ .Values.value | quote }}}\n",
		)
	}
	oldDocsSnapshot := membershipBuild(t, before, "--namespace=apps")
	oldDocs, oldCode := oldDocsSnapshot.documents, oldDocsSnapshot.code
	newDocsSnapshot := membershipBuild(t, after, "--namespace=apps")
	newDocs, newCode := newDocsSnapshot.documents, newDocsSnapshot.code
	if oldCode != 0 || newCode != 0 || len(oldDocs) != 2 || len(newDocs) != 2 {
		t.Fatalf(
			"cross-namespace full builds: codes=%d/%d documents=%d/%d",
			oldCode,
			newCode,
			len(oldDocs),
			len(newDocs),
		)
	}
	result := membershipDiff(t, after, []string{"--path-orig", before, "--namespace=apps"})
	code := result.Code
	if code != 0 || len(result.Suppressed) != 0 {
		t.Errorf("cross-namespace diff exit=%d suppressions=%+v", code, result.Suppressed)
	}
	membershipCompare(t, membershipDelta(oldDocs, newDocs), result.Changes)
	code = membershipBuild(t, invalid).code
	if code != 1 {
		t.Errorf("unrestricted invalid-input control exit=%d, want 1", code)
	}
	code = membershipBuild(t, invalid, "--namespace=apps").code
	if code != 1 {
		t.Errorf("invalid required input exit=%d, want 1", code)
	}
	result = membershipDiff(t, invalid, []string{"--path-orig", before, "--namespace=apps"})
	code = result.Code
	if code != 1 {
		t.Errorf("invalid required input diff exit=%d, want 1", code)
	}
	if len(result.Changes) != 0 {
		t.Errorf("failed cross-namespace producer leaked changes: %+v", result.Changes)
	}
	wantCounts := map[membershipParent]int{}
	for _, doc := range oldDocs {
		wantCounts[doc.Parent]++
	}
	gotCounts := map[membershipParent]int{}
	for _, note := range result.Suppressed {
		if note.Side != "current" {
			t.Errorf("suppression side=%q, want current", note.Side)
		}
		gotCounts[note.Parent] += note.Count
	}
	if difference := cmp.Diff(wantCounts, gotCounts); difference != "" {
		t.Errorf("suppressed producers and withheld counts (-want +got): %s", difference)
	}
}

func membershipConsumerValue(t *testing.T, docs []membershipDocument, want string) {
	t.Helper()
	found := 0
	for _, doc := range docs {
		if doc.Parent.Name != "consumer" {
			continue
		}
		var value struct {
			Kind     string `json:"kind"`
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
			Data map[string]string `json:"data"`
		}
		if err := json.Unmarshal([]byte(doc.Body), &value); err != nil {
			t.Fatal(err)
		}
		if value.Kind != "ConfigMap" || value.Metadata.Name != "output" {
			continue
		}
		found++
		if value.Data["value"] != want {
			t.Errorf("full-build consumer value=%q, want %q", value.Data["value"], want)
		}
	}
	if found != 1 {
		t.Errorf("full-build consumer outputs=%d, want 1", found)
	}
}

func membershipBuild(t *testing.T, root string, flags ...string) membershipSnapshot {
	t.Helper()
	args := slices.Concat([]string{
		"build",
		"all",
		"--path",
		root,
		"--cache-dir",
		t.TempDir(),
		"--concurrency=2",
		"--no-progress",
		"-o",
		"json",
	}, flags)
	resultCLI := runCLI(t, args...)
	out, stderr, code := resultCLI.stdout, resultCLI.stderr, resultCLI.code
	if code != 0 {
		t.Logf("full build exit=%d stderr=%s", code, stderr)
	}
	var docs []map[string]any
	if err := json.Unmarshal([]byte(out), &docs); err != nil {
		t.Fatalf("full build JSON: %v, stdout=%s stderr=%s", err, out, stderr)
	}
	result := make([]membershipDocument, 0, len(docs))
	for _, doc := range docs {
		metadata, _ := doc["metadata"].(map[string]any)
		labels, _ := metadata["labels"].(map[string]any)
		parent := membershipParent{}
		for _, producer := range []struct{ prefix, group, kind string }{
			{"helm.toolkit.fluxcd.io", "helm.toolkit.fluxcd.io", "HelmRelease"},
			{"kustomize.toolkit.fluxcd.io", "kustomize.toolkit.fluxcd.io", "Kustomization"},
		} {
			if name, _ := labels[producer.prefix+"/name"].(string); name != "" {
				parent = membershipParent{Group: producer.group, Kind: producer.kind, Name: name}
				parent.Namespace, _ = labels[producer.prefix+"/namespace"].(string)
				break
			}
		}
		if parent.Kind == "" {
			t.Fatalf("fixture output has no producer association: %+v", doc)
		}
		body, err := json.Marshal(doc)
		if err != nil {
			t.Fatal(err)
		}
		result = append(result, membershipDocument{Parent: parent, Body: string(body)})
	}
	return membershipSnapshot{documents: result, code: code}
}

func membershipDiff(t *testing.T, root string, baseline []string) membershipResult {
	t.Helper()
	args := slices.Concat([]string{
		"diff",
		"all",
		"--path",
		root,
		"--cache-dir",
		t.TempDir(),
		"--concurrency=2",
		"--no-progress",
		"-o",
		"json",
		"--strip-attr=flate.test/oracle-unused",
	}, baseline)
	resultCLI := runCLI(t, args...)
	out, stderr, code := resultCLI.stdout, resultCLI.stderr, resultCLI.code
	var result membershipResult
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("diff JSON: %v, stdout=%s stderr=%s", err, out, stderr)
	}
	result.Code = code
	return result
}

func membershipDelta(before, after []membershipDocument) map[membershipDocument]int {
	result := map[membershipDocument]int{}
	for _, doc := range before {
		result[doc]--
	}
	for _, doc := range after {
		result[doc]++
	}
	for doc, count := range result {
		if count == 0 {
			delete(result, doc)
		}
	}
	return result
}

func membershipCompare(t *testing.T, want map[membershipDocument]int, changes []membershipChange) {
	t.Helper()
	got := map[membershipDocument]int{}
	for i := range changes {
		membershipCompareChange(t, &changes[i], got)
	}
	for doc, count := range got {
		if count == 0 {
			delete(got, doc)
		}
	}
	if difference := cmp.Diff(want, got); difference != "" {
		t.Errorf("independent full-build multiset delta (-want +got):\n%s", difference)
	}
}

func testMembershipFullBuildDelta(t *testing.T, scenario string) {
	t.Helper()
	before, after := t.TempDir(), t.TempDir()
	membershipFixture(t, before, scenario, false)
	membershipFixture(t, after, scenario, true)
	flags := make([]string, 0, 2)
	if scenario == "bare encrypted input" {
		flags = []string{
			"--supply",
			substitutionInput(
				t,
				"apiVersion: v1\nkind: Secret\nmetadata: {name: settings, namespace: apps}\nstringData: {VALUE: supplied}\n",
			),
		}
	}
	oldDocsSnapshot := membershipBuild(t, before, flags...)
	oldDocs := oldDocsSnapshot.documents
	newDocsSnapshot := membershipBuild(t, after, flags...)
	newDocs := newDocsSnapshot.documents
	membershipFullBuildValues(t, scenario, oldDocsSnapshot, newDocsSnapshot)
	want := membershipDelta(oldDocs, newDocs)
	result := membershipDiff(t, after, append(flags, "--path-orig", before))
	code := result.Code
	if code != 0 {
		t.Errorf("diff exit=%d, want 0", code)
	}
	if len(result.Suppressed) != 0 {
		t.Errorf("unexpected suppression: %+v", result.Suppressed)
	}
	membershipCompare(t, want, result.Changes)
	if scenario == "indexed winner" || scenario == "standalone Secret" {
		again := membershipDiff(t, after, []string{"--path-orig", before})
		status := again.Code
		if status != code || cmp.Diff(result, again) != "" {
			t.Errorf("independent cold runs differ (-want +got): %s", cmp.Diff(result, again))
		}
	}
	if !slices.Contains([]string{"addition", "deletion", "file move", "namespace move"}, scenario) {
		return
	}
	cold := t.TempDir()
	repo := testutil.GitInit(t, cold)
	testutil.CopyTree(t, before, cold)
	testutil.GitCommitAll(
		t,
		repo,
		"before",
		&git.CommitOptions{
			Author: &object.Signature{Name: "test", Email: "test@example.test", When: time.Unix(0, 0)},
		},
	)
	if err := os.RemoveAll(filepath.Join(cold, "settings")); err != nil {
		t.Fatal(err)
	}
	testutil.CopyTree(t, after, cold)
	result = membershipDiff(t, cold, []string{"--base", "HEAD"})
	code = result.Code
	if code != 0 {
		t.Errorf("cold Git diff exit=%d, want 0", code)
	}
	if len(result.Suppressed) != 0 {
		t.Errorf("cold Git suppression: %+v", result.Suppressed)
	}
	membershipCompare(t, want, result.Changes)
}

func membershipFullBuildValues(t *testing.T, scenario string, old, current membershipSnapshot) {
	t.Helper()
	oldDocs, newDocs, oldCode, newCode := old.documents, current.documents, old.code, current.code
	fullStatus := 0
	if scenario == "omitted encrypted input" {
		fullStatus = 1
	}
	if oldCode != fullStatus || newCode != fullStatus {
		t.Errorf("full builds: orig=%d current=%d, want %d", oldCode, newCode, fullStatus)
	}
	wantValues := [2]string{"before", "after"}
	switch scenario {
	case "indexed loser":
		wantValues = [2]string{"winner", "winner"}
	case "file move",
		"graph directory",
		"graph direct file",
		"graph explicit namespace",
		"omitted mixed file",
		"indexed document":
		wantValues = [2]string{"stable", "stable"}
	case "addition":
		wantValues[0] = "fallback"
	case "deletion", "namespace move":
		wantValues[1] = "fallback"
	case "bare encrypted input":
		wantValues = [2]string{"supplied", "supplied"}
	}
	if fullStatus == 0 {
		membershipConsumerValue(t, oldDocs, wantValues[0])
		membershipConsumerValue(t, newDocs, wantValues[1])
	}
	if scenario == "omitted mixed file" {
		for _, doc := range append(oldDocs, newDocs...) {
			if strings.Contains(doc.Body, "child-output") || strings.Contains(doc.Body, "excluded") {
				t.Errorf("omitted sibling entered full output: %s", doc.Body)
			}
		}
	}
}

func testMembershipUnavailableProducer(t *testing.T, side string) {
	t.Helper()
	before, after := t.TempDir(), t.TempDir()
	membershipFixture(t, before, "standalone Secret", false)
	membershipFixture(t, after, "standalone Secret", true)
	failed := before
	if side == "current" {
		failed = after
	}
	if err := os.Remove(filepath.Join(failed, "settings", "input.yaml")); err != nil {
		t.Fatal(err)
	}
	oldDocsSnapshot := membershipBuild(t, before)
	oldDocs, oldCode := oldDocsSnapshot.documents, oldDocsSnapshot.code
	newDocsSnapshot := membershipBuild(t, after)
	newDocs, newCode := newDocsSnapshot.documents, newDocsSnapshot.code
	wantOld, wantNew := 1, 0
	if side == "current" {
		wantOld, wantNew = 0, 1
	}
	if oldCode != wantOld || newCode != wantNew {
		t.Errorf("full status orig=%d current=%d, want %d/%d", oldCode, newCode, wantOld, wantNew)
	}
	healthy := oldDocs
	if side == "orig" {
		healthy = newDocs
	}
	result := membershipDiff(t, after, []string{"--path-orig", before})
	code := result.Code
	if code != 1 {
		t.Errorf("diff status=%d, want 1", code)
	}
	if len(result.Changes) != 0 {
		t.Errorf("unavailable producer leaked changes: %+v", result.Changes)
	}
	if len(result.Suppressed) != 1 {
		t.Fatalf("suppression count=%d, want 1", len(result.Suppressed))
	}
	membershipSuppressionMatches(t, &result.Suppressed[0], side, healthy)
}

func membershipSuppressionMatches(
	t *testing.T,
	note *membershipSuppression,
	side string,
	healthy []membershipDocument,
) {
	t.Helper()
	if note.Side != side {
		t.Errorf("suppression side=%q, want %q", note.Side, side)
	}
	parent := membershipParent{
		Group:     "kustomize.toolkit.fluxcd.io",
		Kind:      "Kustomization",
		Namespace: "apps",
		Name:      "consumer",
	}
	if note.Parent != parent {
		t.Errorf("suppressed producer=%+v, want %+v", note.Parent, parent)
	}
	count := 0
	for _, doc := range healthy {
		if doc.Parent == parent {
			count++
		}
	}
	if count == 0 || note.Count != count {
		t.Errorf("withheld=%d, want healthy full-build count %d", note.Count, count)
	}
	if note.Reason == "" {
		t.Error("suppression lost the failure reason")
	}
}

func membershipCompareChange(t *testing.T, change *membershipChange, got map[membershipDocument]int) {
	t.Helper()
	if cmp.Equal(change.Old, change.New) {
		t.Errorf("unchanged document reported as %s by %+v", change.Status, change.Parent)
	}
	wantStatus := "changed"
	if change.Old == nil {
		wantStatus = "added"
	} else if change.New == nil {
		wantStatus = "removed"
	}
	if change.Status != wantStatus {
		t.Errorf("status=%q, want %q for %+v", change.Status, wantStatus, change.Parent)
	}
	for side, doc := range []map[string]any{change.Old, change.New} {
		if doc == nil {
			continue
		}
		metadata, _ := doc["metadata"].(map[string]any)
		name, _ := metadata["name"].(string)
		namespace, _ := metadata["namespace"].(string)
		if change.APIVersion != doc["apiVersion"] || change.Kind != doc["kind"] || change.Name != name ||
			change.Namespace != namespace {
			t.Errorf("change envelope lost full identity: %+v", change)
		}
		body, err := json.Marshal(doc)
		if err != nil {
			t.Fatal(err)
		}
		got[membershipDocument{Parent: change.Parent, Body: string(body)}] += 2*side - 1
	}
}
