package scope_test

import (
	"encoding/json"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

func e2EDisabledAppDiffSidesMembershipNamespaceAuthority(t *testing.T) {
	t.Helper()

	before, payload, shifted := t.TempDir(), t.TempDir(), t.TempDir()
	const entry = "kube/clusters/main/flux"
	for _, tc := range []struct{ root, namespace, value string }{
		{before, "media", "original"},
		{payload, "media", "changed"},
		{shifted, "shifted", "original"},
	} {
		testutil.GitInit(t, tc.root)
		disabledAppFixture(t, tc.root, "nested", true)
		testutil.WriteFile(t, tc.root, entry+"/kustomization.yaml", "resources: [sync.yaml, external.yaml]\n")
		testutil.WriteFile(
			t,
			tc.root,
			entry+"/sync.yaml",
			testutil.KustomizationYAML(
				"sync",
				"flux-system",
				"  path: ./kube/deploy/app\n  targetNamespace: "+tc.namespace+"\n",
			),
		)
		testutil.WriteFile(
			t,
			tc.root,
			entry+"/external.yaml",
			"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: external, namespace: flux-system}\nspec:\n  provider: github\n  url: https://fixture.invalid/external\n  secretRef: {name: unavailable-auth}\n---\n"+testutil.KustomizationYAML(
				"external-sync",
				"flux-system",
				"  path: ./kube/deploy/app\n  sourceRef: {kind: GitRepository, name: external}\n",
			),
		)
		testutil.WriteFile(
			t,
			tc.root,
			"kube/deploy/app/kustomization.yaml",
			"namespace: inner\nresources: [ks.yaml]\n# - commented.yaml\n",
		)
		testutil.WriteFile(
			t,
			tc.root,
			"payload/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: reachable-cm, namespace: apps}\ndata: {value: "+tc.value+"}\n",
		)
	}
	checkDiagnostic := func(t *testing.T, stderr string, skipped bool) {
		t.Helper()
		e2EDisabledAppDiffSidesMembershipNamespaceAuthorityBody(t, stderr, skipped)
	}
	checkBuild := func(t *testing.T, out, namespace, value string) {
		t.Helper()
		e2EDisabledAppDiffSidesMembershipNamespaceAuthorityBody2(t, out, namespace, value)
	}
	flags := []string{
		"--allow-missing-kinds=Secret,ConfigMap",
		"--log-level",
		"info",
		"--concurrency",
		"2",
		"--no-progress",
		"--cache-dir",
		t.TempDir(),
	}
	fullArgs := append([]string{"build", "all", "--path", before}, flags...)
	full, stderr := harness.RequireCLIOK(t, fullArgs...)
	checkDiagnostic(t, stderr, true)
	checkBuild(t, full, "media", "original")
	again, stderr := harness.RequireCLIOK(t, fullArgs...)
	checkDiagnostic(t, stderr, true)
	assert.Equal(t, again, full)
	for _, tc := range []struct{ name, current, original, namespace, oldNamespace, value string }{
		{"payload", payload, before, "media", "media", "changed"},
		{"namespace forward", shifted, before, "shifted", "media", "original"},
		{"namespace reverse", before, shifted, "media", "shifted", "original"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e2EDisabledAppDiffSidesMembershipNamespaceAuthorityName2(t, &tc, &flags, struct {
				checkDiagnostic *func(t *testing.T, stderr string, skipped bool)
				checkBuild      *func(t *testing.T, out string, namespace string, value string)
			}{checkDiagnostic: &checkDiagnostic, checkBuild: &checkBuild})
		})
	}
}

func e2EDisabledAppDiffSidesCase(t *testing.T, scenario *string) {
	t.Helper()

	current, original := t.TempDir(), t.TempDir()
	for _, root := range []string{current, original} {
		testutil.GitInit(t, root)
	}
	currentEnabled, originalEnabled := (*scenario) != "remove", (*scenario) != "restore"
	disabledAppFixture(t, current, "nested", currentEnabled)
	disabledAppFixture(t, original, "nested", originalEnabled)
	if (*scenario) == "payload" {
		testutil.WriteFile(
			t,
			current,
			"payload/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: reachable-cm, namespace: apps}\ndata: {value: changed}\n",
		)
	}
	for _, command := range []string{"build", "diff"} {
		args := []string{
			command,
			"all",
			"--path",
			current,
			"--path-orig",
			original,
			"--allow-missing-kinds=Secret,ConfigMap",
			"--concurrency",
			"2",
			"--no-progress",
			"--cache-dir",
			t.TempDir(),
		}
		if command == "diff" {
			args = append(args, "-o", "json")
		}
		out, stderr, code := harness.RunCLIBuffers(args...)
		assert.FatalIf(
			t,
			code != 0 || strings.Contains(stderr, "dial tcp"),
			"nested %s exit=%d output=%s stderr=%s",
			command,
			code,
			out,
			stderr,
		)
		for _, excluded := range []string{"excluded-cm", "commented-cm", "excluded-app", "commented-app"} {
			assert.FatalIf(
				t,
				strings.Contains(out, excluded),
				"excluded %s appeared in %s: %s",
				excluded,
				command,
				out,
			)
		}
		if command == "build" {
			assert.FatalIf(
				t,
				strings.Contains(out, "name: reachable-cm") != currentEnabled,
				"changed-only current tree selection: %s",
				out,
			)
			assert.FatalIf(
				t,
				(*scenario) == "payload" && !strings.Contains(out, "value: changed"),
				"changed payload not selected: %s",
				out,
			)
			continue
		}
		var result struct {
			Changes []struct {
				Kind   string `json:"Kind"`
				Name   string `json:"Name"`
				Status string `json:"Status"`
				Parent struct {
					Kind      string `json:"Kind"`
					Name      string `json:"Name"`
					Namespace string `json:"Namespace"`
				} `json:"Parent"`
			} `json:"Changes"`
			Suppressed []any `json:"Suppressed"`
		}
		assert.NoError(t, json.Unmarshal([]byte(out), &result))
		assert.FatalIf(t, len(result.Suppressed) != 0, "nested diff suppressed input: %s", out)
		found := 0
		want := map[string]string{"remove": "removed", "restore": "added", "payload": "changed"}[(*scenario)]
		for _, change := range result.Changes {
			if change.Kind != "ConfigMap" || change.Name != "reachable-cm" {
				continue
			}
			assert.FatalIf(
				t,
				change.Status != want || change.Parent.Kind != "Kustomization" || change.Parent.Name != "app" ||
					change.Parent.Namespace != "flux-system",
				"nested diff lost identity/parent: %s",
				out,
			)
			found++
		}
		assert.FatalIf(t, found != 1, "expected one %s reachable payload: %s", want, out)
	}
}

func e2EDisabledAppDiffSidesName(t *testing.T, enabled *bool) {
	t.Helper()

	current, original := t.TempDir(), t.TempDir()
	for _, root := range []string{current, original} {
		testutil.GitInit(t, root)
	}
	disabledAppFixture(t, current, "multi", (*enabled))
	disabledAppFixture(t, original, "multi", !(*enabled))
	for _, command := range []string{"build", "diff"} {
		args := []string{
			command,
			"all",
			"--path",
			current,
			"--path-orig",
			original,
			"--concurrency",
			"2",
			"--no-progress",
		}
		if command == "diff" {
			args = append(args, "-o", "diff")
		}
		out, stderr, code := harness.RunCLIBuffers(args...)
		assert.FatalIf(
			t,
			code != 0 || strings.Contains(stderr, "[OrphanedResource]"),
			"%s exit=%d stderr=%s",
			command,
			code,
			stderr,
		)
		for _, resource := range []string{"game-cm", "backup-cm"} {
			want := (*enabled) || command == "diff"
			assert.FatalIf(
				t,
				strings.Contains(out, resource) != want,
				"%s %s enabled=%t output=%s",
				command,
				resource,
				(*enabled),
				out,
			)
		}
	}
}

func e2EEntryPrerequisiteDiffSidesName(t *testing.T, direction *struct {
	name     string
	current  string
	original string
	value    string
},
) {
	t.Helper()

	e2EEntryPrerequisiteDiffSidesNameLoop(t, struct {
		direction **struct {
			name     string
			current  string
			original string
			value    string
		}
	}{direction: &direction})
}

func e2EDisabledAppDiffSidesMembershipNamespaceAuthorityName(t *testing.T, tc *struct {
	name         string
	current      string
	original     string
	namespace    string
	oldNamespace string
	value        string
}, flags *[]string, capturedInputs struct {
	checkDiagnostic *func(t *testing.T, stderr string, skipped bool)
	checkBuild      *func(t *testing.T, out string, namespace string, value string)
},
) {
	t.Helper()

	args := append([]string{"build", "all", "--path", tc.current, "--path-orig", tc.original}, (*flags)...)
	out, stderr := harness.RequireCLIOK(t, args...)
	(*capturedInputs.checkDiagnostic)(t, stderr, false)
	(*capturedInputs.checkBuild)(t, out, tc.namespace, tc.value)
	args[0] = "diff"
	args = append(args, "-o", "json")
	out, stderr = harness.RequireCLIOK(t, args...)
	(*capturedInputs.checkDiagnostic)(t, stderr, false)
	var result struct {
		Changes []struct {
			Kind      string `json:"Kind"`
			Name      string `json:"Name"`
			Namespace string `json:"Namespace"`
			Status    string `json:"Status"`
			Parent    struct {
				Kind      string `json:"Kind"`
				Name      string `json:"Name"`
				Namespace string `json:"Namespace"`
			} `json:"Parent"`
		} `json:"Changes"`
		Suppressed []any `json:"Suppressed"`
	}
	assert.NoError(t, json.Unmarshal([]byte(out), &result))
	assert.Equal(t, len(result.Suppressed), 0)
	var changes []string
	for _, change := range result.Changes {
		assert.FatalIf(t, change.Namespace == "inner" || change.Parent.Namespace == "inner" ||
			strings.Contains(change.Name, "excluded") ||
			strings.Contains(change.Name, "commented"), "excluded identity in diff: %s", out)
		if change.Kind != "ConfigMap" || change.Name != "reachable-cm" {
			continue
		}
		assert.Equal(t, change.Parent.Kind, "Kustomization")
		assert.Equal(t, change.Parent.Name, "app")
		assert.Equal(t, change.Namespace, "apps")
		changes = append(changes, change.Parent.Namespace+"/"+change.Status)
	}
	want := []string{tc.namespace + "/changed"}
	if tc.namespace != tc.oldNamespace {
		want = []string{tc.namespace + "/added", tc.oldNamespace + "/removed"}
	}
	slices.Sort(changes)
	slices.Sort(want)
	assert.Diff(t, changes, want)
}

func e2EDisabledAppDiffSidesMembershipNamespaceAuthorityName2(t *testing.T, tc *struct {
	name         string
	current      string
	original     string
	namespace    string
	oldNamespace string
	value        string
}, flags *[]string, capturedInputs struct {
	checkDiagnostic *func(t *testing.T, stderr string, skipped bool)
	checkBuild      *func(t *testing.T, out string, namespace string, value string)
},
) {
	t.Helper()

	e2EDisabledAppDiffSidesMembershipNamespaceAuthorityName(t, tc, flags, struct {
		checkDiagnostic *func(t *testing.T, stderr string, skipped bool)
		checkBuild      *func(t *testing.T, out string, namespace string, value string)
	}{checkDiagnostic: capturedInputs.checkDiagnostic, checkBuild: capturedInputs.checkBuild})
}

func e2EEntryPrerequisiteDiffSidesNameLoop(t *testing.T, loopInputs struct {
	direction **struct {
		name     string
		current  string
		original string
		value    string
	}
},
) {
	t.Helper()

	for _, command := range []string{"build", "diff"} {
		args := []string{
			command,
			"all",
			"--path",
			filepath.Join((*loopInputs.direction).current, "entry"),
			"--path-orig",
			filepath.Join((*loopInputs.direction).original, "entry"),
			"--concurrency",
			"2",
			"--no-progress",
			"--cache-dir",
			t.TempDir(),
		}
		if command == "diff" {
			args = append(args, "-o", "json")
		}
		out, _ := harness.RequireCLIOK(t, args...)
		again, _ := harness.RequireCLIOK(t, args...)
		assert.Equal(t, again, out)
		if command == "build" {
			docs, err := testutil.SplitDocs([]byte(out))
			assert.NoError(t, err)
			found := map[string]bool{}
			e2EEntryPrerequisiteDiffSidesNameLoopLoop(t, struct {
				docs       *[]map[string]any
				loopInputs *struct {
					direction **struct {
						name     string
						current  string
						original string
						value    string
					}
				}
				found *map[string]bool
			}{docs: &docs, loopInputs: &loopInputs, found: &found})
			assert.FatalIf(t, len(found) != 2, "changed-only entry/prerequisite output missing: %s", out)
			continue
		}
		var result struct {
			Changes []struct {
				Kind   string `json:"Kind"`
				Name   string `json:"Name"`
				Status string `json:"Status"`
				Parent struct {
					Name string `json:"Name"`
				} `json:"Parent"`
			} `json:"Changes"`
		}
		assert.NoError(t, json.Unmarshal([]byte(out), &result))
		found := map[string]bool{}
		for _, change := range result.Changes {
			if change.Kind == manifest.KindConfigMap &&
				(change.Name == "entry-output" || change.Name == "prerequisite-output") {
				assert.Equal(t, change.Status, "changed")
				if change.Name == "prerequisite-output" {
					assert.Equal(t, change.Parent.Name, "sibling")
				}
				found[change.Name] = true
			}
		}
		assert.FatalIf(t, len(found) != 2, "diff lost entry/prerequisite output: %s", out)
	}
}

func e2EDisabledAppDiffSidesMembershipNamespaceAuthorityBody(t *testing.T, stderr string, skipped bool) {
	t.Helper()
	credentials := 0
	assert.FatalIf(t, strings.Contains(stderr, "base: credentials unavailable") != skipped ||
		skipped && !strings.Contains(stderr, "unavailable-auth") ||
		strings.Contains(stderr, "dial tcp") ||
		strings.Contains(stderr, "[OrphanedResource]") ||
		strings.Contains(
			stderr,
			"[IncompleteInputCoverage]",
		), "unexpected mixed-namespace diagnostics: %s", stderr)
	for line := range strings.SplitSeq(stderr, "\n") {
		if warning := strings.TrimSpace(line); strings.HasPrefix(warning, "[") {
			category, _, _ := strings.Cut(warning[1:], "]")
			if category == manifest.WarnSourceCredentialsUnavailable && skipped &&
				strings.Contains(warning, "flux-system/external") &&
				strings.Contains(warning, "unavailable-auth") {
				credentials++
				continue
			}
			if category == manifest.WarnExternalSource && !skipped &&
				strings.Contains(warning, "flux-system/external") {
				continue
			}
			assert.Equal(t, category, manifest.WarnRenderPersistenceDisabled)
		}
	}
	wantCredentials := 0
	if skipped {
		wantCredentials = 1
	}
	assert.Equal(t, credentials, wantCredentials)
}

func e2EDisabledAppDiffSidesMembershipNamespaceAuthorityBody2(
	t *testing.T,
	out string,
	namespace string,
	value string,
) {
	t.Helper()
	docs, err := testutil.SplitDocs([]byte(out))
	assert.NoError(t, err)
	var identities []manifest.NamedResource
	for _, doc := range docs {
		name, ns := manifest.DocMetadata(doc)
		assert.FatalIf(
			t,
			ns == "inner" || name == "excluded-app" || name == "commented-app" || name == "excluded-cm" ||
				name == "commented-cm",
			"excluded identity rendered: %s/%s",
			ns,
			name,
		)
		if name != "app" && name != "reachable-cm" {
			continue
		}
		group, _, _ := strings.Cut(doc["apiVersion"].(string), "/")
		if group == "v1" {
			group = ""
		}
		identities = append(
			identities,
			manifest.NamedResource{Group: group, Kind: doc["kind"].(string), Namespace: ns, Name: name},
		)
		if name == "reachable-cm" {
			assert.Equal(t, doc["data"].(map[string]any)["value"], any(value))
		}
	}
	slices.SortFunc(identities, manifest.NamedResource.Compare)
	assert.Diff(
		t,
		identities,
		[]manifest.NamedResource{
			{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "reachable-cm"},
			testutil.KustomizationID(namespace, "app"),
		},
	)
}

func e2EEntryPrerequisiteDiffSidesNameLoopLoop(t *testing.T, loopInputs struct {
	docs       *[]map[string]any
	loopInputs *struct {
		direction **struct {
			name     string
			current  string
			original string
			value    string
		}
	}
	found *map[string]bool
},
) {
	t.Helper()

	for _, doc := range *loopInputs.docs {
		name, _ := manifest.DocMetadata(doc)
		if name == "entry-output" || name == "prerequisite-output" {
			assert.Equal(
				t,
				doc["data"].(map[string]any)["value"],
				any((*loopInputs.loopInputs.direction).value),
			)
			(*loopInputs.found)[name] = true
		}
	}
}
