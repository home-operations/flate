package scope_test

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

func e2ESelectedKustomizeRootDiffCase(t *testing.T, tc *struct {
	name   string
	owners [2]string
	nested bool
},
) {
	t.Helper()

	for _, direction := range []struct {
		name      string
		orig, cur int
	}{
		{name: "forward", orig: 0, cur: 1},
		{name: "reverse", orig: 1, cur: 0},
	} {
		t.Run(direction.name, func(t *testing.T) {
			roots := [2]string{t.TempDir(), t.TempDir()}
			for side, root := range roots {
				testutil.GitInit(t, root)
				resources := []string{"missing.yaml"}
				if side == direction.orig {
					resources = []string{"child.yaml"}
					declarations := "app"
					if tc.nested {
						resources = []string{"driver.yaml"}
						declarations = "declarations"
						testutil.WriteFile(
							t,
							root,
							"app/driver.yaml",
							testutil.KustomizationYAML("driver", "apps", "  path: ./declarations\n"),
						)
						testutil.WriteFile(
							t,
							root,
							"declarations/kustomization.yaml",
							"resources: [child.yaml]\n",
						)
					}
					testutil.WriteFile(
						t,
						root,
						declarations+"/child.yaml",
						testutil.KustomizationYAML("child", "apps", "  path: ./leaf\n"),
					)
					testutil.WriteFile(t, root, "leaf/kustomization.yaml", "resources: [cm.yaml]\n")
					testutil.WriteFile(
						t,
						root,
						"leaf/cm.yaml",
						"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: child-output, namespace: apps}\n",
					)
				}
				if owner := tc.owners[side]; owner != "" {
					resources = append(resources, "owner.yaml")
					testutil.WriteFile(
						t,
						root,
						"app/owner.yaml",
						testutil.KustomizationYAML(owner, "apps", "  path: ./app\n"),
					)
				}
				testutil.WriteFile(
					t,
					root,
					"app/kustomization.yaml",
					"resources: ["+strings.Join(resources, ", ")+"]\n",
				)
			}
			out, stderr, code := harness.RunCLIBuffers(
				"diff",
				"ks",
				"child",
				"-o",
				"json",
				"--path",
				filepath.Join(roots[direction.cur], "app"),
				"--path-orig",
				filepath.Join(roots[direction.orig], "app"),
				"--concurrency",
				"2",
				"--no-progress",
				"--cache-dir",
				t.TempDir(),
			)
			assert.FatalIf(
				t,
				code != 1 || !strings.Contains(stderr, "missing.yaml"),
				"named child hid its failed entry: code=%d out=%s stderr=%s",
				code,
				out,
				stderr,
			)
			var result struct {
				Changes    []any `json:"Changes"`
				Suppressed []struct {
					Side   string `json:"Side"`
					Reason string `json:"Reason"`
					Count  int    `json:"Count"`
				} `json:"Suppressed"`
			}
			assert.NoError(t, json.Unmarshal([]byte(out), &result))
			assert.FatalIf(t, len(result.Changes) != 0 || len(result.Suppressed) != 1 ||
				result.Suppressed[0].Side != "current" ||
				result.Suppressed[0].Count != 1 ||
				!strings.Contains(
					result.Suppressed[0].Reason,
					"missing.yaml",
				), "named child lost opposite inventory or failure correspondence: %s", out)
		})
	}
}

func e2EUnownedMalformedEntryAuthorityName(t *testing.T, unreadable *bool) {
	t.Helper()

	dir := t.TempDir()
	testutil.GitInit(t, dir)
	testutil.WriteFile(t, dir, "kustomization.yaml", "resources: [flux/ks.yaml]\n")
	testutil.WriteFile(
		t,
		dir,
		"flux/ks.yaml",
		testutil.KustomizationYAML(
			"owner",
			"flux-system",
			"  path: ./apps\n  sourceRef: {kind: GitRepository, name: flux-system}\n",
		),
	)
	testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		dir,
		"apps/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: healthy-output}\n",
	)
	testutil.WriteFile(t, dir, "pkgdir/bad.yaml", "broken: [\n")
	if *unreadable {
		assert.NoError(t, os.Remove(filepath.Join(dir, "pkgdir/bad.yaml")))
		assert.NoError(t, os.Symlink("missing.yaml", filepath.Join(dir, "pkgdir/bad.yaml")))
		testutil.WriteFile(
			t,
			dir,
			"pkgdir/kustomization.yaml",
			"apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nresources: [bad.yaml]\n",
		)
	}
	for _, tc := range []struct {
		name, path string
		failed     bool
	}{
		{name: "incidental nested file", path: dir},
		{name: "explicit package entry", path: filepath.Join(dir, "pkgdir"), failed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := []string{
				"build",
				"all",
				"--path",
				tc.path,
				"--no-progress",
				"--concurrency",
				"2",
				"--cache-dir",
				t.TempDir(),
			}
			stdout, stderr, code := harness.RunCLIBuffers(args...)
			switch {
			case tc.failed:

				want := "pkgdir/bad.yaml"
				if *unreadable {
					want = "bad.yaml"
				}
				assert.FatalIf(t, code != 1 || !strings.Contains(stderr, want) ||
					strings.Contains(stderr, "Kustomization entry") ||
					!(*unreadable) && !strings.Contains(stderr, "document 1") ||
					strings.Contains(
						stderr,
						"flux-system/owner",
					), "explicit entry lost its input failure: exit=%d\n%s\n%s", code, stdout, stderr)

			case code != 0 || !strings.Contains(stdout, "healthy-output") || strings.Contains(stderr, "bad.yaml"):

				t.Fatalf("incidental file failed independent authority: exit=%d\n%s\n%s", code, stdout, stderr)
			}
			nextOut, nextErr, nextCode := harness.RunCLIBuffers(args...)
			assert.Equal(t, nextCode, code)
			assert.Equal(t, nextOut, stdout)
			assert.Equal(t, nextErr, stderr)
		})
	}
}

func e2EDiffMalformedManifestTransitionsName(t *testing.T, tc *struct {
	name string
	side string
},
) {
	t.Helper()

	orig, current := t.TempDir(), t.TempDir()
	for _, dir := range []string{orig, current} {
		testutil.GitInit(t, dir)
		testutil.WriteFile(
			t,
			dir,
			"flux/ks.yaml",
			testutil.KustomizationYAML(
				"owner",
				"flux-system",
				"  path: ./app\n  sourceRef: {kind: GitRepository, name: flux-system}\n",
			)+"---\n"+testutil.KustomizationYAML(
				"healthy",
				"flux-system",
				"  path: ./healthy\n  sourceRef: {kind: GitRepository, name: flux-system}\n",
			),
		)
		testutil.WriteFile(t, dir, "app/kustomization.yaml", "resources: [child.yaml]\n")
		interval := "10m"
		if tc.side == "orig" && dir == orig || tc.side == "current" && dir == current {
			interval = "[]"
		}
		testutil.WriteFile(
			t,
			dir,
			"app/child.yaml",
			"apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: child, namespace: flux-system}\nspec: {interval: "+interval+", suspend: true, chart: {spec: {chart: charts/demo, sourceRef: {kind: GitRepository, name: flux-system}}}}\n",
		)
		testutil.WriteFile(t, dir, "healthy/kustomization.yaml", "resources: [cm.yaml]\n")
		value := "old"
		if dir == current {
			value = "new"
		}
		testutil.WriteFile(
			t,
			dir,
			"healthy/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: independent-output}\ndata: {value: "+value+"}\n",
		)
	}
	cache := t.TempDir()
	args := []string{
		"diff",
		"all",
		"--path",
		filepath.Join(current, "flux"),
		"--path-orig",
		filepath.Join(orig, "flux"),
		"--concurrency",
		"2",
		"--no-progress",
		"--cache-dir",
		cache,
		"-o",
		"json",
	}
	stdout, stderr, code := harness.RunCLIBuffers(args...)
	assert.FatalIf(
		t,
		code != 1 || !strings.Contains(stderr, "✗  "+tc.side+" snapshot: Kustomization  flux-system/owner"),
		"snapshot failure lost its owner: exit=%d\n%s\n%s",
		code,
		stdout,
		stderr,
	)
	assert.FatalIf(
		t,
		strings.Count(stderr, "snapshot: Kustomization  flux-system/owner") != 1,
		"corrected side inherited failure:\n%s",
		stderr,
	)
	var result struct {
		Changes []struct {
			Name string `json:"Name"`
		} `json:"Changes"`
		Suppressed []struct {
			Parent struct {
				Name string `json:"Name"`
			} `json:"Parent"`
			Side   string `json:"Side"`
			Reason string `json:"Reason"`
		} `json:"Suppressed"`
	}
	assert.NoError(t, json.Unmarshal([]byte(stdout), &result))
	assert.FatalIf(t, len(result.Changes) != 1 || result.Changes[0].Name != "independent-output" ||
		len(result.Suppressed) != 2, "healthy diff or suppression lost:\n%s\n%s", stdout, stderr)
	for _, note := range result.Suppressed {
		switch {
		case note.Parent.Name == "owner":

			assert.FatalIf(
				t,
				note.Side != tc.side || !strings.Contains(note.Reason, "cannot unmarshal"),
				"owner failure belongs to the wrong snapshot:\n%s",
				stdout,
			)

		case note.Parent.Name != "child" || note.Side != "both" || !strings.Contains(note.Reason, "suspended on"):

			t.Fatalf("unexpected descendant suppression:\n%s", stdout)
		}
	}
	nextOut, nextErr, nextCode := harness.RunCLIBuffers(args...)
	assert.Equal(t, nextCode, code)
	assert.Equal(t, nextOut, stdout)
	assert.Equal(t, nextErr, stderr)
}

func e2EDiffIgnoredRequiredSubstitutionCase(t *testing.T, roots *[2]string, reverse *bool, named *bool) {
	t.Helper()

	current, original := roots[1], roots[0]
	if *reverse {
		current, original = original, current
	}
	args := []string{
		"diff",
		"all",
		"--path",
		filepath.Join(current, "flux"),
		"--path-orig",
		filepath.Join(original, "flux"),
		"--cache-dir",
		t.TempDir(),
		"--concurrency",
		"2",
		"--no-progress",
		"-o",
		"diff",
	}
	if *named {
		args[1] = "ks"
		args = append(args, "consumer")
	}
	stdout, stderr, code := harness.RunCLIBuffers(args...)
	assert.FatalIf(
		t,
		code != 1 || !strings.Contains(stderr, "vars") || !strings.Contains(stderr, "consumer"),
		"required-data failure lost: exit=%d stdout=%s stderr=%s",
		code,
		stdout,
		stderr,
	)
	failingSide := "current snapshot"
	if *reverse {
		failingSide = "orig snapshot"
	}
	assert.Equal(t, strings.Contains(stderr, failingSide), true)
	t.Logf("exit=%d diagnostics=%s", code, stderr)
	repeatedOutput, repeatedDiagnostic, repeatedExit := harness.RunCLIBuffers(args...)
	assert.Equal(t, repeatedOutput, stdout)
	assert.Equal(t, repeatedDiagnostic, stderr)
	assert.Equal(t, repeatedExit, code)
}

func e2ESelectedKustomizeRootDiffName2Name(t *testing.T, roots *[2]string, direction *struct {
	name string
	orig int
	cur  int
}, capturedInputs struct {
	producerCounts *[2]int
	owners         *[2][]string
},
) {
	t.Helper()

	args := []string{
		"diff",
		"all",
		"-o",
		"json",
		"--path",
		filepath.Join(roots[direction.cur], "app"),
		"--path-orig",
		filepath.Join(roots[direction.orig], "app"),
		"--concurrency",
		"2",
		"--no-progress",
		"--cache-dir",
		t.TempDir(),
	}
	out, _ := harness.RequireCLIOK(t, args...)
	var result struct {
		Changes []struct {
			Kind string `json:"Kind"`
			Name string `json:"Name"`
			Old  *struct {
				Data map[string]string `json:"Data"`
			} `json:"Old"`
			New *struct {
				Data map[string]string `json:"Data"`
			} `json:"New"`
		} `json:"Changes"`
		Suppressed []struct {
			Side   string `json:"Side"`
			Count  int    `json:"Count"`
			Reason string `json:"Reason"`
		} `json:"Suppressed"`
	}
	assert.NoError(t, json.Unmarshal([]byte(out), &result))
	values := [2]string{"before", "after"}
	oldCount, newCount := 0, 0
	for _, change := range result.Changes {
		if change.Kind != manifest.KindConfigMap || change.Name != "root-output" {
			continue
		}
		if change.Old != nil {
			oldCount++
			assert.Equal(t, change.Old.Data["value"], values[direction.orig])
		}
		if change.New != nil {
			newCount++
			assert.Equal(t, change.New.Data["value"], values[direction.cur])
		}
	}
	assert.ErrorIf(
		t,
		oldCount != capturedInputs.producerCounts[direction.orig] || newCount != capturedInputs.producerCounts[direction.cur] ||
			len(result.Suppressed) != 0,
		"successful entry edit missing: %s",
		out,
	)
	failedRoot := roots[direction.cur]
	builder := filepath.Join(failedRoot, "app/kustomization.yaml")
	body, err := os.ReadFile(builder)
	assert.NoError(t, err)
	testutil.WriteFile(
		t,
		failedRoot,
		"app/kustomization.yaml",
		strings.Replace(string(body), "resources: [", "resources: [missing.yaml, ", 1),
	)
	defer testutil.WriteFile(t, failedRoot, "app/kustomization.yaml", string(body))
	out, stderr, code := harness.RunCLIBuffers(args...)
	assert.FatalIf(
		t,
		code != 1 || !strings.Contains(stderr, "missing.yaml"),
		"failed entry lost its error: code=%d out=%s stderr=%s",
		code,
		out,
		stderr,
	)
	result.Changes, result.Suppressed = nil, nil
	assert.NoError(t, json.Unmarshal([]byte(out), &result))
	withheld := 0
	for _, item := range result.Suppressed {
		withheld += item.Count
		assert.FatalIf(
			t,
			item.Side != "current" || !strings.Contains(item.Reason, "missing.yaml"),
			"failed representative lost local attribution: %s",
			out,
		)
	}
	wantWithheld := capturedInputs.producerCounts[direction.orig] * (1 + len(capturedInputs.owners[direction.orig]))
	assert.FatalIf(
		t,
		len(result.Changes) != 0 || withheld != wantWithheld,
		"failed opposite entry exposed false changes or lost inventory: %s",
		out,
	)
}

func e2ESecretMaterialPolicyCase(t *testing.T, field *string, tc *struct {
	name            string
	allowMissing    bool
	producer        bool
	malformed       bool
	invalidIdentity bool
	wantSkip        bool
}, identity *string,
) {
	t.Helper()

	dir := t.TempDir()
	testutil.GitInit(t, dir)
	url := "https://fixture.invalid/external.git"
	if (*field) == "known_hosts" {
		url = "ssh://git@fixture.invalid/external.git"
	}
	testutil.WriteFile(
		t,
		dir,
		"flux/root.yaml",
		testutil.KustomizationYAML(
			"local",
			"flux-system",
			"  path: ./apps\n  sourceRef: {kind: GitRepository, name: flux-system}\n",
		)+"---\n"+
			fmt.Sprintf(
				`apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: external, namespace: flux-system}
spec:
  provider: github
  url: %s
  secretRef: {name: auth}
`,
				url,
			)+"---\n"+testutil.KustomizationYAML(
			"consumer",
			"flux-system",
			"  path: ./external\n  sourceRef: {kind: GitRepository, name: external}\n",
		),
	)
	material := base64.StdEncoding.EncodeToString([]byte("..PLACEHOLDER_" + (*field) + ".."))
	if tc.malformed {
		material = "%%%fixture%%%"
	}
	keyMaterial := (*identity)
	if tc.invalidIdentity {
		keyMaterial = "invalid-private-fixture"
	}
	testutil.WriteFile(t, dir, "apps/secret.yaml", fmt.Sprintf(`apiVersion: v1
kind: Secret
metadata: {name: auth, namespace: flux-system}
data:
  identity: %s
  username: dXNlcg==
  password: cGFzc3dvcmQ=
  %s: %q
`, base64.StdEncoding.EncodeToString([]byte(keyMaterial)), (*field), material))
	testutil.WriteFile(
		t,
		dir,
		"apps/healthy.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: healthy-output}\ndata: {value: rendered}\n",
	)
	resources := "resources: [secret.yaml, healthy.yaml]\n"
	if tc.producer {
		resources = "resources: [secret.yaml, healthy.yaml, producer.yaml]\n"
		testutil.WriteFile(t, dir, "apps/producer.yaml", `apiVersion: external-secrets.io/v1
kind: ExternalSecret
metadata: {name: producer, namespace: flux-system}
spec:
  target: {name: auth}
`)
	}
	testutil.WriteFile(t, dir, "apps/kustomization.yaml", resources)
	args := []string{
		"build",
		"all",
		"--path",
		filepath.Join(dir, "flux"),
		"--no-progress",
		"--concurrency",
		"2",
		"--cache-dir",
		t.TempDir(),
	}
	if tc.allowMissing {
		args = append(args, "--allow-missing-kinds=Secret,ConfigMap")
	}
	stdout, stderr, code := harness.RunCLIBuffers(args...)
	wantCode := 1
	if tc.wantSkip {
		wantCode = 0
	}
	errorField := (*field)
	if tc.invalidIdentity {
		errorField = "identity"
	}
	assert.ErrorIf(t, !tc.wantSkip &&
		(!strings.Contains(stderr, "flux-system/external") || !strings.Contains(stderr, "flux-system/auth") || !strings.Contains(stderr, errorField)), "missing source/Secret/field failure context:\n%s", stderr)
	assert.FatalIf(
		t,
		code != wantCode || !strings.Contains(stdout, "name: healthy-output"),
		"exit=%d, want %d and healthy local output:\n%s\n%s",
		code,
		wantCode,
		stdout,
		stderr,
	)
	assert.ErrorIf(
		t,
		tc.malformed && (strings.Contains(stderr, "skipped") || !strings.Contains(stderr, "base64")),
		"malformed material must fail without a skip:\n%s",
		stderr,
	)
	assert.ErrorIf(t, tc.invalidIdentity &&
		(strings.Contains(stderr, "skipped") || strings.Contains(stderr, keyMaterial) || !strings.Contains(stderr, "parse SSH identity")), "invalid identity must fail without a skip or payload disclosure:\n%s", stderr)
	assert.ErrorIf(
		t,
		strings.Contains(stderr, "dial tcp") || strings.Contains(stderr, material),
		"material resolution must stop before fetch without exposing payloads:\n%s",
		stderr,
	)
	if tc.wantSkip {
		args[0] = "test"
		out, diagnostic, testCode := harness.RunCLIBuffers(args...)
		assert.FatalIf(
			t,
			testCode != 0,
			"skipped source failed test: exit=%d\n%s\n%s",
			testCode,
			out,
			diagnostic,
		)
		assert.ErrorIf(t, !strings.Contains(out, "flux-system/consumer") ||
			!strings.Contains(out, "flux-system/external") ||
			!strings.Contains(out, "flux-system/local") ||
			!strings.Contains(
				out,
				"1 passed · 2 skipped",
			), "expected dependent skip and local render:\n%s\n%s", out, diagnostic)
	}
}
