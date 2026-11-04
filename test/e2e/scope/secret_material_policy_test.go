package scope_test

import (
	"encoding/base64"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

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
