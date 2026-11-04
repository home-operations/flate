package scope_test

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_MalformedManifestScope(t *testing.T) {
	for _, tc := range []struct {
		name      string
		graph     bool
		owned     bool
		shared    bool
		wide      bool
		suspended bool
		changed   bool
		generated bool
		external  bool
	}{
		{name: "owned graph", graph: true, owned: true},
		{name: "shared graph", graph: true, owned: true, shared: true},
		{name: "owned ad-hoc", owned: true},
		{name: "owned ad-hoc, wide scan", owned: true, wide: true},
		{name: "suspended owner", owned: true, wide: true, suspended: true},
		{name: "changed-only, unselected owner", owned: true, wide: true, changed: true},
		{name: "ResourceSet-generated owner", owned: true, wide: true, generated: true},
		{name: "suspended generated owner", owned: true, wide: true, generated: true, suspended: true},
		{name: "external-source owner", owned: true, wide: true, external: true},
		{name: "unowned graph", graph: true},
		{name: "unowned ad-hoc"},
	} {
		t.Run(tc.name, func(t *testing.T) { e2EMalformedManifestScopeName(t, &tc) })
	}
}

func e2EMalformedManifestScopeName(t *testing.T, tc *struct {
	name      string
	graph     bool
	owned     bool
	shared    bool
	wide      bool
	suspended bool
	changed   bool
	generated bool
	external  bool
},
) {
	t.Helper()

	dir := t.TempDir()
	repo := testutil.GitInit(t, dir)
	testutil.WriteFile(t, dir, "monitoring/blackbox-exporter.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: blackbox-exporter, namespace: flux-system}
spec:
  path: ./monitoring/blackbox-exporter/app
  sourceRef: {kind: GitRepository, name: flux-system}
brokenPointer: [ unclosed
`)
	if tc.graph {
		testutil.WriteFile(t, dir, "monitoring/kustomization.yaml", "resources:\n  - blackbox-exporter.yaml\n")
	}
	testutil.WriteFile(t, dir, "healthy/kustomization.yaml", "resources:\n  - configmap.yaml\n")
	testutil.WriteFile(t, dir, "healthy/configmap.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: sibling-output}
data: {value: rendered}
`)
	scan := filepath.Join(dir, "monitoring")
	if tc.owned {
		scan = filepath.Join(dir, "flux")
		testutil.WriteFile(t, dir, "flux/kustomization.yaml", "resources:\n  - ks.yaml\n")
		owner := testutil.KustomizationYAML(
			"owner",
			"flux-system",
			"  path: ./monitoring\n  sourceRef: {kind: GitRepository, name: flux-system}\n",
		)
		if tc.suspended {
			owner = strings.Replace(owner, "path: ./monitoring", "path: ./monitoring\n  suspend: true", 1)
		}
		if tc.generated {
			owner = `apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: owners, namespace: flux-system}
spec:
  resourcesTemplate: |
    ` + strings.ReplaceAll(strings.TrimSpace(owner), "\n", "\n    ") + "\n"
		}
		if tc.external {
			owner = strings.Replace(owner, "name: flux-system}", "name: external}", 1) + `---
apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: external, namespace: flux-system}
spec: {url: 'https://fixture.invalid/other'}
`
		}
		testutil.WriteFile(t, dir, "flux/ks.yaml", owner+"---\n"+
			testutil.KustomizationYAML("sibling", "flux-system", `  path: ./healthy
  sourceRef: {kind: GitRepository, name: flux-system}
`)+"---\n"+
			testutil.KustomizationYAML("dependent", "flux-system", `  path: ./healthy
  dependsOn: [{name: owner}]
  sourceRef: {kind: GitRepository, name: flux-system}
`))
	}
	if tc.shared {
		testutil.WriteFile(
			t,
			dir,
			"flux/peer.yaml",
			testutil.KustomizationYAML(
				"peer",
				"flux-system",
				"  path: ./monitoring\n  sourceRef: {kind: GitRepository, name: flux-system}\n",
			),
		)
		testutil.WriteFile(t, dir, "flux/kustomization.yaml", "resources: [ks.yaml, peer.yaml]\n")
	}
	if tc.wide {
		testutil.WriteFile(t, dir, "kustomization.yaml", "resources: [flux]\n")
		scan = dir
	}
	args := []string{"build", "all", "--path", scan, "--no-progress", "--concurrency", "2"}
	if tc.changed {
		testutil.GitCommitAll(
			t,
			repo,
			"init",
			&gogit.CommitOptions{
				Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)},
			},
		)
		original := t.TempDir()
		testutil.CopyTree(t, dir, original)
		harness.MutateFile(t, filepath.Join(dir, "healthy/configmap.yaml"), "value: rendered", "value: changed")
		args = append(args, "--path-orig", original)
	}
	if tc.external {
		original := t.TempDir()
		testutil.CopyTree(t, dir, original)
		args = []string{
			"diff",
			"all",
			"--path",
			scan,
			"--path-orig",
			original,
			"--no-progress",
			"--concurrency",
			"2",
		}
	}
	stdout, stderr, code := harness.RunCLIBuffers(args...)
	if tc.external {
		assert.FatalIf(
			t,
			code != 0 || stdout != "" || strings.Contains(stderr, "decode"),
			"external-source owner failed the diff: exit=%d\n%s\n%s",
			code,
			stdout,
			stderr,
		)
		return
	}
	if tc.suspended || tc.changed {
		assert.FatalIf(
			t,
			code != 0 || !strings.Contains(stdout, "sibling-output") || strings.Contains(stderr, "decode"),
			"unreconciled owner failed the run: exit=%d\n%s\n%s",
			code,
			stdout,
			stderr,
		)
		assert.FatalIf(t, tc.changed &&
			(!strings.Contains(stdout, "value: changed") || strings.Contains(stdout, "name: blackbox-exporter")), "changed sibling did not render alone:\n%s", stdout)
		return
	}
	assert.FatalIf(
		t,
		code != 1 || !strings.Contains(stderr, "blackbox-exporter.yaml"),
		"expected exit 1 naming malformed file; got %d:\n%s",
		code,
		stderr,
	)
	e2EMalformedManifestScopeNameLoop(t, struct {
		tc **struct {
			name      string
			graph     bool
			owned     bool
			shared    bool
			wide      bool
			suspended bool
			changed   bool
			generated bool
			external  bool
		}
		stderr *string
		stdout *string
		code   *int
		scan   *string
	}{tc: &tc, stderr: &stderr, stdout: &stdout, code: &code, scan: &scan})
}

func e2EMalformedManifestScopeNameLoop(t *testing.T, loopInputs struct {
	tc **struct {
		name      string
		graph     bool
		owned     bool
		shared    bool
		wide      bool
		suspended bool
		changed   bool
		generated bool
		external  bool
	}
	stderr *string
	stdout *string
	code   *int
	scan   *string
},
) {
	t.Helper()

	if (*loopInputs.tc).owned {
		wantDecodes := 1
		if (*loopInputs.tc).shared {
			wantDecodes++
		}
		{
			got := strings.Count((*loopInputs.stderr), "did not find expected")
			assert.FatalIf(t, got != wantDecodes, "decode error reported %d times:\n%s", got, (*loopInputs.stderr))
		}
		assert.FatalIf(
			t,
			strings.Contains((*loopInputs.stderr), "flate error: loader: decode"),
			"owned decode error escaped resource attribution:\n%s",
			(*loopInputs.stderr),
		)
		assert.FatalIf(
			t,
			!strings.Contains((*loopInputs.stdout), "sibling-output") || !strings.Contains((*loopInputs.stderr), "flux-system/owner") ||
				!strings.Contains(
					(*loopInputs.stderr),
					"flux-system/dependent",
				),
			"expected sibling output and scoped owner/dependent failures:\n%s\n%s",
			(*loopInputs.stdout),
			(*loopInputs.stderr),
		)
		assert.FatalIf(
			t,
			(*loopInputs.tc).shared && !strings.Contains((*loopInputs.stderr), "flux-system/peer"),
			"second owner escaped the malformed file failure:\n%s",
			(*loopInputs.stderr),
		)
		(*loopInputs.stdout), (*loopInputs.stderr), (*loopInputs.code) = harness.RunCLIBuffers(
			"build",
			"ks",
			"sibling",
			"--path",
			(*loopInputs.scan),
			"--no-progress",
			"--concurrency",
			"2",
		)
		assert.FatalIf(
			t,
			(*loopInputs.code) != 0 || !strings.Contains((*loopInputs.stdout), "sibling-output"),
			"healthy named sibling failed: exit=%d\n%s\n%s",
			(*loopInputs.code),
			(*loopInputs.stdout),
			(*loopInputs.stderr),
		)
	} else {
		want := "entry monitoring/blackbox-exporter.yaml: document 1"
		if (*loopInputs.tc).graph {
			want = "kustomize build monitoring"
		}
		assert.FatalIf(
			t,
			!strings.Contains((*loopInputs.stderr), want) ||
				strings.Contains((*loopInputs.stderr), "Kustomization entry"),
			"unowned malformed file lost its owning error:\n%s",
			(*loopInputs.stderr),
		)
	}
}
