package crds_test

import (
	"cmp"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

const issueResourceSet = `apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata:
  name: flux-operator
  namespace: flux-system
spec:
  serviceAccountName: flux-operator
  dependsOn:
    - apiVersion: apiextensions.k8s.io/v1
      kind: CustomResourceDefinition
      name: helmreleases.helm.toolkit.fluxcd.io
  resources:
    - apiVersion: source.toolkit.fluxcd.io/v1
      kind: OCIRepository
      metadata:
        name: flux-operator
        namespace: flux-system
      spec:
        interval: 1h
        url: oci://ghcr.io/controlplaneio-fluxcd/charts/flux-operator
        ref:
          semver: '*'
        verify:
          provider: cosign
          matchOIDCIdentity:
          - issuer: ^https://token\.actions\.githubusercontent\.com$
            subject: ^https://github\.com/controlplaneio-fluxcd/charts/\.github/workflows/release\.yml@refs/tags/v\d+\.\d+\.\d+$
    - apiVersion: helm.toolkit.fluxcd.io/v2
      kind: HelmRelease
      metadata:
        name: flux-operator
        namespace: flux-system
      spec:
        interval: 1h
        releaseName: flux-operator
        serviceAccountName: flux-operator
        chartRef:
          kind: OCIRepository
          name: flux-operator
        values:
          multitenancy:
            enabled: true
            defaultServiceAccount: default
          reporting:
            interval: 5m
`

func missingCRDRepo(t *testing.T, rs string) string {
	t.Helper()
	dir := t.TempDir()
	testutil.GitInit(t, dir)
	testutil.WriteFile(
		t,
		dir,
		"flux/ks.yaml",
		testutil.KustomizationYAML(
			"apps",
			"flux-system",
			" path: ./apps\n sourceRef: {kind: GitRepository, name: flux-system}\n",
		),
	)
	testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources:\n- rs.yaml\n")
	testutil.WriteFile(t, dir, "apps/rs.yaml", rs)
	return dir
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_MissingCRDs_StrictIssue(t *testing.T) {
	dir := missingCRDRepo(t, issueResourceSet)
	runCLIBuffersResult1 := harness.RunCLIBuffers(
		"test",
		"all",
		"--path",
		dir,
		"--cache-dir",
		t.TempDir(),
		"--concurrency",
		"2",
	)
	out, errOut, code := runCLIBuffersResult1.Stdout, runCLIBuffersResult1.Stderr, runCLIBuffersResult1.Code
	assert.FatalIf(t, code != 1 || !strings.Contains(out+errOut, "helmreleases.helm.toolkit.fluxcd.io") ||
		!strings.Contains(out+errOut, "not found"), "strict issue: exit=%d stdout=%s stderr=%s", code, out, errOut)
	t.Logf("strict issue: exit=%d stdout=%s stderr=%s", code, out, errOut)
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_ResourceSet_RuntimeClassDependency(t *testing.T) {
	dir := missingCRDRepo(
		t,
		"apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSet\nmetadata: {name: rs, namespace: flux-system}\nspec:\n  dependsOn: [{apiVersion: node.k8s.io/v1, kind: RuntimeClass, name: nvidia}]\n  resources: [{apiVersion: v1, kind: ConfigMap, metadata: {name: result}}]\n",
	)
	testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources: [rs.yaml, runtime.yaml]\n")
	testutil.WriteFile(
		t,
		dir,
		"apps/runtime.yaml",
		"apiVersion: node.k8s.io/v1\nkind: RuntimeClass\nmetadata: {name: nvidia}\nhandler: nvidia\n",
	)
	runCLIBuffersResult2 := harness.RunCLIBuffers(
		"test",
		"all",
		"--path",
		dir,
		"--cache-dir",
		t.TempDir(),
		"--concurrency",
		"2",
	)
	out, errOut, code := runCLIBuffersResult2.Stdout, runCLIBuffersResult2.Stderr, runCLIBuffersResult2.Code
	assert.FatalIf(t, code != 0, "RuntimeClass dependency: exit=%d stdout=%s stderr=%s", code, out, errOut)
}

const helmReleaseCRD = `apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata: {name: helmreleases.helm.toolkit.fluxcd.io}
spec:
 group: helm.toolkit.fluxcd.io
 names: {kind: HelmRelease, plural: helmreleases, singular: helmrelease}
 scope: Namespaced
 versions:
 - name: v2
   served: true
   storage: true
   schema:
    openAPIV3Schema:
     type: object
     x-kubernetes-preserve-unknown-fields: true
`

func suspendedIssueResourceSet() string {
	return strings.ReplaceAll(
		issueResourceSet,
		"        interval: 1h\n",
		"        suspend: true\n        interval: 1h\n",
	)
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_MissingCRDs_Policy(t *testing.T) {
	for _, tc := range []struct {
		name, kind, supplied, namespace string
		enabled, success, reference     bool
	}{
		{name: "reference CRD", kind: "CustomResourceDefinition", supplied: helmReleaseCRD, reference: true, success: true},
		{name: "reference CRD opt in", kind: "CustomResourceDefinition", supplied: helmReleaseCRD, reference: true, enabled: true, success: true},
		{name: "strict absent", kind: "CustomResourceDefinition"},
		{name: "allowed absent", kind: "CustomResourceDefinition", enabled: true, success: true},
		{name: "supplied CRD", kind: "CustomResourceDefinition", supplied: helmReleaseCRD, success: true},
		{name: "supplied CRD opt in", kind: "CustomResourceDefinition", supplied: helmReleaseCRD, enabled: true, success: true},
		{name: "strict absent explicit CRD", kind: "CustomResourceDefinition", namespace: "flux-system"},
		{name: "allowed absent explicit CRD", kind: "CustomResourceDefinition", namespace: "flux-system", enabled: true, success: true},
		{name: "missing ConfigMap", kind: "ConfigMap", enabled: true},
		{name: "missing Secret", kind: "Secret", enabled: true},
		{name: "missing Widget", kind: "Widget", enabled: true},
		{name: "supplied ConfigMap", kind: "ConfigMap", enabled: true, success: true, supplied: "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: helmreleases.helm.toolkit.fluxcd.io, namespace: flux-system}\ndata: {key: value}\n"},
	} {
		t.Run(tc.name, func(t *testing.T) { e2EMissingCRDsPolicyName(t, &tc) })
	}
}

func TestE2E_MissingCRDs_Environment(t *testing.T) {
	dir := missingCRDRepo(t, suspendedIssueResourceSet())
	t.Setenv("FLATE_ALLOW_MISSING_KINDS", "CustomResourceDefinition")
	args := make([]string, 0, 9)
	args = append(args, "test", "all", "--path", dir, "--cache-dir", t.TempDir(), "--concurrency", "2")
	runCLIBuffersResult3 := harness.RunCLIBuffers(args...)
	out, errOut, code := runCLIBuffersResult3.Stdout, runCLIBuffersResult3.Stderr, runCLIBuffersResult3.Code
	assert.FatalIf(
		t,
		code != 0 || strings.Count(out+errOut, "absent from offline inputs; accepted for") != 1,
		"environment enable: exit=%d stdout=%s stderr=%s",
		code,
		out,
		errOut,
	)
	runCLIBuffersResult4 := harness.RunCLIBuffers(append(args, "--allow-missing-kinds=")...)
	out, errOut, code = runCLIBuffersResult4.Stdout, runCLIBuffersResult4.Stderr, runCLIBuffersResult4.Code
	assert.FatalIf(
		t,
		code != 1 || strings.Contains(out+errOut, "absent from offline inputs; accepted for"),
		"explicit empty must win: exit=%d stdout=%s stderr=%s",
		code,
		out,
		errOut,
	)
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_MissingCRDs_DeterministicWarnings(t *testing.T) {
	var first, firstBuild, firstHR string
	for _, workers := range []int{2, 4} {
		rs := suspendedIssueResourceSet()
		ref := "    - apiVersion: apiextensions.k8s.io/v1\n      kind: CustomResourceDefinition\n      name: helmreleases.helm.toolkit.fluxcd.io\n"
		rs = strings.Replace(
			rs,
			ref,
			ref+ref+strings.Replace(ref, "helmreleases.helm.toolkit.fluxcd.io", "widgets.example.com", 1),
			1,
		)
		rs += "---\n" + strings.Replace(rs, "  name: flux-operator\n", "  name: z-dependent\n", 1)
		dir := missingCRDRepo(t, rs)
		requireCLIOKResult5 := harness.RequireCLIOK(
			t,
			"test",
			"all",
			"--path",
			dir,
			"--cache-dir",
			t.TempDir(),
			"--concurrency",
			strconv.Itoa(workers),
			"--allow-missing-kinds=CustomResourceDefinition",
		)
		out, errOut := requireCLIOKResult5.Stdout, requireCLIOKResult5.Stderr
		lines := []string{}
		for line := range strings.SplitSeq(out+errOut, "\n") {
			if strings.Contains(line, "absent from offline inputs; accepted for") {
				lines = append(lines, strings.TrimSpace(line))
			}
		}
		assert.FatalIf(t, len(lines) != 2, "warnings=%v", lines)
		for _, line := range lines {
			assert.FatalIf(
				t,
				strings.Count(line, "ResourceSet.fluxcd.controlplane.io/flux-system/flux-operator") != 1 ||
					strings.Count(
						line,
						"ResourceSet.fluxcd.controlplane.io/flux-system/z-dependent",
					) != 1,
				"distinct sorted dependents missing: %s",
				line,
			)
		}
		assert.FatalIf(t, !strings.Contains(lines[0], "helmreleases.helm.toolkit.fluxcd.io") ||
			!strings.Contains(lines[1], "widgets.example.com"), "warnings out of order: %v", lines)
		requireCLIOKResult6 := harness.RequireCLIOK(
			t,
			"build",
			"all",
			"--path",
			dir,
			"--cache-dir",
			t.TempDir(),
			"--concurrency",
			strconv.Itoa(workers),
			"--allow-missing-kinds=CustomResourceDefinition",
		)
		built, _ := requireCLIOKResult6.Stdout, requireCLIOKResult6.Stderr
		requireCLIOKResult7 := harness.RequireCLIOK(
			t,
			"get",
			"hr",
			"-o",
			"yaml",
			"--path",
			dir,
			"--cache-dir",
			t.TempDir(),
			"--concurrency",
			strconv.Itoa(workers),
			"--allow-missing-kinds=CustomResourceDefinition",
		)
		hr, _ := requireCLIOKResult7.Stdout, requireCLIOKResult7.Stderr
		if firstBuild == "" {
			firstBuild = built
			firstHR = hr
		}
		assert.FatalIf(
			t,
			built != firstBuild || hr != firstHR,
			"%v",
			"rendered output changed across runs or concurrency levels",
		)
		got := strings.Join(lines, "\n")
		first = cmp.Or(first, got)
		assert.FatalIf(t, got != first, "warnings changed: want %s got %s", first, got)
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_MissingCRDs_DiffSides(t *testing.T) {
	for _, workers := range []int{2, 4} {
		t.Run(strconv.Itoa(workers), func(t *testing.T) {
			before := missingCRDRepo(t, suspendedIssueResourceSet())
			after := missingCRDRepo(t, strings.Replace(suspendedIssueResourceSet(), "interval: 5m", "interval: 10m", 1))
			args := make([]string, 0, 11)
			args = append(args,
				"diff",
				"all",
				"--path",
				after,
				"--path-orig",
				before,
				"--cache-dir",
				t.TempDir(),
				"--concurrency",
				strconv.Itoa(workers),
			)
			runCLIBuffersResult8 := harness.RunCLIBuffers(args...)
			out, errOut, code := runCLIBuffersResult8.Stdout, runCLIBuffersResult8.Stderr, runCLIBuffersResult8.Code
			assert.FatalIf(t, code != 1 ||
				strings.Count(
					errOut,
					"CustomResourceDefinition  helmreleases.helm.toolkit.fluxcd.io  not found",
				) != 2 ||
				!strings.Contains(
					errOut,
					"2 failed · 2 blocked",
				), "strict diff exit=%d stdout=%s stderr=%s", code, out, errOut)
			runCLIBuffersResult9 := harness.RunCLIBuffers(
				append(args, "--allow-missing-kinds=CustomResourceDefinition")...)
			out, errOut, code = runCLIBuffersResult9.Stdout, runCLIBuffersResult9.Stderr, runCLIBuffersResult9.Code
			assert.FatalIf(
				t,
				code != 0 || !strings.Contains(out+errOut, "10m") || !strings.Contains(out+errOut, "5m"),
				"opt-in diff exit=%d stdout=%s stderr=%s",
				code,
				out,
				errOut,
			)
			assert.FatalIf(t, strings.Contains(out, "absent from offline inputs; accepted for") ||
				strings.Count(errOut, "absent from offline inputs; accepted for") != 2 ||
				strings.Count(errOut, "orig snapshot:") != 1 ||
				strings.Count(
					errOut,
					"current snapshot:",
				) != 1, "diff warning destination/count/sides: stdout=%s stderr=%s", out, errOut)
		})
	}
}

func e2EMissingCRDsPolicyName(t *testing.T, tc *struct {
	name      string
	kind      string
	supplied  string
	namespace string
	enabled   bool
	success   bool
	reference bool
},
) {
	t.Helper()

	rs := strings.Replace(suspendedIssueResourceSet(), "kind: CustomResourceDefinition", "kind: "+tc.kind, 1)
	if tc.kind == manifest.KindConfigMap || tc.kind == manifest.KindSecret {
		rs = strings.Replace(rs, "apiVersion: apiextensions.k8s.io/v1", "apiVersion: v1", 1)
	}
	if tc.namespace != "" {
		rs = strings.Replace(
			rs,
			"      kind: "+tc.kind+"\n",
			"      kind: "+tc.kind+"\n      namespace: "+tc.namespace+"\n",
			1,
		)
	}
	dir := missingCRDRepo(t, rs)
	if tc.supplied != "" && !tc.reference {
		testutil.WriteFile(t, dir, "apps/dependency.yaml", tc.supplied)
		testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources:\n- rs.yaml\n- dependency.yaml\n")
	}
	args := []string{"--path", dir, "--cache-dir", t.TempDir(), "--concurrency", "2", "--allow-missing-kinds="}
	if tc.reference {
		inputs := t.TempDir()
		testutil.WriteFile(t, inputs, "dependency.yaml", tc.supplied)
		args = append(args, "--supply", filepath.Join(inputs, "dependency.yaml"))
	}
	if tc.enabled {
		args = append(args, "--allow-missing-kinds=CustomResourceDefinition")
	}
	runCLIBuffersResult10 := harness.RunCLIBuffers(append([]string{"test", "all"}, args...)...)
	out, errOut, code := runCLIBuffersResult10.Stdout, runCLIBuffersResult10.Stderr, runCLIBuffersResult10.Code
	wantCode := 1
	if tc.success {
		wantCode = 0
	}
	assert.FatalIf(t, code != wantCode, "exit=%d want %d stdout=%s stderr=%s", code, wantCode, out, errOut)
	diagnostics := out + errOut
	wantWarning := 0
	if tc.enabled && tc.kind == manifest.KindCustomResourceDefinition && tc.supplied == "" {
		wantWarning = 1
	}
	{
		got := strings.Count(diagnostics, "absent from offline inputs; accepted for")
		assert.FatalIf(t, got != wantWarning, "warning count=%d want %d: %s", got, wantWarning, diagnostics)
	}
	if wantWarning == 1 {
		crd := manifest.NamedResource{
			Group: "apiextensions.k8s.io",
			Kind:  manifest.KindCustomResourceDefinition,
			Name:  "helmreleases.helm.toolkit.fluxcd.io",
		}
		want := "CRD " + crd.String() + " absent from offline inputs; accepted for ResourceSet.fluxcd.controlplane.io/flux-system/flux-operator"
		assert.FatalIf(t, !strings.Contains(diagnostics, want), "canonical warning absent: %s", diagnostics)
	}
	if !tc.success {
		assert.FatalIf(
			t,
			!strings.Contains(diagnostics, "not found"),
			"missing dependency not reported: %s",
			diagnostics,
		)
		return
	}
	requireCLIOKResult11 := harness.RequireCLIOK(t, append([]string{"get", "hr", "-o", "yaml"}, args...)...)
	child, childErr := requireCLIOKResult11.Stdout, requireCLIOKResult11.Stderr
	for _, part := range []string{"name: flux-operator", "kind: OCIRepository", "suspend: true"} {
		assert.FatalIf(t, !strings.Contains(child, part), "missing child spec %q: %s", part, child)
	}
	assert.FatalIf(t, strings.Contains(child, "absent from offline inputs") ||
		strings.Count(
			childErr,
			"absent from offline inputs; accepted for",
		) != wantWarning, "get warning destination/count: stdout=%s stderr=%s", child, childErr)
	e2EMissingCRDsPolicyNameLoop(t, struct {
		args        *[]string
		wantWarning *int
		tc          **struct {
			name      string
			kind      string
			supplied  string
			namespace string
			enabled   bool
			success   bool
			reference bool
		}
	}{args: &args, wantWarning: &wantWarning, tc: &tc})
}

func e2EMissingCRDsPolicyNameLoop(t *testing.T, loopInputs struct {
	args        *[]string
	wantWarning *int
	tc          **struct {
		name      string
		kind      string
		supplied  string
		namespace string
		enabled   bool
		success   bool
		reference bool
	}
},
) {
	t.Helper()

	for _, skip := range []bool{true, false} {
		skipKinds := "Secret"
		if skip {
			skipKinds += ",CustomResourceDefinition"
		}
		requireCLIOKResult12 := harness.RequireCLIOK(
			t,
			append(append([]string{"build", "all"}, (*loopInputs.args)...), "--skip-kinds="+skipKinds)...)
		built, builtErr := requireCLIOKResult12.Stdout, requireCLIOKResult12.Stderr
		assert.FatalIf(t, strings.Contains(built, "absent from offline inputs") ||
			strings.Count(
				builtErr,
				"absent from offline inputs; accepted for",
			) != (*loopInputs.wantWarning), "build warning destination/count: stdout=%s stderr=%s", built, builtErr)
		if (*loopInputs.tc).kind == manifest.KindCustomResourceDefinition && (*loopInputs.tc).supplied != "" {
			docs, err := testutil.DecodeDocs(strings.NewReader(built))
			assert.NoError(t, err)
			found := false
			for _, doc := range docs {
				if manifest.DocKind(doc) == manifest.KindCustomResourceDefinition {
					found = true
				}
			}
			assert.FatalIf(
				t,
				found != (!skip && !(*loopInputs.tc).reference),
				"CRD excluded=%t did not filter only output: %s",
				skip,
				built,
			)
		}
	}
}
