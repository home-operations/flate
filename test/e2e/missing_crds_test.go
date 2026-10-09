package e2e

import (
	"fmt"
	gogit "github.com/go-git/go-git/v5"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"strings"
	"testing"
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
	if _, err := gogit.PlainInit(dir, false); err != nil {
		t.Fatal(err)
	}
	testutil.WriteFile(t, dir, "flux/ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: apps, namespace: flux-system}
spec:
 path: ./apps
 sourceRef: {kind: GitRepository, name: flux-system}
`)
	testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources:\n- rs.yaml\n")
	testutil.WriteFile(t, dir, "apps/rs.yaml", rs)
	return dir
}

func TestE2E_MissingCRDs_StrictIssue(t *testing.T) {
	dir := missingCRDRepo(t, issueResourceSet)
	out, errOut, code := runCLIBuffers("test", "all", "--path", dir, "--cache-dir", t.TempDir(), "--concurrency", "2")
	if code != 1 || !strings.Contains(out+errOut, "helmreleases.helm.toolkit.fluxcd.io") || !strings.Contains(out+errOut, "not found") {
		t.Fatalf("strict issue: exit=%d stdout=%s stderr=%s", code, out, errOut)
	}
	t.Logf("strict issue: exit=%d stdout=%s stderr=%s", code, out, errOut)
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
	return strings.ReplaceAll(issueResourceSet, "        interval: 1h\n", "        suspend: true\n        interval: 1h\n")
}

func TestE2E_MissingCRDs_Policy(t *testing.T) {
	for _, workers := range []int{2, 4} {
		for _, tc := range []struct {
			name, kind, supplied string
			enabled, success     bool
		}{
			{name: "strict absent", kind: "CustomResourceDefinition"},
			{name: "allowed absent", kind: "CustomResourceDefinition", enabled: true, success: true},
			{name: "supplied CRD", kind: "CustomResourceDefinition", supplied: helmReleaseCRD, success: true},
			{name: "supplied CRD opt in", kind: "CustomResourceDefinition", supplied: helmReleaseCRD, enabled: true, success: true},
			{name: "missing ConfigMap", kind: "ConfigMap", enabled: true},
			{name: "missing Secret", kind: "Secret", enabled: true},
			{name: "missing Widget", kind: "Widget", enabled: true},
			{name: "supplied ConfigMap", kind: "ConfigMap", enabled: true, success: true, supplied: "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: helmreleases.helm.toolkit.fluxcd.io, namespace: flux-system}\ndata: {key: value}\n"},
		} {
			t.Run(fmt.Sprintf("workers_%d/%s", workers, tc.name), func(t *testing.T) {
				rs := strings.Replace(suspendedIssueResourceSet(), "kind: CustomResourceDefinition", "kind: "+tc.kind, 1)
				dir := missingCRDRepo(t, rs)
				if tc.supplied != "" {
					testutil.WriteFile(t, dir, "apps/dependency.yaml", tc.supplied)
					testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources:\n- rs.yaml\n- dependency.yaml\n")
				}
				args := []string{"--path", dir, "--cache-dir", t.TempDir(), "--concurrency", fmt.Sprint(workers), "--allow-missing-secrets=false"}
				if tc.enabled {
					args = append(args, "--allow-missing-crds")
				}
				out, errOut, code := runCLIBuffers(append([]string{"test", "all"}, args...)...)
				wantCode := 1
				if tc.success {
					wantCode = 0
				}
				if code != wantCode {
					t.Fatalf("exit=%d want %d stdout=%s stderr=%s", code, wantCode, out, errOut)
				}
				diagnostics := out + errOut
				wantWarning := 0
				if tc.enabled && tc.kind == manifest.KindCustomResourceDefinition && tc.supplied == "" {
					wantWarning = 1
				}
				if got := strings.Count(diagnostics, "absent from offline inputs; accepted for"); got != wantWarning {
					t.Fatalf("warning count=%d want %d: %s", got, wantWarning, diagnostics)
				}
				if wantWarning == 1 && !strings.Contains(diagnostics, "ResourceSet/flux-system/flux-operator") {
					t.Fatalf("dependent absent: %s", diagnostics)
				}
				if !tc.success {
					if !strings.Contains(diagnostics, "not found") {
						t.Fatalf("missing dependency not reported: %s", diagnostics)
					}
					return
				}
				child, childErr := requireCLIOK(t, append([]string{"get", "hr", "-o", "yaml"}, args...)...)
				for _, part := range []string{"name: flux-operator", "kind: OCIRepository", "suspend: true"} {
					if !strings.Contains(child, part) {
						t.Fatalf("missing child spec %q: %s", part, child)
					}
				}
				if strings.Contains(child+childErr, "absent from offline inputs") {
					t.Fatal("get must suppress advisories")
				}
				for _, skip := range []bool{true, false} {
					built, builtErr := requireCLIOK(t, append(append([]string{"build", "all"}, args...), fmt.Sprintf("--skip-crds=%t", skip))...)
					if strings.Contains(built+builtErr, "absent from offline inputs") {
						t.Fatal("build must suppress advisories")
					}
					if tc.kind == manifest.KindCustomResourceDefinition && tc.supplied != "" {
						docs, err := manifest.DecodeDocs(strings.NewReader(built))
						if err != nil {
							t.Fatal(err)
						}
						found := false
						for _, doc := range docs {
							if manifest.DocKind(doc) == manifest.KindCustomResourceDefinition {
								found = true
							}
						}
						if found == skip {
							t.Fatalf("skip-crds=%t did not filter only output: %s", skip, built)
						}
					}
				}
			})
		}
	}
}

func TestE2E_MissingCRDs_Environment(t *testing.T) {
	dir := missingCRDRepo(t, suspendedIssueResourceSet())
	t.Setenv("FLATE_ALLOW_MISSING_CRDS", "true")
	args := []string{"test", "all", "--path", dir, "--cache-dir", t.TempDir(), "--concurrency", "2"}
	out, errOut, code := runCLIBuffers(args...)
	if code != 0 || strings.Count(out+errOut, "absent from offline inputs; accepted for") != 1 {
		t.Fatalf("environment enable: exit=%d stdout=%s stderr=%s", code, out, errOut)
	}
	out, errOut, code = runCLIBuffers(append(args, "--allow-missing-crds=false")...)
	if code != 1 || strings.Contains(out+errOut, "absent from offline inputs; accepted for") {
		t.Fatalf("explicit false must win: exit=%d stdout=%s stderr=%s", code, out, errOut)
	}
}

func TestE2E_MissingCRDs_DeterministicWarnings(t *testing.T) {
	var first, firstBuild, firstHR string
	for _, workers := range []int{2, 4} {
		for range 2 {
			rs := suspendedIssueResourceSet()
			ref := "    - apiVersion: apiextensions.k8s.io/v1\n      kind: CustomResourceDefinition\n      name: helmreleases.helm.toolkit.fluxcd.io\n"
			rs = strings.Replace(rs, ref, ref+ref+strings.Replace(ref, "helmreleases.helm.toolkit.fluxcd.io", "widgets.example.com", 1), 1)
			rs += "---\n" + strings.Replace(rs, "  name: flux-operator\n", "  name: z-dependent\n", 1)
			dir := missingCRDRepo(t, rs)
			out, errOut := requireCLIOK(t, "test", "all", "--path", dir, "--cache-dir", t.TempDir(), "--concurrency", fmt.Sprint(workers), "--allow-missing-crds")
			lines := []string{}
			for line := range strings.SplitSeq(out+errOut, "\n") {
				if strings.Contains(line, "absent from offline inputs; accepted for") {
					lines = append(lines, strings.TrimSpace(line))
				}
			}
			if len(lines) != 2 {
				t.Fatalf("warnings=%v", lines)
			}
			for _, line := range lines {
				if strings.Count(line, "ResourceSet/flux-system/flux-operator") != 1 || strings.Count(line, "ResourceSet/flux-system/z-dependent") != 1 {
					t.Fatalf("distinct sorted dependents missing: %s", line)
				}
			}
			if !strings.Contains(lines[0], "helmreleases.helm.toolkit.fluxcd.io") || !strings.Contains(lines[1], "widgets.example.com") {
				t.Fatalf("warnings out of order: %v", lines)
			}
			built, _ := requireCLIOK(t, "build", "all", "--path", dir, "--cache-dir", t.TempDir(), "--concurrency", fmt.Sprint(workers), "--allow-missing-crds")
			hr, _ := requireCLIOK(t, "get", "hr", "-o", "yaml", "--path", dir, "--cache-dir", t.TempDir(), "--concurrency", fmt.Sprint(workers), "--allow-missing-crds")
			if firstBuild == "" {
				firstBuild = built
				firstHR = hr
			}
			if built != firstBuild || hr != firstHR {
				t.Fatal("rendered output changed across runs or concurrency levels")
			}
			got := strings.Join(lines, "\n")
			if first == "" {
				first = got
			}
			if got != first {
				t.Fatalf("warnings changed: want %s got %s", first, got)
			}
		}
	}
}

func TestE2E_MissingCRDs_DiffSides(t *testing.T) {
	for _, workers := range []int{2, 4} {
		t.Run(fmt.Sprint(workers), func(t *testing.T) {
			before := missingCRDRepo(t, suspendedIssueResourceSet())
			after := missingCRDRepo(t, strings.Replace(suspendedIssueResourceSet(), "interval: 5m", "interval: 10m", 1))
			args := []string{"diff", "all", "--path", after, "--path-orig", before, "--cache-dir", t.TempDir(), "--concurrency", fmt.Sprint(workers)}
			out, errOut, code := runCLIBuffers(args...)
			if code != 1 || !strings.Contains(errOut, "orig snapshot:") || !strings.Contains(errOut, "current snapshot:") {
				t.Fatalf("strict diff exit=%d stdout=%s stderr=%s", code, out, errOut)
			}
			out, errOut, code = runCLIBuffers(append(args, "--allow-missing-crds")...)
			if code != 0 || !strings.Contains(out+errOut, "10m") || !strings.Contains(out+errOut, "5m") {
				t.Fatalf("opt-in diff exit=%d stdout=%s stderr=%s", code, out, errOut)
			}
			if strings.Contains(out+errOut, "absent from offline inputs; accepted for") {
				t.Fatal("diff must suppress advisories")
			}
		})
	}
}
