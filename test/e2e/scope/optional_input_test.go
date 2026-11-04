package scope_test

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/assert"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_CrossSourceOptionalInputRemoval(t *testing.T) {
	roots := testutil.CrossSourceOptionalInputs(t)
	flags := []string{"--concurrency", "2", "--no-progress", "--cache-dir", t.TempDir(), "--log-level", "debug"}
	var rendered [2]string
	for side, root := range roots {
		requireCLIOKResult1 := harness.RequireCLIOK(
			t,
			append([]string{"build", "all", "--path", filepath.Join(root, "flux")}, flags...)...)
		full, _ := requireCLIOKResult1.Stdout, requireCLIOKResult1.Stderr
		want := []string{"available", "fallback"}[side]
		docs, err := testutil.DecodeDocs(strings.NewReader(full))
		assert.NoError(t, err)
		input := false
		for _, doc := range docs {
			metadata := manifest.DocMetadata(doc)
			name := metadata.Name
			input = input || manifest.DocKind(doc) == manifest.KindConfigMap && name == "optional"
		}
		assert.FatalIf(t, !strings.Contains(full, "value: "+want) || !strings.Contains(full, "sentinel: active") ||
			input != (side == 0), "side %d full render lost consumer substitution or supplying input: %s", side, full)
		rendered[side] = t.TempDir()
		testutil.GitInit(t, rendered[side])
		e2ECrossSourceOptionalInputRemovalLoop(t, struct {
			rendered *[2]string
			side     *int
			full     *string
		}{rendered: &rendered, side: &side, full: &full})
	}
	oracleArgs := append(
		[]string{
			"diff",
			"all",
			"--path",
			filepath.Join(rendered[1], "flux"),
			"--path-orig",
			filepath.Join(rendered[0], "flux"),
			"-o",
			"diff",
		},
		flags...)
	requireCLIOKResult2 := harness.RequireCLIOK(t, oracleArgs...)
	oracle, _ := requireCLIOKResult2.Stdout, requireCLIOKResult2.Stderr
	args := append(
		[]string{
			"diff",
			"all",
			"--path",
			filepath.Join(roots[1], "flux"),
			"--path-orig",
			filepath.Join(roots[0], "flux"),
			"-o",
			"diff",
		},
		flags...)
	var previous string
	for run := range 2 {
		requireCLIOKResult3 := harness.RequireCLIOK(t, args...)
		scoped, stderr := requireCLIOKResult3.Stdout, requireCLIOKResult3.Stderr
		assert.FatalIf(t, scoped != oracle || !strings.Contains(scoped, "-  value: available") ||
			!strings.Contains(scoped, "+  value: fallback") ||
			!strings.Contains(
				scoped,
				"name: optional",
			), "cross-source removal differs from full-render oracle: scoped=%s\nfull=%s\nstderr=%s", scoped, oracle, stderr)
		selected := 0
		for line := range strings.SplitSeq(stderr, "\n") {
			if strings.Contains(line, "changed-only keep set") &&
				strings.Contains(line, "Kustomization.kustomize.toolkit.fluxcd.io/flux-system/app") {
				selected++
			}
		}
		assert.FatalIf(t, selected != 2, "consumer must be selected on both sides, got %d: %s", selected, stderr)
		assert.FatalIf(
			t,
			run == 1 && scoped != previous,
			"cross-source removal output changed between runs: %s\n%s",
			previous,
			scoped,
		)
		previous = scoped
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_OptionalInputChangedOnly(t *testing.T) {
	for _, route := range []string{"directory", "committed"} {
		t.Run(route, func(t *testing.T) {
			root, before := t.TempDir(), t.TempDir()
			repo := writeOptionalInputCLICluster(t, root, true)
			options := &git.CommitOptions{
				Author: &object.Signature{Name: "test", Email: "test@example.test", When: time.Unix(0, 0)},
			}
			base := testutil.GitCommitAll(t, repo, "base", options)
			testutil.CopyTree(t, root, before)
			testutil.WriteFile(
				t,
				root,
				"flux/optional.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: optional, namespace: flux-system}\ndata: {VALUE: available}\n",
			)
			baseline := []string{"--path-orig", filepath.Join(before, "flux")}
			if route == "committed" {
				testutil.GitCommitAll(t, repo, "input appears", options)
				baseline = []string{"--base", base.String()}
			}
			args := make([]string, 0, 11+len(baseline))
			args = append(args,
				"build",
				"all",
				"--path",
				filepath.Join(root, "flux"),
				"--concurrency",
				"2",
				"--no-progress",
				"--cache-dir",
				t.TempDir(),
				"--log-level",
				"debug",
			)
			requireCLIOKResult4 := harness.RequireCLIOK(t, args...)
			full, _ := requireCLIOKResult4.Stdout, requireCLIOKResult4.Stderr
			scopedArgs := append(slices.Clone(args), baseline...)
			requireCLIOKResult5 := harness.RequireCLIOK(t, scopedArgs...)
			scoped, stderr := requireCLIOKResult5.Stdout, requireCLIOKResult5.Stderr
			assert.FatalIf(
				t,
				scoped != full || !strings.Contains(scoped, "value: available"),
				"scoped/full output mismatch: scoped=%s\nfull=%s\nstderr=%s",
				scoped,
				full,
				stderr,
			)
			for _, id := range []string{"Kustomization.kustomize.toolkit.fluxcd.io/flux-system/app", "GitRepository.source.toolkit.fluxcd.io/flux-system/cluster"} {
				found := false
				for line := range strings.SplitSeq(stderr, "\n") {
					found = found || strings.Contains(line, "changed-only keep set") && strings.Contains(line, id)
				}
				assert.ErrorIf(t, !found, "selection diagnostic omitted %s: %s", id, stderr)
			}
			requireCLIOKResult6 := harness.RequireCLIOK(t, scopedArgs...)
			second, _ := requireCLIOKResult6.Stdout, requireCLIOKResult6.Stderr
			assert.FatalIf(t, second != scoped, "scoped output changed between runs: %s\n%s", scoped, second)
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_OptionalInputDiffSides(t *testing.T) {
	for _, operation := range []string{"appear", "remove", "projected appear", "projected remove", "required removal"} {
		t.Run(operation, func(t *testing.T) { e2EOptionalInputDiffSidesOperation(t, &operation) })
	}
}

func writeOptionalInputCLICluster(t *testing.T, root string, optional bool) *git.Repository {
	t.Helper()
	repo := testutil.GitInit(t, root)
	{
		_, err := repo.CreateRemote(
			&config.RemoteConfig{Name: "origin", URLs: []string{"https://example.test/cluster.git"}},
		)
		assert.FatalIf(t, err != nil, "%v", err)
	}
	optionalValue := "true"
	if !optional {
		optionalValue = "false"
	}
	testutil.WriteFile(t, root, "flux/cluster.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: cluster, namespace: flux-system}
spec: {url: https://example.test/cluster.git}
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: app, namespace: flux-system}
spec:
  path: ./apps/app
  sourceRef: {kind: GitRepository, name: cluster}
  postBuild:
    substitute: {SENTINEL: active}
    substituteFrom: [{kind: ConfigMap, name: optional, optional: `+optionalValue+`}]
`)
	testutil.WriteFile(t, root, "apps/app/kustomization.yaml", "resources: [output.yaml]\n")
	testutil.WriteFile(t, root, "apps/app/output.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: rendered, namespace: flux-system}
data: {value: "${VALUE:-fallback}", sentinel: "${SENTINEL}"}
`)
	return repo
}

func e2EOptionalInputDiffSidesOperation(t *testing.T, operation *string) {
	t.Helper()

	before, after := t.TempDir(), t.TempDir()
	for _, root := range []string{before, after} {
		writeOptionalInputCLICluster(t, root, (*operation) != "required removal")
		if strings.HasPrefix((*operation), "projected") {
			cluster, err := os.ReadFile(filepath.Join(root, "flux", "cluster.yaml"))
			assert.NoError(t, err)
			testutil.WriteFile(t, root, "flux/cluster.yaml", string(cluster)+"  dependsOn: [{name: inputs}]\n")
			testutil.WriteFile(
				t,
				root,
				"flux/inputs.yaml",
				testutil.KustomizationYAML(
					"inputs",
					"flux-system",
					"  path: ./inputs\n  targetNamespace: flux-system\n  sourceRef: {kind: GitRepository, name: cluster}\n",
				),
			)
			testutil.WriteFile(t, root, "inputs/kustomization.yaml", "resources: []\n")
			testutil.WriteFile(
				t,
				root,
				"flux/other.yaml",
				testutil.KustomizationYAML(
					"other",
					"other",
					"  path: ./apps/other\n  sourceRef: {kind: GitRepository, name: cluster, namespace: flux-system}\n  postBuild:\n    substitute: {SENTINEL: active}\n    substituteFrom: [{kind: ConfigMap, name: optional, optional: true}]\n",
				),
			)
			testutil.WriteFile(t, root, "apps/other/kustomization.yaml", "resources: [output.yaml]\n")
			testutil.WriteFile(
				t,
				root,
				"apps/other/output.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unaffected, namespace: other}\ndata: {value: '${VALUE:-fallback}'}\n",
			)
		}
	}
	present := after
	oldValue, newValue := "fallback", "available"
	if !strings.HasSuffix((*operation), "appear") {
		present, oldValue, newValue = before, "available", "fallback"
	}
	inputPath, namespace := "flux/optional.yaml", ", namespace: flux-system"
	if strings.HasPrefix((*operation), "projected") {
		inputPath, namespace = "inputs/optional.yaml", ""
		testutil.WriteFile(t, present, "inputs/kustomization.yaml", "resources: [optional.yaml]\n")
	}
	testutil.WriteFile(
		t,
		present,
		inputPath,
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: optional"+namespace+"}\ndata: {VALUE: available}\n",
	)
	flags := []string{"--concurrency", "2", "--no-progress", "--cache-dir", t.TempDir()}
	args := append(
		[]string{
			"diff",
			"all",
			"--path",
			filepath.Join(after, "flux"),
			"--path-orig",
			filepath.Join(before, "flux"),
			"-o",
			"diff",
		},
		flags...)
	if (*operation) == "required removal" {
		runCLIBuffersResult7 := harness.RunCLIBuffers(args...)
		_, stderr, code := runCLIBuffersResult7.Stdout, runCLIBuffersResult7.Stderr, runCLIBuffersResult7.Code
		assert.FatalIf(
			t,
			code != 1 || !strings.Contains(stderr, "optional"),
			"required removal exit=%d stderr=%s",
			code,
			stderr,
		)
		return
	}
	var rendered [2]string
	e2EOptionalInputDiffSidesOperationLoop(t, struct {
		before    *string
		after     *string
		flags     *[]string
		rendered  *[2]string
		operation **string
		newValue  *string
	}{before: &before, after: &after, flags: &flags, rendered: &rendered, operation: &operation, newValue: &newValue})
	fullArgs := append(
		[]string{
			"diff",
			"all",
			"--path",
			filepath.Join(rendered[1], "flux"),
			"--path-orig",
			filepath.Join(rendered[0], "flux"),
			"-o",
			"diff",
		},
		flags...)
	requireCLIOKResult8 := harness.RequireCLIOK(t, fullArgs...)
	fullDiff, _ := requireCLIOKResult8.Stdout, requireCLIOKResult8.Stderr
	requireCLIOKResult9 := harness.RequireCLIOK(t, args...)
	scoped, stderr := requireCLIOKResult9.Stdout, requireCLIOKResult9.Stderr
	assert.FatalIf(t, scoped != fullDiff || !strings.Contains(scoped, "-  value: "+oldValue) ||
		!strings.Contains(
			scoped,
			"+  value: "+newValue,
		), "paired diff mismatch: scoped=%s\nfull=%s\nstderr=%s", scoped, fullDiff, stderr)
	requireCLIOKResult10 := harness.RequireCLIOK(t, args...)
	second, _ := requireCLIOKResult10.Stdout, requireCLIOKResult10.Stderr
	assert.FatalIf(t, second != scoped, "diff changed between runs: %s\n%s", scoped, second)
}

func e2ECrossSourceOptionalInputRemovalLoop(t *testing.T, loopInputs struct {
	rendered *[2]string
	side     *int
	full     *string
},
) {
	t.Helper()

	for _, name := range []string{"app", "inputs"} {
		directory := name
		if name == "app" {
			directory = "apps/app"
		}
		testutil.WriteFile(
			t,
			loopInputs.rendered[(*loopInputs.side)],
			"flux/"+name+".yaml",
			testutil.KustomizationYAML(name, "flux-system", "  path: ./"+directory+"\n"),
		)
		output := ""
		var outputSb55 strings.Builder
		for doc := range strings.SplitSeq((*loopInputs.full), "---\n") {
			if strings.Contains(doc, "kustomize.toolkit.fluxcd.io/name: "+name+"\n") {
				outputSb55.WriteString("---\n" + doc)
			}
		}
		output += outputSb55.String()
		resources := "resources: []\n"
		if output != "" {
			resources = "resources: [output.yaml]\n"
			testutil.WriteFile(t, loopInputs.rendered[(*loopInputs.side)], directory+"/output.yaml", output)
		}
		testutil.WriteFile(t, loopInputs.rendered[(*loopInputs.side)], directory+"/kustomization.yaml", resources)
	}
}

func e2EOptionalInputDiffSidesOperationLoop(t *testing.T, loopInputs struct {
	before    *string
	after     *string
	flags     *[]string
	rendered  *[2]string
	operation **string
	newValue  *string
},
) {
	t.Helper()

	for i, root := range []string{(*loopInputs.before), (*loopInputs.after)} {
		buildArgs := append([]string{"build", "all", "--path", filepath.Join(root, "flux")}, (*loopInputs.flags)...)
		requireCLIOKResult11 := harness.RequireCLIOK(t, buildArgs...)
		full, _ := requireCLIOKResult11.Stdout, requireCLIOKResult11.Stderr
		loopInputs.rendered[i] = t.TempDir()
		testutil.GitInit(t, loopInputs.rendered[i])
		testutil.WriteFile(
			t,
			loopInputs.rendered[i],
			"flux/app.yaml",
			testutil.KustomizationYAML("app", "flux-system", "  path: ./apps/app\n"),
		)
		testutil.WriteFile(t, loopInputs.rendered[i], "apps/app/kustomization.yaml", "resources: [output.yaml]\n")
		appOutput, inputOutput, rootOutput := "", "", ""
		var inputOutputSb272 strings.Builder
		for doc := range strings.SplitSeq(full, "---\n") {
			switch {
			case strings.Contains(doc, "kustomize.toolkit.fluxcd.io/name: inputs"):

				inputOutputSb272.WriteString("---\n" + doc)

			case strings.Contains(doc, "kustomize.toolkit.fluxcd.io/name: app"):

				appOutput += "---\n" + doc

			case strings.Contains(doc, "\nkind: ConfigMap\n") && !strings.Contains(doc, "kustomize.toolkit.fluxcd.io/name:"):

				rootOutput += "---\n" + doc
			}
		}
		inputOutput += inputOutputSb272.String()
		if rootOutput != "" {
			testutil.WriteFile(t, loopInputs.rendered[i], "flux/plain.yaml", rootOutput)
		}
		testutil.WriteFile(t, loopInputs.rendered[i], "apps/app/output.yaml", appOutput)
		if strings.HasPrefix((*(*loopInputs.operation)), "projected") {
			testutil.WriteFile(
				t,
				loopInputs.rendered[i],
				"flux/inputs.yaml",
				testutil.KustomizationYAML("inputs", "flux-system", "  path: ./inputs\n"),
			)
			testutil.WriteFile(t, loopInputs.rendered[i], "inputs/kustomization.yaml", "resources: []\n")
			if inputOutput != "" {
				testutil.WriteFile(
					t,
					loopInputs.rendered[i],
					"inputs/kustomization.yaml",
					"resources: [output.yaml]\n",
				)
				testutil.WriteFile(t, loopInputs.rendered[i], "inputs/output.yaml", inputOutput)
			}
		}
		assert.FatalIf(
			t,
			root == (*loopInputs.after) && !strings.Contains(full, "value: "+(*loopInputs.newValue)),
			"current full build did not apply substitution/fallback: %s",
			full,
		)
	}
}
