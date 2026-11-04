package substitution_test

import (
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

func writeDependencyCluster(t *testing.T, root, expression string) {
	t.Helper()
	testutil.WriteFile(t, root, "flux/consumer.yaml", testutil.KustomizationYAML("consumer", "flux-system",
		"  path: ./app\n  dependsOn: [{name: '"+expression+"'}]\n  postBuild: {substitute: {PREFIX: 'prod-'}}\n"))
	testutil.WriteFile(
		t,
		root,
		"flux/config.yaml",
		testutil.KustomizationYAML("prod-config", "flux-system", "  path: ./config\n"),
	)
	testutil.WriteFile(t, root, "config/kustomization.yaml", "resources: []\n")
	testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"app/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: consumer-output}\ndata: {value: before}\n",
	)
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_DependsOnDefaults_SuppliedValues(t *testing.T) {
	for _, expression := range []string{"${PREFIX:-}config", "${PREFIX:-dev-}config"} {
		t.Run(expression, func(t *testing.T) {
			root := t.TempDir()
			writeDependencyCluster(t, root, expression)
			var previous string
			for range 2 {
				stdout, stderr, code := harness.RunCLIBuffers(
					"build",
					"all",
					"--path",
					root,
					"--concurrency",
					"2",
					"--cache-dir",
					t.TempDir(),
					"--no-progress",
				)
				assert.FatalIf(
					t,
					code != 0 || !strings.Contains(stdout, "name: consumer-output"),
					"resolved dependency must render consumer: exit=%d stdout=%s stderr=%s",
					code,
					stdout,
					stderr,
				)
				assert.FatalIf(
					t,
					previous != "" && stdout != previous,
					"output differs between runs: first=%s second=%s",
					previous,
					stdout,
				)
				previous = stdout
			}
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_DependsOnDefaults_ChangedOnly(t *testing.T) {
	root := t.TempDir()
	writeDependencyCluster(t, root, "${PREFIX:-}config")
	testutil.WriteFile(
		t,
		root,
		"flux/unrelated.yaml",
		testutil.KustomizationYAML("unrelated", "flux-system", "  path: ./unrelated\n"),
	)
	testutil.WriteFile(t, root, "unrelated/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"unrelated/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unrelated-output}\n",
	)
	testutil.WriteFile(t, root, "notes.txt", "before\n")
	repo := testutil.GitInit(t, root)
	testutil.GitCommitAll(
		t,
		repo,
		"init",
		&gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}},
	)
	testutil.WriteFile(
		t,
		root,
		"app/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: consumer-output}\ndata: {value: after}\n",
	)
	testutil.WriteFile(t, root, "notes.txt", "unrelated dirty file\n")
	for _, mode := range []string{"build", "diff"} {
		t.Run(mode, func(t *testing.T) {
			args := []string{
				mode,
				"all",
				"--path",
				filepath.Join(root, "flux"),
				"--base",
				"HEAD",
				"--concurrency",
				"2",
				"--cache-dir",
				t.TempDir(),
				"--no-progress",
			}
			if mode == "diff" {
				args = append(args, "-o", "diff")
			}
			stdout, stderr, code := harness.RunCLIBuffers(args...)
			assert.FatalIf(
				t,
				code != 0 || !strings.Contains(stdout, "value: after") || strings.Contains(stdout, "unrelated-output"),
				"changed consumer selection: exit=%d stdout=%s stderr=%s",
				code,
				stdout,
				stderr,
			)
			assert.FatalIf(
				t,
				mode == "diff" && !strings.Contains(stdout, "value: before"),
				"base side missing from diff: %s",
				stdout,
			)
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_DependsOnDefaults_EmittedChild(t *testing.T) {
	for _, supplied := range []bool{false, true} {
		name := "default-only"
		target := "dev-config"
		if supplied {
			name, target = "discovered-supplier", "prod-config"
		}
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			testutil.GitInit(t, root)
			testutil.WriteFile(
				t,
				root,
				"flux/parent.yaml",
				testutil.KustomizationYAML("parent", "flux-system", `  path: ./children
  patches:
  - target: {kind: Kustomization, name: child}
    patch: |-
      - op: add
        path: /spec/dependsOn
        value: [{name: '${PREFIX:-dev-}config'}]
`),
			)
			testutil.WriteFile(
				t,
				root,
				"flux/config.yaml",
				testutil.KustomizationYAML(target, "flux-system", "  path: ./config\n"),
			)
			testutil.WriteFile(t, root, "config/kustomization.yaml", "resources: []\n")
			if supplied {
				testutil.WriteFile(
					t,
					root,
					"flux/supplier.yaml",
					testutil.KustomizationYAML(
						"supplier",
						"flux-system",
						"  path: ./settings\n  postBuild: {substitute: {PREFIX: 'prod-'}}\n",
					),
				)
				testutil.WriteFile(t, root, "settings/kustomization.yaml", "resources: []\n")
			}
			testutil.WriteFile(t, root, "children/kustomization.yaml", "resources: [child.yaml]\n")
			testutil.WriteFile(
				t,
				root,
				"children/child.yaml",
				testutil.KustomizationYAML("child", "flux-system", "  path: ./app\n"),
			)
			testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(
				t,
				root,
				"app/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: emitted-output}\n",
			)
			stdout, stderr, code := harness.RunCLIBuffers(
				"build",
				"all",
				"--path",
				filepath.Join(root, "flux"),
				"--concurrency",
				"2",
				"--cache-dir",
				t.TempDir(),
				"--no-progress",
			)
			assert.FatalIf(
				t,
				code != 0 || !strings.Contains(stdout, "name: emitted-output"),
				"emitted child must resolve dependency on first admission: exit=%d stdout=%s stderr=%s",
				code,
				stdout,
				stderr,
			)
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_DependsOnDefaults_EmptyNameFails(t *testing.T) {
	root := t.TempDir()
	testutil.WriteFile(
		t,
		root,
		"flux/consumer.yaml",
		testutil.KustomizationYAML("consumer", "flux-system", "  path: ./app\n  dependsOn: [{name: '${PREFIX:-}'}]\n"),
	)
	testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, root, "app/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: should-not-render}\n")
	stdout, stderr, code := harness.RunCLIBuffers(
		"build",
		"all",
		"--path",
		filepath.Join(root, "flux"),
		"--concurrency",
		"2",
		"--cache-dir",
		t.TempDir(),
		"--no-progress",
	)
	assert.FatalIf(t, code != 1 || strings.Contains(stdout, "name: should-not-render") ||
		!strings.Contains(stderr, "required by flux-system/consumer") ||
		!strings.Contains(
			stderr,
			"not found",
		), "resolved-empty dependency must fail normally: exit=%d stdout=%s stderr=%s", code, stdout, stderr)
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_ReadyExpr_Kustomization(t *testing.T) {
	root := t.TempDir()
	writeDependencyCluster(t, root, "prod-config")
	testutil.WriteFile(
		t,
		root,
		"flux/consumer.yaml",
		testutil.KustomizationYAML("consumer", "flux-system", `  path: ./app
  dependsOn:
  - name: prod-config
    readyExpr: self.spec.postBuild.substitute.releaseVersion.startsWith(dep.spec.postBuild.substitute.releaseVersion) && dep.metadata.generation == dep.status.observedGeneration
  postBuild: {substitute: {releaseVersion: v1}}
`),
	)
	testutil.WriteFile(
		t,
		root,
		"flux/config.yaml",
		testutil.KustomizationYAML(
			"prod-config",
			"flux-system",
			"  path: ./config\n  postBuild: {substitute: {releaseVersion: v}}\n",
		),
	)
	testutil.WriteFile(t, root, "config/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"config/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: dependency-output}\n",
	)
	testutil.WriteFile(
		t,
		root,
		"app/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: consumer-output}\ndata: {version: '${releaseVersion}'}\n",
	)
	testutil.WriteFile(
		t,
		root,
		"flux/unrelated.yaml",
		testutil.KustomizationYAML("unrelated", "flux-system", "  path: ./unrelated\n"),
	)
	testutil.WriteFile(t, root, "unrelated/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"unrelated/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unrelated-output}\n",
	)
	var previousOut, previousErr string
	for run := range 2 {
		stdout, stderr := harness.RequireCLIOK(
			t,
			"build",
			"all",
			"--path",
			root,
			"--concurrency",
			"2",
			"--cache-dir",
			t.TempDir(),
			"--no-progress",
			"--log-level",
			"warn",
		)
		assert.FatalIf(
			t,
			!strings.Contains(stdout, "name: consumer-output") || !strings.Contains(stdout, "version: v1") ||
				strings.Contains(stderr, "readyExpr"),
			"desired-spec full render: stdout=%s stderr=%s",
			stdout,
			stderr,
		)
		assert.FatalIf(
			t,
			run == 1 && (stdout != previousOut || stderr != previousErr),
			"full render or warnings changed: stdout=%s stderr=%s",
			stdout,
			stderr,
		)
		previousOut, previousErr = stdout, stderr
	}
	repo := testutil.GitInit(t, root)
	testutil.GitCommitAll(
		t,
		repo,
		"init",
		&gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}},
	)
	harness.MutateFile(t, filepath.Join(root, "flux/consumer.yaml"), "releaseVersion: v1", "releaseVersion: v2")
	for _, mode := range []string{"build", "diff", "test"} {
		t.Run(mode, func(t *testing.T) {
			args := []string{
				mode,
				"all",
				"--path",
				filepath.Join(root, "flux"),
				"--base",
				"HEAD",
				"--concurrency",
				"2",
				"--cache-dir",
				t.TempDir(),
				"--no-progress",
				"--log-level",
				"warn",
			}
			if mode == "diff" {
				args = append(args, "-o", "diff")
			}
			stdout, stderr := harness.RequireCLIOK(t, args...)
			assert.FatalIf(t, strings.Contains(stderr, "readyExpr") || strings.Contains(stdout, "unrelated-output") ||
				strings.Contains(
					stdout,
					"dependency-output",
				), "changed-only selection: stdout=%s stderr=%s", stdout, stderr)
			switch {
			case mode == "test":

				harness.MustExtractSkipLine(t, stdout, "flux-system/prod-config")
				harness.MustExtractLine(t, stdout, "flux-system/consumer")

			case !strings.Contains(stdout, "version: v2") || mode == "diff" && !strings.Contains(stdout, "version: v1"):

				t.Fatalf("changed desired field or diff side missing: %s", stdout)
			}
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_ReadyExpr_EmittedIndexed(t *testing.T) {
	root := t.TempDir()
	testutil.WriteFile(
		t,
		root,
		"flux/parent.yaml",
		testutil.KustomizationYAML("parent", "flux-system", `  path: ./children
  patches:
  - target: {kind: Kustomization, name: child}
    patch: |-
      - op: replace
        path: /spec/postBuild/substitute/releaseVersion
        value: v1
`),
	)
	testutil.WriteFile(t, root, "children/kustomization.yaml", "namespace: apps\nresources: [child.yaml]\n")
	testutil.WriteFile(t, root, "children/child.yaml", testutil.KustomizationYAML("child", "authored", `  path: ./app
  targetNamespace: apps
  dependsOn:
  - name: config
    readyExpr: self.metadata.namespace == 'apps' && self.metadata.namespace == dep.metadata.namespace && self.spec.postBuild.substitute.releaseVersion == dep.spec.postBuild.substitute.releaseVersion
  postBuild:
    substitute: {releaseVersion: unpatched}
    substituteFrom: [{kind: ConfigMap, name: settings}]
`))
	testutil.WriteFile(
		t,
		root,
		"flux/indexer.yaml",
		testutil.KustomizationYAML("indexer", "flux-system", "  path: ./indexed\n  suspend: true\n"),
	)
	testutil.WriteFile(
		t,
		root,
		"indexed/config.yaml",
		testutil.KustomizationYAML(
			"config",
			"apps",
			"  path: ./config\n  postBuild: {substitute: {releaseVersion: v1}}\n",
		),
	)
	testutil.WriteFile(t, root, "config/kustomization.yaml", "resources: []\n")
	testutil.WriteFile(
		t,
		root,
		"flux/settings.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: apps}\ndata: {FLAVOR: promoted}\n",
	)
	testutil.WriteFile(t, root, "unowned/ks.yaml", testutil.KustomizationYAML("unowned", "apps", "  path: ./unowned\n"))
	testutil.WriteFile(t, root, "unowned/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"unowned/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unowned-output}\n",
	)
	testutil.WriteFile(
		t,
		root,
		"flux/unrelated.yaml",
		testutil.KustomizationYAML("unrelated", "apps", "  path: ./unrelated\n"),
	)
	testutil.WriteFile(t, root, "unrelated/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"unrelated/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unrelated-output}\n",
	)
	testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"app/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: emitted-output}\ndata: {version: '${releaseVersion}-${FLAVOR}', revision: before}\n",
	)
	repo := testutil.GitInit(t, root)
	testutil.GitCommitAll(
		t,
		repo,
		"init",
		&gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}},
	)
	for _, changed := range []bool{false, true} {
		t.Run(strconv.FormatBool(changed), func(t *testing.T) {
			args := []string{
				"build",
				"all",
				"--path",
				filepath.Join(root, "flux"),
				"--concurrency",
				"2",
				"--cache-dir",
				t.TempDir(),
				"--no-progress",
				"--log-level",
				"warn",
			}
			if changed {
				harness.MutateFile(t, filepath.Join(root, "app/cm.yaml"), "revision: before", "revision: after")
				args = append(args, "--base", "HEAD")
			}
			stdout, stderr := harness.RequireCLIOK(t, args...)
			for _, want := range []string{"name: emitted-output", "namespace: apps", "version: v1-promoted"} {
				assert.FatalIf(
					t,
					!strings.Contains(stdout, want),
					"emitted desired spec/indexed input missing %q: stdout=%s stderr=%s",
					want,
					stdout,
					stderr,
				)
			}
			revision := "before"
			if changed {
				revision = "after"
			}
			assert.FatalIf(t, !strings.Contains(stdout, "revision: "+revision) ||
				strings.Contains(
					stdout,
					"unrelated-output",
				) == changed, "changed-only consumer selection: stdout=%s stderr=%s", stdout, stderr)
			assert.FatalIf(
				t,
				strings.Contains(stdout, "unowned-output") || strings.Contains(stderr, "readyExpr"),
				"unowned sibling or readiness failure: stdout=%s stderr=%s",
				stdout,
				stderr,
			)
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_DependsOnProducedSource(t *testing.T) {
	for _, name := range []string{"interval", "substitution"} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			consumer := testutil.KustomizationYAML(
				"consumer",
				"apps",
				"  interval: 1h\n  path: ./app\n  sourceRef: {kind: GitRepository, name: input}\n  postBuild: {substitute: {VALUE: before}}\n",
			)
			testutil.WriteFile(t, root, "flux/consumer.yaml", consumer)
			testutil.WriteFile(
				t,
				root,
				"flux/producer.yaml",
				testutil.KustomizationYAML("producer", "apps", "  path: ./producer\n"),
			)
			testutil.WriteFile(t, root, "producer/kustomization.yaml", "resources: [dependency.yaml]\n")
			input := t.TempDir()
			testutil.WriteFile(t, input, "app/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(
				t,
				input,
				"app/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: consumer-output}\ndata: {value: '${VALUE}'}\n",
			)
			inputRepo := testutil.GitInit(t, input)
			testutil.GitCommitAll(
				t,
				inputRepo,
				"input",
				&gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}},
			)
			testutil.WriteFile(
				t,
				root,
				"producer/dependency.yaml",
				"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: input, namespace: apps}\nspec: {interval: 1h, url: 'file://"+filepath.ToSlash(
					input,
				)+"'}\n",
			)
			testutil.WriteFile(
				t,
				root,
				"flux/unrelated.yaml",
				testutil.KustomizationYAML("unrelated", "apps", "  path: ./unrelated\n"),
			)
			testutil.WriteFile(t, root, "unrelated/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(
				t,
				root,
				"unrelated/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unrelated-output}\n",
			)
			verifyProducedDependencyEdit(t, root, name == "interval")
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_DependsOnProducedTarget(t *testing.T) {
	for _, name := range []string{"interval", "substitution"} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			testutil.WriteFile(
				t,
				root,
				"flux/consumer.yaml",
				testutil.KustomizationYAML(
					"consumer",
					"apps",
					"  interval: 1h\n  path: ./app\n  dependsOn: [{name: generated-raw-done}]\n  postBuild: {substitute: {VALUE: before}}\n",
				),
			)
			testutil.WriteFile(
				t,
				root,
				"flux/producer.yaml",
				testutil.KustomizationYAML("producer", "apps", "  path: ./producer\n"),
			)
			testutil.WriteFile(
				t,
				root,
				"producer/kustomization.yaml",
				"resources: [dependency.yaml]\nnamePrefix: generated-\nnameSuffix: -done\n",
			)
			testutil.WriteFile(
				t,
				root,
				"producer/dependency.yaml",
				testutil.KustomizationYAML("raw", "apps", "  path: ./dependency\n"),
			)
			testutil.WriteFile(t, root, "dependency/kustomization.yaml", "resources: []\n")
			testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(
				t,
				root,
				"app/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: consumer-output}\ndata: {value: '${VALUE}'}\n",
			)
			testutil.WriteFile(
				t,
				root,
				"flux/unrelated.yaml",
				testutil.KustomizationYAML("unrelated", "apps", "  path: ./unrelated\n"),
			)
			testutil.WriteFile(t, root, "unrelated/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(
				t,
				root,
				"unrelated/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unrelated-output}\n",
			)
			verifyProducedDependencyEdit(t, root, name == "interval")
		})
	}
}
