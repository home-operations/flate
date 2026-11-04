package sourceref_test

import (
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_SourceWarnings_BootstrapSubmoduleAliases(t *testing.T) {
	root := t.TempDir()
	repo := testutil.GitInit(t, root)
	_, err := repo.CreateRemote(&config.RemoteConfig{Name: "origin", URLs: []string{"https://example.invalid/cluster"}})
	assert.NoError(t, err)
	for _, name := range []string{"one", "two"} {
		testutil.WriteFile(
			t,
			root,
			"flux/"+name+".yaml",
			"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: "+name+", namespace: flux-system}\nspec: {interval: 1m, url: https://example.invalid/cluster}\n---\n"+testutil.KustomizationYAML(
				name,
				"flux-system",
				"  path: ./apps\n  sourceRef: {kind: GitRepository, name: "+name+"}\n",
			),
		)
	}
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"apps/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: changed}\ndata: {value: before}\n",
	)
	base := testutil.GitCommitAll(
		t,
		repo,
		"base",
		&gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}},
	)
	commit, err := repo.CommitObject(base)
	assert.NoError(t, err)
	tree, err := commit.Tree()
	assert.NoError(t, err)
	tree.Entries = append(tree.Entries, object.TreeEntry{Name: "module", Mode: filemode.Submodule, Hash: base})
	slices.SortFunc(tree.Entries, func(a, b object.TreeEntry) int { return strings.Compare(a.Name, b.Name) })
	encoded := repo.Storer.NewEncodedObject()
	assert.NoError(t, tree.Encode(encoded))
	commit.TreeHash, err = repo.Storer.SetEncodedObject(encoded)
	assert.NoError(t, err)
	encoded = repo.Storer.NewEncodedObject()
	assert.NoError(t, commit.Encode(encoded))
	base, err = repo.Storer.SetEncodedObject(encoded)
	assert.NoError(t, err)
	assert.NoError(
		t,
		repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewTagReferenceName("baseline"), base)),
	)
	testutil.WriteFile(
		t,
		root,
		"apps/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: changed}\ndata: {value: after}\n",
	)
	cache := t.TempDir()
	var wantOut, wantErr string
	for run := range 2 {
		requireCLIOKResult1 := harness.RequireCLIOK(
			t,
			"diff",
			"all",
			"--path",
			filepath.Join(root, "flux"),
			"--base",
			"baseline",
			"--cache-dir",
			cache,
			"--concurrency=2",
			"--no-progress",
		)
		stdout, stderr := requireCLIOKResult1.Stdout, requireCLIOKResult1.Stderr
		_, snapshots, hasOrig := strings.Cut(stderr, "orig snapshot:")
		orig, current, hasCurrent := strings.Cut(snapshots, "current snapshot:")
		assert.FatalIf(t, !hasOrig || !hasCurrent || strings.Count(stderr, "[SkippedSubmodule]") != 1 ||
			strings.Count(orig, "[SkippedSubmodule]") != 1 ||
			strings.Contains(current, "[SkippedSubmodule]") ||
			!strings.Contains(orig, "module") ||
			!strings.Contains(
				orig,
				"flux-system/flux-system",
			), "bootstrap submodule attribution: stdout=%s stderr=%s", stdout, stderr)
		assert.FatalIf(t, !strings.Contains(stdout, "before") || !strings.Contains(stdout, "after") ||
			strings.Contains(stdout, "skipping submodule") ||
			strings.Contains(stdout, "[SkippedSubmodule]"), "diff product: %s", stdout)
		if run == 0 {
			wantOut, wantErr = stdout, stderr
		} else {
			assert.Diff(t, stdout, wantOut)
			assert.Diff(t, stderr, wantErr)
		}
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_SourceWarnings(t *testing.T) {
	for _, condition := range []string{"aliases", "unavailable-ref"} {
		t.Run(condition, func(t *testing.T) { e2ESourceWarningsCondition(t, &condition) })
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_LoaderIncidentalParseFailure(t *testing.T) {
	for _, site := range []string{"sibling", "component"} {
		t.Run(site, func(t *testing.T) {
			root := t.TempDir()
			testutil.GitInit(t, root)
			testutil.WriteFile(
				t,
				root,
				"flux.yaml",
				testutil.KustomizationYAML("apps", "flux-system", "  path: ./apps\n"),
			)
			testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(t, root, "apps/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: rendered}\n")
			if site == "component" {
				testutil.WriteFile(t, root, "kustomization.yaml", "resources: [flux.yaml, component-indexer.yaml]\n")
				testutil.WriteFile(
					t,
					root,
					"component-indexer.yaml",
					testutil.KustomizationYAML(
						"component-indexer",
						"flux-system",
						"  path: ./component\n  suspend: true\n",
					),
				)
				testutil.WriteFile(
					t,
					root,
					"component/kustomization.yaml",
					"apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nresources: [bad.yaml]\n",
				)
				testutil.WriteFile(
					t,
					root,
					"component/bad.yaml",
					"apiVersion: v1\nkind: ConfigMap\nbroken: [unclosed\n",
				)
			} else {
				testutil.WriteFile(t, root, "apps/bad.yaml", "apiVersion: v1\nkind: ConfigMap\nbroken: [unclosed\n")
			}
			for _, verb := range []string{"build", "test"} {
				t.Run(verb, func(t *testing.T) {
					var previous string
					for range 2 {
						requireCLIOKResult2 := harness.RequireCLIOK(t, verb, "all", "--path", root, "--concurrency=2")
						stdout, stderr := requireCLIOKResult2.Stdout, requireCLIOKResult2.Stderr
						assert.FatalIf(t, strings.Contains(stdout+stderr, "[InputParseFailed]") ||
							strings.Contains(stdout+stderr, "bad.yaml") ||
							strings.Contains(
								stdout,
								"warnings (",
							), "parse destination: stdout=%s stderr=%s", stdout, stderr)
						assert.FatalIf(t, verb == "build" && !strings.Contains(stdout, "name: rendered") ||
							verb == "test" && !strings.Contains(stdout, "passed"), "parse product: %s", stdout)
						assert.FatalIf(t, previous != "" && stderr != previous, "parse order changed: %s", stderr)
						previous = stderr
					}
				})
			}
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_KustomizationParseFailure(t *testing.T) {
	root := t.TempDir()
	testutil.GitInit(t, root)
	testutil.WriteFile(t, root, "flux/apps.yaml", testutil.KustomizationYAML("apps", "flux-system", "  path: ./apps\n"))
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [unclosed\n")
	for _, verb := range []string{"build", "test"} {
		runCLIBuffersResult3 := harness.RunCLIBuffers(
			verb,
			"all",
			"--path",
			filepath.Join(root, "flux"),
			"--concurrency=2",
		)
		stdout, stderr, code := runCLIBuffersResult3.Stdout, runCLIBuffersResult3.Stderr, runCLIBuffersResult3.Code
		assert.FatalIf(t, code != 1 || !strings.Contains(stdout+stderr, "flux kustomize generator") ||
			!strings.Contains(stdout+stderr, "error converting YAML") ||
			strings.Contains(stdout, "[InputParseFailed]") ||
			strings.Contains(
				stderr,
				"[InputParseFailed]",
			), "malformed builder input: exit=%d stdout=%s stderr=%s", code, stdout, stderr)
	}
}

func e2ESourceWarningsCondition(t *testing.T, condition *string) {
	t.Helper()

	root := t.TempDir()
	repo := testutil.GitInit(t, root)
	{
		_, err := repo.CreateRemote(
			&config.RemoteConfig{Name: "origin", URLs: []string{"https://example.invalid/cluster"}},
		)
		assert.FatalIf(t, err != nil, "%v", err)
	}
	ref, category, message := "", "BootstrapAliases", "aliased multiple bootstrap sources"
	if (*condition) == "unavailable-ref" {
		ref = "  provider: github\n  ref: {tag: missing}\n  secretRef: {name: unavailable-auth}\n"
		category, message = "SourceRefUnavailable", "declared source ref cannot be satisfied locally"
	}
	for _, name := range []string{"one", "two"} {
		testutil.WriteFile(
			t,
			root,
			"flux/"+name+".yaml",
			"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: "+name+", namespace: flux-system}\nspec:\n  interval: 1m\n  url: https://example.invalid/cluster\n"+ref+"---\n"+testutil.KustomizationYAML(
				name,
				"flux-system",
				"  path: ./"+name+"\n  sourceRef: {kind: GitRepository, name: "+name+"}\n",
			),
		)
		testutil.WriteFile(t, root, name+"/kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(
			t,
			root,
			name+"/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: "+name+"}\n",
		)
	}
	testutil.GitCommitAll(
		t,
		repo,
		"fixture",
		&gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}},
	)
	for _, verb := range []string{"build", "test", "get"} {
		t.Run(
			verb,
			func(t *testing.T) {
				e2ESourceWarningsConditionVerb(t, &verb, &root, struct {
					condition **string
					category  *string
					message   *string
				}{condition: &condition, category: &category, message: &message})
			},
		)
	}
}

func e2ESourceWarningsConditionVerb(t *testing.T, verb *string, root *string, capturedInputs struct {
	condition **string
	category  *string
	message   *string
},
) {
	t.Helper()

	var wantErr, wantOut string
	for run := range 2 {
		requireCLIOKResult4 := harness.RequireCLIOK(
			t,
			(*verb),
			"all",
			"--path",
			filepath.Join((*root), "flux"),
			"--allow-missing-kinds=Secret,ConfigMap",
			"--concurrency=2",
			"--no-progress",
		)
		stdout, stderr := requireCLIOKResult4.Stdout, requireCLIOKResult4.Stderr
		count := 1
		if (*(*capturedInputs.condition)) == "unavailable-ref" {
			count = 2
		}
		assert.FatalIf(
			t,
			strings.Count(stderr, "["+(*capturedInputs.category)+"]") != count || !strings.Contains(stderr, (*capturedInputs.message)) ||
				strings.Contains(stdout, (*capturedInputs.message)) ||
				strings.Contains(stdout, "warnings (") ||
				strings.Contains(stdout, "notes ("),
			"%s: stdout=%s stderr=%s",
			(*(*capturedInputs.condition)),
			stdout,
			stderr,
		)
		assert.FatalIf(t, (*(*capturedInputs.condition)) == "aliases" && (*verb) == "build" &&
			(!strings.Contains(stdout, "name: one") || !strings.Contains(stdout, "name: two")) ||
			(*verb) == "test" && !strings.Contains(stdout, "passed") ||
			(*verb) == "get" &&
				(!strings.Contains(stdout, "Kustomization/one") || !strings.Contains(stdout, "Kustomization/two")), "product lost: %s", stdout)
		switch {
		case run == 0:

			wantErr, wantOut = stderr, stdout

		case stderr != wantErr || (*verb) != "test" && stdout != wantOut:

			t.Fatal("warning order/count or product changed")
		}
	}
}
