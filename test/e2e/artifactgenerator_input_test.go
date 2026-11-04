package e2e_test

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/assert"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_ArtifactGenerator_InputSourceChangedOnly(t *testing.T) {
	root := t.TempDir()
	repo := testutil.GitInit(t, root)
	{
		_, err := repo.CreateRemote(
			&config.RemoteConfig{Name: "origin", URLs: []string{"https://example.test/cluster.git"}},
		)
		assert.FatalIf(t, err != nil, "%v", err)
	}
	options := &git.CommitOptions{
		Author: &object.Signature{Name: "test", Email: "test@example.test", When: time.Unix(0, 0)},
	}
	testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [cm.yaml]\n")
	var content [2]string
	for i, value := range []string{"before", "after"} {
		testutil.WriteFile(
			t,
			root,
			"app/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: generated-config, namespace: apps}\ndata: {value: "+value+"}\n",
		)
		content[i] = testutil.GitCommitAll(t, repo, value+" content", options).String()
	}
	source := func(ref string) string {
		return fmt.Sprintf(
			"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: input, namespace: apps}\nspec: {url: https://example.test/cluster.git, ref: {commit: %s}}\n",
			ref,
		)
	}
	testutil.WriteFile(t, root, "entry/generator.yaml", `apiVersion: source.extensions.fluxcd.io/v1beta1
kind: ArtifactGenerator
metadata: {name: generator, namespace: apps, uid: authored}
spec:
  sources: [{alias: repo, kind: GitRepository, name: input}]
  artifacts:
    - name: generated
      copy:
        - {from: '@repo/app/kustomization.yaml', to: '@artifact/render/kustomization.yaml'}
        - {from: '@repo/app/cm.yaml', to: '@artifact/render/cm.yaml'}
`)
	testutil.WriteFile(
		t,
		root,
		"entry/consumer.yaml",
		testutil.KustomizationYAML(
			"consumer",
			"apps",
			"  path: ./render\n  sourceRef: {kind: ExternalArtifact, name: generated}\n",
		),
	)
	testutil.WriteFile(
		t,
		root,
		"entry/independent.yaml",
		"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: independent, namespace: apps}\nspec: {url: https://example.test/cluster.git}\n",
	)
	testutil.WriteFile(
		t,
		root,
		"entry/unrelated.yaml",
		testutil.KustomizationYAML(
			"unrelated",
			"apps",
			"  path: ./unrelated\n  sourceRef: {kind: GitRepository, name: independent}\n",
		),
	)
	testutil.WriteFile(t, root, "unrelated/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"unrelated/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unrelated-config, namespace: apps}\n",
	)
	var revisions [2]string
	for i, ref := range content {
		testutil.WriteFile(t, root, "entry/source.yaml", source(ref))
		revisions[i] = testutil.GitCommitAll(t, repo, fmt.Sprintf("input %d", i), options).String()
	}
	before := t.TempDir()
	testutil.CopyTree(t, root, before)
	testutil.WriteFile(t, before, "entry/source.yaml", source(content[0]))
	beforeRepo, err := git.PlainOpen(before)
	assert.NoError(t, err)
	testutil.GitCommitAll(t, beforeRepo, "reverse input", options)
	flags := []string{"--concurrency=2", "--no-progress", "--cache-dir", t.TempDir(), "--skip-kinds="}
	for i, dir := range []string{before, root} {
		args := append([]string{"build", "all", "--path", filepath.Join(dir, "entry")}, flags...)
		requireCLIOKResult1 := harness.RequireCLIOK(t, args...)
		stdout, _ := requireCLIOKResult1.Stdout, requireCLIOKResult1.Stderr
		value := []string{"before", "after"}[i]
		if !strings.Contains(stdout, "value: "+value) || strings.Count(stdout, "name: generated-config") != 1 {
			t.Fatalf("input %s did not resolve %s content:\n%s", content[i], value, stdout)
		}
		t.Logf("full build resolved %s content %s", value, content[i])
	}
	for _, route := range []string{"directory", "committed"} {
		t.Run(route, func(t *testing.T) {
			e2EArtifactGeneratorInputSourceChangedOnlyRoute(t, &root, &before, struct {
				revisions *[2]string
				flags     *[]string
				route     *string
			}{revisions: &revisions, flags: &flags, route: &route})
		})
	}
}

func e2EArtifactGeneratorInputSourceChangedOnlyRoute(t *testing.T, root *string, before *string, capturedInputs struct {
	revisions *[2]string
	flags     *[]string
	route     *string
},
) {
	t.Helper()

	for _, tc := range []struct {
		name, command, current, original, baseline, oldValue, newValue string
	}{
		{"build", "build", (*root), (*before), capturedInputs.revisions[0], "before", "after"},
		{"forward", "diff", (*root), (*before), capturedInputs.revisions[0], "before", "after"},
		{"reverse", "diff", (*before), (*root), capturedInputs.revisions[1], "after", "before"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := append(
				[]string{tc.command, "all", "--path", filepath.Join(tc.current, "entry")},
				(*capturedInputs.flags)...)
			if (*capturedInputs.route) == "directory" {
				args = append(args, "--path-orig", filepath.Join(tc.original, "entry"))
			} else {
				args = append(args, "--base", tc.baseline)
			}
			if tc.command == "diff" {
				args = append(args, "-o", "diff")
			}
			requireCLIOKResult2 := harness.RequireCLIOK(t, args...)
			stdout, stderr := requireCLIOKResult2.Stdout, requireCLIOKResult2.Stderr
			assert.FatalIf(t, !strings.Contains(stdout, "value: "+tc.newValue) ||
				strings.Contains(
					stdout,
					"unrelated-config",
				), "changed input lost consumer output or selected unrelated workload:\n%s\n%s", stdout, stderr)
			assert.FatalIf(
				t,
				tc.command == "build" && strings.Count(stdout, "name: generated-config") != 1,
				"consumer output was missing or duplicated:\n%s",
				stdout,
			)
			assert.FatalIf(t, tc.command == "diff" &&
				(strings.Count(stdout, "-  value: "+tc.oldValue) != 1 || strings.Count(stdout, "+  value: "+tc.newValue) != 1), "diff lost one generated side:\n%s", stdout)
			requireCLIOKResult3 := harness.RequireCLIOK(t, args...)
			again, _ := requireCLIOKResult3.Stdout, requireCLIOKResult3.Stderr
			assert.FatalIf(t, stdout != again, "output changed between equal runs:\n%s\n%s", stdout, again)
		})
	}
}
