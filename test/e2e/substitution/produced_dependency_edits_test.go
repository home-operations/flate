package substitution_test

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

func verifyProducedDependencyEdit(t *testing.T, root string, intervalOnly bool) {
	t.Helper()
	testutil.WriteFile(
		t,
		root,
		"flux/kustomization.yaml",
		"resources: [consumer.yaml, producer.yaml, unrelated.yaml]\n",
	)
	oldValue, newValue := "VALUE: before", "VALUE: after"
	if intervalOnly {
		oldValue, newValue = "interval: 1h", "interval: 2h"
	}
	repo := testutil.GitInit(t, root)
	testutil.GitCommitAll(
		t,
		repo,
		"init",
		&gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}},
	)
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
	}
	requireCLIOKResult1 := harness.RequireCLIOK(t, args...)
	before, _ := requireCLIOKResult1.Stdout, requireCLIOKResult1.Stderr
	payloadArgs := append([]string{"build", "ks", "consumer"}, args[2:]...)
	requireCLIOKResult2 := harness.RequireCLIOK(t, payloadArgs...)
	payloadBefore, _ := requireCLIOKResult2.Stdout, requireCLIOKResult2.Stderr
	harness.MutateFile(t, filepath.Join(root, "flux", "consumer.yaml"), oldValue, newValue)
	requireCLIOKResult3 := harness.RequireCLIOK(t, args...)
	after, _ := requireCLIOKResult3.Stdout, requireCLIOKResult3.Stderr
	requireCLIOKResult4 := harness.RequireCLIOK(t, payloadArgs...)
	payloadAfter, _ := requireCLIOKResult4.Stdout, requireCLIOKResult4.Stderr
	assert.FatalIf(t, !strings.Contains(before, "value: before") || !strings.Contains(before, "unrelated-output") ||
		!strings.Contains(
			after,
			"unrelated-output",
		), "full-render controls missing output: before=%s after=%s", before, after)
	assert.FatalIf(t, !strings.Contains(payloadBefore, "name: consumer-output") ||
		!strings.Contains(payloadAfter, "name: consumer-output") ||
		intervalOnly && payloadBefore != payloadAfter ||
		!intervalOnly &&
			!strings.Contains(after, "value: after"), "full-render edit: before=%s after=%s", before, after)
	for side, output := range []string{before, after} {
		docs, err := testutil.DecodeDocs(strings.NewReader(output))
		assert.NoError(t, err)
		declarations := 0
		for _, doc := range docs {
			id := manifest.DocIdentity(doc)
			if id.Group != manifest.FluxKustomizeDomain || id.Kind != manifest.KindKustomization ||
				id.Namespace != "apps" ||
				id.Name != "consumer" {
				continue
			}
			declarations++
			want := "1h"
			if intervalOnly && side == 1 {
				want = "2h"
			}
			assert.FatalIf(
				t,
				doc["spec"].(map[string]any)["interval"] != want,
				"root-applied consumer interval: %s",
				output,
			)
		}
		assert.FatalIf(t, declarations != 1, "root-applied consumer count=%d, want 1: %s", declarations, output)
	}
	for _, mode := range []string{"build", "diff"} {
		t.Run(mode, func(t *testing.T) { verifyProducedDependencyEditMode(t, &mode, &args, &intervalOnly) })
	}
}

func verifyProducedDependencyEditMode(t *testing.T, mode *string, args *[]string, intervalOnly *bool) {
	t.Helper()

	changedArgs := append([]string{(*mode)}, (*args)[1:]...)
	changedArgs = append(changedArgs, "--base", "HEAD")
	if (*mode) == "diff" {
		format := "diff"
		if *intervalOnly {
			format = "json"
		}
		changedArgs = append(changedArgs, "-o", format)
	}
	var previous string
	for run := range 2 {
		requireCLIOKResult5 := harness.RequireCLIOK(t, changedArgs...)
		stdout, _ := requireCLIOKResult5.Stdout, requireCLIOKResult5.Stderr
		assert.FatalIf(
			t,
			strings.Contains(stdout, "unrelated-output"),
			"unrelated sibling selected: %s",
			stdout,
		)
		switch {
		case (*mode) == "diff" && (*intervalOnly):

			var result struct {
				Changes []struct {
					Kind      string `json:"Kind"`
					Namespace string `json:"Namespace"`
					Name      string `json:"Name"`
					Status    string `json:"Status"`
					Parent    struct {
						Kind string `json:"Kind"`
						Name string `json:"Name"`
						Path string `json:"Path"`
					} `json:"Parent"`
					Old map[string]any `json:"Old"`
					New map[string]any `json:"New"`
				} `json:"Changes"`
				Suppressed []any `json:"Suppressed"`
			}
			assert.NoError(t, json.Unmarshal([]byte(stdout), &result))
			assert.FatalIf(
				t,
				len(result.Changes) != 1 || len(result.Suppressed) != 0,
				"interval edit changed producer payload or lost root declaration: %s",
				stdout,
			)
			change := result.Changes[0]
			assert.Equal(t, change.Kind, manifest.KindKustomization)
			assert.Equal(t, change.Namespace, "apps")
			assert.Equal(t, change.Name, "consumer")
			assert.Equal(t, change.Status, "changed")
			assert.Equal(t, change.Parent.Kind, "")
			assert.Equal(t, change.Parent.Name, "")
			assert.Equal(t, change.Parent.Path, "flux")
			expected := manifest.DeepCopyMap(change.Old)
			assert.Equal(t, expected["spec"].(map[string]any)["interval"], any("1h"))
			expected["spec"].(map[string]any)["interval"] = "2h"
			assert.Diff(t, change.New, expected)
			consumerArgs := append([]string{"diff", "ks", "consumer"}, (*args)[2:]...)
			consumerArgs = append(consumerArgs, "--base", "HEAD", "-o", "diff")
			requireCLIOKResult6 := harness.RequireCLIOK(t, consumerArgs...)
			consumerDiff, _ := requireCLIOKResult6.Stdout, requireCLIOKResult6.Stderr
			assert.FatalIf(t, consumerDiff != "", "interval edit changed consumer payload: %s", consumerDiff)

		case *intervalOnly:

			docs, err := testutil.DecodeDocs(strings.NewReader(stdout))
			assert.NoError(t, err)
			declarations := map[string]int{}
			for _, doc := range docs {
				id := manifest.DocIdentity(doc)
				assert.Equal(t, id.Group, manifest.FluxKustomizeDomain)
				assert.Equal(t, id.Kind, manifest.KindKustomization)
				assert.Equal(t, id.Namespace, "apps")
				declarations[id.Name]++
				if id.Name == "consumer" {
					assert.Equal(t, doc["spec"].(map[string]any)["interval"], any("2h"))
				}
			}
			assert.Diff(t, declarations, map[string]int{"consumer": 1, "producer": 1, "unrelated": 1})

		default:

			value := "after"
			assert.FatalIf(t, !strings.Contains(stdout, "value: "+value) ||
				(*mode) == "diff" &&
					!strings.Contains(
						stdout,
						"value: before",
					), "selected consumer or diff side missing: %s", stdout)
		}
		assert.FatalIf(
			t,
			run == 1 && stdout != previous,
			"output differs between runs: first=%s second=%s",
			previous,
			stdout,
		)
		previous = stdout
	}
}
