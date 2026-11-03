package sourceref_test

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

func e2ESourceRefPinnedSourceDiscoveredAfterConsumerNameCommandCase(
	t *testing.T,
	command **string,
	root **string,
	capturedInputs struct {
		concurrency    *string
		capturedInputs *struct {
			tt **struct {
				name          string
				appsFile      string
				sourcesFile   string
				sourcesName   string
				sourceRefName string
				sourceURL     string
				noRef         bool
			}
			transport **sourceRefLoader
			wantError *string
		}
		previous *string
	},
) {
	t.Helper()

	args := []string{
		(*(*command)), "all", "--path", filepath.Join((*(*root)), "flux"),
		"--concurrency", (*capturedInputs.concurrency), "--cache-dir", t.TempDir(),
	}
	if (*(*command)) == "diff" {
		args = append(args, "--base", "HEAD", "-o", "diff")
	}
	out, stderr, code := harness.RunCLIBuffers(args...)
	if (*capturedInputs.capturedInputs.tt).sourceURL != "" {
		wantCode, wantReason := 0, "external source GitRepository.source.toolkit.fluxcd.io/flux-system/pinned is outside the local tree; not rendered"
		if (*(*command)) == "build" {
			wantCode, wantReason = 1, "repository not found"
		}
		if code != wantCode || !strings.Contains(stderr, wantReason) ||
			strings.Contains(stderr, "loader: decode") ||
			(*capturedInputs.capturedInputs.transport).calls.Load() != 0 ||
			((*(*command)) == "diff" && out != "") {
			t.Fatalf(
				"external source must release the working-tree error: exit=%d transport=%d\n%s\nstderr:\n%s",
				code,
				(*capturedInputs.capturedInputs.transport).calls.Load(),
				out,
				stderr,
			)
		}
		return
	}
	if (*capturedInputs.capturedInputs.wantError) != "" {
		wantReason := "kustomize build ./apps"
		if code != 1 || !strings.Contains(stderr, wantReason) ||
			(*capturedInputs.capturedInputs.transport).calls.Load() != 0 ||
			((*(*command)) == "diff" && out != "") {
			t.Fatalf(
				"unpinned Kustomization failure: exit=%d transport=%d\n%s\nstderr:\n%s",
				code,
				(*capturedInputs.capturedInputs.transport).calls.Load(),
				out,
				stderr,
			)
		}
		return
	}
	if code != 0 || (*capturedInputs.capturedInputs.transport).calls.Load() != 0 ||
		((*(*command)) == "build" && (!strings.Contains(out, "value: pinned") ||
			!strings.Contains(out, "value: pinned-deleted") || !strings.Contains(out, "value: pinned-extra") ||
			strings.Contains(out, "value: dirty") || strings.Contains(out, "value: newer"))) ||
		((*(*command)) == "diff" && out != "") {
		t.Fatalf(
			"late source pin: exit=%d transport=%d\n%s\nstderr:\n%s",
			code,
			(*capturedInputs.capturedInputs.transport).calls.Load(),
			out,
			stderr,
		)
	}
	assert.FatalIf(
		t,
		(*capturedInputs.concurrency) == "8" && out != (*capturedInputs.previous),
		"output differs across concurrency levels (-want +got):\n%s",
		cmp.Diff((*capturedInputs.previous), out),
	)
	(*capturedInputs.previous) = out
}

func e2EPinnedDiffRootPinStageScopeName(t *testing.T, side *struct {
	name     string
	current  string
	original string
	before   string
	after    string
}, capturedInputs *struct {
	scope     *string
	stage     **string
	transport ***sourceRefLoader
},
) {
	t.Helper()

	args := []string{
		"diff",
		"all",
		"--path",
		filepath.Join(side.current, (*capturedInputs.scope)),
		"--path-orig",
		filepath.Join(side.original, (*capturedInputs.scope)),
		"--concurrency",
		"2",
		"--cache-dir",
		t.TempDir(),
	}
	out, _ := harness.RequireCLIOK(t, append(args, "-o", "diff")...)
	if (*(*capturedInputs.stage)) == "dirty-only" {
		again, _ := harness.RequireCLIOK(t, append(args, "-o", "diff")...)
		assert.Equal(t, again, out)
		assert.Equal(t, out, "")
	}
	switch {
	case (*(*capturedInputs.stage)) == "dirty-only" && (*capturedInputs.scope) == ".":

		product, _ := harness.RequireCLIOK(t, append(args, "-o", "json")...)
		var result struct {
			Changes    []any `json:"Changes"`
			Suppressed []any `json:"Suppressed"`
		}
		assert.NoError(t, json.Unmarshal([]byte(product), &result))
		assert.Equal(t, len(result.Changes), 0)
		assert.Equal(t, len(result.Suppressed), 0)

	case (*(*capturedInputs.stage)) == "dirty-only":

		assert.FatalIf(t, out != "", "dirty content changed the root-pin diff:\n%s", out)

	default:

		assertSourceValueDiff(t, out, side.before, side.after)
	}
	args[0] = "build"
	out, _ = harness.RequireCLIOK(t, args...)
	switch {
	case (*(*capturedInputs.stage)) == "dirty-only" && (*capturedInputs.scope) == ".":

		docs, err := testutil.DecodeDocs(strings.NewReader(out))
		assert.NoError(t, err)
		ids := map[string]int{}
		for _, doc := range docs {
			name, namespace := manifest.DocMetadata(doc)
			ids[manifest.DocKind(doc)+"/"+namespace+"/"+name]++
			if manifest.DocKind(doc) == manifest.KindConfigMap {
				assert.Diff(t, doc["data"].(map[string]any)["value"], "v1.0.0")
				metadata := doc["metadata"].(map[string]any)
				labels, _ := metadata["labels"].(map[string]any)
				assert.Equal(t, labels[manifest.FluxKustomizeDomain+"/name"], "apps")
				assert.Equal(
					t,
					labels[manifest.FluxKustomizeDomain+"/namespace"],
					"flux-system",
				)
			}
		}
		assert.Diff(
			t,
			ids,
			map[string]int{
				"Kustomization/flux-system/apps":    1,
				"GitRepository/flux-system/cluster": 1,
			},
		)

	case (*(*capturedInputs.stage)) == "dirty-only":

		assert.FatalIf(t, out != "", "dirty content selected the pinned root:\n%s", out)

	case !strings.Contains(out, "value: "+side.after) || strings.Contains(out, "value: dirty"):

		t.Fatalf("tag change did not select the pinned root:\n%s", out)
	}
	if (*(*capturedInputs.transport)).calls.Load() != 0 {
		t.Fatalf("root pin accessed transport %d times", (*(*capturedInputs.transport)).calls.Load())
	}
}
