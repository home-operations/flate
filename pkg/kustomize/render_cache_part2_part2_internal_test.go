package kustomize

import (
	"reflect"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func testRenderCacheFramesCase(t *testing.T, testCaseInput *testRenderCacheFramesCaseInputs) {
	t.Helper()

	root := testutil.WriteTree(t, map[string]string{
		"kustomization.yaml": existingKustomization("cm.yaml"),
		"cm.yaml":            cmYAML("fresh"),
	})
	if testCaseInput.tc.invalid {
		testutil.WriteFile(t, root, "cm.yaml", "apiVersion: [\n")
	}
	cacheDir := t.TempDir()
	cache := newTestTreeCache(t, cacheDir)
	source := localSource(t, root)
	if testCaseInput.tc.immutable {
		source.Key, source.Content = "immutable-pin", "immutable-generation"
		source.Revision, source.Digest = "revision", "digest"
	}
	req := RenderRequest{Source: source, Label: root, SubPath: ".", Spec: demoSpec}
	var fresh []map[string]any
	var freshErr error
	if testCaseInput.tc.corrupt {
		fresh, _, freshErr = testutil.RenderMaps(
			Render(t.Context(), newTestTreeCache(t, ""), req, sourcecache.RetryConfig{}),
		)
	}
	key := renderKey(&req, root, nil)
	frame := testRenderFrame(t, testCaseInput.tc.snapshot, testRenderPayload(t, "STALE"))
	if (*testCaseInput.tc).malformed {
		frame = []byte{0, 0, 0, 1, 'x'}
	} else if testCaseInput.tc.corrupt || (*testCaseInput.tc).name == "mismatched" {
		frame = testRenderFrame(t, testCaseInput.tc.snapshot, []byte("corrupt"))
	} else if testCaseInput.tc.empty {
		frame = testRenderFrame(t, testCaseInput.tc.snapshot, testRenderEncoding(t, nil))
	}
	publishTestFrame(
		t,
		cache.owner,
		sourcecache.BlobKey{Content: key, Render: sourcecache.RenderPending},
		frame,
	)
	out, receipt22, err := testutil.RenderMaps(Render(t.Context(), cache, req, sourcecache.RetryConfig{}))
	if err == nil {
		{
			recordErr := cache.RecordRender(
				t.Context(),
				RenderAcceptance{Owner: demoSpec.Named(), Receipt: receipt22},
			)
			assert.NoError(t, recordErr)
		}
	}

	if testCaseInput.tc.invalid {
		{
			assert.FatalIf(
				t,
				err == nil || freshErr == nil || err.Error() != freshErr.Error() || len(out) != 0,
				"fresh render failure: %v, %v; want %v",
				out,
				err,
				freshErr,
			)
		}
		return
	}
	{
		assert.NoError(t, err)
	}
	if testCaseInput.tc.corrupt {
		assert.NoError(t, freshErr)
		assert.Diff(t, out, fresh)
		again, receipt23, err := testutil.RenderMaps(Render(t.Context(), cache, req, sourcecache.RetryConfig{}))
		if err == nil {
			{
				recordErr := cache.RecordRender(
					t.Context(),
					RenderAcceptance{Owner: demoSpec.Named(), Receipt: receipt23},
				)
				assert.NoError(t, recordErr)
			}
		}

		assert.NoError(t, err)
		assert.Diff(t, again, fresh)
	}
	if testCaseInput.tc.immutable {
		{
			assert.FatalIf(
				t,
				!hasRenderField(out, "witness", "STALE"),
				"immutable pin replayed mutable validation: %s",
				out,
			)
		}
	} else if testCaseInput.tc.empty {
		{
			assert.FatalIf(t, len(out) != 0, "complete empty frame missed: %s", out)
		}
	} else {
		assert.FatalIf(t, !hasRenderField(out, "name", "fresh"), "candidate prevented a fresh render: %s", out)
	}
	cache.Close()
	{
		err := cache.owner.Close()
		assert.NoError(t, err)
	}
	cache = newTestTreeCache(t, cacheDir)
	_, stored, hit := readTestRenderFrame(t, cache, key)
	if hit != (*testCaseInput.eligible) || hit && !reflect.DeepEqual(stored, out) {
		t.Fatalf("completed frame = %s, %v; eligible = %v", stored, hit, renderPersistenceEligible(t))
	}
	if testCaseInput.tc.corrupt {
		again, receipt24, err := testutil.RenderMaps(Render(t.Context(), cache, req, sourcecache.RetryConfig{}))
		if err == nil {
			{
				recordErr := cache.RecordRender(
					t.Context(),
					RenderAcceptance{Owner: demoSpec.Named(), Receipt: receipt24},
				)
				assert.NoError(t, recordErr)
			}
		}

		assert.NoError(t, err)
		assert.Diff(t, again, fresh)
		if *testCaseInput.eligible {
			t.Log("verified corrupt payload recovery, replacement, subsequent and reopened hits")
		}
	}
}
