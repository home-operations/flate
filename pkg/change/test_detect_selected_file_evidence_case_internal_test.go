package change

import (
	"context"
	"errors"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/go-git/go-git/v5/plumbing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

func testDetectSelectedFileEvidenceCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	files := [2]fstest.MapFS{
		{
			"same":          {Data: []byte("same")},
			"changed":       {Data: []byte("old")},
			"gone":          {Data: []byte("gone")},
			".sourceignore": {Data: []byte("ignored\n")},
			"ignored":       {Data: []byte("old")},
		},
		{
			"same":          {Data: []byte("same")},
			"changed":       {Data: []byte("new")},
			"added":         {Data: []byte("new")},
			".sourceignore": {Data: []byte("ignored\n")},
			"ignored":       {Data: []byte("new")},
		},
	}
	want := []string{"added", "changed", "gone"}
	roots := [2]string{"/before", "/after"}
	if (*testCaseInput) == "empty" {
		files = [2]fstest.MapFS{{}, {"added": {Data: []byte("new")}}}
		want = []string{"added"}
	}
	if (*testCaseInput) == "nested" || (*testCaseInput) == "nested_file" {
		files = [2]fstest.MapFS{
			{"same": {Data: []byte("same")}, "peer/hidden": {Data: []byte("excluded")}},
			{"same": {Data: []byte("same")}},
		}
		if (*testCaseInput) == "nested_file" {
			files[0] = fstest.MapFS{"same": {Data: []byte("same")}, "peer": {Data: []byte("excluded")}}
		}
		roots[1] = roots[0] + "/peer"
		want = nil
	}
	var inputs [2]*store.SourceArtifact
	var readers [2]*digestReadFS
	for i := range inputs {
		readers[i] = &digestReadFS{FS: files[i]}
		input, _, err := gittree.New(
			t.Context(),
			nil,
			plumbing.ZeroHash,
			gittree.ViewOptions{Mode: gittree.Direct, Current: readers[i]},
		)
		{
			assert.FatalIf(t, err != nil, "%v", err)
		}
		inputs[i] = &store.SourceArtifact{
			FS:                 input,
			LocalPath:          roots[i],
			ContentFingerprint: gittree.ContentFingerprint,
		}
		if (*testCaseInput) != "neither" && ((*testCaseInput) != "before" || i != 1) &&
			((*testCaseInput) != "after" || i != 0) {
			{
				{
					_, err := gittree.ContentFingerprint(t.Context(), input)
					assert.FatalIf(t, err != nil, "%v", err)
				}
			}
		}
	}
	opens := readers[0].opens.Load() + readers[1].opens.Load()
	lists := readers[0].lists.Load() + readers[1].lists.Load()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if (*testCaseInput) == "canceled" {
		cancel()
	}
	got, err := Detect(ctx, inputs[0], inputs[1])
	if (*testCaseInput) == "canceled" {
		{
			assert.FatalIf(t, !errors.Is(err, context.Canceled), "Detect error = %v, want canceled", err)
		}
		return
	}
	{
		assert.FatalIf(t, err != nil, "%v", err)
	}
	if !slices.Equal(got.Paths(), want) {
		t.Fatalf("paths = %v, want %v", got.Paths(), want)
	}
	if (*testCaseInput) == "before" || (*testCaseInput) == "after" || (*testCaseInput) == "neither" {
		{
			assert.FatalIf(
				t,
				readers[0].opens.Load()+readers[1].opens.Load() == opens,
				"%v",
				"missing pair evidence skipped byte comparison",
			)
		}
	} else {
		assert.FatalIf(
			t,
			readers[0].opens.Load()+readers[1].opens.Load() != opens ||
				readers[0].lists.Load()+readers[1].lists.Load() != lists,
			"%v",
			"completed evidence was read or walked again",
		)
	}
}
