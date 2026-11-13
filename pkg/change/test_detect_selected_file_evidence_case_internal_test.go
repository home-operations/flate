package change

import (
	"context"
	"errors"
	"io/fs"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/go-git/go-git/v6/plumbing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

func testDetectSelectedFileEvidenceCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	fixture := selectedFileEvidenceFixture(*testCaseInput)
	files, want := fixture.files, fixture.paths
	roots := [2]string{"/before", "/after"}
	if *testCaseInput == "nested" || *testCaseInput == "nested_file" {
		roots[1] = roots[0] + "/peer"
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
		assert.FatalIf(t, err != nil, "%v", err)
		inputs[i] = &store.SourceArtifact{
			FS:                 input,
			LocalPath:          roots[i],
			ContentFingerprint: gittree.ContentFingerprint,
		}
		if (*testCaseInput) != "neither" && ((*testCaseInput) != "before" || i != 1) &&
			((*testCaseInput) != "after" || i != 0) {
			{
				_, err := gittree.ContentFingerprint(t.Context(), input)
				assert.FatalIf(t, err != nil, "%v", err)
			}
		}
	}
	opens := readers[0].opens.Load() + readers[1].opens.Load()
	lists := readers[0].lists.Load() + readers[1].lists.Load()
	stats := readers[0].stats.Load() + readers[1].stats.Load()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if (*testCaseInput) == "canceled" {
		cancel()
	}
	config := DetectConfig{}
	if *testCaseInput == "ignore_exclude" || *testCaseInput == "ignore_reinclude" {
		config.Candidates = map[string]bool{".sourceignore": false}
	}
	got, err := Detect(ctx, inputs[0], inputs[1], config)
	if (*testCaseInput) == "canceled" {
		assert.FatalIf(t, !errors.Is(err, context.Canceled), "Detect error = %v, want canceled", err)
		return
	}
	assert.FatalIf(t, err != nil, "%v", err)
	if !slices.Equal(got.Paths(), want) {
		t.Fatalf("paths = %v, want %v", got.Paths(), want)
	}
	if (*testCaseInput) == "before" || (*testCaseInput) == "after" || (*testCaseInput) == "neither" {
		assert.FatalIf(
			t,
			readers[0].opens.Load()+readers[1].opens.Load() == opens,
			"%v",
			"missing pair evidence skipped byte comparison",
		)
	} else {
		assert.FatalIf(
			t,
			readers[0].opens.Load()+readers[1].opens.Load() != opens ||
				readers[0].lists.Load()+readers[1].lists.Load() != lists,
			"%v",
			"completed evidence was read or walked again",
		)
	}
	if *testCaseInput == "mode" || *testCaseInput == "reverse_mode" || *testCaseInput == "normalized_mode" {
		assert.Equal(t, readers[0].stats.Load()+readers[1].stats.Load() > stats, true)
	}
}

type selectedFileFixture struct {
	files [2]fstest.MapFS
	paths []string
}

func selectedFileEvidenceFixture(testCase string) selectedFileFixture {
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
	switch testCase {
	case "empty":
		files = [2]fstest.MapFS{{}, {"added": {Data: []byte("new")}}}
		want = []string{"added"}
	case "nested", "nested_file":
		files = [2]fstest.MapFS{
			{"same": {Data: []byte("same")}, "peer/hidden": {Data: []byte("excluded")}},
			{"same": {Data: []byte("same")}},
		}
		if testCase == "nested_file" {
			files[0] = fstest.MapFS{"same": {Data: []byte("same")}, "peer": {Data: []byte("excluded")}}
		}
		want = nil
	case "mode", "reverse_mode", "normalized_mode":
		modes := [2]fs.FileMode{0o644, 0o755}
		switch testCase {
		case "reverse_mode":
			modes = [2]fs.FileMode{0o755, 0o644}
		case "normalized_mode":
			modes = [2]fs.FileMode{0o700, 0o755}
		}
		for side := range files {
			files[side] = fstest.MapFS{"file": {Data: []byte("same"), Mode: modes[side]}}
		}
		want = []string{"file"}
		if testCase == "normalized_mode" {
			want = nil
		}
	case "ignore_exclude", "ignore_reinclude":
		for side := range files {
			rules := "# keep\n"
			if (side == 0) == (testCase == "ignore_reinclude") {
				rules = "member\n"
			}
			files[side] = fstest.MapFS{".sourceignore": {Data: []byte(rules)}, "member": {Data: []byte("same")}}
		}
		want = []string{".sourceignore", "member"}
	}
	return selectedFileFixture{files: files, paths: want}
}
