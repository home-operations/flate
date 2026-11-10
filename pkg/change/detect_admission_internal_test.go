package change

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

func TestDetect_AdmittedCandidates(t *testing.T) {
	t.Parallel()
	candidates := map[string]bool{"safe": false, "link": false, "alias": false, "alias/file": false}
	for _, tc := range []struct {
		name           string
		candidates     map[string]bool
		completed, raw bool
	}{
		{name: "full"},
		{name: "candidates", candidates: candidates},
		{name: "completed", candidates: candidates, completed: true},
		{name: "raw_full", raw: true},
		{name: "raw_candidates", candidates: candidates, raw: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var inputs [2]*store.SourceArtifact
			for side := range inputs {
				root := t.TempDir()
				testutil.WriteFile(t, root, "safe", strconv.Itoa(side))
				testutil.WriteFile(t, root, "real/file", "same")
				assert.NoError(t, os.Symlink("missing-"+strconv.Itoa(side), filepath.Join(root, "link")))
				assert.NoError(t, os.Symlink("real", filepath.Join(root, "alias")))
				input := &store.SourceArtifact{FS: os.DirFS(root), LocalPath: root}
				if !tc.raw {
					var err error
					input, err = gittree.AdmitDirectory(t.Context(), &gitrepo.Location{ContentRoot: root})
					assert.NoError(t, err)
				}
				inputs[side] = input
				if tc.completed {
					_, err := gittree.ContentFingerprint(t.Context(), input.FS)
					assert.NoError(t, err)
				}
			}
			got, err := Detect(t.Context(), inputs[0], inputs[1], DetectConfig{Candidates: tc.candidates})
			assert.NoError(t, err)
			assert.Diff(t, got.Paths(), []string{"safe"})
		})
	}
}
