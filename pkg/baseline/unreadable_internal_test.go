package baseline

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func TestAutoResolve_UnreadableCheckoutRetainsError(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	base := initRepoWithFile(t, root, "input.yaml", "before")
	testutil.WriteFile(t, root, "input.yaml", "after")
	file := filepath.Join(root, "input.yaml")
	assert.NoError(t, os.Chmod(file, 0))
	t.Cleanup(func() { assert.NoError(t, os.Chmod(file, 0o600)) })
	_, err := os.ReadFile(file)
	assert.FatalIf(t, !errors.Is(err, fs.ErrPermission), "fixture is readable: %v", err)
	_, err = AutoResolve(t.Context(), root, base.String(), cachetest.New(t, "", sourcecache.New))
	assert.FatalIf(t, !errors.Is(err, fs.ErrPermission), "checkout read error lost: %v", err)
}
