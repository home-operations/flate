package baseline

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	sourcecache "github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/internal/testutil/networktest"
)

func TestAutoResolve_UnreadableCheckoutRetainsError(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	base := initRepoWithFile(t, root, "input.yaml", "before")
	testutil.WriteFile(t, root, "input.yaml", "after")
	file := filepath.Join(root, "input.yaml")
	assert.NoError(t, os.Chmod(file, 0))
	t.Cleanup(func() { assert.NoError(t, os.Chmod(file, 0o600)) })
	if _, err := os.ReadFile(file); err == nil {
		t.Skip("file modes do not restrict this user")
	} else {
		assert.FatalIf(t, !errors.Is(err, fs.ErrPermission), "fixture read failed unexpectedly: %v", err)
	}
	_, err := AutoResolve(
		t.Context(),
		Config{
			Network: networktest.New(t),
			Path:    root,
			Base:    base.String(),
			Cache:   cachetest.New(t, "", sourcecache.New),
		},
	)
	assert.FatalIf(t, !errors.Is(err, fs.ErrPermission), "checkout read error lost: %v", err)
}
