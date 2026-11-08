package cache

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
)

func TestNew_InvalidLayoutPreservesIndex(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	c := cachetest.New(t, root, New)
	{
		err := c.Close()
		assert.NoError(t, err)
	}

	index := filepath.Join(root, indexPath())
	want, err := os.ReadFile(index)
	assert.NoError(t, err)
	{
		err := os.WriteFile(
			filepath.Join(root, ociDir, "oci-layout"),
			[]byte(`{"imageLayoutVersion":`),
			0o600,
		)
		assert.NoError(t, err)
	}
	{
		_, err := New(t.Context(), root)
		assert.FatalIf(t, err == nil, "%s", "invalid layout accepted")
	}

	{
		got, err := os.ReadFile(index)
		assert.FatalIf(
			t,
			err != nil || !bytes.Equal(got, want),
			"unrelated layout error changed index: %q %v",
			got,
			err,
		)
	}
}
