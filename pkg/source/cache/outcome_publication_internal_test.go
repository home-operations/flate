package cache

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/content"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestOutcome_PublicationFailure(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"tag", "push", "existing config corrupt"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			testOutcomePublicationFailureCase(t, &scenario)
		},
		)
	}
}

func testOutcomePublicationFailureCase(t *testing.T, scenario *string,
) {
	t.Helper()

	c := cachetest.New(t, t.TempDir(), New)
	live := outcomeFailure(connReset())
	o := newOutcome(live, time.Now().UTC(), 1, &ocispec.Descriptor{})
	switch *scenario {
	case "tag":
		index := filepath.Join(c.root.Name(), indexPath())
		{
			err := os.Remove(index)
			assert.NoError(t, err)
		}
		{
			err := os.Mkdir(index, 0o750)
			assert.NoError(t, err)
		}

	case "push":
		{
			err := c.root.RemoveAll("oci/blobs")
			assert.NoError(t, err)
		}
		{
			err := c.root.WriteFile("oci/blobs", []byte("blocked"), 0o600)
			assert.NoError(t, err)
		}

	case "existing config corrupt":
		config, data, _ := o.encode()
		pushOutcomeFixture(t, c, config, data)
		desc := content.NewDescriptorFromBytes(outcomeConfig, config)
		{
			err := c.root.Chmod(blobPath(desc.Digest), 0o600)
			assert.NoError(t, err)
		}
		{
			err := c.root.WriteFile(blobPath(desc.Digest), []byte("corrupt"), 0o600)
			assert.NoError(t, err)
		}
	}
	if _, err := c.publishOutcome(
		t.Context(),
		"failure",
		o,
		live,
	); err == nil || !errors.Is(err, syscall.ECONNRESET) ||
		!errors.Is(err, manifest.ErrFlux) {
		t.Fatalf("publication lost live failure: %v", err)
	} else {
		_, ok := errors.AsType[*CacheError](
			err,
		)
		assert.FatalIf(t, !ok, "publication not CacheError: %v", err)
	}
}
