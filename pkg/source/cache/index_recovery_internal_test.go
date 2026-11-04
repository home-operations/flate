package cache

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

//nolint:paralleltest // The test changes the process logging destination.
//nolint:paralleltest // The test changes process logging state.
func TestNew_UnreadableIndexRebuilds(t *testing.T) {
	saved := slog.Default()
	t.Cleanup(func() { slog.SetDefault(saved) })
	var logs bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	root := t.TempDir()
	c := cachetest.New(t, root, New)
	key := ArchiveKey{Content: "source-index-recovery"}
	var payloadTar bytes.Buffer
	testutil.WriteTar(t, &payloadTar, map[string]string{"file": "same"}, 0o644)
	data := payloadTar.Bytes()
	{
		_, _, _, err := c.Acquire(
			t.Context(),
			key,
			Policy{MaxAge: time.Hour},
			producer(data, &ocispec.Descriptor{}),
		)
		assert.NoError(t, err)
	}
	{
		err := c.Close()
		assert.NoError(t, err)
	}

	index := filepath.Join(root, indexPath())
	{
		err := os.WriteFile(index, []byte(`{"schemaVersion":`), 0o600)
		assert.NoError(t, err)
	}

	c = cachetest.New(t, root, New)
	{
		got := logs.String()
		assert.FatalIf(t, strings.Count(got, "level=DEBUG msg=\"cache: rebuilding unreadable store index\"") != 1 ||
			strings.Contains(got, "level=WARN"), "index recovery log: %s", got)
	}
	calls := 0
	input, _, _, err := c.Acquire(
		t.Context(),
		key,
		Policy{MaxAge: time.Hour},
		func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
			calls++
			return producer(data, &ocispec.Descriptor{})(ctx, w)
		},
	)
	assert.NoError(t, err)

	{
		got, err := fs.ReadFile(input, "file")
		assert.FatalIf(
			t,
			err != nil || string(got) != "same" || calls != 1,
			"recovery: content=%q calls=%d err=%v",
			got,
			calls,
			err,
		)
	}
}

func TestNew_IndexRecoveryRejectsEscapingParent(t *testing.T) {
	t.Parallel()
	for _, component := range []string{"parent", "final"} {
		t.Run(component, func(t *testing.T) {
			t.Parallel()
			testNewIndexRecoveryRejectsEscapingParentCase(
				t,
				&component,
			)
		},
		)
	}
}

func testNewIndexRecoveryRejectsEscapingParentCase(
	t *testing.T, component *string,
) {
	t.Helper()

	c := cachetest.New(t, t.TempDir(), New)
	outside := cachetest.New(t, t.TempDir(), New)
	{
		err := errors.Join(c.Close(), outside.Close())
		assert.NoError(t, err)
	}

	index := filepath.Join(outside.root.Name(), indexPath())
	testutil.WriteFile(
		t,
		filepath.Join(outside.root.Name(), ociDir),
		ocispec.ImageIndexFile,
		`{"schemaVersion":`,
	)
	link, target := filepath.Join(c.root.Name(), ociDir), filepath.Join(outside.root.Name(), ociDir)
	if (*component) == "final" {
		link = filepath.Join(link, ocispec.ImageIndexFile)
		target = index
	}
	{
		err := os.Rename(link, link+"-saved")
		assert.NoError(t, err)
	}
	{
		err := os.Symlink(target, link)
		assert.NoError(t, err)
	}

	before := sweepSnapshot(t, outside.root.Name())
	recovered, err := New(t.Context(), c.root.Name())
	if (*component) == "parent" {
		{
			_, ok := errors.AsType[*CacheError](
				err,
			)
			assert.FatalIf(t, !ok || recovered != nil ||
				!errors.Is(err, manifest.ErrFlux), "parent recovery: cache=%v, error=%v", recovered, err)
		}
		{
			removeErr2 := os.Remove(link)
			assert.NoError(t, removeErr2)
		}
		{
			renameErr := os.Rename(link+"-saved", link)
			assert.NoError(t, renameErr)
		}
	} else {
		assert.NoError(t, err)
		{
			err := recovered.Close()
			assert.NoError(t, err)
		}

		info, err := os.Lstat(link)
		assert.FatalIf(t, err != nil || !info.Mode().IsRegular(), "rebuilt index: %v, %v", info, err)
		{
			err := os.Remove(link + "-saved")
			assert.NoError(t, err)
		}
	}
	{
		diff := cmp.Diff(before, sweepSnapshot(t, outside.root.Name()))
		assert.FatalIf(t, diff != "", "outside index changed (-want +got):\n%s", diff)
	}
	reopened := cachetest.New(t, c.root.Name(), New)
	{
		err := reopened.Close()
		assert.NoError(t, err)
	}
}
