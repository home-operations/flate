package cache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/errdef"

	"github.com/home-operations/flate/internal/source/safepath"
)

func (c *Cache) publish(
	ctx context.Context,
	tmp *os.File,
	payload digest.Digest,
	e *entry,
) (ocispec.Descriptor, string, error) {
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return ocispec.Descriptor{}, "", fmt.Errorf("rewind cache payload: %w", err)
	}
	info, err := tmp.Stat()
	if err != nil {
		return ocispec.Descriptor{}, "", err //nolint:wrapcheck // The cache producer and stream adapters preserve the original error tree for replay and io sentinel handling.
	}
	stored := e.storedDescriptor(payload, info.Size())
	if err := c.store.Push(ctx, stored, tmp); err != nil {
		if !errors.Is(err, errdef.ErrAlreadyExists) {
			return stored, "", fmt.Errorf("publish cache payload: %w", err)
		}
		repair, repairErr := c.repairPayload(tmp, &stored)
		return stored, repair, repairErr
	}
	c.snapshotDirty.Store(true)
	return stored, "", nil
}

func (e *entry) storedDescriptor(payload digest.Digest, size int64) ocispec.Descriptor {
	stored := ocispec.Descriptor{
		MediaType:   "application/octet-stream",
		Digest:      payload,
		Size:        size,
		Annotations: maps.Clone(e.descriptor.Annotations),
	}
	if stored.Annotations == nil {
		stored.Annotations = make(map[string]string)
	}
	for k, v := range map[string]string{
		"resolved-at":         e.resolved.Format(time.RFC3339Nano),
		"upstream-digest":     e.descriptor.Digest.String(),
		"upstream-size":       strconv.FormatInt(e.descriptor.Size, 10),
		"upstream-media-type": e.descriptor.MediaType,
	} {
		stored.Annotations[prefix+k] = v
	}
	return stored
}

func (p *payload) cleanup(root *os.Root, repairName string) error {
	var cleanup error
	if p.discard != nil {
		cleanup = p.discard()
	}
	if p.file != nil {
		cleanup = errors.Join(cleanup, p.file.Close())
	}
	for _, name := range [2]string{p.name, repairName} {
		if name != "" {
			cleanup = errors.Join(cleanup, root.Remove(name))
		}
	}
	return cleanup
}

func (c *Cache) persistPayload(
	ctx context.Context,
	key *acquireKey,
	staged *payload,
	e *entry,
) (ocispec.Descriptor, string, error) {
	var stored ocispec.Descriptor
	var repairName string
	var err error
	if staged.writer == nil {
		if stageErr := staged.stage(); stageErr != nil {
			return ocispec.Descriptor{}, repairName, stageErr
		}
	}
	if staged.unchanged {
		stored = e.storedDescriptor(e.stored.Digest, e.stored.Size)
	} else {
		stored, repairName, err = c.publish(ctx, staged.file, staged.hash.Digest(), e)
		if err != nil {
			return ocispec.Descriptor{}, repairName, err
		}
		if restoreErr := key.producer.restore(ctx, c, stored, e); restoreErr != nil {
			return ocispec.Descriptor{}, repairName, restoreErr
		}
	}
	closeErr := staged.file.Close()
	staged.file = nil
	removeErr := c.root.Remove(staged.name)
	if removeErr == nil {
		staged.name = ""
	}
	if joinErr := errors.Join(closeErr, removeErr); joinErr != nil {
		return ocispec.Descriptor{}, repairName, joinErr
	}
	return stored, repairName, nil
}

func (c *Cache) repairPayload(tmp *os.File, stored *ocispec.Descriptor) (string, error) {
	info, err := os.Stat(filepath.Join(c.root.Name(), blobPath(stored.Digest)))
	if err != nil {
		return "", err //nolint:wrapcheck // Filesystem adapters retain fs.PathError operation, path, and concrete error identity.
	}
	if info.Size() == stored.Size {
		return "", nil
	}
	{
		name, err := filepath.Rel(c.root.Name(), tmp.Name())
		if err != nil {
			return "", fmt.Errorf("locate cache payload: %w", err)
		}
		name, err = safepath.ArchiveName(name)
		if err != nil {
			return "", err
		}
		if err := tmp.Chmod(immutableFileMode); err != nil {
			return "", err //nolint:wrapcheck // The cache producer and stream adapters preserve the original error tree for replay and io sentinel handling.
		}
		repair := acquirePath(uuid.NewString())
		if err := c.root.Link(name, repair); err != nil {
			return "", err //nolint:wrapcheck // Filesystem adapters retain fs.PathError operation, path, and concrete error identity.
		}
		if err := c.root.Rename(repair, blobPath(stored.Digest)); err != nil {
			return repair, err //nolint:wrapcheck // Filesystem adapters retain fs.PathError operation, path, and concrete error identity.
		}
	}
	return "", nil
}
