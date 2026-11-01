package cache

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"maps"
	"path"
	"slices"
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
	"oras.land/oras-go/v2/errdef"
)

const historyAnnotation = "flate.admission-history"
const acceptedRenderAnnotation = "flate.accepted-render"

// AcceptedInventory publishes one complete inventory and its source evidence.
type AcceptedInventory[T io.WriterTo] struct {
	Key, Lookup, Acceptance string
	History                 RenderHistory
	Content                 T
}

// RecordInventory atomically replaces accepted content and its searchable provenance.
func RecordInventory[T io.WriterTo](ctx context.Context, c *Cache, inventory AcceptedInventory[T]) error {
	descriptor, _, enabled, err := c.RenderMetadata(ctx, inventory.Key, inventory.Lookup)
	if err != nil && !errors.Is(err, errdef.ErrNotFound) {
		return err
	}
	if !enabled {
		return nil
	}
	metadata, err := inventory.History.metadata(inventory.Acceptance)
	if err != nil {
		return err
	}
	current := descriptor.Annotations[acceptedRenderAnnotation] == inventory.Acceptance
	for name, value := range metadata {
		current = current && descriptor.Annotations[name] == value
	}
	if current {
		return nil
	}
	if metadata == nil {
		metadata = make(map[string]string, 1)
	}
	metadata[acceptedRenderAnnotation] = inventory.Acceptance
	content := inventory.Content
	_, _, _, err = c.Blob(ctx, BlobKey{Content: inventory.Key, Render: RenderAccepted}, Policy{Retry: RetryConfig{Attempts: 1}}, func(_ context.Context, output ProducerInput) (ProducerResult, error) {
		_, err := content.WriteTo(output)
		return ProducerResult{Annotations: metadata}, err
	})
	return err
}

// RenderHistory supplies source paths associated with one accepted inventory.
type RenderHistory struct {
	Owner   manifest.NamedResource
	Sources []manifest.SourceEvidence
}

func (h RenderHistory) metadata(acceptance string) (map[string]string, error) {
	if h.Sources == nil || acceptance == "" {
		return nil, nil
	}
	data, err := encodeHistory(&h, acceptance)
	if err != nil {
		return nil, &CacheError{err}
	}
	return map[string]string{historyAnnotation: data}, nil
}

type historyPaths struct {
	Source string
	Paths  []historyPath
}

type historyPath struct {
	Path    string
	Subtree bool
}

type historyRecord struct {
	Acceptance string
	Build      string
	Owner      manifest.NamedResource
	Sources    []historyPaths
}

func encodeHistory(history *RenderHistory, acceptance string) (string, error) {
	record := historyRecord{Build: processIdentity, Owner: history.Owner, Acceptance: acceptance}
	paths := map[string][]historyPath{}
	identities := map[manifest.SourceIdentity]string{}
	for _, evidence := range history.Sources {
		if evidence.Source == (manifest.SourceIdentity{}) {
			continue
		}
		id := identities[evidence.Source]
		if id == "" {
			id = manifest.Fingerprint(evidence.Source)
			identities[evidence.Source] = id
		}
		file := path.Clean(evidence.Path)
		if !fs.ValidPath(file) {
			continue
		}
		paths[id] = append(paths[id], historyPath{Path: file, Subtree: evidence.Subtree})
	}
	for _, source := range slices.Sorted(maps.Keys(paths)) {
		files := paths[source]
		slices.SortFunc(files, func(a, b historyPath) int {
			if order := strings.Compare(a.Path, b.Path); order != 0 {
				return order
			}
			if a.Subtree == b.Subtree {
				return 0
			}
			if a.Subtree {
				return 1
			}
			return -1
		})
		record.Sources = append(record.Sources, historyPaths{Source: source, Paths: slices.Compact(files)})
	}
	data, err := json.Marshal(record)
	return string(data), err
}

// ProducersForPaths returns producers whose accepted source paths reach the supplied changes.
// The result is positive admission evidence, never proof that an inventory remains complete.
func (c *Cache) ProducersForPaths(ctx context.Context, query []manifest.SourceEvidence) ([]manifest.NamedResource, error) {
	if err := ctx.Err(); err != nil {
		return nil, &CacheError{err}
	}
	if !c.CanPersistRenders() || len(query) == 0 {
		return nil, nil
	}
	changes := map[string]map[string]struct{}{}
	identities := map[manifest.SourceIdentity]string{}
	for _, evidence := range query {
		if evidence.Source != (manifest.SourceIdentity{}) {
			id := identities[evidence.Source]
			if id == "" {
				id = manifest.Fingerprint(evidence.Source)
				identities[evidence.Source] = id
			}
			file := path.Clean(evidence.Path)
			if !fs.ValidPath(file) {
				continue
			}
			if changes[id] == nil {
				changes[id] = map[string]struct{}{}
			}
			changes[id][file] = struct{}{}
		}
	}

	var tags []string
	if err := c.store.Tags(ctx, "", func(batch []string) error {
		tags = append(tags, batch...)
		return nil
	}); err != nil {
		return nil, &CacheError{err}
	}
	owners := map[manifest.NamedResource]struct{}{}
	seen := map[struct{ digest, metadata string }]struct{}{}
	for _, tag := range tags {
		if !strings.HasPrefix(tag, "render-") {
			continue
		}
		if err := ctx.Err(); err != nil {
			return nil, &CacheError{err}
		}
		descriptor, err := c.store.Resolve(ctx, tag)
		if errors.Is(err, errdef.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, &CacheError{err}
		}
		data := descriptor.Annotations[historyAnnotation]
		if data == "" {
			continue
		}
		identity := struct{ digest, metadata string }{descriptor.Digest.String(), data}
		if _, ok := seen[identity]; ok {
			continue
		}
		seen[identity] = struct{}{}
		var record historyRecord
		if json.Unmarshal([]byte(data), &record) != nil || record.Build != processIdentity || record.Owner.Name == "" || record.Owner.Kind == "" || record.Acceptance == "" || record.Acceptance != descriptor.Annotations[acceptedRenderAnnotation] {
			continue
		}
		matched := false
		for _, source := range record.Sources {
			for file := range changes[source.Source] {
				for _, root := range source.Paths {
					if !fs.ValidPath(root.Path) {
						continue
					}
					matched = matched || file == root.Path || root.Subtree && (root.Path == "." || strings.HasPrefix(file, root.Path+"/"))
				}
			}
		}
		if !matched {
			continue
		}
		info, err := c.root.Stat(blobPath(descriptor.Digest))
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, &CacheError{err}
		}
		if info.Mode().IsRegular() && info.Size() == descriptor.Size {
			owners[record.Owner] = struct{}{}
		}
	}
	return slices.SortedFunc(maps.Keys(owners), manifest.NamedResource.Compare), nil
}
