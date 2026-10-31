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
const historyIdentityAnnotation = "flate.admission-history-key"
const acceptedRenderAnnotation = "flate.accepted-render"

// AcceptedInventory publishes one complete inventory and its source evidence.
type AcceptedInventory[T io.WriterTo] struct {
	Key, Lookup, Acceptance string
	History                 RenderHistory
	Content                 T
}

// RecordInventory atomically replaces accepted content and its searchable provenance.
func RecordInventory[T io.WriterTo](ctx context.Context, c *Cache, inventory AcceptedInventory[T]) error {
	enabled, err := c.renderEnabled(ctx, inventory.Key)
	if err != nil || !enabled {
		return err
	}
	descriptor, _, _, lookupErr := c.RenderMetadata(ctx, inventory.Key, inventory.Lookup)
	if lookupErr != nil && !errors.Is(lookupErr, errdef.ErrNotFound) {
		return lookupErr
	}
	var history *historyRecord
	if snapshot := c.snapshot.Load(); snapshot != nil {
		history = snapshot.histories[inventory.Key]
	}
	if lookupErr == nil && descriptor.Annotations[acceptedRenderAnnotation] == inventory.Acceptance && history.matches(inventory.History, inventory.Acceptance) {
		identity := ""
		if history != nil {
			identity = history.identity
		}
		if descriptor.Annotations[historyIdentityAnnotation] == identity {
			return ctx.Err()
		}
	}
	history = inventory.History.record(inventory.Acceptance)
	acceptance := ""
	if history != nil {
		acceptance = manifest.Fingerprint(history)
		history.identity = acceptance
	}
	producer := blobProducer{content: inventory.Key, render: RenderAccepted}
	_, _, err = c.acquire(ctx, acquireKey{producer: producer, id: inventory.Lookup, render: true}, Policy{MaxAge: Forever, Retry: RetryConfig{Attempts: 1}}, acquisitionCallbacks{
		validate: func(_ context.Context, e *entry) (bool, error) {
			if e.descriptor.Annotations[acceptedRenderAnnotation] != inventory.Acceptance || e.descriptor.Annotations[historyIdentityAnnotation] != acceptance {
				return false, nil
			}
			_, err := c.root.Stat(e.fs.(fileFS).backingPath)
			return err == nil, err
		},
		produce: func(ctx context.Context, c *Cache, staged *payload) (*entry, error) {
			result, err := producer.produce(ctx, c, staged, func(_ context.Context, output ProducerInput) (ProducerResult, error) {
				metadata, err := history.metadata()
				if err != nil {
					return ProducerResult{}, err
				}
				if metadata == nil {
					metadata = make(map[string]string, 2)
				}
				metadata[acceptedRenderAnnotation] = inventory.Acceptance
				metadata[historyIdentityAnnotation] = acceptance
				_, err = inventory.Content.WriteTo(output)
				return ProducerResult{Annotations: metadata}, err
			})
			if result != nil {
				result.inventoryKey, result.history = inventory.Key, history
			}
			return result, err
		},
	})
	return err
}

// RenderHistory supplies source paths associated with one accepted inventory.
type RenderHistory struct {
	Owner   manifest.NamedResource
	Sources []manifest.SourceEvidence
}

func (r *historyRecord) metadata() (map[string]string, error) {
	if r == nil {
		return nil, nil
	}
	data, err := json.Marshal(r)
	if err != nil {
		return nil, &CacheError{err}
	}
	return map[string]string{historyAnnotation: string(data)}, nil
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
	identity   string
	identities map[manifest.SourceIdentity]string
	Acceptance string
	Build      string
	Owner      manifest.NamedResource
	Sources    []historyPaths
}

func (h RenderHistory) record(acceptance string) *historyRecord {
	if h.Sources == nil || acceptance == "" {
		return nil
	}
	record := historyRecord{Build: processIdentity, Owner: h.Owner, Acceptance: acceptance}
	paths := map[string][]historyPath{}
	identities := map[manifest.SourceIdentity]string{}
	record.identities = identities
	for _, evidence := range h.Sources {
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
	return &record
}

func (r *historyRecord) matches(h RenderHistory, acceptance string) bool {
	if h.Sources == nil || acceptance == "" {
		return r == nil
	}
	if r == nil || r.Owner != h.Owner || r.Acceptance != acceptance || r.Build != processIdentity {
		return false
	}
	for _, evidence := range h.Sources {
		file := path.Clean(evidence.Path)
		if evidence.Source == (manifest.SourceIdentity{}) || !fs.ValidPath(file) {
			continue
		}
		source := r.identities[evidence.Source]
		if source == "" {
			return false
		}
		i, found := slices.BinarySearchFunc(r.Sources, source, func(paths historyPaths, source string) int { return strings.Compare(paths.Source, source) })
		if !found || !slices.Contains(r.Sources[i].Paths, historyPath{Path: file, Subtree: evidence.Subtree}) {
			return false
		}
	}
	for _, source := range r.Sources {
		for _, file := range source.Paths {
			if !slices.ContainsFunc(h.Sources, func(evidence manifest.SourceEvidence) bool {
				return r.identities[evidence.Source] == source.Source && path.Clean(evidence.Path) == file.Path && evidence.Subtree == file.Subtree
			}) {
				return false
			}
		}
	}
	return true
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

	c.prepareSnapshot()
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
