package cache

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"maps"
	"path"
	"slices"
	"strings"

	"oras.land/oras-go/v2/errdef"

	"github.com/home-operations/flate/pkg/manifest"
)

const (
	historyAnnotation         = "flate.admission-history"
	historyIdentityAnnotation = "flate.admission-history-key"
	acceptedRenderAnnotation  = "flate.accepted-render"
)

// AcceptedInventory publishes one complete inventory and its source evidence.
type AcceptedInventory[T io.WriterTo] struct {
	Key, Lookup, Acceptance string
	History                 RenderHistory
	Content                 T
	Work                    *manifest.AcquisitionWork
}

// RecordInventory atomically replaces accepted content and its searchable provenance.
func RecordInventory[T io.WriterTo](ctx context.Context, c *Cache, inventory AcceptedInventory[T]) error {
	enabled, err := c.renderEnabled(ctx, inventory.Key)
	if err != nil || !enabled {
		return err
	}
	metadata, lookupErr := c.RenderMetadata(ctx, inventory.Key, inventory.Lookup, inventory.Work)

	if lookupErr != nil && !errors.Is(lookupErr, errdef.ErrNotFound) {
		return lookupErr
	}
	var history *historyRecord
	if snapshot := c.snapshot.Load(); snapshot != nil {
		history = snapshot.histories[inventory.Key]
	}
	if lookupErr == nil && metadata.Descriptor.Annotations[acceptedRenderAnnotation] == inventory.Acceptance &&
		history.matches(&inventory.History, inventory.Acceptance) {
		identity := ""
		if history != nil {
			identity = history.identity
		}
		if metadata.Descriptor.Annotations[historyIdentityAnnotation] == identity {
			return ctx.Err() //nolint:wrapcheck // Cancellation already identifies its cause; another operation prefix would add no resource information.
		}
	}
	history = new(inventory.History).record(inventory.Acceptance)
	acceptance := ""
	if history != nil {
		acceptance = manifest.Fingerprint(history)
		history.identity = acceptance
	}
	producer := blobProducer{content: inventory.Key, render: RenderAccepted}
	_, _, err = c.acquire(
		ctx,
		new(acquireKey{producer: producer, id: inventory.Lookup, render: true}),
		&Policy{MaxAge: Forever, Retry: RetryConfig{Attempts: 1}, Work: inventory.Work},
		acquisitionCallbacks{
			validate: func(_ context.Context, e *entry) (bool, error) {
				if e.descriptor.Annotations[acceptedRenderAnnotation] != inventory.Acceptance ||
					e.descriptor.Annotations[historyIdentityAnnotation] != acceptance {
					return false, nil
				}
				_, statErr := c.root.Stat(
					e.fs.(fileFS).backingPath, //nolint:errcheck // The owner stores this exact concrete type; a mismatched assertion must retain its panic.
				)
				return statErr == nil, statErr //nolint:wrapcheck // Filesystem adapters retain fs.PathError operation, path, and concrete error identity.
			},
			produce: func(ctx context.Context, c *Cache, staged *payload) (*entry, error) {
				result, produceErr := producer.produce(
					ctx,
					c,
					staged,
					func(_ context.Context, output ProducerInput) (ProducerResult, error) {
						return inventory.write(history, acceptance, output)
					},
				)
				if result != nil {
					result.inventoryKey, result.history = inventory.Key, history
				}
				return result, produceErr
			},
		},
	)
	return err
}

func (i AcceptedInventory[T]) write(
	history *historyRecord, acceptance string, output ProducerInput,
) (ProducerResult, error) {
	metadata, metadataErr := history.metadata()
	if metadataErr != nil {
		return ProducerResult{}, metadataErr
	}
	if metadata == nil {
		metadata = make(map[string]string, inventoryAnnotationCount)
	}
	metadata[acceptedRenderAnnotation] = i.Acceptance
	metadata[historyIdentityAnnotation] = acceptance
	_, metadataErr = i.Content.WriteTo(output)
	return ProducerResult{
		Annotations: metadata,
	}, metadataErr //nolint:wrapcheck // The cache producer and stream adapters preserve the original error tree for replay and io sentinel handling.
}

// RenderHistory supplies source paths associated with one accepted inventory.
type RenderHistory struct {
	Owner   manifest.NamedResource `json:"Owner"`
	Sources []manifest.SourceEvidence
}

type historyRecord struct {
	identity   string
	identities map[manifest.SourceIdentity]string
	Acceptance string                 `json:"Acceptance"`
	Build      string                 `json:"Build"`
	Owner      manifest.NamedResource `json:"Owner"`
	Sources    []historyPaths         `json:"Sources"`
}

func (r *historyRecord) metadata() (map[string]string, error) {
	if r == nil {
		return nil, nil //nolint:nilnil // A nil value and nil error represent an optional absence or a reusable-cache miss.
	}
	data, err := json.Marshal(r)
	if err != nil {
		return nil, &CacheError{err}
	}
	return map[string]string{historyAnnotation: string(data)}, nil
}

type historyPaths struct {
	Source string        `json:"Source"`
	Paths  []historyPath `json:"Paths"`
}

type historyPath struct {
	Path    string `json:"Path"`
	Subtree bool   `json:"Subtree"`
}

func (hv *RenderHistory) record(acceptance string) *historyRecord {
	h := *hv
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

func (r *historyRecord) matches(hValue *RenderHistory, acceptance string) bool {
	h := *hValue
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
		i, found := slices.BinarySearchFunc(
			r.Sources,
			source,
			func(paths historyPaths, source string) int { return strings.Compare(paths.Source, source) },
		)
		if !found || !slices.Contains(r.Sources[i].Paths, historyPath{Path: file, Subtree: evidence.Subtree}) {
			return false
		}
	}
	for _, source := range r.Sources {
		for _, file := range source.Paths {
			if !slices.ContainsFunc(h.Sources, func(evidence manifest.SourceEvidence) bool {
				return r.identities[evidence.Source] == source.Source && path.Clean(evidence.Path) == file.Path &&
					evidence.Subtree == file.Subtree
			}) {
				return false
			}
		}
	}
	return true
}

// ProducersForPaths returns producers whose accepted source paths reach the supplied changes.
// The result is positive admission evidence, never proof that an inventory remains complete.
func (c *Cache) ProducersForPaths(
	ctx context.Context,
	query []manifest.SourceEvidence,
) (map[manifest.NamedResource][]manifest.SourceEvidence, error) {
	if err := ctx.Err(); err != nil {
		return nil, &CacheError{err}
	}
	if !c.CanPersistRenders() || len(query) == 0 {
		return nil, nil //nolint:nilnil // Disabled persistence or an empty query supplies no history evidence.
	}
	changes := changedHistoryPaths(query)

	c.prepareSnapshot()
	var tags []string
	if err := c.store.Tags(ctx, "", func(batch []string) error {
		tags = append(tags, batch...)
		return nil
	}); err != nil {
		return nil, &CacheError{err}
	}
	owners := map[manifest.NamedResource][]manifest.SourceEvidence{}
	seen := map[historyVersion]struct{}{}
	for _, tag := range tags {
		producer, matched, err := c.pathProducer(ctx, tag, changes, seen)
		if err != nil {
			return nil, err
		}
		if len(matched) != 0 {
			owners[producer] = append(owners[producer], matched...)
		}
	}

	for owner, evidence := range owners {
		slices.SortFunc(evidence, func(a, b manifest.SourceEvidence) int {
			order := cmp.Or(
				strings.Compare(a.Source.Kind, b.Source.Kind),
				strings.Compare(a.Source.URL, b.Source.URL),
				strings.Compare(
					a.Source.Key,
					b.Source.Key,
				),
				strings.Compare(a.Source.Root, b.Source.Root),
				strings.Compare(a.Path, b.Path),
			)
			if order != 0 || a.Subtree == b.Subtree {
				return order
			}
			if a.Subtree {
				return 1
			}
			return -1
		})
		owners[owner] = slices.Compact(evidence)
	}
	return owners, nil
}

func changedHistoryPaths(query []manifest.SourceEvidence) map[string][]manifest.SourceEvidence {
	changes := map[string][]manifest.SourceEvidence{}
	identities := map[manifest.SourceIdentity]string{}
	for _, evidence := range query {
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
		evidence.Path = file
		changes[id] = append(changes[id], evidence)
	}

	return changes
}

func (r *historyRecord) reaches(changes map[string][]manifest.SourceEvidence) []manifest.SourceEvidence {
	var matched []manifest.SourceEvidence
	for _, source := range r.Sources {
		for _, evidence := range changes[source.Source] {
			for _, root := range source.Paths {
				if !fs.ValidPath(root.Path) {
					continue
				}
				if evidence.Path == root.Path ||
					root.Subtree && (root.Path == "." || strings.HasPrefix(evidence.Path, root.Path+"/")) {
					matched = append(matched, evidence)
					break
				}
			}
		}
	}
	return matched
}

type historyVersion struct{ digest, metadata string }

func (c *Cache) pathProducer(
	ctx context.Context,
	tag string,
	changes map[string][]manifest.SourceEvidence,
	seen map[historyVersion]struct{},
) (manifest.NamedResource, []manifest.SourceEvidence, error) {
	if !strings.HasPrefix(tag, "render-") {
		return manifest.NamedResource{}, nil, nil
	}
	if err := ctx.Err(); err != nil {
		return manifest.NamedResource{}, nil, &CacheError{err}
	}
	descriptor, err := c.store.Resolve(ctx, tag)
	if errors.Is(err, errdef.ErrNotFound) {
		return manifest.NamedResource{}, nil, nil
	}
	if err != nil {
		return manifest.NamedResource{}, nil, &CacheError{err}
	}
	data := descriptor.Annotations[historyAnnotation]
	if data == "" {
		return manifest.NamedResource{}, nil, nil
	}
	identity := historyVersion{descriptor.Digest.String(), data}
	if _, ok := seen[identity]; ok {
		return manifest.NamedResource{}, nil, nil
	}
	seen[identity] = struct{}{}
	var record historyRecord
	if json.Unmarshal([]byte(data), &record) != nil || record.Build != processIdentity || record.Owner.Name == "" ||
		record.Owner.Kind == "" ||
		record.Acceptance == "" ||
		record.Acceptance != descriptor.Annotations[acceptedRenderAnnotation] {
		//nolint:nilerr // Invalid persisted history metadata is a cache miss, including decode failures.
		return manifest.NamedResource{}, nil, nil
	}
	matched := record.reaches(changes)
	if len(matched) == 0 {
		return manifest.NamedResource{}, nil, nil
	}
	info, err := c.root.Stat(blobPath(descriptor.Digest))
	if errors.Is(err, fs.ErrNotExist) {
		return manifest.NamedResource{}, nil, nil
	}
	if err != nil {
		return manifest.NamedResource{}, nil, &CacheError{err}
	}
	if !info.Mode().IsRegular() || info.Size() != descriptor.Size {
		return manifest.NamedResource{}, nil, nil
	}
	return record.Owner, matched, nil
}
