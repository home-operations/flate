package cache

import (
	"context"
	"io/fs"
	"time"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/home-operations/flate/pkg/manifest"
)

// Composition MUST retain the caller identity unchanged through acquisition.
type Composition struct {
	// Content MUST include all input identities and authored facts that affect the output.
	// The cache passes this opaque value directly into ArchiveKey.Content.
	Content any
	Inputs  []CompositionInput
	Copies  []Copy
	Policy  string
}

// CompositionInput MUST identify immutable backing or force fresh production.
type CompositionInput struct {
	FS fs.FS
	// Empty Content forces fresh production because the backing can change.
	Content string
}

// Copy paths MUST remain relative to their input and output filesystems.
type Copy struct {
	Input   int
	From    string
	To      string
	Exclude []string
	Mode    CopyMode
	// ExtractContent MUST identify the selected archive and its destination.
	// It constructs opaque facts only, without filesystem access or publication.
	ExtractContent func(path, destination string) any
}

// CopyMode MUST select one of the supported operations.
type CopyMode uint8

// Copy modes retain the established overwrite, merge, and extraction semantics.
const (
	CopyFiles CopyMode = iota
	MergeYAML
	ExtractArchive
)

type composition struct {
	request Composition
	owner   *Cache
	work    manifest.AcquisitionWork
	maxAge  time.Duration
}

// Compose borrows input filesystems until production returns. Output backing belongs to the cache.
//
//nolint:gocritic // The SDK takes an independent composition value snapshot.
func (c *Cache) Compose(
	ctx context.Context,
	request Composition,
	policyValue *Policy,
) (fs.FS, ocispec.Descriptor, string, error) {
	policy := *policyValue
	for _, input := range request.Inputs {
		if input.Content == "" {
			policy.MaxAge = 0
			break
		}
	}
	maxAge := policy.MaxAge
	return c.Acquire(ctx, ArchiveKey{Content: request.Content, Policy: request.Policy}, &policy,
		func(ctx context.Context, target ProducerInput) (ProducerResult, error) {
			e := composition{
				request: request, owner: c, maxAge: maxAge,
				work: manifest.AcquisitionWork{Scope: target.Work().Attribution()},
			}
			var identity string
			if acquisition := target.Work().Physical(); acquisition != nil {
				identity = acquisition.Key.Input
				acquisition.Add(&manifest.WorkCounts{LocalAcquisitions: 1})
			}
			work := e.work.Attribution().Record(manifest.WorkEvaluation, identity)
			work.Add(&manifest.WorkCounts{Attempts: 1})
			err := e.produce(ctx, target)
			work.Finish(err)
			return ProducerResult{}, err
		},
	)
}
