package cache

import (
	"context"
	"io/fs"
	"time"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/home-operations/flate/pkg/manifest"
)

// Composition MUST identify the complete base and ordered operations before cache reuse.
type Composition struct {
	// Content retains contextual facts beyond the cache-derived filesystem and operation identity.
	Content any
	Base    CompositionInput
	Inputs  []CompositionInput
	Copies  []Copy
	Policy  string
	Ignore  string
}

// CompositionInput MUST identify immutable backing or force fresh production.
type CompositionInput struct {
	FS fs.FS
	// Empty Content forces fresh production because the backing can change.
	Content string
}

// Copy paths MUST use secure resolution for PlaceTree and relative archive paths otherwise.
type Copy struct {
	Input   int
	From    string
	To      string
	Exclude []string
	Mode    CopyMode
	// ExtractContent retains contextual identity facts for nested extraction acquisitions.
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
	PlaceTree
)

type copyIdentity struct {
	Input    int
	From, To string
	Exclude  []string
	Mode     CopyMode
}

func (r *Composition) key() ArchiveKey {
	inputs := make([]string, len(r.Inputs))
	for i := range r.Inputs {
		inputs[i] = r.Inputs[i].Content
	}
	copies := make([]copyIdentity, len(r.Copies))
	for i := range r.Copies {
		op := &r.Copies[i]
		copies[i] = copyIdentity{op.Input, op.From, op.To, op.Exclude, op.Mode}
	}
	return ArchiveKey{
		Content: struct {
			Context     any
			BasePresent bool
			Base        string
			Inputs      []string
			Copies      []copyIdentity
		}{r.Content, r.Base.FS != nil, r.Base.Content, inputs, copies},
		Policy: r.Policy, Ignore: r.Ignore,
	}
}

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
	if request.Base.FS != nil && request.Base.Content == "" {
		policy.MaxAge = 0
	}
	for _, input := range request.Inputs {
		if input.Content == "" {
			policy.MaxAge = 0
			break
		}
	}
	maxAge := policy.MaxAge
	return c.Acquire(ctx, request.key(), &policy,
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
