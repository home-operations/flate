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
	Base     CompositionInput
	Inputs   []CompositionInput
	Copies   []Copy
	Policy   string
	Ignore   string
	Captures []StagedCapture
}

// StagedCapture keys a bounded read from the unfiltered tree by purpose and path.
type StagedCapture struct {
	Purpose string
	Path    string
	Read    func(context.Context, string, fs.FS) (string, error) `json:"-"`
}

const stagedCapturePrefix = "staged-capture-"

// StagedCaptureAnnotation identifies a captured value in the composition descriptor.
func StagedCaptureAnnotation(purpose string) string { return prefix + stagedCapturePrefix + purpose }

// CompositionInput MUST identify immutable backing or force fresh production.
type CompositionInput struct {
	FS    fs.FS
	Alias string
	// Empty Content forces fresh production because the backing can change.
	Content string
	// Backing distinguishes mutable inputs that can coexist under the same recipe.
	Backing string
}

// Copy paths MUST use secure resolution for PlaceTree and relative archive paths otherwise.
type Copy struct {
	Input    int
	From     string
	To       string
	Exclude  []string
	Optional bool
	Mode     CopyMode
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

type compositionIdentity struct {
	Content, Backing string
	Alias            string `json:",omitempty"`
}

func (i *CompositionInput) identity() compositionIdentity {
	if i.Content != "" {
		return compositionIdentity{Content: i.Content, Alias: i.Alias}
	}
	return compositionIdentity{Backing: i.Backing, Alias: i.Alias}
}

type copyIdentity struct {
	Input    int
	From, To string
	Exclude  []string
	Optional bool `json:",omitempty"`
	Mode     CopyMode
}

func (r *Composition) key() ArchiveKey {
	inputs := make([]compositionIdentity, len(r.Inputs))
	for i := range r.Inputs {
		inputs[i] = r.Inputs[i].identity()
	}
	copies := make([]copyIdentity, len(r.Copies))
	for i := range r.Copies {
		op := &r.Copies[i]
		copies[i] = copyIdentity{op.Input, op.From, op.To, op.Exclude, op.Optional, op.Mode}
	}
	key := ArchiveKey{
		Content: struct {
			BasePresent bool
			Base        compositionIdentity
			Inputs      []compositionIdentity
			Copies      []copyIdentity
			Captures    []StagedCapture `json:",omitempty"`
		}{r.Base.FS != nil, r.Base.identity(), inputs, copies, r.Captures},
		Policy: r.Policy, Ignore: r.Ignore,
	}
	for i := range r.Captures {
		key.Required = append(key.Required, stagedCapturePrefix+r.Captures[i].Purpose)
	}
	return key
}

type composition struct {
	request Composition
	owner   *Cache
	work    manifest.AcquisitionWork
	maxAge  time.Duration
}

// Compose borrows input filesystems until production returns. Output backing belongs to the cache.
//
//nolint:gocritic // The cache takes an independent composition value snapshot.
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
			result, err := e.produce(ctx, target)
			work.Finish(err)
			return result, err
		},
	)
}
