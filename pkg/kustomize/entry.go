package kustomize

import (
	"context"
	"errors"
	"io/fs"
	"iter"
	"log/slog"
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

// EntryEvidence retains the accepted producer inventory that qualified an entry application.
type EntryEvidence struct {
	Application map[string]any                                              `json:"Application"`
	Owner       manifest.NamedResource                                      `json:"Owner"`
	Records     iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord] `json:"Records"`
	Receipt     *manifest.RenderReceipt                                     `json:"Receipt"`
	Inputs      []EntryInput                                                `json:"Inputs"`
	Renders     []*manifest.RenderReceipt                                   `json:"Renders"`
}

// EntryInput binds one resolved qualification input to owner-derived content.
type EntryInput struct {
	Resource manifest.NamedResource `json:"Resource"`
	Content  string                 `json:"Content"`
	Binding  store.ArtifactInput    `json:"Binding"`
	Artifact *store.SourceArtifact  `json:"-"`
	Data     manifest.Data          `json:"-"`
}

func resolvedEntryInput(ctx context.Context, observed *EntryInput) (EntryInput, bool, error) {
	input := *observed
	resolved := EntryInput{Resource: input.Resource}
	if input.Data != nil {
		resolved.Content = manifest.Fingerprint(input.Data.Snapshot())
		//nolint:wrapcheck // Cancellation must preserve the caller-visible sentinel identity.
		return resolved, resolved.Content != "", ctx.Err()
	}
	artifact := input.Artifact
	if artifact == nil || artifact.FS == nil || !artifact.Reads.Valid() {
		//nolint:wrapcheck // Cancellation must preserve the caller-visible sentinel identity.
		return resolved, false, ctx.Err()
	}
	resolved.Binding = artifact.Input(store.ArtifactContent | store.ArtifactRevision)
	resolved.Binding.LocalPath, resolved.Binding.LocalRoot = "", ""
	resolved.Content = artifact.Content
	if resolved.Content == "" && artifact.ContentFingerprint != nil {
		var err error
		resolved.Content, err = artifact.ContentFingerprint(ctx, artifact.FS)
		if err != nil {
			//nolint:wrapcheck // Cancellation must preserve the caller-visible sentinel identity.
			return resolved, false, ctx.Err()
		}
	}
	//nolint:wrapcheck // Cancellation must preserve the caller-visible sentinel identity.
	return resolved, resolved.Content != "", ctx.Err()
}

//nolint:gocritic // Slice comparison callbacks require these value inputs and their snapshots.
func equalEntryInput(a, b EntryInput) bool {
	return a.Resource == b.Resource && a.Content == b.Content && a.Binding == b.Binding
}

// ApplicationReceipt captures complete local and resolved entry qualification inputs.
//
//nolint:gocritic // Preserve the existing value API and its snapshot semantics.
func (*TreeCache) ApplicationReceipt(
	ctx context.Context,
	source *store.SourceArtifact,
	evidence EntryEvidence,
	files []string,
) (*manifest.RenderReceipt, error) {
	for _, receipt := range evidence.Renders {
		if frame, ok := receipt.Value().(renderFrame); ok && len(frame.header.Reads.Remotes) != 0 {
			//nolint:nilnil // An unavailable receipt is a cache miss, not a render failure.
			return nil, nil
		}
	}
	inputs := make([]EntryInput, 0, len(evidence.Inputs))
	{
		inputValues := evidence.Inputs
		for inputIndex := range inputValues {
			input := inputValues[inputIndex]
			resolved, complete, err := resolvedEntryInput(ctx, &input)
			if err != nil || !complete {
				return nil, err
			}
			inputs = append(inputs, resolved)
		}
	}
	slices.SortFunc(inputs, func(a, b EntryInput) int { return a.Resource.Compare(b.Resource) })
	inputs = slices.CompactFunc(inputs, equalEntryInput)
	reads := newReadSet(source.LocalPath)
	for _, file := range slices.Sorted(slices.Values(files)) {
		if err := ctx.Err(); err != nil {
			//nolint:wrapcheck // Cancellation must preserve the caller-visible sentinel identity.
			return nil, err
		}
		if !fs.ValidPath(file) {
			//nolint:nilnil // An unavailable receipt is a cache miss, not a render failure.
			return nil, nil
		}
		data, err := fs.ReadFile(source.FS, file)
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			//nolint:nilnil // An unavailable receipt is a cache miss, not a render failure.
			return nil, nil
		}
		reads.recordFile(filepath.Join(source.LocalPath, filepath.FromSlash(file)), data, err)
	}
	inventory, err := prepareAccepted(evidence.Owner, &renderFrame{}, "", evidence.Records)
	if err != nil {
		return nil, err
	}
	slog.Debug("kustomize: entry application captured", "producer", evidence.Owner, "files", len(files))
	return manifest.NewRenderReceipt(
		entryApplication{
			Application: evidence.Application,
			Reads:       reads.snapshot(),
			Inputs:      inputs,
			Complete:    true,
			Inventory:   inventory,
		},
		equalEntryApplication,
	), nil
}

func equalEntryApplication(a, b any) bool {
	left, ok := a.(entryApplication)
	right, other := b.(entryApplication)
	return ok && other && left.Complete && right.Complete && manifest.ValueEqual(left.Application, right.Application) &&
		slices.Equal(left.Reads.Files, right.Reads.Files) &&
		slices.EqualFunc(left.Inputs, right.Inputs, equalEntryInput) &&
		left.Inventory.equal(&right.Inventory)
}

// EntryValid qualifies provisional evidence against current resolved input content.
//
//nolint:gocritic // Preserve the existing value API and its snapshot semantics.
func (*TreeCache) EntryValid(ctx context.Context, evidence EntryEvidence, st *store.Store) bool {
	{
		expectedValues := evidence.Inputs
		for expectedIndex := range expectedValues {
			expected := expectedValues[expectedIndex]
			input := EntryInput{Resource: expected.Resource}
			if expected.Binding.Present {
				input.Artifact, _ = st.GetArtifact(expected.Resource, store.ArtifactContent).(*store.SourceArtifact)
			} else {
				input.Data = st.ReferenceFacts(expected.Resource, nil, manifest.NamedResource{}).Data
			}
			actual, complete, err := resolvedEntryInput(ctx, &input)
			if err != nil || !complete || !equalEntryInput(actual, expected) {
				slog.Debug(
					"kustomize: entry application input changed",
					"resource",
					expected.Resource,
					"complete",
					complete,
					"recorded",
					expected.Content,
					"resolved",
					actual.Content,
					"binding_matches",
					expected.Binding == actual.Binding,
				)
				return false
			}
		}
	}
	return evidence.Receipt != nil && ctx.Err() == nil
}

// PreviousEntry returns provisional evidence requiring EntryValid after freshness acquisition.
//
//nolint:gocritic // Preserve the public value API and its render-request snapshot.
func (c *TreeCache) PreviousEntry(ctx context.Context, req RenderRequest) EntryEvidence {
	if c == nil || ctx.Err() != nil || !c.owner.CanPersistRenders() {
		return EntryEvidence{}
	}
	prepared, err := c.prepareRender(ctx, &req)
	if err != nil {
		return EntryEvidence{}
	}
	reader, _, _, err := c.owner.Blob(
		ctx,
		sourcecache.BlobKey{Content: prepared.key, Render: sourcecache.RenderAccepted},
		sourcecache.Policy{MaxAge: sourcecache.Forever},
		nil,
	)
	if err != nil {
		return EntryEvidence{}
	}
	data, err := fs.ReadFile(reader, "content")
	if err != nil {
		return EntryEvidence{}
	}
	frame, err := decodeFrame(data)
	if err != nil || frame.accepted == nil || frame.accepted.Owner != req.Spec.Named() ||
		frame.header.Key != prepared.key ||
		frame.header.Binding != manifest.Fingerprint(prepared.binding) {
		return EntryEvidence{}
	}
	if frame.header.Entry == nil {
		valid, localsValidErr := frame.header.Reads.localsValid(ctx, prepared.lower, req.Source.Content != "")
		if localsValidErr != nil || !valid {
			return EntryEvidence{}
		}
		records, _ := frame.history(req.Spec.Named())
		return EntryEvidence{Owner: req.Spec.Named(), Records: records}
	}
	application := *frame.header.Entry
	valid, err := application.Reads.localsValid(ctx, req.Source.FS, false)
	if err != nil || !valid {
		return EntryEvidence{}
	}
	records, _ := (renderFrame{accepted: &application.Inventory}).history(application.Inventory.Owner)
	return EntryEvidence{
		Application: application.Application,
		Owner:       application.Inventory.Owner,
		Records:     records,
		Inputs:      application.Inputs,
		Receipt:     manifest.NewRenderReceipt(application, equalEntryApplication),
	}
}
