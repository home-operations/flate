package kustomize

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	Producer    manifest.NamedResource                                      `json:"Producer"`
	Application map[string]any                                              `json:"Application"`
	Root        manifest.NamedResource                                      `json:"Root"`
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
func (c *TreeCache) ApplicationReceipt(
	ctx context.Context,
	request RenderRequest,
	evidence EntryEvidence,
	files []string,
) (EntryEvidence, error) {
	for _, receipt := range evidence.Renders {
		if frame, ok := receipt.Value().(renderFrame); ok && len(frame.header.Reads.Remotes) != 0 {
			return EntryEvidence{}, nil
		}
	}
	inputs := make([]EntryInput, 0, len(evidence.Inputs))
	{
		inputValues := evidence.Inputs
		for inputIndex := range inputValues {
			input := inputValues[inputIndex]
			resolved, complete, err := resolvedEntryInput(ctx, &input)
			if err != nil || !complete {
				return EntryEvidence{}, err
			}
			inputs = append(inputs, resolved)
		}
	}
	slices.SortFunc(inputs, func(a, b EntryInput) int { return a.Resource.Compare(b.Resource) })
	inputs = slices.CompactFunc(inputs, equalEntryInput)
	request.Work = nil
	prepared, err := c.prepareEntry(ctx, &request)
	if err != nil {
		return EntryEvidence{}, err
	}
	source := request.Source
	reads := newReadSet(source.LocalPath)
	for _, file := range slices.Sorted(slices.Values(files)) {
		if cancelErr := ctx.Err(); cancelErr != nil {
			//nolint:wrapcheck // Cancellation must preserve the caller-visible sentinel identity.
			return EntryEvidence{}, cancelErr
		}
		if !fs.ValidPath(file) {
			return EntryEvidence{}, nil
		}
		data, readErr := fs.ReadFile(source.FS, file)
		if readErr != nil && !errors.Is(readErr, fs.ErrNotExist) {
			return EntryEvidence{}, nil
		}
		reads.recordFile(filepath.Join(source.LocalPath, filepath.FromSlash(file)), data, readErr)
	}
	inventory, err := prepareAccepted(evidence.Owner, &renderFrame{}, "", evidence.Records)
	if err != nil {
		return EntryEvidence{}, err
	}
	slog.Debug("kustomize: entry application captured", "producer", evidence.Owner, "files", len(files))
	snapshot := entrySnapshot{
		Key:      prepared.key,
		Binding:  manifest.Fingerprint(prepared.binding),
		Producer: evidence.Producer,
		Application: entryApplication{
			Application: evidence.Application,
			Root:        evidence.Root,
			Reads:       reads.snapshot(),
			Inputs:      inputs,
			Complete:    true,
			Inventory:   inventory,
		},
	}
	return snapshot.evidence(), nil
}

func equalEntryApplication(a, b any) bool {
	x, ok := a.(entrySnapshot)
	y, other := b.(entrySnapshot)
	if !ok || !other || x.Key != y.Key || x.Binding != y.Binding || x.Producer != y.Producer {
		return false
	}
	left, right := x.Application, y.Application
	return ok && other && left.Complete && right.Complete && left.Root == right.Root &&
		manifest.ValueEqual(left.Application, right.Application) &&
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

type entrySnapshot struct {
	Producer    manifest.NamedResource `json:"Producer"`
	Key         string                 `json:"Key"`
	Binding     string                 `json:"Binding"`
	Application entryApplication       `json:"Application"`
}

func (c *TreeCache) prepareEntry(ctx context.Context, req *RenderRequest) (renderRequest, error) {
	req.Spec = nil
	req.SubPath = manifest.NormalizeClaimBase(req.SubPath)
	prepared, err := c.prepareRender(ctx, req)
	if err != nil {
		return renderRequest{}, err
	}
	prepared.key = manifest.Fingerprint(struct {
		EntryPath string
		Source    renderBinding
	}{req.SubPath, prepared.binding})
	req.Work.Fingerprint(prepared.key)
	return prepared, nil
}

// RecordEntry MUST publish only complete evidence with current captured inputs.
func (c *TreeCache) RecordEntry(ctx context.Context, receipt *manifest.RenderReceipt, reads *store.ReadSet) error {
	if err := ctx.Err(); err != nil {
		//nolint:wrapcheck // Cancellation must preserve the caller-visible sentinel identity.
		return err
	}
	if receipt == nil || !reads.Valid() || !c.owner.CanPersistRenders() {
		//nolint:wrapcheck // Cancellation must preserve the caller-visible sentinel identity.
		return ctx.Err()
	}
	snapshot, ok := receipt.Value().(entrySnapshot)
	if !ok || !snapshot.Application.Complete || !snapshot.Application.Inventory.Complete ||
		snapshot.Application.Inputs == nil {
		return manifest.NewError(manifest.ErrFlux, nil, "incomplete entry qualification evidence")
	}
	data, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("encode entry receipt: %w", err)
	}
	if len(data) > frameHeaderLimit {
		return manifest.NewError(manifest.ErrFlux, nil, "entry receipt exceeds frame limit")
	}
	if !reads.Valid() {
		return nil
	}
	return sourcecache.RecordInventory(ctx, c.owner, sourcecache.AcceptedInventory[*bytes.Reader]{
		Key: snapshot.Key, Acceptance: manifest.Fingerprint(data), Content: bytes.NewReader(data),
	})
}

// PreviousEntry MUST treat persisted evidence as provisional until current inputs qualify it.
//
//nolint:gocritic // Preserve the existing value API and its snapshot semantics.
func (c *TreeCache) PreviousEntry(ctx context.Context, req RenderRequest) EntryEvidence {
	if c == nil || ctx.Err() != nil || !c.owner.CanPersistRenders() {
		return EntryEvidence{}
	}
	prepared, err := c.prepareEntry(ctx, &req)
	if err != nil {
		return EntryEvidence{}
	}
	reader, _, _, err := c.owner.Blob(
		ctx,
		sourcecache.BlobKey{Content: prepared.key, Render: sourcecache.RenderAccepted},
		&sourcecache.Policy{MaxAge: sourcecache.Forever, Work: req.Work.History()},
		nil,
	)
	if err != nil {
		return EntryEvidence{}
	}
	data, err := fs.ReadFile(reader, "content")
	if err != nil {
		return EntryEvidence{}
	}
	var snapshot entrySnapshot
	if len(data) > frameHeaderLimit || json.Unmarshal(data, &snapshot) != nil || snapshot.Key != prepared.key ||
		snapshot.Binding != manifest.Fingerprint(prepared.binding) {
		return EntryEvidence{}
	}
	application := snapshot.Application
	if !application.Complete || application.Inputs == nil || !application.Inventory.Complete ||
		application.Inventory.Version != acceptedRenderVersion {
		return EntryEvidence{}
	}
	valid, err := application.Reads.localsValid(ctx, req.Source.FS, false)
	if err != nil || !valid {
		return EntryEvidence{}
	}
	return snapshot.evidence()
}

func (s *entrySnapshot) evidence() EntryEvidence {
	application := s.Application
	records, _ := (renderFrame{accepted: &application.Inventory}).history(application.Inventory.Owner)
	return EntryEvidence{
		Producer:    s.Producer,
		Application: application.Application,
		Root:        application.Root,
		Owner:       application.Inventory.Owner,
		Records:     records,
		Inputs:      application.Inputs,
		Receipt:     manifest.NewRenderReceipt(*s, equalEntryApplication),
	}
}
