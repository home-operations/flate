package cache

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
)

type acceptedRoot struct {
	Source manifest.NamedResource `json:"Source"`
	Paths  []string               `json:"Paths"`
}

type acceptedRecord struct {
	ID     manifest.NamedResource   `json:"ID"`
	Record manifest.DiscoveryRecord `json:"Record"`
	Roots  []acceptedRoot           `json:"Roots"`
}

type acceptedRender struct {
	Version  int                    `json:"Version"`
	Complete bool                   `json:"Complete"`
	Owner    manifest.NamedResource `json:"Owner"`
	Raw      string                 `json:"Raw"`
	Final    string                 `json:"Final"`
	Records  []acceptedRecord       `json:"Records"`
}

func (a *acceptedRender) equal(b *acceptedRender) bool {
	if a == nil || b == nil {
		return a == b
	}
	identityMatches := a.Version == b.Version && a.Owner == b.Owner && a.Raw == b.Raw && a.Final == b.Final
	if !identityMatches || a.Complete != b.Complete {
		return false
	}
	if (a.Records == nil) != (b.Records == nil) || len(a.Records) != len(b.Records) {
		return false
	}
	for i := range a.Records {
		x, y := &a.Records[i], &b.Records[i]
		if x.ID != y.ID || !x.Record.Equal(y.Record) || (x.Roots == nil) != (y.Roots == nil) ||
			len(x.Roots) != len(y.Roots) {
			return false
		}
		for j, root := range x.Roots {
			other := y.Roots[j]
			if root.Source != other.Source || (root.Paths == nil) != (other.Paths == nil) ||
				!slices.Equal(root.Paths, other.Paths) {
				return false
			}
		}
	}
	return true
}

func prepareAccepted(
	owner manifest.NamedResource,
	frame *Frame,
	fingerprint string,
	records iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord],
) (acceptedRender, error) {
	accepted := acceptedRender{
		Version:  frameVersion,
		Complete: true,
		Owner:    owner,
		Raw:      frame.raw,
		Final:    fingerprint,
	}
	//nolint:gocritic // The iterator supplies immutable value snapshots.
	for id, record := range records {
		item := acceptedRecord{ID: id, Record: record}
		item.Record.ContentRoots = nil
		item.Record.Inputs = slices.Clone(record.Inputs)
		item.Record.Refs = slices.Clone(record.Refs)
		slices.SortFunc(item.Record.Refs, manifest.NamedResource.Compare)
		item.Record.Refs = slices.Compact(item.Record.Refs)
		item.Record.Productions = slices.Clone(record.Productions)
		slices.SortFunc(item.Record.Productions, func(a, b manifest.Production) int {
			return cmp.Or(
				a.Owner.Compare(b.Owner),
				a.Declaration.Compare(b.Declaration),
				a.Authored.Compare(b.Authored),
				a.Source.Compare(b.Source),
				cmp.Compare(a.Path, b.Path),
				cmp.Compare(a.File, b.File),
			)
		})
		item.Record.Productions = slices.Compact(item.Record.Productions)
		for source, paths := range record.ContentRoots {
			paths = slices.Clone(paths)
			slices.Sort(paths)
			item.Roots = append(item.Roots, acceptedRoot{Source: source, Paths: slices.Compact(paths)})
		}
		slices.SortFunc(item.Roots, func(a, b acceptedRoot) int { return a.Source.Compare(b.Source) })
		accepted.Records = append(accepted.Records, item)
	}
	slices.SortFunc(accepted.Records, func(a, b acceptedRecord) int { return a.ID.Compare(b.ID) })
	for i := 1; i < len(accepted.Records); i++ {
		if accepted.Records[i-1].ID == accepted.Records[i].ID {
			return acceptedRender{}, errors.New("duplicate accepted render identity")
		}
	}
	return accepted, nil
}

func (a *acceptedRender) ordered() bool {
	for i := range a.Records {
		item := &a.Records[i]
		if i > 0 && a.Records[i-1].ID.Compare(item.ID) >= 0 {
			return false
		}
		for j, root := range item.Roots {
			if j > 0 && item.Roots[j-1].Source.Compare(root.Source) >= 0 {
				return false
			}
		}
	}
	return true
}

func (a *acceptedRender) valid(raw string) bool {
	bound := a.Version == frameVersion && a.Raw == raw
	return bound && a.Complete && a.Final != "" && a.ordered()
}

// FramePublicationInput MUST describe the accepted output under Owner.
type FramePublicationInput struct {
	Owner    manifest.NamedResource
	Content  any
	Records  iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
	Complete bool
}

// PreparePublication returns no publication when complete evidence cannot be fingerprinted or encoded within the limit.
//
//nolint:gocritic // Value input retains the publication argument snapshot.
func (f *Frame) PreparePublication(ctx context.Context, input FramePublicationInput) (*FramePublication, error) {
	if err := ctx.Err(); err != nil {
		return nil, &CacheError{err}
	}
	frame := Frame{
		envelope: f.envelope, header: f.header, payload: f.payload,
		generation: f.generation, available: f.available, raw: f.raw,
	}
	if data := f.hydrated.Load(); data != nil {
		frame.payload = *data
	}
	publication := &FramePublication{frame: &frame, identity: f.raw}
	if !input.Complete || input.Records == nil {
		if f.accepted != nil {
			return nil, nil //nolint:nilnil // Incomplete evidence must not erase accepted history.
		}
		return publication, nil
	}
	fingerprint := manifest.Fingerprint(input.Content)
	if fingerprint == "" || !f.available {
		return nil, nil //nolint:nilnil // Unavailable acceptance must not replace previously accepted evidence.
	}
	accepted, err := prepareAccepted(input.Owner, f, fingerprint, input.Records)
	if err != nil {
		return nil, manifest.NewError(manifest.ErrFlux, fmt.Errorf("prepare accepted frame: %w", err), "")
	}
	if f.accepted.equal(&accepted) {
		publication.frame = f
		publication.tail, publication.identity = f.acceptedData, f.acceptedKey
		return publication, nil
	}
	data, err := json.Marshal(accepted)
	if err != nil {
		return nil, manifest.NewError(manifest.ErrFlux, fmt.Errorf("encode accepted frame: %w", err), "")
	}
	if len(data) > frameHeaderLimit {
		return nil, nil //nolint:nilnil // Oversized acceptance disables optional persistence without replacing prior evidence.
	}
	publication.tail, publication.identity = data, manifest.Fingerprint(accepted)
	frame.accepted, frame.acceptedKey, frame.acceptedData = &accepted, publication.identity, data
	return publication, nil
}

// History requires current final content before it supplies accepted records.
func (f *Frame) History(
	owner manifest.NamedResource,
	content any,
) (iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord], bool) {
	if f.accepted == nil || !f.available || f.accepted.Owner != owner {
		return nil, false
	}
	if !f.accepted.valid(f.raw) || f.accepted.Final != manifest.Fingerprint(content) {
		return nil, false
	}
	return func(yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {
		for i := range f.accepted.Records {
			item := &f.accepted.Records[i]
			record := item.Record
			record.Inputs = slices.Clone(record.Inputs)
			record.Refs = slices.Clone(record.Refs)
			record.Productions = slices.Clone(record.Productions)
			if len(item.Roots) > 0 {
				record.ContentRoots = make(map[manifest.NamedResource][]string, len(item.Roots))
			}
			for _, root := range item.Roots {
				record.ContentRoots[root.Source] = slices.Clone(root.Paths)
			}
			if !yield(item.ID, record) {
				return
			}
		}
	}, true
}
