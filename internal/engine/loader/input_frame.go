package loader

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"slices"

	"github.com/home-operations/flate/internal/core/referencerole"
	"github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/pkg/manifest"
)

type inputFrame struct {
	Visits           []cachedVisit `json:"Visits,omitempty"`
	frame            *cache.Frame
	DeclarationBytes int                    `json:"DeclarationBytes,omitempty"`
	Files            []cachedInput          `json:"Files,omitempty"`
	Walks            []cachedWalk           `json:"Walks,omitempty"`
	Loads            []cachedLoad           `json:"Loads,omitempty"`
	Digests          map[string]string      `json:"Digests,omitempty"`
	Types            map[string]fs.FileMode `json:"Types,omitempty"`
}

type inputFrameContent struct{ Origin, URL, Content string }

// Object and projection payloads MUST stay lazy. Encoded declaration bytes let
// context-free snapshots materialize observations without I/O.
type inputPayload struct {
	observation *fileObservation
	frame       *cache.Frame
	offset      int64
	size        int
	data        []byte
}

func (p *inputPayload) read(ctx context.Context) ([]byte, error) {
	if p.data != nil {
		return p.data, nil
	}
	return p.frame.ReadPayload(ctx, p.offset, p.size)
}

func readInputFrame(ctx context.Context, generation fs.FS) (*inputFrame, error) {
	var frame inputFrame
	content, err := cache.OpenFrame(ctx, "loader-inputs", generation, &frame)
	if err != nil {
		return nil, err
	}
	for i := range frame.Walks {
		walk := &frame.Walks[i]
		walk.dictionary = frame.Visits
		if slices.ContainsFunc(walk.VisitIDs, func(id int) bool { return id < 0 || id >= len(frame.Visits) }) {
			return nil, manifest.NewError(manifest.ErrFlux, nil, "decode discovery inputs: invalid visit reference")
		}
	}
	if restoreErr := frame.restoreReferenceRoles(); restoreErr != nil {
		return nil, restoreErr
	}
	frame.frame = content
	var declarations []byte
	if frame.DeclarationBytes != 0 {
		declarations, err = content.ReadPayload(ctx, 0, frame.DeclarationBytes)
		if err != nil {
			return nil, err
		}
	}
	count := 0
	for range frame.payloads {
		count++
	}
	payloads := make([]inputPayload, count)
	i := 0
	for reference := range frame.payloads {
		payload := &payloads[i]
		i++
		payload.frame, payload.offset, payload.size = content, *reference.offset, *reference.size
		if reference.declaration {
			if payload.offset < 0 || payload.size < 0 || payload.offset > int64(len(declarations)) ||
				payload.size > len(declarations)-int(payload.offset) {
				return nil, manifest.NewError(
					manifest.ErrFlux, nil, "decode discovery inputs: invalid declaration payload",
				)
			}
			payload.data = declarations[payload.offset : payload.offset+int64(payload.size)]
		}
		*reference.payload = payload
	}
	return &frame, nil
}

func (f *inputFrame) restoreReferenceRoles() error {
	for i := range f.Files {
		objects := f.Files[i].Objects
		for j := range objects {
			facts := &objects[j].Facts
			roles := facts.SourceRoles
			facts.SourceRoles = nil
			if len(roles) == 0 {
				continue
			}
			if len(roles) != len(facts.Refs) {
				return manifest.NewError(manifest.ErrFlux, nil, "decode discovery inputs: invalid reference roles")
			}
			for k, role := range roles {
				referencerole.Restore(&facts.Refs[k], role)
			}
		}
	}
	return nil
}

func (f *inputFrame) prepare(ctx context.Context) (*cache.Frame, error) {
	f.indexVisits()
	var payload bytes.Buffer
	var encoder *json.Encoder
	var encoded *cachedFileDeclarations
	f.DeclarationBytes = 0
	for reference := range f.payloads {
		part := *reference.payload
		if part == nil {
			continue
		}
		start := payload.Len()
		if observation := part.observation; observation != nil {
			if encoder == nil {
				encoder, encoded = json.NewEncoder(&payload), &cachedFileDeclarations{}
			}
			encoded.Present, encoded.Complete, encoded.Limited = observation.Present, observation.Complete, observation.limited
			encoded.Objects = slices.Grow(encoded.Objects[:0], len(observation.Objects))
			for id, fingerprint := range observation.Objects {
				encoded.Objects = append(encoded.Objects, cachedDeclaration{Identity: id, Fingerprint: fingerprint})
			}
			slices.SortFunc(encoded.Objects, func(a, b cachedDeclaration) int { return a.Identity.Compare(b.Identity) })
			if err := encoder.Encode(encoded); err != nil {
				return nil, manifest.NewError(manifest.ErrFlux, fmt.Errorf("encode declaration inventory: %w", err), "")
			}
		} else {
			data, err := part.read(ctx)
			if err != nil {
				return nil, err
			}
			payload.Write(data)
		}
		*reference.offset, *reference.size = int64(start), payload.Len()-start
		if reference.declaration {
			f.DeclarationBytes += *reference.size
		}
	}
	return f.encode(payload.Bytes())
}

func (f *inputFrame) encode(payload []byte) (*cache.Frame, error) {
	for i := range f.Files {
		objects := f.Files[i].Objects
		for j := range objects {
			facts := &objects[j].Facts
			facts.SourceRoles = encodeReferenceRoles(facts.Refs)
		}
	}
	frame, err := cache.NewFrame("loader-inputs", f, payload)
	for i := range f.Files {
		objects := f.Files[i].Objects
		for j := range objects {
			objects[j].Facts.SourceRoles = nil
		}
	}
	return frame, err
}

func encodeReferenceRoles(refs []manifest.Reference) []referencerole.Roles {
	var roles []referencerole.Roles
	for i := range refs {
		role := referencerole.Capture(&refs[i])
		if role == 0 {
			continue
		}
		if roles == nil {
			roles = make([]referencerole.Roles, len(refs))
		}
		roles[i] = role
	}
	return roles
}

func (f *inputFrame) indexVisits() {
	f.Visits = nil
	visits := map[cachedVisit]int{}
	for i := range f.Walks {
		walk := &f.Walks[i]
		var ids []int
		for visit := range walk.visits() {
			id, known := visits[*visit]
			if !known {
				id = len(f.Visits)
				visits[*visit] = id
				f.Visits = append(f.Visits, *visit)
			}
			ids = append(ids, id)
		}
		walk.VisitIDs = ids
		walk.dictionary = nil
	}
	for i := range f.Walks {
		f.Walks[i].dictionary = f.Visits
		f.Walks[i].Visits = nil
	}
}

func (f *cachedInput) projectionPayload() *inputPayload {
	if f.Projection != nil {
		return f.Projection.payload
	}
	return nil
}

type inputPayloadReference struct {
	payload     **inputPayload
	offset      *int64
	size        *int
	declaration bool
}

func (f *inputFrame) payloads(yield func(inputPayloadReference) bool) {
	for i := range f.Files {
		file := &f.Files[i]
		if (file.declarations != nil || file.DeclarationSize != 0) && !yield(inputPayloadReference{
			payload: &file.declarations, offset: &file.DeclarationOffset,
			size: &file.DeclarationSize, declaration: true,
		}) {
			return
		}
	}
	for i := range f.Files {
		file := &f.Files[i]
		if projection := file.Projection; projection != nil {
			if !yield(
				inputPayloadReference{
					payload: &projection.payload,
					offset:  &projection.Offset,
					size:    &projection.Size,
				},
			) {
				return
			}
		}
		for j := range file.Objects {
			object := &file.Objects[j]
			if (object.payload != nil || object.Size != 0) &&
				!yield(inputPayloadReference{payload: &object.payload, offset: &object.Offset, size: &object.Size}) {
				return
			}
		}
	}
}
