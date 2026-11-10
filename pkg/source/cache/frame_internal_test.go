package cache

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"maps"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func frameGeneration(data []byte) fs.FS {
	return fileFS{
		name: "content", size: int64(len(data)),
		open: func() (io.ReadCloser, error) { return memoryBlob{bytes.NewReader(data)}, nil },
	}
}

//nolint:gocritic // Value input preserves the publication snapshot used by the test.
func frameBytes(t *testing.T, frame *Frame, input FramePublicationInput) []byte {
	t.Helper()
	publication, err := frame.PreparePublication(t.Context(), input)
	assert.NoError(t, err)
	assert.FatalIf(t, publication == nil, "publication unavailable")
	var output bytes.Buffer
	n, err := publication.WriteTo(&output)
	assert.NoError(t, err)
	assert.Equal(t, n, int64(output.Len()))
	return output.Bytes()
}

func TestFrameHistory_FinalContent(t *testing.T) {
	t.Parallel()
	for _, domain := range []string{"helm-output", "kustomize-output", inputFrameDomain, "kustomize-entry"} {
		t.Run(domain, func(t *testing.T) {
			t.Parallel()
			owner := manifest.NamedResource{Kind: manifest.KindKustomization, Name: "owner"}
			child := manifest.NamedResource{Kind: "ConfigMap", Name: "child"}
			metadata := map[string]string{"key": "value"}
			frame, err := NewFrame(domain, metadata, []byte("output"))
			assert.NoError(t, err)
			input := FramePublicationInput{
				Owner: owner, Content: metadata, Complete: true,
				Records: maps.All(map[manifest.NamedResource]manifest.DiscoveryRecord{child: {Path: "apps/cm.yaml"}}),
			}
			wire := frameBytes(t, frame, input)
			again := frameBytes(t, frame, input)
			assert.Equal(t, bytes.Equal(wire, again), true)
			var decoded map[string]string
			acquired, err := OpenFrame(t.Context(), domain, frameGeneration(wire), &decoded)
			assert.NoError(t, err)
			assert.Diff(t, decoded, metadata)
			history, complete := acquired.History(owner, metadata)
			assert.Equal(t, complete, true)
			assert.Equal(t, maps.Collect(history)[child].Path, "apps/cm.yaml")
			_, complete = acquired.History(owner, map[string]string{"key": "changed"})
			assert.Equal(t, complete, false)
			_, complete = acquired.History(manifest.NamedResource{Kind: owner.Kind, Name: "foreign"}, metadata)
			assert.Equal(t, complete, false)
			_, complete = acquired.History(owner, make(chan int))
			assert.Equal(t, complete, false)
			data, err := acquired.Payload(t.Context())
			assert.NoError(t, err)
			assert.Equal(t, string(data), "output")
			_, err = OpenFrame(t.Context(), domain+"-foreign", frameGeneration(wire), &decoded)
			assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
			for _, incomplete := range []FramePublicationInput{{}, {Owner: owner, Content: metadata, Records: input.Records}, {Complete: true}} {
				raw := frameBytes(t, frame, incomplete)
				assert.Equal(t, len(raw), len(frame.header)+len(frame.payload))
				decodedFrame, err := OpenFrame(t.Context(), domain, frameGeneration(raw), &decoded)
				assert.NoError(t, err)
				_, complete := decodedFrame.History(owner, metadata)
				assert.Equal(t, complete, false)
				data, err := decodedFrame.Payload(t.Context())
				assert.NoError(t, err)
				assert.Equal(t, string(data), "output")
			}
		})
	}
}

func TestFrame_Malformed(t *testing.T) {
	t.Parallel()
	frame, err := NewFrame("domain", map[string]string{"key": "value"}, []byte("output"))
	assert.NoError(t, err)
	raw := frameBytes(t, frame, FramePublicationInput{})
	for _, tc := range []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{"minimum", func(data []byte) []byte { return data[:7:7] }},
		{"domain prefix", func(data []byte) []byte { data[0] ^= 1; return data }},
		{"prefix truncated", func(data []byte) []byte { return data[:framePrefixSize-1] }},
		{"version", func(data []byte) []byte { data[7]++; return data }},
		{"envelope beyond data", func(data []byte) []byte {
			binary.BigEndian.PutUint32(data[8:12], uint32(len(data)))
			return data
		}},
		{"envelope limit", func(data []byte) []byte {
			size := int(binary.BigEndian.Uint32(data[8:12]))
			header := append(bytes.Clone(data[12:12+size]), bytes.Repeat([]byte(" "), frameHeaderLimit+1-size)...)
			wire := append(bytes.Clone(data[:12]), header...)
			binary.BigEndian.PutUint32(wire[8:12], uint32(len(header)))
			return append(wire, data[12+size:]...)
		}},
		{"envelope overflow", func(data []byte) []byte { binary.BigEndian.PutUint32(data[8:12], math.MaxUint32); return data }},
		{"envelope truncated", func(data []byte) []byte { return data[:len(frame.header)-1] }},
		{"envelope syntax", func(data []byte) []byte { data[framePrefixSize] = '!'; return data }},
		{"metadata type", func(data []byte) []byte {
			return bytes.Replace(data, []byte(`"Metadata":{"key":"value"}`), []byte(`"Metadata":{"key":1234567}`), 1)
		}},
		{"payload negative", func(data []byte) []byte { return rewriteFrameEnvelope(t, data, func(e *frameEnvelope) { e.Size = -1 }) }},
		{"payload overflow", func(data []byte) []byte {
			return rewriteFrameEnvelope(t, data, func(e *frameEnvelope) { e.Size = math.MaxInt64 })
		}},
		{"payload identity absent", func(data []byte) []byte {
			return rewriteFrameEnvelope(t, data, func(e *frameEnvelope) { e.Payload = "" })
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var metadata map[string]string
			_, err := OpenFrame(t.Context(), "domain", frameGeneration(tc.mutate(bytes.Clone(raw))), &metadata)
			assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
			if tc.name == "envelope syntax" {
				_, syntax := errors.AsType[*json.SyntaxError](err)
				assert.Equal(t, syntax, true)
			}
		})
	}
	for _, tc := range []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{"payload beyond data", func(data []byte) []byte {
			return rewriteFrameEnvelope(t, data, func(e *frameEnvelope) { e.Size = int64(len(data) + 1) })
		}},
		{"truncated payload", func(data []byte) []byte { return data[:len(data)-1] }},
		{"payload identity", func(data []byte) []byte { data[len(data)-1] ^= 1; return data }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var metadata map[string]string
			acquired, err := OpenFrame(t.Context(), "domain", frameGeneration(tc.mutate(bytes.Clone(raw))), &metadata)
			assert.NoError(t, err)
			_, err = acquired.Payload(t.Context())
			assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
			_, complete := acquired.History(manifest.NamedResource{}, metadata)
			assert.Equal(t, complete, false)
		})
	}
}

func rewriteFrameEnvelope(t *testing.T, data []byte, mutate func(*frameEnvelope)) []byte {
	t.Helper()
	size := int(binary.BigEndian.Uint32(data[8:12]))
	var envelope frameEnvelope
	assert.NoError(t, json.Unmarshal(data[12:12+size], &envelope))
	mutate(&envelope)
	header, err := json.Marshal(envelope)
	assert.NoError(t, err)
	result := append(bytes.Clone(data[:12]), header...)
	binary.BigEndian.PutUint32(result[8:12], uint32(len(header)))
	return append(result, data[12+size:]...)
}

func TestFrameHistory_MalformedTail(t *testing.T) {
	t.Parallel()
	owner := manifest.NamedResource{Kind: manifest.KindKustomization, Name: "owner"}
	child := manifest.NamedResource{Kind: "ConfigMap", Name: "child"}
	frame, err := NewFrame("domain", "metadata", []byte("output"))
	assert.NoError(t, err)
	input := FramePublicationInput{
		Owner: owner, Content: "final", Complete: true,
		Records: maps.All(map[manifest.NamedResource]manifest.DiscoveryRecord{
			child: {ContentRoots: map[manifest.NamedResource][]string{owner: {"apps"}}},
		}),
	}
	publication, err := frame.PreparePublication(t.Context(), input)
	assert.NoError(t, err)
	raw := frameBytes(t, frame, FramePublicationInput{})
	for _, tc := range []struct {
		name   string
		mutate func(*acceptedRender)
	}{
		{"version", func(a *acceptedRender) { a.Version++ }},
		{"incomplete", func(a *acceptedRender) { a.Complete = false }},
		{"raw", func(a *acceptedRender) { a.Raw = "foreign" }},
		{"final", func(a *acceptedRender) { a.Final = "" }},
		{"duplicate record", func(a *acceptedRender) { a.Records = append(a.Records, a.Records[0]) }},
		{"duplicate root", func(a *acceptedRender) { a.Records[0].Roots = append(a.Records[0].Roots, a.Records[0].Roots[0]) }},
		{"record order", func(a *acceptedRender) {
			previous := a.Records[0]
			previous.ID.Name = "a"
			a.Records = append(a.Records, previous)
		}},
		{"root order", func(a *acceptedRender) {
			previous := a.Records[0].Roots[0]
			previous.Source.Name = "a"
			a.Records[0].Roots = append(a.Records[0].Roots, previous)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var accepted acceptedRender
			assert.NoError(t, json.Unmarshal(publication.tail, &accepted))
			tc.mutate(&accepted)
			tail, marshalErr := json.Marshal(accepted)
			assert.NoError(t, marshalErr)
			var metadata string
			acquired, openErr := OpenFrame(
				t.Context(),
				"domain",
				frameGeneration(append(bytes.Clone(raw), tail...)),
				&metadata,
			)
			assert.NoError(t, openErr)
			_, complete := acquired.History(owner, "final")
			assert.Equal(t, complete, false)
			data, payloadErr := acquired.Payload(t.Context())
			assert.NoError(t, payloadErr)
			assert.Equal(t, string(data), "output")
		})
	}
	var fields map[string]json.RawMessage
	assert.NoError(t, json.Unmarshal(publication.tail, &fields))
	fields["Records"] = json.RawMessage(`{}`)
	invalidRecords, err := json.Marshal(fields)
	assert.NoError(t, err)
	for _, tc := range []struct {
		name string
		tail []byte
	}{
		{"syntax", []byte("{")},
		{"record type", invalidRecords},
		{"accepted limit", append(bytes.Clone(publication.tail), bytes.Repeat([]byte(" "), frameHeaderLimit+1-len(publication.tail))...)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var metadata string
			acquired, openErr := OpenFrame(
				t.Context(),
				"domain",
				frameGeneration(append(bytes.Clone(raw), tc.tail...)),
				&metadata,
			)
			assert.NoError(t, openErr)
			assert.Equal(t, acquired.accepted == nil, true)
			_, complete := acquired.History(owner, "final")
			assert.Equal(t, complete, false)
			data, payloadErr := acquired.Payload(t.Context())
			assert.NoError(t, payloadErr)
			assert.Equal(t, string(data), "output")
		})
	}
}

func TestFrameHistory_CanonicalRecords(t *testing.T) {
	t.Parallel()
	owner := manifest.NamedResource{Kind: manifest.KindKustomization, Name: "owner"}
	a, b := manifest.NamedResource{Name: "a"}, manifest.NamedResource{Name: "b"}
	record := manifest.DiscoveryRecord{
		Inputs: []string{}, Refs: []manifest.NamedResource{b, a, b},
		Productions:  []manifest.Production{{Owner: b}, {Owner: a}, {Owner: b}},
		ContentRoots: map[manifest.NamedResource][]string{b: {"z", "a", "z"}, a: nil},
	}
	frame, err := NewFrame("domain", "metadata", nil)
	assert.NoError(t, err)
	calls := 0
	input := FramePublicationInput{
		Owner: owner, Content: "final", Complete: true,
		Records: func(yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {
			calls++
			assert.Equal(t, calls, 1)
			if yield(b, record) {
				yield(a, manifest.DiscoveryRecord{})
			}
		},
	}
	wire := frameBytes(t, frame, input)
	assert.Equal(t, calls, 1)
	record.Refs[0].Name = "mutated"
	record.Productions[0].Path = "mutated"
	record.ContentRoots[b][0] = "mutated"
	var metadata string
	acquired, err := OpenFrame(t.Context(), "domain", frameGeneration(wire), &metadata)
	assert.NoError(t, err)
	history, complete := acquired.History(owner, "final")
	assert.Equal(t, complete, true)
	var ids []manifest.NamedResource
	for id := range history {
		ids = append(ids, id)
	}
	assert.Diff(t, ids, []manifest.NamedResource{a, b})
	got := maps.Collect(history)
	assert.Diff(t, got[b].Refs, []manifest.NamedResource{a, b})
	assert.Diff(t, got[b].Productions, []manifest.Production{{Owner: a}, {Owner: b}})
	assert.Diff(t, got[b].ContentRoots[b], []string{"a", "z"})
	assert.Equal(t, got[b].ContentRoots[a] == nil, true)
	assert.Equal(t, got[b].Inputs == nil, false)
	assert.Equal(t, got[a].Inputs == nil, true)
	got[b].Refs[0].Name = "mutated"
	got[b].Productions[0].Path = "mutated"
	got[b].ContentRoots[b][0] = "mutated"
	delete(got[b].ContentRoots, a)
	untouched := maps.Collect(history)
	assert.Diff(t, untouched[b].Refs, []manifest.NamedResource{a, b})
	assert.Diff(t, untouched[b].Productions, []manifest.Production{{Owner: a}, {Owner: b}})
	assert.Diff(t, untouched[b].ContentRoots[b], []string{"a", "z"})
	_, found := untouched[b].ContentRoots[a]
	assert.Equal(t, found, true)
	emptyInput := FramePublicationInput{
		Owner: owner, Content: "final", Complete: true,
		Records: maps.All(map[manifest.NamedResource]manifest.DiscoveryRecord{}),
	}
	emptyWire := frameBytes(t, frame, emptyInput)
	empty, err := OpenFrame(t.Context(), "domain", frameGeneration(emptyWire), &metadata)
	assert.NoError(t, err)
	emptyHistory, complete := empty.History(owner, "final")
	assert.Equal(t, complete && emptyHistory != nil, true)
	assert.Equal(t, len(maps.Collect(emptyHistory)), 0)
	empty.accepted = nil
	_, complete = empty.History(owner, "final")
	assert.Equal(t, complete, false)
	input.Records = func(yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {
		if yield(a, record) {
			yield(a, record)
		}
	}
	_, err = frame.PreparePublication(t.Context(), input)
	assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
	_, err = empty.PreparePublication(t.Context(), FramePublicationInput{
		Owner: owner, Content: "final", Complete: true, Records: emptyInput.Records,
	})
	assert.NoError(t, err)
}

func TestFrame_ReadPayload(t *testing.T) {
	t.Parallel()
	frame, err := NewFrame(inputFrameDomain, "index", []byte("firstsecond"))
	assert.NoError(t, err)
	wire := frameBytes(t, frame, FramePublicationInput{})
	var metadata string
	acquired, err := OpenFrame(t.Context(), inputFrameDomain, frameGeneration(wire), &metadata)
	assert.NoError(t, err)
	for _, f := range []*Frame{frame, acquired} {
		data, readErr := f.ReadPayload(t.Context(), 5, 6)
		assert.NoError(t, readErr)
		assert.Equal(t, string(data), "second")
		for _, coordinate := range []struct {
			offset int64
			size   int
		}{{-1, 1}, {0, -1}, {math.MaxInt64, 1}, {10, 2}} {
			_, coordinateErr := f.ReadPayload(t.Context(), coordinate.offset, coordinate.size)
			assert.Equal(t, errors.Is(coordinateErr, manifest.ErrFlux), true)
		}
	}
	truncated := frameGeneration(wire[:len(frame.header)])
	acquired, err = OpenFrame(t.Context(), inputFrameDomain, truncated, &metadata)
	assert.NoError(t, err)
	_, err = acquired.ReadPayload(t.Context(), 0, 5)
	assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
	_, complete := acquired.History(manifest.NamedResource{}, "index")
	assert.Equal(t, complete, false)
	empty, err := NewFrame(inputFrameDomain, "empty index", nil)
	assert.NoError(t, err)
	emptyWire := frameBytes(t, empty, FramePublicationInput{})
	empty, err = OpenFrame(t.Context(), inputFrameDomain, frameGeneration(emptyWire), &metadata)
	assert.NoError(t, err)
	data, err := empty.Payload(t.Context())
	assert.NoError(t, err)
	assert.Diff(t, data, []byte{})
}

func TestFrame_Cancellation(t *testing.T) {
	t.Parallel()
	frame, err := NewFrame("domain", "metadata", []byte("output"))
	assert.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	var metadata string
	_, err = OpenFrame(ctx, "domain", frameGeneration(frameBytes(t, frame, FramePublicationInput{})), &metadata)
	assert.Equal(t, errors.Is(err, context.Canceled), true)
	_, err = frame.Payload(ctx)
	assert.Equal(t, errors.Is(err, context.Canceled), true)
	_, err = frame.ReadPayload(ctx, 0, 1)
	assert.Equal(t, errors.Is(err, context.Canceled), true)
	_, err = frame.PreparePublication(ctx, FramePublicationInput{})
	assert.Equal(t, errors.Is(err, context.Canceled), true)
	acquired, err := OpenFrame(
		t.Context(),
		"domain",
		frameGeneration(frameBytes(t, frame, FramePublicationInput{})),
		&metadata,
	)
	assert.NoError(t, err)
	_, err = acquired.Payload(ctx)
	assert.Equal(t, errors.Is(err, context.Canceled), true)
	_, storage := errors.AsType[*CacheError](err)
	assert.Equal(t, storage, true)
}

func TestFrame_StorageErrors(t *testing.T) {
	t.Parallel()
	for _, stage := range []string{"metadata", "payload"} {
		t.Run(stage, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			file := filepath.Join(root, payloadName)
			var metadata string
			var err error
			if stage == "metadata" {
				assert.NoError(t, os.Mkdir(file, 0o750))
				_, err = OpenFrame(t.Context(), "domain", os.DirFS(root), &metadata)
			} else {
				frame, frameErr := NewFrame("domain", "metadata", []byte("payload"))
				assert.NoError(t, frameErr)
				assert.NoError(t, os.WriteFile(file, frameBytes(t, frame, FramePublicationInput{}), 0o600))
				acquired, frameErr := OpenFrame(t.Context(), "domain", os.DirFS(root), &metadata)
				assert.NoError(t, frameErr)
				assert.NoError(t, os.Remove(file))
				_, err = acquired.Payload(t.Context())
			}
			_, storage := errors.AsType[*CacheError](err)
			_, pathError := errors.AsType[*fs.PathError](err)
			assert.Equal(t, storage, true)
			assert.Equal(t, pathError, true)
			assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
		})
	}
}

type frameErrorWriter struct {
	remaining int
	err       error
}

func (w *frameErrorWriter) Write(data []byte) (int, error) {
	n := min(len(data), w.remaining)
	w.remaining -= n
	if n < len(data) {
		return n, w.err
	}
	return n, nil
}

func TestFramePublication_Writer(t *testing.T) {
	t.Parallel()
	frame, err := NewFrame("domain", "metadata", []byte("output"))
	assert.NoError(t, err)
	publication, err := frame.PreparePublication(t.Context(), FramePublicationInput{
		Content: "final", Complete: true, Records: maps.All(map[manifest.NamedResource]manifest.DiscoveryRecord{}),
	})
	assert.NoError(t, err)
	failure := errors.New("writer failure")
	for _, count := range []int{0, len(frame.header) + 2, len(frame.header) + len(frame.payload) + 3} {
		n, err := publication.WriteTo(&frameErrorWriter{remaining: count, err: failure})
		assert.Equal(t, n, int64(count))
		assert.Equal(t, err, failure)
	}
	var first, second bytes.Buffer
	var failures [2]error
	var writers sync.WaitGroup
	for i, output := range []*bytes.Buffer{&first, &second} {
		writers.Go(func() { _, failures[i] = publication.WriteTo(output) })
	}
	writers.Wait()
	for _, err := range failures {
		assert.NoError(t, err)
	}
	assert.Equal(t, bytes.Equal(first.Bytes(), second.Bytes()), true)
}

func TestInputFrameIdentity_ContentAndScope(t *testing.T) {
	t.Parallel()
	type observations struct {
		Files     map[string]string
		Types     map[string]fs.FileMode
		Traversal []string
	}
	initial := observations{
		Files:     map[string]string{"a.yaml": "digest"},
		Types:     map[string]fs.FileMode{"a.yaml": 0},
		Traversal: []string{"."},
	}
	frame, err := NewFrame(inputFrameDomain, initial, []byte("payload"))
	assert.NoError(t, err)
	lookup, content := InputFrameIdentity("/source", "https://fixture.invalid/one", frame)
	assert.Equal(t, lookup.Render, RenderAccepted)
	assert.FatalIf(t, content == "", "missing certified content identity")
	emptyLookup, emptyContent := InputFrameIdentity("/source", "https://fixture.invalid/one", nil)
	assert.Equal(t, emptyLookup.Content, lookup.Content)
	assert.Equal(t, emptyContent, "")
	for _, change := range []struct {
		name     string
		metadata observations
		payload  string
	}{
		{"file", observations{Files: map[string]string{"a.yaml": "edited"}, Types: initial.Types, Traversal: initial.Traversal}, "payload"},
		{"type", observations{Files: initial.Files, Types: map[string]fs.FileMode{"a.yaml": fs.ModeDir}, Traversal: initial.Traversal}, "payload"},
		{"traversal", observations{Files: initial.Files, Types: initial.Types, Traversal: []string{".", "apps"}}, "payload"},
		{"payload", initial, "changed"},
	} {
		t.Run(change.name, func(t *testing.T) {
			t.Parallel()
			changed, frameErr := NewFrame(inputFrameDomain, change.metadata, []byte(change.payload))
			assert.NoError(t, frameErr)
			currentLookup, currentContent := InputFrameIdentity("/source", "https://fixture.invalid/one", changed)
			assert.Equal(t, currentLookup.Content, lookup.Content)
			assert.Equal(t, currentContent != content, true)
		})
	}
	for _, scope := range [][2]string{{"/other", "https://fixture.invalid/one"}, {"/source", "https://fixture.invalid/two"}} {
		other, _ := InputFrameIdentity(scope[0], scope[1], frame)
		assert.Equal(t, other.Content != lookup.Content, true)
	}
}

func TestFramePublication_UnavailableContent(t *testing.T) {
	t.Parallel()
	frame, err := NewFrame(inputFrameDomain, "index", []byte("valid output"))
	assert.NoError(t, err)
	input := FramePublicationInput{
		Content:  "final",
		Complete: true,
		Records:  maps.All(map[manifest.NamedResource]manifest.DiscoveryRecord{}),
	}
	input.Content = make(chan int)
	publication, err := frame.PreparePublication(t.Context(), input)
	assert.NoError(t, err)
	assert.Equal(t, publication == nil, true)
	data, err := frame.Payload(t.Context())
	assert.NoError(t, err)
	assert.Equal(t, string(data), "valid output")
	oversized, err := NewFrame(inputFrameDomain, strings.Repeat("x", frameHeaderLimit), data)
	assert.NoError(t, err)
	assert.Equal(t, oversized == nil, true)
}

func TestFrame_AcquiredGeneration(t *testing.T) {
	t.Parallel()
	c := cachetest.New(t, t.TempDir(), New)
	var frames [2]*Frame
	for i, content := range []string{"first", "second"} {
		frame, err := NewFrame(inputFrameDomain, content, []byte(content))
		assert.NoError(t, err)
		wire := frameBytes(t, frame, FramePublicationInput{})
		generation, _, _, err := c.Blob(
			t.Context(),
			BlobKey{Content: "shared"},
			&Policy{},
			func(_ context.Context, out ProducerInput) (ProducerResult, error) {
				_, writeErr := out.Write(wire)
				return ProducerResult{}, writeErr
			},
		)
		assert.NoError(t, err)
		var metadata string
		frames[i], err = OpenFrame(t.Context(), inputFrameDomain, generation, &metadata)
		assert.NoError(t, err)
	}
	var failures [2]error
	var outputs [2][]byte
	var readers sync.WaitGroup
	for i := range frames {
		readers.Go(
			func() { outputs[i], failures[i] = frames[i].ReadPayload(t.Context(), 0, int(frames[i].envelope.Size)) },
		)
	}
	readers.Wait()
	for _, err := range failures {
		assert.NoError(t, err)
	}
	assert.Diff(t, outputs, [2][]byte{[]byte("first"), []byte("second")})
	for _, frame := range frames {
		var loaded [2][]byte
		for i := range loaded {
			readers.Go(func() { loaded[i], failures[i] = frame.Payload(t.Context()) })
		}
		readers.Wait()
		for _, failure := range failures {
			assert.NoError(t, failure)
		}
		assert.Diff(t, loaded[0], loaded[1])
	}
	assert.NoError(t, c.Close())
	for i, frame := range frames {
		data, readErr := frame.ReadPayload(t.Context(), 0, int(frame.envelope.Size))
		assert.NoError(t, readErr)
		assert.Diff(t, data, outputs[i])
		publication, prepareErr := frame.PreparePublication(t.Context(), FramePublicationInput{})
		assert.NoError(t, prepareErr)
		var published bytes.Buffer
		_, writeErr := publication.WriteTo(&published)
		assert.NoError(t, writeErr)
		var metadata string
		restored, openErr := OpenFrame(t.Context(), inputFrameDomain, frameGeneration(published.Bytes()), &metadata)
		assert.NoError(t, openErr)
		data, readErr = restored.Payload(t.Context())
		assert.NoError(t, readErr)
		assert.Diff(t, data, outputs[i])
	}
}

func TestFrame_CompactBacking(t *testing.T) {
	t.Parallel()
	owner := manifest.NamedResource{Kind: "owner", Name: "renderer"}
	payload := make([]byte, len("original"), 1<<20)
	copy(payload, "original")
	metadata := map[string]string{"key": "original"}
	frame, err := NewFrame("domain", metadata, payload)
	assert.NoError(t, err)
	assert.FatalIf(t, cap(frame.payload) >= cap(payload), "frame retains caller payload backing")
	wire := make([]byte, 0, 1<<20)
	wire = append(wire, frameBytes(t, frame, FramePublicationInput{
		Owner: owner, Content: "final", Complete: true,
		Records: maps.All(map[manifest.NamedResource]manifest.DiscoveryRecord{}),
	})...)
	c := cachetest.New(t, "", New)
	generation, _, _, err := c.Blob(
		t.Context(),
		BlobKey{Content: "compact"},
		&Policy{},
		func(_ context.Context, out ProducerInput) (ProducerResult, error) {
			_, writeErr := out.Write(wire)
			return ProducerResult{}, writeErr
		},
	)
	assert.NoError(t, err)
	var decoded map[string]string
	acquired, err := OpenFrame(t.Context(), "domain", generation, &decoded)
	assert.NoError(t, err)
	assert.FatalIf(
		t,
		acquired.payload != nil || cap(acquired.header) >= cap(wire) || cap(acquired.acceptedData) >= cap(wire),
		"decoded frame retains serialized buffer backing",
	)
	assert.FatalIf(
		t,
		acquired.accepted == nil || acquired.acceptedKey == "" || acquired.raw == "",
		"compact frame lost accepted evidence",
	)
	for i := range wire {
		wire[i] = 'x'
	}
	for i := range payload {
		payload[i] = 'x'
	}
	metadata["key"] = "mutated"
	assert.Equal(t, string(frame.payload), "original")
	assert.Equal(t, string(frame.envelope.Metadata), `{"key":"original"}`)
	data, err := acquired.Payload(t.Context())
	assert.NoError(t, err)
	assert.Equal(t, string(data), "original")
	assert.Equal(t, decoded["key"], "original")
	previous, complete := acquired.History(owner, "final")
	assert.FatalIf(t, !complete || previous == nil, "input-buffer mutation changed compact history")
	for range previous {
		t.Fatal("complete-empty compact history gained a child")
	}
}

func TestFramePublication_PreparedSnapshot(t *testing.T) {
	t.Parallel()
	owner := manifest.NamedResource{Group: "group", Kind: "owner", Namespace: "team", Name: "renderer"}
	frame, err := NewFrame("domain", "metadata", []byte("output"))
	assert.NoError(t, err)
	input := FramePublicationInput{
		Owner: owner, Content: "final", Complete: true,
		Records: maps.All(map[manifest.NamedResource]manifest.DiscoveryRecord{}),
	}
	publication, err := frame.PreparePublication(t.Context(), input)
	assert.NoError(t, err)
	_, complete := frame.History(owner, "final")
	assert.Equal(t, complete, false)
	prepared := publication.Frame()
	history, complete := prepared.History(owner, "final")
	assert.Equal(t, complete && history != nil, true)
	assert.Equal(t, len(maps.Collect(history)), 0)
	for _, tc := range []struct {
		name   string
		mutate func(*manifest.NamedResource)
	}{
		{"group", func(id *manifest.NamedResource) { id.Group = "foreign" }},
		{"kind", func(id *manifest.NamedResource) { id.Kind = "foreign" }},
		{"namespace", func(id *manifest.NamedResource) { id.Namespace = "foreign" }},
		{"name", func(id *manifest.NamedResource) { id.Name = "foreign" }},
	} {
		t.Run("owner/"+tc.name, func(t *testing.T) {
			t.Parallel()
			foreign := owner
			tc.mutate(&foreign)
			_, foreignComplete := prepared.History(foreign, "final")
			assert.Equal(t, foreignComplete, false)
		})
	}
	again, err := prepared.PreparePublication(t.Context(), input)
	assert.NoError(t, err)
	assert.Equal(t, again.Identity(), publication.Identity())
	assert.Equal(t, bytes.Equal(again.tail, publication.tail), true)
	for _, incomplete := range []FramePublicationInput{{}, {Owner: owner, Content: "final", Records: input.Records}, {Complete: true}} {
		replacement, prepareErr := prepared.PreparePublication(t.Context(), incomplete)
		assert.NoError(t, prepareErr)
		assert.Equal(t, replacement == nil, true)
		_, retainedComplete := prepared.History(owner, "final")
		assert.Equal(t, retainedComplete, true)
	}
	changed := input
	changed.Content = "changed"
	replacement, err := prepared.PreparePublication(t.Context(), changed)
	assert.NoError(t, err)
	_, complete = replacement.Frame().History(owner, "changed")
	assert.Equal(t, complete, true)
	_, complete = prepared.History(owner, "final")
	assert.Equal(t, complete, true)
	_, complete = prepared.History(owner, "changed")
	assert.Equal(t, complete, false)
}

func TestFramePublication_AcceptanceLimit(t *testing.T) {
	t.Parallel()
	frame, err := NewFrame("domain", "metadata", []byte("valid output"))
	assert.NoError(t, err)
	publication, err := frame.PreparePublication(t.Context(), FramePublicationInput{
		Content: "final", Complete: true,
		Records: maps.All(map[manifest.NamedResource]manifest.DiscoveryRecord{
			{Name: "child"}: {Inputs: []string{strings.Repeat("x", frameHeaderLimit+1)}},
		}),
	})
	assert.NoError(t, err)
	assert.Equal(t, publication == nil, true)
	data, err := frame.Payload(t.Context())
	assert.NoError(t, err)
	assert.Equal(t, string(data), "valid output")
}
