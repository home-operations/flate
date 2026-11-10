package cache

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"sync/atomic"

	"github.com/home-operations/flate/pkg/manifest"
)

const (
	frameVersion     = 1
	framePrefix      = "flatefr1"
	framePrefixSize  = 12
	frameHeaderLimit = 16 << 20
	inputFrameDomain = "loader-inputs"
)

type frameEnvelope struct {
	Domain   string          `json:"Domain"`
	Metadata json.RawMessage `json:"Metadata"`
	Size     int64           `json:"Size"`
	Payload  string          `json:"Payload"`
}

// Frame retains immutable metadata and the acquired generation, never a mutable lookup.
// It MUST NOT be copied after payload access.
type Frame struct {
	envelope     frameEnvelope
	header       []byte
	payload      []byte
	hydrated     atomic.Pointer[[]byte]
	generation   fs.FS
	available    bool
	raw          string
	accepted     *acceptedRender
	acceptedKey  string
	acceptedData []byte
}

// NewFrame returns no frame when metadata exceeds the optional persistence limit.
func NewFrame(domain string, metadata any, payload []byte) (*Frame, error) {
	if domain == "" {
		return nil, manifest.NewError(manifest.ErrFlux, nil, "empty cache frame domain")
	}
	data, err := jsonv2.Marshal(metadata, json.DefaultOptionsV1(), jsonv2.OmitZeroStructFields(true))
	if err != nil {
		return nil, manifest.NewError(manifest.ErrFlux, fmt.Errorf("encode cache metadata: %w", err), "")
	}
	if payload == nil {
		payload = []byte{}
	}
	envelope := frameEnvelope{
		Domain: domain, Metadata: data, Size: int64(len(payload)), Payload: manifest.Fingerprint(payload),
	}
	data, err = json.Marshal(envelope)
	if err != nil {
		return nil, manifest.NewError(manifest.ErrFlux, fmt.Errorf("encode cache frame: %w", err), "")
	}
	if len(data) > frameHeaderLimit {
		return nil, nil //nolint:nilnil // Oversized metadata disables optional persistence without failing valid output.
	}
	header := make([]byte, framePrefixSize+len(data))
	copy(header, framePrefix)
	binary.BigEndian.PutUint32(
		header[8:framePrefixSize],
		uint32(len(data)), //nolint:gosec // frameHeaderLimit bounds this length.
	)
	copy(header[framePrefixSize:], data)
	return &Frame{
		envelope: envelope, header: header, payload: bytes.Clone(payload), available: true,
		raw: frameIdentity(data),
	}, nil
}

// OpenFrame decodes the index without hydrating payloads or retaining an open file.
func OpenFrame(ctx context.Context, domain string, generation fs.FS, metadata any) (*Frame, error) {
	if err := ctx.Err(); err != nil {
		return nil, &CacheError{err}
	}
	input, err := generation.Open(payloadName)
	if err != nil {
		return nil, &CacheError{err}
	}
	defer func() { _ = input.Close() }()
	info, err := input.Stat()
	if err != nil {
		return nil, &CacheError{err}
	}
	var prefix [framePrefixSize]byte
	if _, err := io.ReadFull(input, prefix[:]); err != nil {
		if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, &CacheError{err}
		}
		return nil, manifest.NewError(manifest.ErrFlux, fmt.Errorf("read cache frame: %w", err), "")
	}
	if string(prefix[:8]) != framePrefix {
		return nil, manifest.NewError(manifest.ErrFlux, nil, "invalid cache frame version")
	}
	size := int64(binary.BigEndian.Uint32(prefix[8:]))
	if size > frameHeaderLimit || size > info.Size()-framePrefixSize {
		return nil, manifest.NewError(manifest.ErrFlux, nil, "invalid cache envelope length")
	}
	header := make([]byte, framePrefixSize+size)
	copy(header, prefix[:])
	data := header[framePrefixSize:]
	if _, err := io.ReadFull(input, data); err != nil {
		if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, &CacheError{err}
		}
		return nil, manifest.NewError(manifest.ErrFlux, fmt.Errorf("read cache envelope: %w", err), "")
	}
	var envelope frameEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, manifest.NewError(manifest.ErrFlux, fmt.Errorf("decode cache envelope: %w", err), "")
	}
	if domain == "" || envelope.Domain != domain {
		return nil, manifest.NewError(manifest.ErrFlux, nil, "invalid cache frame domain")
	}
	if envelope.Size < 0 || envelope.Size > math.MaxInt64-int64(len(header)) || envelope.Payload == "" {
		return nil, manifest.NewError(manifest.ErrFlux, nil, "invalid cache payload length or identity")
	}
	if err := json.Unmarshal(envelope.Metadata, metadata); err != nil {
		return nil, manifest.NewError(manifest.ErrFlux, fmt.Errorf("decode cache metadata: %w", err), "")
	}
	frame := &Frame{
		envelope: envelope, header: header, generation: generation, raw: frameIdentity(data),
		available: envelope.Size <= info.Size()-int64(len(header)),
	}
	return frame.readAccepted(input, info.Size())
}

func (f *Frame) readAccepted(input fs.File, size int64) (*Frame, error) {
	if !f.available {
		return f, nil
	}
	end := int64(len(f.header)) + f.envelope.Size
	tailSize := size - end
	if tailSize == 0 || tailSize > frameHeaderLimit {
		return f, nil
	}
	positioned, ok := input.(io.ReaderAt)
	if !ok {
		return f, nil
	}
	tail := make([]byte, tailSize)
	if _, err := positioned.ReadAt(tail, end); err != nil {
		return nil, &CacheError{err}
	}
	var accepted acceptedRender
	if json.Unmarshal(tail, &accepted) == nil && accepted.valid(f.raw) {
		f.accepted, f.acceptedKey = &accepted, manifest.Fingerprint(accepted)
		f.acceptedData = tail
	}
	return f, nil
}

// Payload returns immutable bytes after validating the complete payload identity.
func (f *Frame) Payload(ctx context.Context) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, &CacheError{err}
	}
	return f.payloadBytes()
}

func (f *Frame) payloadBytes() ([]byte, error) {
	if f.payload != nil {
		return f.payload, nil
	}
	if data := f.hydrated.Load(); data != nil {
		return *data, nil
	}
	if f.envelope.Size > int64(math.MaxInt) {
		return nil, manifest.NewError(manifest.ErrFlux, nil, "cache payload exceeds addressable memory")
	}
	data, err := f.readPayload(0, int(f.envelope.Size))
	if err != nil {
		return nil, err
	}
	if manifest.Fingerprint(data) != f.envelope.Payload {
		return nil, manifest.NewError(manifest.ErrFlux, nil, "invalid cache payload identity")
	}
	if !f.hydrated.CompareAndSwap(nil, &data) {
		return *f.hydrated.Load(), nil
	}
	return data, nil
}

// ReadPayload uses payload-relative coordinates within the retained immutable generation.
func (f *Frame) ReadPayload(ctx context.Context, offset int64, size int) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, &CacheError{err}
	}
	return f.readPayload(offset, size)
}

func (f *Frame) readPayload(offset int64, size int) ([]byte, error) {
	if offset < 0 || size < 0 || offset > f.envelope.Size-int64(size) || !f.available {
		return nil, manifest.NewError(manifest.ErrFlux, nil, "cache payload exceeds its generation")
	}
	if f.payload != nil {
		return f.payload[offset : offset+int64(size)], nil
	}
	if data := f.hydrated.Load(); data != nil {
		return (*data)[offset : offset+int64(size)], nil
	}
	if size == 0 {
		return []byte{}, nil
	}
	input, err := f.generation.Open(payloadName)
	if err != nil {
		return nil, &CacheError{err}
	}
	defer func() { _ = input.Close() }()
	positioned, ok := input.(io.ReaderAt)
	if !ok {
		return nil, manifest.NewError(manifest.ErrFlux, nil, "cache payload requires positioned reads")
	}
	data := make([]byte, size)
	if _, err := positioned.ReadAt(data, int64(len(f.header))+offset); err != nil {
		if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, &CacheError{err}
		}
		return nil, manifest.NewError(manifest.ErrFlux, fmt.Errorf("read cache payload: %w", err), "")
	}
	return data, nil
}

func frameIdentity(envelope []byte) string {
	return manifest.Fingerprint(struct {
		Version  int
		Envelope json.RawMessage
	}{Version: frameVersion, Envelope: envelope})
}

// InputFrameIdentity separates stable source lookup from certified frame content.
//
//nolint:gocritic // Distinct result types separate the lookup handle from certified content.
func InputFrameIdentity(origin, url string, content *Frame) (BlobKey, string) {
	lookup := manifest.Fingerprint(struct {
		Version             int
		Domain, Origin, URL string
	}{Version: frameVersion, Domain: inputFrameDomain, Origin: origin, URL: url})
	identity := ""
	if content != nil {
		identity = content.raw
	}
	return BlobKey{Content: lookup, Render: RenderAccepted}, identity
}

// FramePublication keeps prepared bytes and identities immutable across concurrent callers.
type FramePublication struct {
	frame    *Frame
	tail     []byte
	identity string
}

// Identity binds deduplication to the complete prepared publication.
func (p *FramePublication) Identity() string { return p.identity }

// Frame exposes the prepared snapshot without changing the original acceptance.
func (p *FramePublication) Frame() *Frame { return p.frame }

// WriteTo preserves the original writer error and byte count.
func (p *FramePublication) WriteTo(w io.Writer) (int64, error) {
	payload, err := p.frame.payloadBytes()
	if err != nil {
		return 0, err
	}
	//nolint:wrapcheck // Publication preserves the writer's original error and byte count.
	return io.Copy(
		w,
		io.MultiReader(bytes.NewReader(p.frame.header), bytes.NewReader(payload), bytes.NewReader(p.tail)),
	)
}

var _ io.WriterTo = (*FramePublication)(nil)
