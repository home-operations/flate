package cache

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/url"
	"os"
	"reflect"
	"slices"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/pkg/manifest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/content"
	"oras.land/oras-go/v2/errdef"
	"oras.land/oras-go/v2/registry/remote/errcode"
)

const (
	outcomeArtifact      = "application/vnd.flate.failure.v1"
	outcomeConfig        = "application/vnd.flate.failure.config.v1+json"
	outcomeConfigLimit   = 4096
	outcomeManifestLimit = 8192
	outcomeNodeLimit     = 128
	outcomeDepthLimit    = 16
)

// Standard wrapper identity is inspected only during bounded failure encoding.
var (
	fmtWrapType  = reflect.TypeOf(fmt.Errorf("%w", io.EOF))
	fmtWrapsType = reflect.TypeOf(fmt.Errorf("%w %w", io.EOF, io.ErrUnexpectedEOF))
	joinType     = reflect.TypeOf(errors.Join(io.EOF))
)

type outcomeCause struct {
	Kind      string         `json:"kind"`
	Op        string         `json:"op,omitempty"`
	Network   string         `json:"network,omitempty"`
	Status    int            `json:"status,omitempty"`
	Code      string         `json:"code,omitempty"`
	Temporary bool           `json:"temporary,omitempty"`
	NotFound  bool           `json:"notFound,omitempty"`
	Children  []outcomeCause `json:"children,omitempty"`
}

type outcomeRecord struct {
	Version   int          `json:"version"`
	Completed time.Time    `json:"completed"`
	Count     uint32       `json:"count"`
	Cause     outcomeCause `json:"cause"`
}

type outcome struct {
	record   outcomeRecord
	lastGood ocispec.Descriptor
	observed time.Time
	invalid  bool
}

func nextOutcomeCount(count uint32) uint32 {
	if count == math.MaxUint32 {
		return count
	}
	return count + 1
}

func outcomeDeadline(completed time.Time, count uint32) (time.Time, bool) {
	_, offset := completed.Zone()
	if completed.IsZero() || completed.Year() < 1 || completed.Year() > 9999 || offset != 0 || count == 0 {
		return time.Time{}, false
	}
	delay, _ := backoffSchedule(uint64(count-1), 0, 0)
	deadline := completed.Add(delay)
	return deadline, deadline.Year() <= 9999 && deadline.After(completed)
}

// Observation is protected by the acquisition's existing key lock. Rollback invalidation
// MUST survive subsequent observations, even when the clock remains after completion.
func (o *outcome) observe(now time.Time) (fresh, valid bool) {
	deadline, valid := outcomeDeadline(o.record.Completed, o.record.Count)
	if !valid || now.Before(o.record.Completed) || !o.observed.IsZero() && now.Before(o.observed) {
		o.invalid = true
	}
	o.observed = now
	return !o.invalid && now.Before(deadline), !o.invalid
}

func newOutcome(err error, completed time.Time, count uint32, lastGood ocispec.Descriptor) *outcome {
	if _, valid := outcomeDeadline(completed, count); !valid {
		return nil
	}
	codec := outcomeCodecState{}
	cause, ok := codec.encode(err, 0, 1)
	if !ok || !codec.terminal || !codec.remote {
		return nil
	}
	lastGood, ok = outcomeLayer(lastGood)
	if !ok {
		return nil
	}
	return &outcome{record: outcomeRecord{Version: 1, Completed: completed, Count: count, Cause: cause}, lastGood: lastGood}
}

// Only the anchor's generation ID is needed by existing graph maintenance. No
// upstream diagnostics, URLs, inline data or platform metadata enter a failure.
func outcomeLayer(desc ocispec.Descriptor) (ocispec.Descriptor, bool) {
	if desc.Digest == "" {
		return ocispec.Descriptor{}, true
	}
	if desc.Digest.Validate() != nil || desc.Size < 0 || desc.MediaType != "application/octet-stream" {
		return ocispec.Descriptor{}, false
	}
	layer := ocispec.Descriptor{MediaType: desc.MediaType, Digest: desc.Digest, Size: desc.Size}
	if id := desc.Annotations[prefix+"git-generation"]; id != "" {
		parsed, err := uuid.Parse(id)
		if err != nil || parsed.String() != id {
			return ocispec.Descriptor{}, false
		}
		layer.Annotations = map[string]string{prefix + "git-generation": id}
	}
	return layer, true
}

type outcomeContext uint8

const (
	outcomeTransport outcomeContext = 1 << iota
	outcomeStatus
)

type outcomeCodec struct {
	minChildren, maxChildren          int
	enter, require                    outcomeContext
	transient, code, terminal, remote bool
	sentinel                          error
	build                             func(outcomeCause, []error) (error, bool)
}

var outcomeCodecs = map[string]outcomeCodec{
	"terminal": {minChildren: 1, maxChildren: 1, terminal: true, build: func(_ outcomeCause, children []error) (error, bool) {
		return &TerminalError{Err: children[0], Attempted: false}, true
	}},
	"producer": {minChildren: 1, maxChildren: 1, build: func(_ outcomeCause, children []error) (error, bool) { return &producerError{children[0]}, true }},
	"url": {minChildren: 1, maxChildren: 1, enter: outcomeTransport, build: func(n outcomeCause, children []error) (error, bool) {
		return &url.Error{Op: n.Op, Err: children[0]}, true
	}},
	"net": {minChildren: 1, maxChildren: 1, enter: outcomeTransport, build: func(n outcomeCause, children []error) (error, bool) {
		return &net.OpError{Op: n.Op, Net: n.Network, Err: children[0]}, true
	}},
	"syscall": {minChildren: 1, maxChildren: 1, build: func(n outcomeCause, children []error) (error, bool) {
		return &os.SyscallError{Syscall: n.Op, Err: children[0]}, true
	}},
	"wrap": {minChildren: 1, maxChildren: 1, build: func(_ outcomeCause, children []error) (error, bool) {
		return fmt.Errorf("remote acquisition: %w", children[0]), true
	}},
	"join": {minChildren: 1, maxChildren: outcomeNodeLimit, build: func(_ outcomeCause, children []error) (error, bool) { return errors.Join(children...), true }},
	"dns": {maxChildren: 1, remote: true, build: func(n outcomeCause, children []error) (error, bool) {
		return &net.DNSError{Err: "remote timeout", IsTimeout: true, IsTemporary: n.Temporary, IsNotFound: n.NotFound, UnwrapErr: children[0]}, true
	}},
	"status": {maxChildren: 1, enter: outcomeStatus, remote: true, transient: true, build: func(n outcomeCause, children []error) (error, bool) {
		return &fetcherr.ResponseError{StatusCode: n.Status, Op: n.Op, Err: children[0]}, true
	}},
	"response": {maxChildren: 1, require: outcomeStatus, transient: true, build: func(n outcomeCause, children []error) (error, bool) {
		e := &errcode.ErrorResponse{StatusCode: n.Status, Method: n.Op}
		if children[0] != nil {
			var ok bool
			e.Errors, ok = children[0].(errcode.Errors) //nolint:errorlint // the codec encodes concrete node identity, not a wrapped match
			if !ok {
				return nil, false
			}
		}
		return e, true
	}},
	"codes": {maxChildren: outcomeNodeLimit, require: outcomeStatus, build: func(_ outcomeCause, children []error) (error, bool) {
		codes := make(errcode.Errors, 0, len(children))
		for _, child := range children {
			code, ok := child.(errcode.Error) //nolint:errorlint // the codec encodes concrete node identity, not a wrapped match
			if !ok {
				return nil, false
			}
			codes = append(codes, code)
		}
		return codes, true
	}},
	"code":         {require: outcomeStatus, code: true, build: func(n outcomeCause, _ []error) (error, bool) { return errcode.Error{Code: n.Code}, true }},
	"code-pointer": {require: outcomeStatus, code: true, build: func(n outcomeCause, _ []error) (error, bool) { return &errcode.Error{Code: n.Code}, true }},
	"reset":        {remote: true, sentinel: syscall.ECONNRESET},
	"refused":      {remote: true, sentinel: syscall.ECONNREFUSED},
	"timeout":      {remote: true, sentinel: syscall.ETIMEDOUT},
	"deadline":     {remote: true, sentinel: os.ErrDeadlineExceeded},
	"eof":          {remote: true, require: outcomeTransport, sentinel: io.ErrUnexpectedEOF},
	"flux":         {sentinel: manifest.ErrFlux},
}

type outcomeCodecState struct {
	nodes            int
	terminal, remote bool
}

func (c *outcomeCodecState) kind(n outcomeCause, children int, context outcomeContext, depth int) (outcomeCodec, bool) {
	c.nodes++
	k, ok := outcomeCodecs[n.Kind]
	if !ok || depth > outcomeDepthLimit || children < k.minChildren || children > k.maxChildren || children > outcomeNodeLimit-c.nodes || context&k.require != k.require || k.transient && fetcherr.Classify(n.Status) != fetcherr.Transient || k.code && !outcomeCode(n.Code) {
		return k, false
	}
	c.terminal, c.remote = c.terminal || k.terminal, c.remote || k.remote
	return k, true
}

func (c *outcomeCodecState) encode(err error, context outcomeContext, depth int) (outcomeCause, bool) {
	if e, ok := err.(*TerminalError); ok && (e == nil || !e.Attempted) || //nolint:errorlint // the codec encodes concrete node identity, not a wrapped match
		err == manifest.ErrInput || err == manifest.ErrObjectNotFound || //nolint:errorlint // the codec encodes concrete node identity, not a wrapped match
		err == manifest.ErrMissingSecret || err == manifest.ErrSourceSkipped { //nolint:errorlint // the codec encodes concrete node identity, not a wrapped match
		return outcomeCause{}, false
	}
	n, children, ok := outcomeNode(err)
	if !ok {
		return n, false
	}
	k, ok := c.kind(n, len(children), context, depth)
	if !ok {
		return n, false
	}
	for _, child := range children {
		encoded, ok := c.encode(child, context|k.enter, depth+1)
		if !ok {
			return n, false
		}
		n.Children = append(n.Children, encoded)
	}
	return n, true
}

//nolint:revive // The returned error is decoded data; the boolean reports validity.
func (c *outcomeCodecState) decode(n outcomeCause, context outcomeContext, depth int) (error, bool) {
	k, ok := c.kind(n, len(n.Children), context, depth)
	if !ok {
		return nil, false
	}
	children := make([]error, 0, len(n.Children))
	for _, child := range n.Children {
		err, ok := c.decode(child, context|k.enter, depth+1)
		if !ok {
			return nil, false
		}
		children = append(children, err)
	}
	if k.maxChildren == 1 && len(children) == 0 {
		children = append(children, nil)
	}
	result := k.sentinel
	if result == nil {
		result, ok = k.build(n, children)
	}
	shape, _, valid := outcomeNode(result)
	shape.Children = n.Children
	return result, ok && valid && reflect.DeepEqual(n, shape)
}

func outcomeNode(err error) (outcomeCause, []error, bool) {
	var n outcomeCause
	var children []error
	switch e := err.(type) { //nolint:errorlint // the codec encodes concrete node identity, not a wrapped match
	case *TerminalError:
		n.Kind, children = "terminal", []error{e.Err}
	case *producerError:
		n.Kind, children = "producer", []error{e.error}
	case *url.Error:
		n.Kind, n.Op, children = "url", outcomeOp(e.Op), []error{e.Err}
	case *net.OpError:
		n.Kind, n.Op, n.Network, children = "net", outcomeOp(e.Op), outcomeNetwork(e.Net), []error{e.Err}
	case *os.SyscallError:
		n.Kind, n.Op, children = "syscall", outcomeOp(e.Syscall), []error{e.Err}
	case *net.DNSError:
		if !e.IsTimeout {
			return n, nil, false
		}
		n.Kind, n.Temporary, n.NotFound = "dns", e.IsTemporary, e.IsNotFound
		if e.UnwrapErr != nil {
			children = []error{e.UnwrapErr}
		}
	case *fetcherr.ResponseError:
		n.Kind, n.Op, n.Status = "status", outcomeOp(e.Op), e.StatusCode
		if e.Err != nil {
			children = []error{e.Err}
		}
	case *errcode.ErrorResponse:
		n.Kind, n.Op, n.Status = "response", outcomeOp(e.Method), e.StatusCode
		if len(e.Errors) > 0 {
			children = []error{e.Errors}
		}
	case errcode.Errors:
		if len(e) > outcomeNodeLimit {
			return n, nil, false
		}
		n.Kind = "codes"
		for _, child := range e {
			children = append(children, child)
		}
	case errcode.Error:
		n.Kind, n.Code = "code", e.Code
	case *errcode.Error:
		n.Kind, n.Code = "code-pointer", e.Code
	default:
		for kind, codec := range outcomeCodecs {
			if codec.sentinel != nil && err == codec.sentinel { //nolint:errorlint // the codec encodes concrete node identity, not a wrapped match
				return outcomeCause{Kind: kind}, nil, true
			}
		}
		if reflect.TypeOf(err) == fmtWrapType {
			return outcomeCause{Kind: "wrap"}, []error{err.(interface{ Unwrap() error }).Unwrap()}, true
		}
		if reflect.TypeOf(err) == fmtWrapsType || reflect.TypeOf(err) == joinType {
			return outcomeCause{Kind: "join"}, err.(interface{ Unwrap() []error }).Unwrap(), true
		}
		return n, nil, false
	}
	return n, children, true
}

func outcomeOp(op string) string {
	if slices.Contains([]string{"GET", "HEAD", "POST", "PUT", "DELETE", "PATCH", "Get", "Head", "Post", "Put", "Delete", "Patch", "read", "write", "dial", "connect", "accept", "readfrom", "writeto", "recvfrom", "sendto"}, op) {
		return op
	}
	return ""
}

func outcomeNetwork(network string) string {
	if slices.Contains([]string{"tcp", "tcp4", "tcp6", "udp", "udp4", "udp6", "ip", "ip4", "ip6"}, network) {
		return network
	}
	return ""
}

func outcomeCode(code string) bool {
	return slices.Contains([]string{errcode.ErrorCodeBlobUnknown, errcode.ErrorCodeBlobUploadUnknown, errcode.ErrorCodeManifestBlobUnknown, errcode.ErrorCodeManifestUnknown, errcode.ErrorCodeNameUnknown}, code)
}

func (o *outcome) replay() error {
	codec := outcomeCodecState{}
	err, _ := codec.decode(o.record.Cause, 0, 1)
	return err
}

func (o *outcome) encode() ([]byte, []byte, bool) {
	codec := outcomeCodecState{}
	if _, ok := codec.decode(o.record.Cause, 0, 1); !ok || !codec.terminal || !codec.remote || o.record.Version != 1 {
		return nil, nil, false
	}
	if _, ok := outcomeDeadline(o.record.Completed, o.record.Count); !ok {
		return nil, nil, false
	}
	layer, ok := outcomeLayer(o.lastGood)
	if !ok {
		return nil, nil, false
	}
	config, err := json.Marshal(o.record)
	if err != nil || len(config) > outcomeConfigLimit {
		return nil, nil, false
	}
	m := ocispec.Manifest{SchemaVersion: 2, MediaType: ocispec.MediaTypeImageManifest, ArtifactType: outcomeArtifact,
		Config: content.NewDescriptorFromBytes(outcomeConfig, config), Layers: []ocispec.Descriptor{}}
	if o.lastGood.Digest != "" {
		m.Layers = append(m.Layers, layer)
	}
	data, err := json.Marshal(m)
	if err != nil || len(data) > outcomeManifestLimit {
		return nil, nil, false
	}
	return config, data, true
}

func (c *Cache) publishOutcome(ctx context.Context, id string, o *outcome, remote error) (ocispec.Descriptor, error) {
	config, data, ok := o.encode()
	if !ok {
		return ocispec.Descriptor{}, nil
	}
	if _, err := c.pushOutcomeBlob(ctx, outcomeConfig, config); err != nil {
		return ocispec.Descriptor{}, &CacheError{errors.Join(remote, err)}
	}
	desc, err := c.pushOutcomeBlob(ctx, ocispec.MediaTypeImageManifest, data)
	if err != nil {
		return ocispec.Descriptor{}, &CacheError{errors.Join(remote, err)}
	}
	if err := c.store.Tag(ctx, desc, id); err != nil {
		return ocispec.Descriptor{}, &CacheError{errors.Join(remote, err)}
	}
	return desc, nil
}

func (c *Cache) pushOutcomeBlob(ctx context.Context, mediaType string, data []byte) (ocispec.Descriptor, error) {
	desc := content.NewDescriptorFromBytes(mediaType, data)
	if err := c.store.Push(ctx, desc, bytes.NewReader(data)); err != nil {
		if errors.Is(err, errdef.ErrAlreadyExists) {
			_, err = c.readOutcomeBlob(ctx, desc)
		}
		if err != nil {
			return ocispec.Descriptor{}, err
		}
	}
	return desc, nil
}

func (c *Cache) readOutcome(ctx context.Context, desc ocispec.Descriptor) (*outcome, error) {
	if desc.MediaType != ocispec.MediaTypeImageManifest || desc.Size < 0 || desc.Size > outcomeManifestLimit {
		return nil, nil
	}
	data, err := c.readOutcomeBlob(ctx, desc)
	if err != nil {
		return nil, err
	}
	var m ocispec.Manifest
	if !outcomeJSON(data, &m) || m.SchemaVersion != 2 || m.MediaType != ocispec.MediaTypeImageManifest || m.ArtifactType != outcomeArtifact || m.Config.MediaType != outcomeConfig || len(m.Layers) > 1 || m.Config.Size < 0 || m.Config.Size > outcomeConfigLimit || m.Subject != nil {
		return nil, nil
	}
	config, err := c.readOutcomeBlob(ctx, m.Config)
	if err != nil {
		return nil, err
	}
	o := &outcome{}
	if !outcomeJSON(config, &o.record) || o.record.Version != 1 {
		return nil, nil
	}
	if _, valid := outcomeDeadline(o.record.Completed, o.record.Count); !valid {
		return nil, nil
	}
	codec := outcomeCodecState{}
	if _, ok := codec.decode(o.record.Cause, 0, 1); !ok || !codec.terminal || !codec.remote {
		return nil, nil
	}
	if len(m.Layers) == 1 {
		var ok bool
		o.lastGood, ok = outcomeLayer(m.Layers[0])
		if !ok || o.lastGood.Digest == "" {
			return nil, nil
		}
		if _, err := c.root.Stat(blobPath(o.lastGood.Digest)); err != nil {
			return nil, &CacheError{err}
		}
	}
	return o, nil
}

func outcomeJSON(data []byte, target any) bool {
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	return d.Decode(target) == nil && d.Decode(new(any)) == io.EOF
}

func (c *Cache) readOutcomeBlob(ctx context.Context, desc ocispec.Descriptor) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, &CacheError{err}
	}
	if err := desc.Digest.Validate(); err != nil {
		return nil, &CacheError{err}
	}
	r, err := c.root.Open(blobPath(desc.Digest))
	if err != nil {
		return nil, &CacheError{err}
	}
	data, readErr := content.ReadAll(r, desc)
	if err := errors.Join(readErr, r.Close()); err != nil {
		return nil, &CacheError{err}
	}
	return data, nil
}
