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
	"net/http"
	"net/url"
	"os"
	"reflect"
	"slices"
	"syscall"
	"time"

	"github.com/google/uuid"
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
	delay := 750 * time.Millisecond
	for n := uint32(1); n < count && delay < 15*time.Minute; n++ {
		delay = min(delay*2, 15*time.Minute)
	}
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
	codec := outcomeEncoder{}
	cause, ok := codec.encode(err, false, 1)
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

type outcomeEncoder struct {
	nodes    int
	terminal bool
	remote   bool
}

func (c *outcomeEncoder) encode(err error, transport bool, depth int) (outcomeCause, bool) {
	c.nodes++
	if err == nil || c.nodes > outcomeNodeLimit || depth > outcomeDepthLimit {
		return outcomeCause{}, false
	}
	var n outcomeCause
	switch err {
	case manifest.ErrInput, manifest.ErrObjectNotFound, manifest.ErrMissingSecret, manifest.ErrSourceSkipped:
		return n, false
	}
	var children []error
	switch e := err.(type) {
	case *TerminalError:
		if e == nil || !e.Attempted {
			return n, false
		}
		c.terminal = true
		n.Kind, children = "terminal", []error{e.Err}
	case *producerError:
		n.Kind, children = "producer", []error{e.error}
	case *url.Error:
		n.Kind, n.Op, children = "url", outcomeOp(e.Op), []error{e.Err}
		transport = true
	case *net.OpError:
		n.Kind, n.Op, n.Network, children = "net", outcomeOp(e.Op), outcomeNetwork(e.Net), []error{e.Err}
		transport = true
	case *os.SyscallError:
		n.Kind, n.Op, children = "syscall", outcomeOp(e.Syscall), []error{e.Err}
	case *net.DNSError:
		if !e.IsTimeout {
			return n, false
		}
		c.remote = true
		n.Kind, n.Temporary, n.NotFound = "dns", e.IsTemporary, e.IsNotFound
		if e.UnwrapErr != nil {
			children = []error{e.UnwrapErr}
		}
	case *errcode.ErrorResponse:
		if e.StatusCode != http.StatusTooManyRequests && (e.StatusCode < 500 || e.StatusCode >= 600) {
			return n, false
		}
		c.remote = true
		n.Kind, n.Op, n.Status = "response", outcomeOp(e.Method), e.StatusCode
		if len(e.Errors) > 0 {
			children = []error{e.Errors}
		}
	case errcode.Errors:
		if len(e) > outcomeNodeLimit-c.nodes {
			return n, false
		}
		n.Kind = "codes"
		for _, child := range e {
			children = append(children, child)
		}
	case errcode.Error:
		if !outcomeCode(e.Code) {
			return n, false
		}
		n.Kind, n.Code = "code", e.Code
	case *errcode.Error:
		if !outcomeCode(e.Code) {
			return n, false
		}
		n.Kind, n.Code = "code-pointer", e.Code
	default:
		switch err {
		case syscall.ECONNRESET:
			c.remote = true
			n.Kind = "reset"
		case syscall.ECONNREFUSED:
			c.remote = true
			n.Kind = "refused"
		case syscall.ETIMEDOUT:
			c.remote = true
			n.Kind = "timeout"
		case os.ErrDeadlineExceeded:
			c.remote = true
			n.Kind = "deadline"
		case io.ErrUnexpectedEOF:
			if !transport {
				return n, false
			}
			c.remote = true
			n.Kind = "eof"
		case manifest.ErrFlux:
			n.Kind = "flux"
		default:
			switch reflect.TypeOf(err) {
			case fmtWrapType:
				n.Kind, children = "wrap", []error{err.(interface{ Unwrap() error }).Unwrap()}
			case fmtWrapsType, joinType:
				n.Kind, children = "join", err.(interface{ Unwrap() []error }).Unwrap()
			default:
				return n, false
			}
		}
	}
	if len(children) > outcomeNodeLimit-c.nodes {
		return n, false
	}
	for _, child := range children {
		encoded, ok := c.encode(child, transport, depth+1)
		if !ok {
			return n, false
		}
		n.Children = append(n.Children, encoded)
	}
	return n, true
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

type outcomeDecoder struct {
	nodes    int
	terminal bool
	remote   bool
}

//nolint:revive // The returned error is decoded data; the boolean reports validity.
func (c *outcomeDecoder) decode(n outcomeCause, transport bool, depth int) (error, bool) {
	c.nodes++
	if c.nodes > outcomeNodeLimit || depth > outcomeDepthLimit || len(n.Children) > outcomeNodeLimit-c.nodes {
		return nil, false
	}
	var children []error
	for _, child := range n.Children {
		err, ok := c.decode(child, transport || n.Kind == "url" || n.Kind == "net", depth+1)
		if !ok {
			return nil, false
		}
		children = append(children, err)
	}
	shape := outcomeCause{Kind: n.Kind, Children: n.Children}
	var result error
	switch n.Kind {
	case "terminal", "producer", "url", "net", "syscall", "wrap":
		if len(children) != 1 {
			return nil, false
		}
		switch n.Kind {
		case "terminal":
			c.terminal = true
			result = &TerminalError{Err: children[0], Attempted: false}
		case "producer":
			result = &producerError{children[0]}
		case "url":
			shape.Op = outcomeOp(n.Op)
			result = &url.Error{Op: shape.Op, Err: children[0]}
		case "net":
			shape.Op, shape.Network = outcomeOp(n.Op), outcomeNetwork(n.Network)
			result = &net.OpError{Op: shape.Op, Net: shape.Network, Err: children[0]}
		case "syscall":
			shape.Op = outcomeOp(n.Op)
			result = &os.SyscallError{Syscall: shape.Op, Err: children[0]}
		case "wrap":
			result = fmt.Errorf("remote acquisition: %w", children[0])
		}
	case "join":
		if len(children) == 0 {
			return nil, false
		}
		result = errors.Join(children...)
	case "dns":
		c.remote = true
		if len(children) > 1 {
			return nil, false
		}
		shape.Temporary, shape.NotFound = n.Temporary, n.NotFound
		e := &net.DNSError{Err: "remote timeout", IsTimeout: true, IsTemporary: n.Temporary, IsNotFound: n.NotFound}
		if len(children) == 1 {
			e.UnwrapErr = children[0]
		}
		result = e
	case "response":
		c.remote = true
		if len(children) > 1 || n.Status != http.StatusTooManyRequests && (n.Status < 500 || n.Status >= 600) {
			return nil, false
		}
		shape.Status, shape.Op = n.Status, outcomeOp(n.Op)
		e := &errcode.ErrorResponse{StatusCode: n.Status, Method: shape.Op}
		if len(children) == 1 {
			var ok bool
			e.Errors, ok = children[0].(errcode.Errors)
			if !ok {
				return nil, false
			}
		}
		result = e
	case "codes":
		codes := make(errcode.Errors, 0, len(children))
		for _, child := range children {
			code, ok := child.(errcode.Error)
			if !ok {
				return nil, false
			}
			codes = append(codes, code)
		}
		result = codes
	case "code", "code-pointer":
		if len(children) != 0 || !outcomeCode(n.Code) {
			return nil, false
		}
		shape.Code = n.Code
		code := errcode.Error{Code: n.Code}
		result = code
		if n.Kind == "code-pointer" {
			result = &code
		}
	default:
		if len(children) != 0 {
			return nil, false
		}
		switch n.Kind {
		case "reset":
			c.remote = true
			result = syscall.ECONNRESET
		case "refused":
			c.remote = true
			result = syscall.ECONNREFUSED
		case "timeout":
			c.remote = true
			result = syscall.ETIMEDOUT
		case "deadline":
			c.remote = true
			result = os.ErrDeadlineExceeded
		case "eof":
			if !transport {
				return nil, false
			}
			c.remote = true
			result = io.ErrUnexpectedEOF
		case "flux":
			result = manifest.ErrFlux
		default:
			return nil, false
		}
	}
	return result, reflect.DeepEqual(n, shape)
}

func (o *outcome) replay() error {
	codec := outcomeDecoder{}
	err, _ := codec.decode(o.record.Cause, false, 1)
	return err
}

func (o *outcome) encode() ([]byte, []byte, bool) {
	codec := outcomeDecoder{}
	if _, ok := codec.decode(o.record.Cause, false, 1); !ok || !codec.terminal || !codec.remote || o.record.Version != 1 {
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
	limit := outcomeManifestLimit
	if mediaType == outcomeConfig {
		limit = outcomeConfigLimit
	}
	if len(data) > limit {
		return ocispec.Descriptor{}, nil
	}
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
	codec := outcomeDecoder{}
	if _, ok := codec.decode(o.record.Cause, false, 1); !ok || !codec.terminal || !codec.remote {
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
