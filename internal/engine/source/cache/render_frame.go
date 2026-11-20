package cache

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"iter"

	"github.com/home-operations/flate/pkg/manifest"
)

// RenderDomain supplies the policy and metadata rules for one render format.
type RenderDomain[P, H any] struct {
	Name         string
	EqualPolicy  func(P, P) bool
	ValidHeader  func(H) bool
	EqualHeader  func(H, H) bool
	Prepare      func(H) H
	Accept       func(H, *RenderAcceptance) (any, []manifest.SourceEvidence)
	equalReceipt func(any, any) bool
}

// NewRenderDomain MUST run once per domain so receipts share the comparator.
func NewRenderDomain[P, H any](domain RenderDomain[P, H]) *RenderDomain[P, H] {
	domain.equalReceipt = equalRenderReceipts[P, H]
	return &domain
}

// RenderFrame MUST retain Frame by pointer because Frame contains atomic state.
type RenderFrame struct {
	Key     string
	Lookup  string
	Binding string
	Output  []byte
	Stored  *Frame
}

// RenderReceipt retains immutable output and the domain's acceptance evidence.
type RenderReceipt[P, H any] struct {
	RenderFrame

	Inputs RenderBinding[P]
	Header H
	domain *RenderDomain[P, H]
}

// Receipt preserves the caller's snapshot for comparison and publication.
func (d *RenderDomain[P, H]) Receipt(f RenderReceipt[P, H]) *manifest.RenderReceipt {
	f.domain = d
	return manifest.NewRenderReceipt(f, d.equalReceipt)
}

func equalRenderReceipts[P, H any](a, b any) bool {
	left, leftOK := a.(RenderReceipt[P, H])
	right, rightOK := b.(RenderReceipt[P, H])
	return leftOK && rightOK && left.equal(&right)
}

func (f *RenderReceipt[P, H]) equal(other *RenderReceipt[P, H]) bool {
	if f.domain == nil || f.domain != other.domain || f.Key != other.Key {
		return false
	}
	return f.Inputs.Equal(&other.Inputs, f.domain.EqualPolicy) && bytes.Equal(f.Output, other.Output) &&
		f.domain.EqualHeader(f.Header, other.Header)
}

func (d *RenderDomain[P, H]) Read(ctx context.Context, generation fs.FS) (RenderReceipt[P, H], bool, error) {
	var f RenderReceipt[P, H]
	if generation == nil {
		return f, false, ctx.Err()
	}
	var err error
	f.RenderFrame, err = OpenRenderFrame(ctx, d.Name, generation, &f.Header)
	if err != nil {
		if _, storage := errors.AsType[*Error](err); storage {
			return f, false, err
		}
		return f, false, nil
	}
	return f, true, nil
}

func (d *RenderDomain[P, H]) matches(f *RenderReceipt[P, H], key string) bool {
	return f.Key != "" && f.Key == key && d.ValidHeader(f.Header)
}

// RenderValidation distinguishes a cache miss from a valid empty render.
type RenderValidation struct {
	Documents []manifest.DocOccurrence
	Valid     bool
	Rebound   bool
}

// RenderCheck supplies domain evidence. A history check MUST NOT acquire remote inputs.
type RenderCheck[H any] struct {
	History bool
	Header  func(H) (bool, error)
}

// Validate applies the same eligibility rules to fresh and retained generations.
// A history probe MUST reject a different binding before it consults domain evidence.
func (d *RenderDomain[P, H]) Validate(
	ctx context.Context,
	f *RenderReceipt[P, H],
	inputs *RenderBinding[P],
	check RenderCheck[H],
) (RenderValidation, error) {
	if err := ctx.Err(); err != nil {
		return RenderValidation{}, err
	}
	if !d.matches(f, inputs.Key) {
		return RenderValidation{}, nil
	}
	binding := f.Binding
	if binding == "" || f.Inputs.Key == "" || !f.Inputs.Equal(inputs, d.EqualPolicy) {
		binding = inputs.Fingerprint()
	}
	rebound := f.Binding != binding
	if check.History && rebound {
		return RenderValidation{}, nil
	}
	valid, err := check.Header(f.Header)
	if !valid || err != nil {
		return RenderValidation{}, err
	}
	docs, err := manifest.DecodeRender(f.Output)
	if err != nil {
		return RenderValidation{}, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return RenderValidation{}, err
	}
	f.Inputs = *inputs
	f.Rebind(binding)
	return RenderValidation{Documents: docs, Valid: true, Rebound: rebound}, nil
}

// History requires a bound frame and complete inventory for the current content.
func (f *RenderFrame) History(
	owner manifest.NamedResource,
	content any,
) (iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord], bool) {
	if f.Stored == nil || f.Binding == "" || content == nil {
		return nil, false
	}
	return f.Stored.History(owner, content)
}

type renderEnvelope struct {
	Key      string `json:"Key"`
	Binding  string `json:"Binding"`
	Metadata any    `json:"Metadata"`
}

// OpenRenderFrame MUST preserve storage error classification while hydrating reusable output.
func OpenRenderFrame(ctx context.Context, domain string, generation fs.FS, metadata any) (RenderFrame, error) {
	envelope := renderEnvelope{Metadata: metadata}
	stored, err := OpenFrame(ctx, domain, generation, &envelope)
	if err != nil {
		return RenderFrame{}, err
	}
	output, err := stored.Payload(ctx)
	if err != nil {
		return RenderFrame{}, err
	}
	return RenderFrame{Key: envelope.Key, Binding: envelope.Binding, Output: output, Stored: stored}, nil
}

// Rebind MUST operate on a renderer's copied frame to preserve previous receipts.
func (f *RenderFrame) Rebind(binding string) {
	if f.Binding != binding {
		f.Binding, f.Stored = binding, nil
	}
}

// Encode MUST reuse immutable stored envelopes until the binding changes.
func (f *RenderFrame) Encode(domain string, metadata any) (*Frame, error) {
	if f.Stored != nil {
		return f.Stored, nil
	}
	return NewFrame(domain, renderEnvelope{Key: f.Key, Binding: f.Binding, Metadata: metadata}, f.Output)
}
