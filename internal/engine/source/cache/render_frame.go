package cache

import (
	"context"
	"io/fs"
)

// RenderFrame MUST retain Frame by pointer because Frame contains atomic state.
type RenderFrame struct {
	Key     string
	Lookup  string
	Binding string
	Output  []byte
	Stored  *Frame
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
