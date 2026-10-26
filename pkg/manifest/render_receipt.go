package manifest

// RenderReceipt carries immutable renderer-owned output and prepared metadata.
// It belongs to the accepting attempt and its existing output lifetime.
type RenderReceipt struct {
	value any
	equal func(any, any) bool
}

// NewRenderReceipt freezes the ownership of value and its semantic comparison.
// The renderer MUST keep value and everything it references immutable.
func NewRenderReceipt(value any, equal func(any, any) bool) *RenderReceipt {
	return &RenderReceipt{value: value, equal: equal}
}

// Value returns the renderer's immutable prepared value.
func (r *RenderReceipt) Value() any { return r.value }

// Equal reports whether two receipts carry the same pre-admission render.
func (r *RenderReceipt) Equal(other *RenderReceipt) bool {
	return r == other || r != nil && other != nil && r.equal != nil && r.equal(r.value, other.value)
}
