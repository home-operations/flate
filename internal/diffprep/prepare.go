package diffprep

import "github.com/home-operations/flate/pkg/manifest"

// Parent identifies a producer and its display path.
type Parent struct {
	manifest.NamedResource
	Path string
}

// Doc borrows an immutable document and its producer attribution.
type Doc struct {
	Manifest map[string]any
	Parent   Parent
}

// Options controls normalization before canonical pairing.
type Options struct {
	StripAttrs  []string
	StripFields []string
	Normalize   func(map[string]any)
}

// Prepared retains unchanged pairs for native document-count and ordering semantics.
type Prepared struct {
	Pairs                   []Pair
	Warnings                []manifest.Warning
	leftCopied, rightCopied bool
}
