package manifest

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"

	yaml "go.yaml.in/yaml/v4"
)

// decodedMapPool recycles top-level map[string]any documents produced
// by DecodeDocs. yaml.v4's libyaml constructor reuses a pre-populated
// non-nil map when decoding (constructor.go: `if out.IsNil() {
// MakeMap }`), so handing it a cleared pooled map skips the
// 16-bucket map allocation per document — meaningful for multi-doc
// helm output (50+ docs) and tight loader loops.
//
// Pool elements are *map[string]any to avoid escaping a fresh
// interface wrapper per Get / Put. The 16-entry seed mirrors the
// typical Flux CR top-level shape (apiVersion, kind, metadata, spec,
// plus a handful of misc keys).
var decodedMapPool = sync.Pool{
	New: func() any {
		m := make(map[string]any, 16)
		return &m
	},
}

// getDecodedMap returns a cleared, ready-to-use map from the pool.
func getDecodedMap() map[string]any {
	return *decodedMapPool.Get().(*map[string]any)
}

// putDecodedMap clears and returns the map to the pool. The clear is
// mandatory: pooled maps must not leak prior entries into the next
// caller. Callers must ensure no live reference to the top-level map
// remains — parsers that retain the top-level doc (currently only
// Kustomization's Contents field) must NOT release it. Inner submaps
// retained by other parsers (ConfigMap.Data, ConfigMap.BinaryData)
// stay alive independently — clear(m) only removes top-level entries.
func putDecodedMap(m map[string]any) {
	if m == nil {
		return
	}
	clear(m)
	decodedMapPool.Put(&m)
}

// ReleaseDoc returns a decoded document to the internal pool. Safe to
// call only after the caller is done reading the top-level map AND
// has confirmed no parser retained it. Use ReleaseIfNotRetained for
// the post-ParseDoc dispatch helper.
//
// Calling ReleaseDoc with a map that did not originate from
// DecodeDocs is allowed but pollutes the pool; reserve for the
// internal load path.
func ReleaseDoc(m map[string]any) {
	putDecodedMap(m)
}

// ReleaseIfNotRetained returns doc to the pool unless obj retains a
// reference to the top-level map. Currently only *Kustomization
// retains (via its Contents field) — every other ParseDoc result
// either round-trips through JSON (decodeInto) or aliases an inner
// submap that clear(top) doesn't touch.
//
// Callers should invoke this after every successful ParseDoc whose
// result they don't keep around in a form that aliases doc.
func ReleaseIfNotRetained(doc map[string]any, obj BaseManifest) {
	if _, retained := obj.(*Kustomization); retained {
		return
	}
	putDecodedMap(doc)
}

// DecodeDocs reads zero or more YAML documents from r and returns each
// as a generic map. Documents that are empty, null, or whose root is not a
// mapping (a top-level sequence or scalar — i.e. not a Kubernetes manifest)
// are skipped; only a genuine YAML syntax error aborts.
//
// Decodes directly into map[string]any via the yaml.v4 library — same
// shape sigs.k8s.io/yaml uses behind real Flux. Letting the library
// own the walk means YAML 1.2 features (anchors, aliases — including
// aliases-as-keys, merge keys, tagged scalars) round-trip correctly
// without a hand-rolled node visitor playing catch-up.
//
// Each returned map is drawn from an internal sync.Pool. The loader
// pipeline returns them via ReleaseIfNotRetained after ParseDoc;
// external callers that mutate / retain the returned slice should
// NOT release (the entries are still owned by them).
//
// Retained-and-never-released is the CORRECT lifecycle, not a leak: a
// controller that stores SplitDocs output on a render artifact owns
// those maps for the run, so they legitimately never return to the
// pool. Drawing from the pool still pays off — it skips the initial
// 16-bucket allocation on every Get, retained or not. Deep-copying a
// retained doc just to recycle the pooled original is net-negative: the
// copy allocates the whole nested tree, far more than the single map
// alloc it would recover (see BenchmarkArtifactRetain_Current vs
// _DeepCopyRelease — the copy path costs more time, memory, and allocs).
func DecodeDocs(r io.Reader) ([]map[string]any, error) {
	dec := yaml.NewDecoder(r)
	var out []map[string]any
	for {
		m := getDecodedMap()
		if err := dec.Decode(&m); err != nil {
			putDecodedMap(m)
			if errors.Is(err, io.EOF) {
				return out, nil
			}
			// Valid YAML whose root is not a mapping (a top-level sequence or
			// scalar — e.g. an ansible task file) can't decode into
			// map[string]any. yaml.v4 records that as a recoverable
			// *yaml.LoadErrors and the decoder advances past the doc, so the
			// loop continues with the next one. Such a document simply isn't a
			// Kubernetes manifest, so skip it like an empty doc rather than
			// failing the whole file. Decoding into map[string]any, a construct
			// error can ONLY mean a non-mapping root: every value fits `any`, so
			// no nested type mismatch is possible. A genuine YAML syntax error
			// is a single *yaml.LoadError (not the plural *LoadErrors) and so
			// falls through to the hard error below.
			var notManifest *yaml.LoadErrors
			if errors.As(err, &notManifest) {
				continue
			}
			return nil, fmt.Errorf("%w: %w", ErrInput, err)
		}
		if len(m) == 0 {
			putDecodedMap(m)
			continue
		}
		out = append(out, m)
	}
}

// SplitDocs is the byte-slice convenience wrapper for DecodeDocs.
func SplitDocs(data []byte) ([]map[string]any, error) {
	return DecodeDocs(bytes.NewReader(data))
}
