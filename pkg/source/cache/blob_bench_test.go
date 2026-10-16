package cache

import (
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

func incompressible(n int, seed uint64) []byte {
	b := make([]byte, n)
	x := seed
	for i := range b {
		x = x*6364136223846793005 + 1442695040888963407
		b[i] = byte(x >> 56)
	}
	return b
}

func mixedWindowPayload(seed uint64) []byte {
	payload := incompressible(4<<20, seed)
	manifests := syntheticManifests(200)
	const segment = 64 << 10
	for offset := 0; offset < len(payload); offset += 2 * segment {
		copy(payload[offset:offset+segment], manifests)
	}
	return payload
}

func syntheticManifests(docs int) []byte {
	var b strings.Builder
	for i := range docs {
		fmt.Fprintf(&b, `---
apiVersion: v1
kind: ConfigMap
metadata:
  name: app-config-%d
  namespace: flux-system
  labels:
    app.kubernetes.io/name: app-%d
    app.kubernetes.io/instance: app-%d
    kustomize.toolkit.fluxcd.io/name: apps
    kustomize.toolkit.fluxcd.io/namespace: flux-system
data:
  KEY_ONE: "value-one-%d"
  KEY_TWO: "value-two-%d"
  CONFIG: |
    server.host=example.local
    server.port=8080
    log.level=info
`, i, i, i, i, i)
	}
	return []byte(b.String())
}

func BenchmarkBlob_Put(b *testing.B) {
	c, err := New(b.Context(), b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = c.Close() })
	payload := syntheticManifests(200)
	key := BlobKey{Content: "render-benchmark"}
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	for b.Loop() {
		r, _, _, err := c.Blob(b.Context(), key, Policy{MaxAge: 0}, producer(payload, ocispec.Descriptor{}))
		if err != nil {
			b.Fatal(err)
		}
		if err := r.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBlob_Get(b *testing.B) {
	c, err := New(b.Context(), b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = c.Close() })
	payload := syntheticManifests(200)
	key := BlobKey{Content: "render-benchmark"}
	r, _, _, err := c.Blob(b.Context(), key, Policy{MaxAge: 0}, producer(payload, ocispec.Descriptor{}))
	if err != nil {
		b.Fatal(err)
	}
	if err := r.Close(); err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	for b.Loop() {
		r, _, _, err := c.Blob(b.Context(), key, Policy{MaxAge: Forever}, nil)
		if err != nil {
			b.Fatal(err)
		}
		_, err = io.Copy(io.Discard, r)
		closeErr := r.Close()
		if err != nil || closeErr != nil {
			b.Fatalf("read=%v, close=%v", err, closeErr)
		}
	}
}

// Each operation is an eight-write batch; keys are reused to bound disk occupancy.
func BenchmarkBlob_PutParallel(b *testing.B) {
	const callers = 8
	c, err := New(b.Context(), b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = c.Close() })
	keys := make([]BlobKey, callers)
	payloads := make([][]byte, callers)
	var batchBytes int64
	for i := range callers {
		keys[i] = BlobKey{Content: fmt.Sprintf("render-benchmark-%d", i)}
		payloads[i] = mixedWindowPayload(uint64(i + 1))
		batchBytes += int64(len(payloads[i]))
	}
	b.SetBytes(batchBytes)
	b.ReportAllocs()
	for b.Loop() {
		start := make(chan struct{})
		var ready, joined sync.WaitGroup
		ready.Add(callers)
		for i := range callers {
			joined.Go(func() {
				ready.Done()
				<-start
				r, _, _, err := c.Blob(b.Context(), keys[i], Policy{MaxAge: 0}, producer(payloads[i], ocispec.Descriptor{}))
				if err != nil {
					b.Error(err)
					return
				}
				if err := r.Close(); err != nil {
					b.Error(err)
				}
			})
		}
		ready.Wait()
		close(start)
		joined.Wait()
	}
	b.ReportMetric(callers, "writes/op")
}
