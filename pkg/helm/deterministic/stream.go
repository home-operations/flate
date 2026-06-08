package deterministic

import (
	"crypto/sha256"
	"math/rand/v2"
)

// SeedFor derives a render's deterministic seed from inputs already folded
// into computeTemplateKey — the release name and namespace (both in keyHR).
// Seeding only from key inputs is the load-bearing invariant: a cache hit
// and a cache miss for the same key must produce identical bytes, so the
// seed must never draw from anything outside the key (least of all the wall
// clock). The NUL separator keeps ("ab","c") from colliding with ("a","bc").
func SeedFor(releaseName, releaseNamespace string) []byte {
	sum := sha256.Sum256([]byte(releaseName + "\x00" + releaseNamespace))
	return sum[:]
}

// stream is the single deterministic randomness source a render's overrides
// share: a ChaCha8 keyed by the seed. Its Read returns exactly len(p) bytes
// with a nil error — the contract crypto/rand-style consumers (crypto/rand.Int,
// io.ReadFull, and, in the cert tier, rsa.GenerateKey / x509.CreateCertificate)
// expect from an io.Reader. ChaCha8's algorithm is fixed by the standard
// library, so a given seed yields the same byte sequence across Go releases.
//
// A stream is stateful — each draw advances it — and therefore NOT safe for
// concurrent use. Build one per render (Helm renders a chart's templates
// sequentially, so a single stream is consumed in a deterministic order).
type stream struct{ c *rand.ChaCha8 }

func newStream(seed []byte) *stream {
	var key [32]byte
	copy(key[:], seed)
	return &stream{c: rand.NewChaCha8(key)}
}

func (s *stream) Read(p []byte) (int, error) { return s.c.Read(p) }
