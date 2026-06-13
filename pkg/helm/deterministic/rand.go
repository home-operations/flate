package deterministic

import (
	"encoding/base64"
	"io"
	"math/rand/v2"
	"text/template"

	"github.com/google/uuid"
)

const (
	charsAlphaNum = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	charsAlpha    = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	charsNumeric  = "0123456789"
)

// charsASCII is the printable ASCII range (bytes 32..126) — sprig's randAscii
// character set.
var charsASCII = func() string {
	b := make([]byte, 0, 95)
	for c := byte(32); c <= 126; c++ {
		b = append(b, c)
	}
	return string(b)
}()

// randFuncs returns deterministic overrides for sprig's crypto/rand- and
// math/rand-backed string/byte/int/uuid functions, all drawing from the
// per-render stream. Output is not byte-identical to sprig — it only needs to be
// deterministic and structurally valid (right length, right character class).
func randFuncs(s *stream) template.FuncMap {
	return template.FuncMap{
		"randAlphaNum": func(n int) string { return randString(s, n, charsAlphaNum) },
		"randAlpha":    func(n int) string { return randString(s, n, charsAlpha) },
		"randNumeric":  func(n int) string { return randString(s, n, charsNumeric) },
		"randAscii":    func(n int) string { return randString(s, n, charsASCII) },
		"randBytes":    func(n int) (string, error) { return randBytes(s, n) },
		"randInt":      func(minN, maxN int) int { return randInt(s, minN, maxN) },
		"uuidv4":       func() string { return uuidv4(s) },
		"shuffle":      func(in string) string { return shuffle(s, in) },
	}
}

// shuffle returns a deterministic permutation of in's runes, drawn from the
// stream. sprig maps "shuffle" to xstrings.Shuffle, which draws from a
// process-global, time-seeded RNG — nondeterministic across renders. It is the
// source of flipping checksum/secret annotations on charts whose secret
// template passes a generated value through `| shuffle` (bitnami common's
// passwords.manage with strong=true does exactly this for its secret_key).
// Drawing from the per-render stream makes it reproducible; the result is a
// valid permutation, not byte-identical to sprig.
func shuffle(s *stream, in string) string {
	r := []rune(in)
	//nolint:gosec // G404: deterministic rendering deliberately draws from the seeded ChaCha8, not crypto/rand.
	rand.New(s.c).Shuffle(len(r), func(i, j int) { r[i], r[j] = r[j], r[i] })
	return string(r)
}

// randString returns n characters, each one stream byte mapped into charset by
// modulo. The bias from a charset length that doesn't divide 256 is irrelevant
// for a render placeholder. n<=0 yields "".
func randString(s *stream, n int, charset string) string {
	if n <= 0 {
		return ""
	}
	buf := make([]byte, n)
	_, _ = io.ReadFull(s, buf)
	for i, b := range buf {
		buf[i] = charset[int(b)%len(charset)]
	}
	return string(buf)
}

func randBytes(s *stream, n int) (string, error) {
	if n < 0 {
		return "", nil
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(s, buf); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(buf), nil
}

// randInt mirrors sprig's randInt(min,max): an int in [min,max). A non-positive
// span yields min (sprig would panic via math/rand.Intn; a renderer prefers a
// benign value). The draw comes from the same ChaCha8 the stream wraps.
func randInt(s *stream, minN, maxN int) int {
	span := maxN - minN
	if span <= 0 {
		return minN
	}
	//nolint:gosec // G404: deterministic rendering deliberately draws from the seeded ChaCha8, not crypto/rand.
	return minN + rand.New(s.c).IntN(span)
}

func uuidv4(s *stream) string {
	u, err := uuid.NewRandomFromReader(s)
	if err != nil {
		// NewRandomFromReader only fails on a short read, which the stream
		// (ChaCha8) never produces.
		panic(err)
	}
	return u.String()
}
