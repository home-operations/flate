package deterministic

import (
	"crypto/rand"
	"encoding/base64"
	"io"
	"math"
	"math/big"
	"text/template"
	"unicode"

	"github.com/google/uuid"
)

// randFuncs returns deterministic overrides for sprig's crypto/rand- and
// math/rand-backed string/byte/int/uuid functions, all drawing from the
// per-render stream instead of the process entropy source.
//
// The string generators are a faithful port of Masterminds/goutils
// CryptoRandom (the consumer behind sprig's randAlpha* helpers) with its sole
// randomness draw — getCryptoRandomInt = crypto/rand.Int(rand.Reader, …) —
// swapped to read from the stream. Because crypto/rand.Int's reduction is
// unchanged, output is byte-identical to goutils given the same stream bytes;
// only the bytes' source is deterministic.
func randFuncs(s *stream) template.FuncMap {
	return template.FuncMap{
		"randAlphaNum": func(n int) string { return cryptoRandom(s, n, 0, 0, true, true) },
		"randAlpha":    func(n int) string { return cryptoRandom(s, n, 0, 0, true, false) },
		"randNumeric":  func(n int) string { return cryptoRandom(s, n, 0, 0, false, true) },
		"randAscii":    func(n int) string { return cryptoRandom(s, n, 32, 127, false, false) },
		"randBytes":    func(n int) (string, error) { return randBytes(s, n) },
		"randInt":      func(minN, maxN int) int { return int(randomInt(s, maxN-minN)) + minN },
		"uuidv4":       func() string { return uuidv4(s) },
	}
}

// randomInt mirrors goutils.getCryptoRandomInt but reads from the stream.
func randomInt(s *stream, n int) int64 {
	v, err := rand.Int(s, big.NewInt(int64(n)))
	if err != nil {
		// crypto/rand.Int only errors on a non-positive bound; sprig's
		// randInt(min,max) with max<=min and randAscii/etc. with count>0
		// never reach it. Match math/rand.Intn's panic on a bad bound.
		panic(err)
	}
	return v.Int64()
}

// cryptoRandom is a faithful port of Masterminds/goutils CryptoRandom (chars
// always nil in sprig's usage) with its randomness draw replaced by the
// stream. count<=0 yields "" — matching sprig, which swallows goutils' error.
func cryptoRandom(s *stream, count, start, end int, letters, numbers bool) string {
	if count <= 0 {
		return ""
	}
	if start == 0 && end == 0 {
		if !letters && !numbers {
			end = math.MaxInt32
		} else {
			end = 'z' + 1
			start = ' '
		}
	}
	buffer := make([]rune, count)
	gap := end - start
	for count != 0 {
		count--
		//nolint:gosec // G115: randomInt(s,gap) ∈ [0,gap) so the sum is ≤ end ≤ MaxInt32 — always a valid rune.
		ch := rune(randomInt(s, gap) + int64(start))
		if letters && unicode.IsLetter(ch) || numbers && unicode.IsDigit(ch) || !letters && !numbers {
			switch {
			case ch >= 56320 && ch <= 57343: // low surrogate range
				if count == 0 {
					count++
				} else {
					buffer[count] = ch
					count--
					//nolint:gosec // G115: 55296+[0,128) is a valid surrogate-range rune.
					buffer[count] = rune(55296 + randomInt(s, 128))
				}
			case ch >= 55296 && ch <= 56191: // high surrogate range (partial)
				if count == 0 {
					count++
				} else {
					//nolint:gosec // G115: 56320+[0,128) is a valid surrogate-range rune.
					buffer[count] = rune(56320 + randomInt(s, 128))
					count--
					buffer[count] = ch
				}
			case ch >= 56192 && ch <= 56319: // private high surrogate, skip
				count++
			default:
				buffer[count] = ch
			}
		} else {
			count++
		}
	}
	return string(buffer)
}

// randBytes mirrors sprig.randBytes (base64 of count random bytes), reading
// from the stream instead of crypto/rand.
func randBytes(s *stream, count int) (string, error) {
	buf := make([]byte, count)
	if _, err := io.ReadFull(s, buf); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(buf), nil
}

// uuidv4 mirrors sprig.uuidv4 (a v4 UUID) but draws its 16 bytes from the
// stream, so the same seed yields the same UUID.
func uuidv4(s *stream) string {
	u, err := uuid.NewRandomFromReader(s)
	if err != nil {
		panic(err)
	}
	return u.String()
}
