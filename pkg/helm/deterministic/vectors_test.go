package deterministic

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func TestRandFuncs_MixedSequenceVector(t *testing.T) {
	fm := Funcs(SeedFor("rel", "ns"))
	assert.Equal(t, fm["randAlphaNum"].(func(int) string)(12), "BK5DvWnIyqNn")
	assert.Equal(t, fm["randInt"].(func(int, int) int)(10, 1010), 893)
	assert.Equal(t, fm["shuffle"].(func(string) string)("aβcdéfg"), "adgfβéc")
	assert.Equal(t, fm["randAlpha"].(func(int) string)(12), "ryamVbHHdcVG")
	assert.Equal(t, fm["randNumeric"].(func(int) string)(12), "258471887394")
	assert.Equal(t, fm["randAscii"].(func(int) string)(12), "<nOD=*ozHeo)")
	bytes, err := fm["randBytes"].(func(int) (string, error))(12)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, bytes, "Svcsot6PCYFEL6oY")
	assert.Equal(t, fm["uuidv4"].(func() string)(), "4b13b688-d2c4-4b1c-a49b-05a3b746efe1")
	assert.Equal(t, fm["randAlphaNum"].(func(int) string)(12), "3PKiE0bvwUwP")
}

func BenchmarkFuncs_Construct(b *testing.B) {
	seed := SeedFor("rel", "ns")
	b.ReportAllocs()
	for b.Loop() {
		_ = Funcs(seed)
	}
}

func BenchmarkRandInt_Draw(b *testing.B) {
	s := newStream(SeedFor("rel", "ns"))
	b.ReportAllocs()
	for b.Loop() {
		_ = randInt(s, 0, 1000)
	}
}

func BenchmarkShuffle_Draw(b *testing.B) {
	s := newStream(SeedFor("rel", "ns"))
	b.ReportAllocs()
	for b.Loop() {
		_ = shuffle(s, "abcdefghijklmno")
	}
}
