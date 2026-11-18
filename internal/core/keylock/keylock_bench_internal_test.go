package keylock

import "testing"

func BenchmarkKeyMap_AcquireRelease(b *testing.B) {
	m := New[string]()
	ctx := b.Context()
	release, err := m.Acquire(ctx, "k")
	if err != nil {
		b.Fatal(err)
	}
	release()
	b.ReportAllocs()
	for b.Loop() {
		release, err := m.Acquire(ctx, "k")
		if err != nil {
			b.Fatal(err)
		}
		release()
	}
}
