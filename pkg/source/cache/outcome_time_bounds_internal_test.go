package cache

import (
	"math"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/home-operations/flate/internal/assert"
)

func TestOutcome_Time(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	for _, tc := range []struct {
		count uint32
		delay time.Duration
	}{{1, 750 * time.Millisecond}, {2, 1500 * time.Millisecond}, {3, 3 * time.Second}, {11, 768 * time.Second}, {12, 15 * time.Minute}, {math.MaxInt32 + 1, 15 * time.Minute}, {math.MaxUint32, 15 * time.Minute}} {
		deadline, valid := outcomeDeadline(at, tc.count)
		if !valid || deadline.Sub(at) != tc.delay {
			t.Fatalf("count %d: %s, %t", tc.count, deadline.Sub(at), valid)
		}
	}
	assert.FatalIf(
		t,
		nextOutcomeCount(0) != 1 || nextOutcomeCount(1) != 2 || nextOutcomeCount(math.MaxUint32) != math.MaxUint32,
		"%s",
		"failure count did not saturate",
	)

	for _, tc := range []struct {
		name  string
		at    time.Time
		count uint32
	}{
		{"zero", time.Time{}, 1},
		{"zero count", at, 0},
		{"year zero", time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), 1},
		{"year overflow", time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC), 1},
		{"deadline overflow", time.Date(9999, 12, 31, 23, 59, 59, 500000000, time.UTC), 1},
		{"non-UTC", at.In(time.FixedZone("offset", 3600)), 1},
	} {
		t.Run(tc.name, func(t *testing.T) { t.Parallel(); testOutcomeTimeCase(t, &tc) })
	}
	for _, tc := range []struct {
		name         string
		observations []time.Duration
		fresh        []bool
		valid        []bool
	}{
		{"boundary", []time.Duration{0, 749 * time.Millisecond, 750 * time.Millisecond, time.Second}, []bool{true, true, false, false}, []bool{true, true, true, true}},
		{"future", []time.Duration{-1, 0, time.Second}, []bool{false, false, false}, []bool{false, false, false}},
		{"rollback after completion", []time.Duration{500 * time.Millisecond, 499 * time.Millisecond, 600 * time.Millisecond}, []bool{true, false, false}, []bool{true, false, false}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testOutcomeTimeCase2(t, &at, &tc)
		},
		)
	}
}

func testOutcomeTimeCase(t *testing.T, tc *struct {
	name  string
	at    time.Time
	count uint32
},
) {
	t.Helper()
	{
		_, valid := outcomeDeadline(tc.at, tc.count)
		assert.FatalIf(t, valid, "%s", "invalid time accepted")
	}
}

func testOutcomeTimeCase2(t *testing.T, at *time.Time,
	tc *struct {
		name         string
		observations []time.Duration
		fresh        []bool
		valid        []bool
	},
) {
	t.Helper()

	o := newOutcome(outcomeFailure(connReset()), (*at), 1, &ocispec.Descriptor{})
	before := o.record
	for i, delta := range tc.observations {
		freshness := o.observe(at.Add(delta))

		assert.FatalIf(
			t,
			freshness.fresh != tc.fresh[i] || freshness.valid != tc.valid[i],
			"observe %s: %t/%t",
			delta,
			freshness.fresh,
			freshness.valid,
		)
	}
	{
		diff := cmp.Diff(before, o.record)
		assert.FatalIf(t, diff != "", "observation renewed record (-want +got):\n%s", diff)
	}
}
