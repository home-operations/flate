package deterministic

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/home-operations/flate/internal/assert"
)

// TestDateInZone_DefaultsToFixedClockAndForcesUTC pins the two determinism
// fixes in the ported date helper: an unrecognized argument falls back to
// fixedTime (not the wall clock), and an empty or "Local" zone is treated
// as UTC so formatting never depends on $TZ. An explicit time and an
// explicit named zone are honored unchanged.
func TestDateInZone_DefaultsToFixedClockAndForcesUTC(t *testing.T) {
	t.Parallel()
	// Unrecognized arg → fixedTime, not time.Now().
	if got := dateInZone("20060102150405", "not-a-time", "UTC"); got != "20200101000000" {
		t.Errorf(`dateInZone default branch = %q, want "20200101000000"`, got)
	}
	// Explicit time honored.
	ts := time.Date(2021, 6, 7, 8, 9, 10, 0, time.UTC)
	if got := dateInZone("2006-01-02", ts, "UTC"); got != "2021-06-07" {
		t.Errorf("dateInZone(ts) = %q, want 2021-06-07", got)
	}
	// Empty and "Local" zones both render in UTC (machine-independent).
	empty := dateInZone("150405", ts, "")
	local := dateInZone("150405", ts, "Local")
	if empty != "080910" || local != "080910" {
		t.Errorf(`empty/Local zone not forced to UTC: ""=%q Local=%q, want 080910`, empty, local)
	}
}

func TestClockFuncs_ParsersUseUTC(t *testing.T) {
	fm := Funcs(SeedFor("rel", "ns"))
	toDate, ok := fm["toDate"].(func(string, string) time.Time)
	if !ok {
		t.Fatalf("toDate override has unexpected type %T", fm["toDate"])
	}
	mustToDate, ok := fm["mustToDate"].(func(string, string) (time.Time, error))
	if !ok {
		t.Fatalf("mustToDate override has unexpected type %T", fm["mustToDate"])
	}
	local := time.Local
	t.Cleanup(func() { time.Local = local })
	for _, tc := range []struct {
		name   string
		offset int
	}{
		{"UTC", 0},
		{"UTC-6", -21600},
	} {
		t.Run(tc.name, func(t *testing.T) {
			time.Local = time.FixedZone(tc.name, tc.offset)
			assert.Equal(t, toDate("2006-01-02", "2020-01-01").Unix(), int64(1577836800))
			parsed, err := mustToDate("2006-01-02", "2020-01-01")
			assert.NoError(t, err)
			assert.Equal(t, parsed.Unix(), int64(1577836800))
			assert.Equal(t, toDate(time.RFC3339, "2019-12-31T18:00:00-06:00").Unix(), int64(1577836800))
			parsed, err = mustToDate(time.RFC3339, "2019-12-31T18:00:00-06:00")
			assert.NoError(t, err)
			assert.Equal(t, parsed.Unix(), int64(1577836800))
			if got := toDate("2006-01-02", "invalid"); !got.IsZero() {
				t.Errorf("invalid toDate = %v, want zero time", got)
			}
			if _, err := mustToDate("2006-01-02", "invalid"); err == nil {
				t.Error("invalid mustToDate succeeded")
			}
		})
	}
}

func TestClockFuncs_DurationRoundFixedClock(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		fm := Funcs(SeedFor("rel", "ns"))
		now := fm["now"].(func() time.Time)
		round, ok := fm["durationRound"].(func(any) string)
		if !ok {
			t.Fatalf("durationRound override has unexpected type %T", fm["durationRound"])
		}
		instant := now()
		assert.Equal(t, round(instant), "0s")
		assert.Equal(t, round(instant.Add(-2*time.Hour)), "2h")
		time.Sleep(3 * time.Hour)
		assert.Equal(t, now(), instant)
		assert.Equal(t, round(instant), "0s")
		assert.Equal(t, round(instant.Add(-2*time.Hour)), "2h")
	})
}

func TestClockFuncs_DurationRoundInputs(t *testing.T) {
	t.Parallel()
	round, ok := Funcs(SeedFor("rel", "ns"))["durationRound"].(func(any) string)
	if !ok {
		t.Fatal("durationRound override has unexpected type")
	}
	for _, tc := range []struct {
		name  string
		input any
		want  string
	}{
		{"string", "2h", "2h"},
		{"nanoseconds", int64(2 * time.Hour), "2h"},
		{"negative nanoseconds", int64(-2 * time.Hour), "2h"},
		{"past", fixedTime.Add(-2 * time.Hour), "2h"},
		{"future", fixedTime.Add(2 * time.Hour), "2h"},
		{"invalid", "invalid", "0s"},
		{"unsupported", nil, "0s"},
		{"native int", int(2 * time.Hour), "0s"},
		{"duration", 2 * time.Hour, "0s"},
		{"boundary", "1h", "60m"},
		{"above boundary", "1h1ns", "1h"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, round(tc.input), tc.want)
		})
	}
}

// TestClockFuncs_DateForcesUTC pins that the registered "date" override
// formats in UTC with a fixedTime fallback — so `{{ now | date … }}` is
// reproducible across machines, not just across runs on one machine.
func TestClockFuncs_DateForcesUTC(t *testing.T) {
	t.Parallel()
	fm := clockFuncs()
	date, ok := fm["date"].(func(string, any) string)
	if !ok {
		t.Fatalf(`"date" override has unexpected type %T`, fm["date"])
	}
	if got := date("20060102150405", fixedTime); got != "20200101000000" {
		t.Errorf(`date(fixedTime) = %q, want "20200101000000"`, got)
	}
}
