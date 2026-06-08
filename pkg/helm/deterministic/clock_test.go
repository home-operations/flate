package deterministic

import (
	"testing"
	"time"
)

// TestDateInZone_DefaultsToFixedTimeAndForcesUTC pins the two determinism
// fixes in the ported date helper: an unrecognized argument falls back to
// FixedTime (not the wall clock), and an empty or "Local" zone is treated
// as UTC so formatting never depends on $TZ. An explicit time and an
// explicit named zone are honored unchanged.
func TestDateInZone_DefaultsToFixedTimeAndForcesUTC(t *testing.T) {
	// Unrecognized arg → FixedTime, not time.Now().
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

// TestClockFuncs_DateForcesUTC pins that the registered "date" override
// formats in UTC with a FixedTime fallback — so `{{ now | date … }}` is
// reproducible across machines, not just across runs on one machine.
func TestClockFuncs_DateForcesUTC(t *testing.T) {
	fm := clockFuncs()
	date, ok := fm["date"].(func(string, any) string)
	if !ok {
		t.Fatalf(`"date" override has unexpected type %T`, fm["date"])
	}
	if got := date("20060102150405", FixedTime); got != "20200101000000" {
		t.Errorf(`date(FixedTime) = %q, want "20200101000000"`, got)
	}
}
