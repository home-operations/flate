package deterministic

import (
	"text/template"
	"time"
)

// FixedTime is the instant every time-based template function resolves to
// under deterministic rendering. It is deliberately a fixed point in the
// past: a chart computing `NotAfter = now + daysValid` still lands
// plausibly in the future for any normal validity window, and the value
// reads as a recognizable sentinel in rendered output. It must never
// derive from the wall clock — reproducibility is the whole point.
var FixedTime = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

// clockFuncs returns deterministic replacements for sprig's time-based
// functions. Two independent nondeterminism sources have to go:
//
//   - `now` is time.Now, and date/dateInZone/htmlDate/ago fall back to
//     time.Now() whenever their argument isn't a recognized time type — so
//     a bare `{{ date "fmt" }}` leaks the wall clock even with `now`
//     overridden. Every fallback here uses FixedTime instead.
//   - `date`/`htmlDate` format in the machine's "Local" zone, so one instant
//     renders differently across machines. These force UTC, and an empty or
//     "Local" zone is remapped to UTC, so output never depends on $TZ. An
//     explicit named zone is still honored — it's a literal template
//     argument and therefore deterministic.
//
// dateModify/toDate/unixEpoch/duration/durationRound are already
// deterministic given their arguments and are left to sprig.
func clockFuncs() template.FuncMap {
	return template.FuncMap{
		"now":            func() time.Time { return FixedTime },
		"date":           func(format string, date any) string { return dateInZone(format, date, "UTC") },
		"dateInZone":     dateInZone,
		"date_in_zone":   dateInZone,
		"htmlDate":       func(date any) string { return dateInZone("2006-01-02", date, "UTC") },
		"htmlDateInZone": func(date any, zone string) string { return dateInZone("2006-01-02", date, zone) },
		"ago":            dateAgo,
	}
}

// dateInZone mirrors sprig.dateInZone but (1) defaults to FixedTime rather
// than time.Now() for an unrecognized date argument and (2) treats an empty
// or "Local" zone as UTC, so formatting never depends on $TZ.
func dateInZone(format string, date any, zone string) string {
	var t time.Time
	switch date := date.(type) {
	default:
		t = FixedTime
	case time.Time:
		t = date
	case *time.Time:
		t = *date
	case int64:
		t = time.Unix(date, 0)
	case int:
		t = time.Unix(int64(date), 0)
	case int32:
		t = time.Unix(int64(date), 0)
	}
	if zone == "" || zone == "Local" {
		zone = "UTC"
	}
	loc, err := time.LoadLocation(zone)
	if err != nil {
		loc = time.UTC
	}
	return t.In(loc).Format(format)
}

// dateAgo mirrors sprig.dateAgo but measures elapsed time from FixedTime
// rather than the wall clock, so a chart embedding `{{ ago … }}` renders
// reproducibly.
func dateAgo(date any) string {
	var t time.Time
	switch date := date.(type) {
	default:
		t = FixedTime
	case time.Time:
		t = date
	case int64:
		t = time.Unix(date, 0)
	case int:
		t = time.Unix(int64(date), 0)
	}
	return FixedTime.Sub(t).Round(time.Second).String()
}
