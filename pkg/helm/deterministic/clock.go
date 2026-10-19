package deterministic

import (
	"text/template"
	"time"

	"github.com/Masterminds/sprig/v3"
)

// fixedTime is the instant every time-based template function resolves to
// under deterministic rendering. It is deliberately a fixed point in the
// past: a chart computing `NotAfter = now + daysValid` still lands
// plausibly in the future for any normal validity window, and the value
// reads as a recognizable sentinel in rendered output. It must never
// derive from the wall clock — reproducibility is the whole point.
var fixedTime = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

var sprigDurationRound = sprig.TxtFuncMap()["durationRound"].(func(any) string)

// Unrecognized date arguments MUST use fixedTime rather than the wall clock.
// Empty or "Local" zones MUST resolve to UTC; explicit named zones stay honored.
// UTC parsing and fixed elapsed time prevent host and clock dependence.
func clockFuncs() template.FuncMap {
	return template.FuncMap{
		"now":            func() time.Time { return fixedTime },
		"date":           func(format string, date any) string { return dateInZone(format, date, "UTC") },
		"dateInZone":     dateInZone,
		"date_in_zone":   dateInZone,
		"htmlDate":       func(date any) string { return dateInZone("2006-01-02", date, "UTC") },
		"htmlDateInZone": func(date any, zone string) string { return dateInZone("2006-01-02", date, zone) },
		"ago":            dateAgo,
		"toDate": func(layout, value string) time.Time {
			t, _ := time.ParseInLocation(layout, value, time.UTC)
			return t
		},
		"mustToDate": func(layout, value string) (time.Time, error) {
			return time.ParseInLocation(layout, value, time.UTC)
		},
		"durationRound": func(value any) string {
			if t, ok := value.(time.Time); ok {
				value = int64(fixedTime.Sub(t))
			}
			return sprigDurationRound(value)
		},
	}
}

// dateInZone mirrors sprig.dateInZone but (1) defaults to fixedTime rather
// than time.Now() for an unrecognized date argument and (2) treats an empty
// or "Local" zone as UTC, so formatting never depends on $TZ.
func dateInZone(format string, date any, zone string) string {
	var t time.Time
	switch date := date.(type) {
	default:
		t = fixedTime
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

// dateAgo mirrors sprig.dateAgo but measures elapsed time from fixedTime
// rather than the wall clock, so a chart embedding `{{ ago … }}` renders
// reproducibly.
func dateAgo(date any) string {
	var t time.Time
	switch date := date.(type) {
	default:
		t = fixedTime
	case time.Time:
		t = date
	case int64:
		t = time.Unix(date, 0)
	case int:
		t = time.Unix(int64(date), 0)
	}
	return fixedTime.Sub(t).Round(time.Second).String()
}
