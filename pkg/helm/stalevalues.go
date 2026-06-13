package helm

import (
	"context"
	"slices"
	"strings"

	chart "helm.sh/helm/v4/pkg/chart/v2"

	"github.com/home-operations/flate/pkg/manifest"
)

// StaleValuePaths returns the top-level release-value keys a HelmRelease supplies
// that NO chart template references — the #744 signal that an override has gone
// stale (a key renamed/removed when the chart was upgraded silently does
// nothing) or was never consumed.
//
// It measures actual consumption, not schema shape: it scans every template in
// the chart and its (sub)charts for a `.Values.<key>` reference (dotted, or a
// quoted "<key>" for index/pluck/hasKey/dig forms). A key referenced nowhere is
// reported. This is what a values.schema.json can't tell us — real charts ship
// incomplete, open schemas that permit (but never consume) extra keys, so a
// dead key like a renamed override passes schema validation untouched.
//
// Top-level only, and deliberately conservative to avoid false positives:
//   - Keys addressed to a subchart (a dependency name/alias) or Helm's shared
//     "global" are always treated as used — the subchart consumes them.
//   - If the chart accesses `.Values` opaquely — ranges/dumps the whole map
//     (toYaml .Values), binds it to a variable, or indexes it with a non-literal
//     key — we can't statically prove any key unused, so we report NOTHING.
//
// Schema-independent and advisory only: fail-open (nil) on a load error or a
// chart with no templates. The controller surfaces the result as a
// manifest.Warning; this function never logs or fails a render.
func (c *Client) StaleValuePaths(ctx context.Context, hr *manifest.HelmRelease, values map[string]any) []string {
	if len(values) == 0 {
		return nil
	}
	loaded, err := c.LoadChart(ctx, hr)
	if err != nil || loaded.Chart == nil {
		return nil
	}
	return unreferencedValuePaths(chartTemplateText(loaded.Chart), values, knownTopLevelKeys(loaded.Chart))
}

// unreferencedValuePaths returns the sorted top-level value keys absent from the
// template source, skipping the always-used allowlist. It is pure and fail-open:
// empty source or any opaque whole-.Values access yields nil (never a misleading
// warning).
func unreferencedValuePaths(src string, values map[string]any, allow map[string]struct{}) []string {
	if src == "" || opaqueValuesAccess(src) {
		return nil
	}
	var out []string
	for k := range values {
		if _, ok := allow[k]; ok {
			continue
		}
		if !valuesKeyReferenced(src, k) {
			out = append(out, k)
		}
	}
	slices.Sort(out)
	return out
}

// knownTopLevelKeys are release top-level keys always treated as used,
// regardless of the parent chart's templates: Helm's shared "global" key, and
// every declared dependency's name/alias (values addressed to a subchart nest
// under these and are consumed by the subchart, not the parent).
func knownTopLevelKeys(ch *chart.Chart) map[string]struct{} {
	allow := map[string]struct{}{"global": {}}
	if ch == nil {
		return allow
	}
	if ch.Metadata != nil {
		for _, d := range ch.Metadata.Dependencies {
			if d == nil {
				continue
			}
			if d.Name != "" {
				allow[d.Name] = struct{}{}
			}
			if d.Alias != "" {
				allow[d.Alias] = struct{}{}
			}
		}
	}
	for _, sub := range ch.Dependencies() {
		if n := sub.Name(); n != "" {
			allow[n] = struct{}{}
		}
	}
	return allow
}

// chartTemplateText concatenates every template (including _helpers.tpl and
// NOTES.txt) of ch and its subcharts. Subchart templates are included because a
// parent that passes context to a common/library chart has its values consumed
// there — so a `.Values.<key>` reference anywhere in the tree counts.
func chartTemplateText(ch *chart.Chart) string {
	var b strings.Builder
	var add func(*chart.Chart)
	add = func(c *chart.Chart) {
		if c == nil {
			return
		}
		for _, f := range c.Templates {
			if f != nil {
				b.Write(f.Data)
				b.WriteByte('\n')
			}
		}
		for _, sub := range c.Dependencies() {
			add(sub)
		}
	}
	add(ch)
	return b.String()
}

// opaqueValuesAccess reports whether src reads `.Values` in a way that could
// consume an arbitrary key without naming it — ranging or dumping the whole map
// (range .Values, toYaml .Values), binding it to a variable (:= .Values),
// passing it to a template (include "x" .Values), or indexing it with a
// non-literal key (index .Values $k). When true, no key can be proven unused, so
// the caller reports nothing.
//
// The test is structural: every `.Values` token whose next non-space byte is
// neither `.` (a field selector, .Values.foo) nor `"` (a literal index,
// `.Values "foo"` / `index .Values "foo"`) accesses the map as a whole.
func opaqueValuesAccess(src string) bool {
	const tok = ".Values"
	for i := 0; ; {
		j := strings.Index(src[i:], tok)
		if j < 0 {
			return false
		}
		next := i + j + len(tok)
		for next < len(src) && (src[next] == ' ' || src[next] == '\t') {
			next++
		}
		if next >= len(src) {
			return true
		}
		if c := src[next]; c != '.' && c != '"' {
			return true
		}
		i += j + len(tok)
	}
}

// valuesKeyReferenced reports whether src names top-level value key — either as
// a dotted field `.Values.<key>` (with a trailing identifier boundary, so
// `.Values.foobar` doesn't satisfy "foo") or as a quoted "<key>" token (the
// index/pluck/hasKey/dig forms). The quoted check is intentionally broad: a
// spurious match only suppresses a warning (a false negative), never invents one.
func valuesKeyReferenced(src, key string) bool {
	if strings.Contains(src, `"`+key+`"`) {
		return true
	}
	needle := ".Values." + key
	for i := 0; ; {
		j := strings.Index(src[i:], needle)
		if j < 0 {
			return false
		}
		end := i + j + len(needle)
		if end >= len(src) || !isIdentByte(src[end]) {
			return true
		}
		i += j + len(needle)
	}
}

func isIdentByte(b byte) bool {
	return b == '_' ||
		(b >= 'a' && b <= 'z') ||
		(b >= 'A' && b <= 'Z') ||
		(b >= '0' && b <= '9')
}
