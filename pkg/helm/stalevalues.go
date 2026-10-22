package helm

import (
	"bytes"
	"context"
	"slices"
	"strings"

	chart "helm.sh/helm/v4/pkg/chart/v2"

	"github.com/home-operations/flate/pkg/manifest"
)

const valuesToken = ".Values"

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
//   - Keys gating a dependency are always treated as used: the top-level
//     segment of the root chart's dependencies[].condition paths, and "tags"
//     when any dependency in the tree declares tags — Helm's dependency
//     evaluation consumes them without any template naming them (#930).
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
	loaded, err := c.describeChart(ctx, hr)
	if err == nil && loaded.analysis == nil {
		err = c.decodeChart(ctx, &loaded)
	}
	if err != nil {
		return nil
	}
	return loaded.analyze().unreferencedValuePaths(values)
}

func requiresChartLoader(ch *chart.Chart) bool {
	for _, file := range ch.Raw {
		if file != nil && (file.Name == "requirements.yaml" || file.Name == "requirements.lock") {
			return true
		}
	}
	return slices.ContainsFunc(ch.Dependencies(), requiresChartLoader)
}

func (a *chartAnalysis) unreferencedValuePaths(values map[string]any) []string {
	if a.Templates == "" {
		return nil
	}
	var out []string
	for k := range values {
		if _, ok := a.Allowed[k]; ok {
			continue
		}
		if !valuesKeyReferenced(a.Templates, k) {
			out = append(out, k)
		}
	}
	slices.Sort(out)
	return out
}

// Nested conditions resolve under their parent dependency's name; tags share
// the root values table at every depth.
func knownTopLevelKeys(ch *chart.Chart) map[string]struct{} {
	allow := map[string]struct{}{"global": {}}
	if ch == nil {
		return allow
	}
	var hasTags bool
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
			for cond := range strings.SplitSeq(d.Condition, ",") {
				if top, _, _ := strings.Cut(strings.TrimSpace(cond), "."); top != "" {
					allow[top] = struct{}{}
				}
			}
			hasTags = hasTags || len(d.Tags) > 0
		}
	}
	for _, sub := range ch.Dependencies() {
		if n := sub.Name(); n != "" {
			allow[n] = struct{}{}
		}
		hasTags = hasTags || anyDependencyTags(sub)
	}
	if hasTags {
		allow["tags"] = struct{}{}
	}
	return allow
}

// anyDependencyTags reports whether any dependency in ch's tree declares tags.
func anyDependencyTags(ch *chart.Chart) bool {
	if ch == nil {
		return false
	}
	if ch.Metadata != nil {
		for _, d := range ch.Metadata.Dependencies {
			if d != nil && len(d.Tags) > 0 {
				return true
			}
		}
	}
	return slices.ContainsFunc(ch.Dependencies(), anyDependencyTags)
}

// Opaque .Values access suppresses every warning, so its source MUST not be retained.
func chartTemplateText(ch *chart.Chart) string {
	var b strings.Builder
	var add func(*chart.Chart) bool
	add = func(c *chart.Chart) bool {
		if c == nil {
			return true
		}
		for _, f := range c.Templates {
			if f != nil {
				if opaqueValuesAccess(f.Data) {
					return false
				}
				b.Write(f.Data)
				b.WriteByte('\n')
			}
		}
		for _, sub := range c.Dependencies() {
			if !add(sub) {
				return false
			}
		}
		return true
	}
	if !add(ch) {
		return ""
	}
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
func opaqueValuesAccess(src []byte) bool {
	for i := 0; ; {
		j := bytes.Index(src[i:], []byte(valuesToken))
		if j < 0 {
			return false
		}
		next := i + j + len(valuesToken)
		for next < len(src) && (src[next] == ' ' || src[next] == '\t') {
			next++
		}
		if next >= len(src) {
			return true
		}
		if c := src[next]; c != '.' && c != '"' {
			return true
		}
		i += j + len(valuesToken)
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
	needle := valuesToken + "." + key
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
