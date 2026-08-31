package diff

import (
	"cmp"
	"fmt"
	"slices"
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
)

// Side names the side of a comparison. Left is the orig (base, --path-orig)
// tree and right the current (head, --path) tree, matching RenderDocs' and
// Changes' argument order.
type Side string

// Side values, in the vocabulary the CLI uses for --path-orig / --path.
const (
	SideOrig    Side = "orig"
	SideCurrent Side = "current"
)

// Suppression records a producer (Kustomization or HelmRelease) whose
// rendered objects were withheld from a diff because it failed to render on
// exactly one side. Left in, everything the healthy side produced would pair
// against nothing and read as a wholesale add or delete, when the real event
// is a render failure the failure summary already reports.
type Suppression struct {
	Parent Parent
	Side   Side   // the side that failed
	Count  int    // objects the other side rendered and the diff withheld
	Reason string // the failing side's failure message
}

// String renders the suppression as the one-line note the formats emit.
func (s Suppression) String() string {
	noun := "objects"
	if s.Count == 1 {
		noun = "object"
	}
	return fmt.Sprintf("%s %s: %d %s not rendered on the %s side; diff suppressed (%s)",
		s.Parent.Kind, joinNS(s.Parent.Namespace, s.Parent.Name), s.Count, noun, s.Side,
		strings.Join(strings.Fields(s.Reason), " "))
}

// SuppressFailed withholds every doc whose parent is in exactly one of
// leftFailed / rightFailed (parent id -> failure message) and returns the
// trimmed sets plus one Suppression per affected parent, sorted. Docs are
// dropped from BOTH sides so a stale artifact on the failing side cannot
// surface as a change either. A parent failed on both sides, or with nothing
// rendered on the healthy side, yields no Suppression. Pass the result to
// RenderDocs via Options.Suppressed so the withheld set is disclosed in the
// output; feed the trimmed sets to Changes the same way.
func SuppressFailed(left, right []Doc, leftFailed, rightFailed map[manifest.NamedResource]string) (l, r []Doc, suppressed []Suppression) {
	failedOn := func(p Parent) (Side, string, bool) {
		id := manifest.NamedResource{Kind: p.Kind, Namespace: p.Namespace, Name: p.Name}
		lm, lf := leftFailed[id]
		rm, rf := rightFailed[id]
		switch {
		case lf && !rf:
			return SideOrig, lm, true
		case rf && !lf:
			return SideCurrent, rm, true
		}
		return "", "", false
	}
	byParent := map[Parent]*Suppression{}
	keep := func(docs []Doc, this Side) []Doc {
		out := make([]Doc, 0, len(docs))
		for _, d := range docs {
			side, reason, ok := failedOn(d.Parent)
			if !ok {
				out = append(out, d)
				continue
			}
			s := byParent[d.Parent]
			if s == nil {
				s = &Suppression{Parent: d.Parent, Side: side, Reason: reason}
				byParent[d.Parent] = s
			}
			// Only the healthy side's docs are what the reader would have
			// mistaken for a change; the failing side's are stale.
			if side != this {
				s.Count++
			}
		}
		return out
	}
	l = keep(left, SideOrig)
	r = keep(right, SideCurrent)
	for _, s := range byParent {
		if s.Count > 0 {
			suppressed = append(suppressed, *s)
		}
	}
	slices.SortFunc(suppressed, func(a, b Suppression) int {
		return cmp.Or(
			cmp.Compare(a.Parent.Kind, b.Parent.Kind),
			cmp.Compare(a.Parent.Namespace, b.Parent.Namespace),
			cmp.Compare(a.Parent.Name, b.Parent.Name),
			cmp.Compare(a.Parent.Path, b.Parent.Path),
		)
	})
	return l, r, suppressed
}

// suppressionHeader renders opts.Suppressed as a leading block, one prefixed
// line per entry, so the reader meets the explanation before any body. Empty
// when nothing was suppressed.
func suppressionHeader(opts Options, prefix string) []byte {
	if len(opts.Suppressed) == 0 {
		return nil
	}
	var b strings.Builder
	for _, s := range opts.Suppressed {
		b.WriteString(prefix)
		b.WriteString(s.String())
		b.WriteByte('\n')
	}
	return []byte(b.String())
}

// withHeader prepends header to body, separated by a blank line when both are
// non-empty.
func withHeader(header, body []byte) []byte {
	switch {
	case len(header) == 0:
		return body
	case len(body) == 0:
		return header
	}
	out := make([]byte, 0, len(header)+1+len(body))
	out = append(out, header...)
	out = append(out, '\n')
	return append(out, body...)
}

// notePrefix is the line marker a style uses for the suppression header:
// the diff-syntax styles reuse their change marker so a ```diff fence
// highlights the note; the human styles get a plain warning glyph.
func notePrefix(f Format) string {
	switch f {
	case FormatGitHub, FormatGitea:
		return "! "
	case FormatGitLab, FormatDiff:
		return "# "
	}
	return "⚠ "
}
