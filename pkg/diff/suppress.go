package diff

import (
	"fmt"
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
)

// Side names the side of a comparison. Left is the orig (base, --path-orig)
// tree and right the current (head, --path) tree. Suppression also uses "both"
// when output is withheld for unavailable producers on both sides.
type Side string

// Side values, in the vocabulary the CLI uses for --path-orig / --path.
const (
	SideOrig    Side = "orig"
	SideCurrent Side = "current"
	SideBoth    Side = "both"
)

// Suppression records a producer whose rendered objects were withheld because
// rendering failed, was soft-skipped, or was suspended.
type Suppression struct {
	Subject manifest.DiagnosticIdentity `json:"-"`
	Parent  Parent                      `json:"parent"`
	Side    Side                        `json:"side"`   // orig, current, or both unavailable sides
	Count   int                         `json:"count"`  // withheld healthy-side entries, or all entries for both
	Reason  string                      `json:"reason"` // the render failure, skip, or suspension reason
}

func (s Suppression) String() string {
	noun := "objects"
	if s.Count == 1 {
		noun = "object"
	}
	reason := strings.Join(strings.Fields(s.Reason), " ")
	if s.Side == SideBoth {
		return fmt.Sprintf("%d %s withheld; diff suppressed (%s)", s.Count, noun, reason)
	}
	return fmt.Sprintf("%d %s not rendered on the %s side; diff suppressed (%s)", s.Count, noun, s.Side, reason)
}
