package diff

import (
	"fmt"
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
)

type side string

const (
	sideOrig    side = "orig"
	sideCurrent side = "current"
	sideBoth    side = "both"
)

// Suppression records a producer whose rendered objects were withheld because
// rendering failed, was soft-skipped, or was suspended.
type Suppression struct {
	Subject manifest.DiagnosticIdentity `json:"-"`
	Parent  Parent                      `json:"parent"`
	Side    side                        `json:"side"`   // orig, current, or both unavailable sides
	Count   int                         `json:"count"`  // withheld healthy-side entries, or all entries for both
	Reason  string                      `json:"reason"` // the render failure, skip, or suspension reason
}

func (s Suppression) String() string {
	noun := "objects"
	if s.Count == 1 {
		noun = "object"
	}
	reason := strings.Join(strings.Fields(s.Reason), " ")
	if s.Side == sideBoth {
		return fmt.Sprintf("%d %s withheld; diff suppressed (%s)", s.Count, noun, reason)
	}
	return fmt.Sprintf("%d %s not rendered on the %s side; diff suppressed (%s)", s.Count, noun, s.Side, reason)
}
