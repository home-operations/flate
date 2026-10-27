package diff

import "github.com/home-operations/flate/internal/diffprep"

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
	Parent Parent `json:"parent"`
	Side   Side   `json:"side"`   // orig, current, or both unavailable sides
	Count  int    `json:"count"`  // withheld healthy-side entries, or all entries for both
	Reason string `json:"reason"` // the render failure, skip, or suspension reason
}

// String renders the suppression as a human-readable sentence.
func (s Suppression) String() string {
	return (diffprep.Suppression{Parent: diffprep.Parent(s.Parent), Side: diffprep.Side(s.Side), Count: s.Count, Reason: s.Reason}).String()
}
