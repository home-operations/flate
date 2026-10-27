// Package claimpath queries slash-terminated claim prefixes.
package claimpath

import (
	"iter"
	"strings"
)

// Groups yields prefix lengths and their nonempty groups, longest first.
// The coordinate is used verbatim; yielded groups are borrowed and MUST remain immutable.
func Groups[T any](groups map[string][]T, coordinate string) iter.Seq2[int, []T] {
	return func(yield func(int, []T) bool) {
		for end := len(coordinate); ; {
			prefix := coordinate[:end]
			if group := groups[prefix]; len(group) != 0 && !yield(end, group) {
				return
			}
			if end == 0 {
				return
			}
			end = strings.LastIndexByte(coordinate[:end-1], '/') + 1
		}
	}
}
