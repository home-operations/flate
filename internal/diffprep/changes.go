package diffprep

import (
	"iter"
	"reflect"

	"github.com/home-operations/flate/pkg/manifest"
)

// Changes yields changed pairs and statuses with manifests independent of the inputs.
func (p Prepared) Changes() iter.Seq2[Pair, string] {
	return func(yield func(Pair, string) bool) {
		paired, leftCopied, rightCopied := p.Pairs, p.leftCopied, p.rightCopied
		for _, p := range paired {
			status := p.Status()
			if status == "" {
				continue
			}
			if !leftCopied {
				p.Old = manifest.DeepCopyMap(p.Old)
			}
			if !rightCopied {
				p.New = manifest.DeepCopyMap(p.New)
			}
			if !yield(p, status) {
				return
			}
		}
	}
}

// Status classifies the pair, returning an empty string for equal sides.
func (p Pair) Status() string {
	switch {
	case p.Old == nil:
		return "added"
	case p.New == nil:
		return "removed"
	case reflect.DeepEqual(p.Old, p.New):
		return ""
	default:
		return "changed"
	}
}

func chartLabel(m map[string]any) string {
	if m == nil {
		return ""
	}
	meta, ok := m["metadata"].(map[string]any)
	if !ok {
		return ""
	}
	labels, ok := meta["labels"].(map[string]any)
	if !ok {
		return ""
	}
	s, _ := labels[helmChartLabel].(string)
	return s
}
