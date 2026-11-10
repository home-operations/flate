package diff

import (
	"iter"
	"reflect"

	"github.com/home-operations/flate/pkg/manifest"
)

func (p prepared) changes() iter.Seq2[pair, string] {
	return func(yield func(pair, string) bool) {
		paired, leftCopied, rightCopied := p.Pairs, p.leftCopied, p.rightCopied
		for i := range paired {
			p := paired[i]
			status := p.status()
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

func (p *pair) status() string {
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
