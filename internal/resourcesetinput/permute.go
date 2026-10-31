package resourcesetinput

import (
	"fmt"
	"hash/adler32"
	"strconv"
	"strings"
	"unicode"
)

const maxPermutations = 10000

// Permute combines provider groups with the Flux Cartesian input semantics.
func Permute(groups []Group, includeEmpty bool) ([]map[string]any, error) {
	scoped := make([]scopedProvider, 0, len(groups))
	expected := uint64(1)
	for _, g := range groups {
		if len(g.Inputs) == 0 {
			if includeEmpty {
				return nil, nil
			}
			continue
		}
		norm := normalizeKeyForTemplate(g.Name)
		if norm == "" {
			return nil, fmt.Errorf("permute: provider name %q normalizes to empty", g.Name)
		}
		expected *= uint64(len(g.Inputs))
		if expected > maxPermutations {
			return nil, fmt.Errorf("permute: would exceed %d permutations (provider %q contributes %d inputs)",
				maxPermutations, g.Name, len(g.Inputs))
		}
		scoped = append(scoped, scopedProvider{name: norm, sets: g.Inputs})
	}
	if len(scoped) == 0 {
		return nil, nil
	}

	out := make([]map[string]any, 0, expected)
	sel := make([]int, len(scoped))
	idParts := make([]string, len(scoped))
	for {
		perm := make(map[string]any, len(scoped)+1)
		for i, p := range scoped {
			perm[p.name] = p.sets[sel[i]]
			idParts[i] = p.name + "=" + strconv.Itoa(sel[i])
		}
		perm["id"] = permID(strings.Join(idParts, "/"))
		out = append(out, perm)

		i := len(scoped) - 1
		for i >= 0 {
			sel[i]++
			if sel[i] < len(scoped[i].sets) {
				break
			}
			sel[i] = 0
			i--
		}
		if i < 0 {
			break // overflowed the leftmost — done.
		}
	}
	return out, nil
}

type scopedProvider struct {
	name string
	sets []map[string]any
}

func permID(s string) string {
	return strconv.FormatUint(uint64(adler32.Checksum([]byte(s))), 10)
}

func normalizeKeyForTemplate(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	pendingUnderscore := false
	for _, r := range s {
		r = unicode.ToLower(r)
		if unicode.IsSpace(r) || unicode.IsPunct(r) {
			pendingUnderscore = true
			continue
		}
		if ('a' <= r && r <= 'z') || ('0' <= r && r <= '9') {
			if pendingUnderscore && b.Len() > 0 {
				b.WriteByte('_')
			}
			pendingUnderscore = false
			b.WriteRune(r)
			continue
		}
	}
	return b.String()
}
