package resourceset

import (
	"fmt"
	"hash/adler32"
	"strings"
	"unicode"
)

// maxPermutations caps the Cartesian product at the same threshold
// flux-operator/internal/inputs/permuter.go uses. A ResourceSet that
// asks for more wins a fail-loud error rather than burning host RAM
// on a runaway combination set.
const maxPermutations = 10000

// permute returns the Cartesian product across groups, with each
// provider's input set nested under its normalized name. Each output
// map carries an `id` field — an adler32 hash of the
// "<name>=<index>/..." selection — matching upstream's permuter.go.
//
// includeEmpty controls behavior when a provider exports zero input
// sets: when false (the default, matching upstream), the empty
// provider is silently dropped from the product; when true, the
// product collapses to zero results.
func permute(groups []providerInputs, includeEmpty bool) ([]map[string]any, error) {
	// Validate names + project to per-provider scoped lists. The
	// normalized name keys the nested wrapper; templates access values
	// via `inputs.<normalized-name>.foo`.
	scoped := make([]scopedProvider, 0, len(groups))
	expected := uint64(1)
	for _, g := range groups {
		if !includeEmpty && len(g.inputs) == 0 {
			continue
		}
		norm := normalizeKeyForTemplate(g.name)
		if norm == "" {
			return nil, fmt.Errorf("permute: provider name %q normalizes to empty", g.name)
		}
		if len(scoped) == 0 {
			expected = uint64(len(g.inputs))
		} else {
			expected *= uint64(len(g.inputs))
		}
		if expected > maxPermutations {
			return nil, fmt.Errorf("permute: would exceed %d permutations (provider %q contributes %d inputs)",
				maxPermutations, g.name, len(g.inputs))
		}
		scoped = append(scoped, scopedProvider{name: norm, sets: g.inputs})
	}
	if len(scoped) == 0 {
		return nil, nil
	}
	// One empty provider collapses the product. Matches upstream:
	// computePermutationsWithBacktracking returns early if any
	// scoped list is empty.
	for _, p := range scoped {
		if len(p.sets) == 0 {
			return nil, nil
		}
	}

	out := make([]map[string]any, 0, expected)
	// Index vector — selectedInputs[i] is the index of the chosen
	// input set within provider i. Standard mixed-radix counter.
	sel := make([]int, len(scoped))
	for {
		perm := make(map[string]any, len(scoped)+1)
		idParts := make([]string, 0, len(scoped))
		for i, p := range scoped {
			perm[p.name] = p.sets[sel[i]]
			idParts = append(idParts, fmt.Sprintf("%s=%d", p.name, sel[i]))
		}
		perm["id"] = permID(strings.Join(idParts, "/"))
		out = append(out, perm)

		// Increment from the rightmost provider, carrying as needed.
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

// scopedProvider holds one provider's input list keyed by the
// normalized name templates will use to dereference it.
type scopedProvider struct {
	name string
	sets []map[string]any
}

// permID is upstream's id-format for permutations: an adler32 hash
// of the slash-joined selection string, rendered as a decimal. Keeps
// flate's render byte-equivalent with what flux-operator computes
// in-cluster so diffs surface the actual configuration delta rather
// than an id-format mismatch.
func permID(s string) string {
	return fmt.Sprintf("%d", adler32.Checksum([]byte(s)))
}

// normalizeKeyForTemplate mirrors flux-operator/internal/inputs/keys.go:
// lowercase, replace whitespace + punctuation with "_", drop characters
// outside [a-z0-9_], then collapse runs of "_" to a single "_" with
// leading/trailing trimmed. The output is the key under which a
// provider's input set sits in the rendered permutation, so flate
// MUST match upstream exactly or templates that work in-cluster
// would silently fail to dereference here.
//
// Used here rather than re-exporting from upstream because flux-operator's
// inputs package is in internal/.
func normalizeKeyForTemplate(s string) string {
	mapped := strings.Map(func(r rune) rune {
		r = unicode.ToLower(r)
		if unicode.IsSpace(r) || unicode.IsPunct(r) {
			return '_'
		}
		if ('a' <= r && r <= 'z') || ('0' <= r && r <= '9') {
			return r
		}
		return -1
	}, s)
	parts := strings.Split(mapped, "_")
	out := parts[:0]
	for _, p := range parts {
		if p != "" {
			out = append(out, p)
		}
	}
	return strings.Join(out, "_")
}
