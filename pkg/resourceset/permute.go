package resourceset

import (
	"fmt"
	"hash/adler32"
	"strconv"
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
	//
	// Empty providers short-circuit the entire product: when
	// includeEmpty=true and any provider has zero inputs, the
	// Cartesian product is zero — return early so the size cap
	// check stays meaningful (the previous second-pass at the end
	// caught this but the size accumulator went through 0 in the
	// process, making the cap check vacuous for any single empty
	// provider). Mirrors upstream
	// computePermutationsWithBacktracking's early-return.
	scoped := make([]scopedProvider, 0, len(groups))
	expected := uint64(1)
	for _, g := range groups {
		if len(g.inputs) == 0 {
			if includeEmpty {
				return nil, nil
			}
			continue
		}
		norm := normalizeKeyForTemplate(g.name)
		if norm == "" {
			return nil, fmt.Errorf("permute: provider name %q normalizes to empty", g.name)
		}
		// expected starts at 1, so multiplying for the first non-empty
		// provider yields its input count — no special-case needed.
		expected *= uint64(len(g.inputs))
		if expected > maxPermutations {
			return nil, fmt.Errorf("permute: would exceed %d permutations (provider %q contributes %d inputs)",
				maxPermutations, g.name, len(g.inputs))
		}
		scoped = append(scoped, scopedProvider{name: norm, sets: g.inputs})
	}
	if len(scoped) == 0 {
		return nil, nil
	}

	out := make([]map[string]any, 0, expected)
	// Index vector — sel[i] is the index of the chosen input set
	// within provider i. Standard mixed-radix counter.
	sel := make([]int, len(scoped))
	// idParts is pre-allocated and reused across iterations; each
	// element is overwritten before strings.Join, so no GC churn.
	idParts := make([]string, len(scoped))
	for {
		perm := make(map[string]any, len(scoped)+1)
		for i, p := range scoped {
			perm[p.name] = p.sets[sel[i]]
			idParts[i] = p.name + "=" + strconv.Itoa(sel[i])
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
	return strconv.FormatUint(uint64(adler32.Checksum([]byte(s))), 10)
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
		// Drop characters outside [a-z0-9] without emitting underscore
		// (they are neither word chars nor separators — rare Unicode).
	}
	return b.String()
}
