package claimpath

import (
	"slices"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func TestGroups_BorrowedPrefixes(t *testing.T) {
	t.Parallel()
	groups := map[string][]int{
		"": {0}, "apps/": {1, 2}, "apps/nested/": {3}, "../pin/": {4}, "../pin/app/": {5}, "unused/": nil,
	}
	for _, tc := range []struct {
		coordinate string
		want       []string
	}{
		{"apps/nested/file.yaml", []string{"apps/nested/", "apps/", ""}},
		{"apps/nested/", []string{"apps/nested/", "apps/", ""}},
		{"apps/nested", []string{"apps/", ""}},
		{"../pin/app/file.yaml", []string{"../pin/app/", "../pin/", ""}},
		{"..", []string{""}},
		{"../", []string{""}},
		{"", []string{""}},
		{"unused/file.yaml", []string{""}},
	} {
		t.Run(tc.coordinate, func(t *testing.T) {
			t.Parallel()
			query := Groups(groups, tc.coordinate)
			for range 2 {
				var got []string
				for end, group := range query {
					prefix := tc.coordinate[:end]
					got = append(got, prefix)
					assert.FatalIf(t, &group[0] != &groups[prefix][0], "group %q was copied", prefix)
				}
				assert.FatalIf(t, !slices.Equal(got, tc.want), "prefixes = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestGroups_EarlyStop(t *testing.T) {
	t.Parallel()
	groups := map[string][]int{"": {0}, "apps/": {1}, "apps/nested/": {2, 3}}
	calls := 0
	Groups(groups, "apps/nested/file.yaml")(func(end int, group []int) bool {
		prefix := "apps/nested/file.yaml"[:end]
		calls++
		assert.FatalIf(
			t,
			prefix != "apps/nested/" || !slices.Equal(group, []int{2, 3}),
			"first group = (%q, %v)",
			prefix,
			group,
		)
		return false
	})
	assert.FatalIf(t, calls != 1, "early stop called yield %d times, want 1", calls)
}

//nolint:paralleltest // allocation measurements require serial execution.
func TestGroups_NoAllocations(t *testing.T) {
	prefix := strings.Repeat("directory/", 100)
	coordinate := prefix + "file.yaml"
	groups := map[string][]int{"": {1}, prefix: {2, 3}}
	for _, stop := range []bool{false, true} {
		allocations := testing.AllocsPerRun(100, func() {
			count := 0
			for end, group := range Groups(groups, coordinate) {
				count += end + len(group)
				if stop {
					break
				}
			}
			if count == 0 {
				t.Fatal("no groups yielded")
			}
		})
		assert.FatalIf(t, allocations != 0, "allocations with stop=%v = %g, want 0", stop, allocations)
	}
}
