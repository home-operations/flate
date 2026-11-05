package scope_test

import "testing"

//nolint:paralleltest // cli.Run changes process-wide logging and environment.
func TestE2E_SelectedKustomizeRootDiff(t *testing.T) {
	for _, name := range []string{
		"selected-diff-referenced-patch",
		"selected-diff-resource-removed",
		"selected-diff-resource-added",
		"selected-diff-list-member-added",
		"selected-diff-list-member-removed",
		"selected-diff-declaration-removed",
		"selected-diff-declaration-added",
		"selected-diff-excluded-sibling",
		"selected-diff-plain-dependency",
		"selected-diff-owner-added-forward",
		"selected-diff-owner-added-reverse",
		"selected-diff-owner-removed-forward",
		"selected-diff-owner-removed-reverse",
		"selected-diff-owner-renamed-forward",
		"selected-diff-owner-renamed-reverse",
		"selected-diff-owner-winner-changed-forward",
		"selected-diff-owner-winner-changed-reverse",
		"named-missing-child-owner-added-forward",
		"named-missing-child-owner-added-reverse",
		"named-missing-child-owner-removed-forward",
		"named-missing-child-owner-removed-reverse",
		"named-missing-child-owner-renamed-forward",
		"named-missing-child-owner-renamed-reverse",
		"named-missing-child-owner-renamed-nested-forward",
		"named-missing-child-owner-renamed-nested-reverse",
	} {
		t.Run(name, func(t *testing.T) {
			testScopeCase(t, "entry-membership", name)
		})
	}
}
