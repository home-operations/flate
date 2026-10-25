package manifest

import (
	"errors"
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func TestTreeID_ValidateAndOrder(t *testing.T) {
	t.Parallel()
	for _, path := range []ClusterID{".", "arbitrary/config", "a-b/c_d"} {
		t.Run(string(path), func(t *testing.T) {
			t.Parallel()
			assert.NoError(t, (TreeID{Cluster: path, Side: TreeHead}).Validate())
		})
	}
	for _, path := range []ClusterID{"", "/root", "../escape", "a/../b", "a//b", "a/", "a\\b", "C:/root", "a\x00b"} {
		t.Run("invalid "+string(path), func(t *testing.T) {
			t.Parallel()
			err := path.Validate()
			if !errors.Is(err, ErrInput) || !errors.Is(err, ErrFlux) {
				t.Fatalf("unclassified coordinate error: %v", err)
			}
		})
	}
	if err := (TreeID{Cluster: ".", Side: TreeSide(2)}).Validate(); !errors.Is(err, ErrInput) {
		t.Fatalf("invalid side: %v", err)
	}
	ids := []TreeID{{"z", TreeHead}, {"a", TreeHead}, {"z", TreeBase}, {".", TreeHead}, {"a", TreeBase}}
	slices.SortFunc(ids, TreeID.Compare)
	assert.Diff(t, ids, []TreeID{{".", TreeHead}, {"a", TreeBase}, {"a", TreeHead}, {"z", TreeBase}, {"z", TreeHead}})
}
