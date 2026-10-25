package manifest

import (
	"cmp"
	"fmt"
	"io/fs"
	"strings"
)

// ClusterID identifies a normalized repository-relative execution boundary.
type ClusterID string

// Validate rejects coordinates outside the admitted repository.
func (c ClusterID) Validate() error {
	if !fs.ValidPath(string(c)) || strings.ContainsAny(string(c), "\\:\x00") {
		return fmt.Errorf("%w: invalid cluster path %q", ErrInput, c)
	}
	return nil
}

// TreeSide identifies the base or head of a comparison.
type TreeSide uint8

const (
	// TreeBase identifies the original input.
	TreeBase TreeSide = iota
	// TreeHead identifies the current input.
	TreeHead
)

// TreeID identifies one isolated execution by cluster and comparison side.
type TreeID struct {
	Cluster ClusterID
	Side    TreeSide
}

// Validate checks both execution coordinates.
func (t TreeID) Validate() error {
	if err := t.Cluster.Validate(); err != nil {
		return err
	}
	if t.Side > TreeHead {
		return fmt.Errorf("%w: invalid tree side %d", ErrInput, t.Side)
	}
	return nil
}

// Compare orders clusters lexically with base before head.
func (t TreeID) Compare(other TreeID) int {
	return cmp.Or(cmp.Compare(t.Cluster, other.Cluster), cmp.Compare(t.Side, other.Side))
}
