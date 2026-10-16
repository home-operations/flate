package orchestrator

import (
	"errors"
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func TestJoinTreeErrors_Sides(t *testing.T) {
	t.Parallel()
	base, head := errors.New("base failure"), errors.New("head failure")
	for _, tt := range []struct {
		name       string
		base, head error
		message    string
	}{
		{name: "neither"},
		{name: "base", base: base, message: "base tree: base failure"},
		{name: "head", head: head, message: "head tree: head failure"},
		{name: "both", base: base, head: head,
			message: "both trees had reconcile failures\nbase tree: base failure\nhead tree: head failure"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := joinTreeErrors(tt.base, tt.head)
			if tt.message == "" {
				assert.Equal(t, got, nil)
				return
			}
			if got == nil {
				t.Fatal("missing tree failure")
			}
			assert.Equal(t, got.Error(), tt.message)
			assert.Equal(t, errors.Is(got, base), tt.base != nil)
			assert.Equal(t, errors.Is(got, head), tt.head != nil)
		})
	}
}
