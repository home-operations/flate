package cli

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/output"
)

func TestPrepareGetTables_SharedColumns(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		groups int
	}{
		{name: "single", groups: 1},
		{name: "multiple", groups: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			columns := []output.Column{{Header: "NAME", Key: "name"}}
			rows := []map[string]string{{"cluster": "alpha", "name": "shared"}}
			original := []output.Table{{Columns: columns, Rows: rows}}
			prepared, multi := prepareGetTables(&selectedInvocation{groups: make([]selectedGroup, tc.groups)}, original)
			assert.Equal(t, multi, tc.groups == 2)
			want := columns
			if multi {
				want = []output.Column{{Header: "CLUSTER", Key: "cluster"}, {Header: "NAME", Key: "name"}}
			}
			assert.Diff(t, prepared[0].Columns, want)
			assert.Diff(t, original, []output.Table{{
				Columns: []output.Column{{Header: "NAME", Key: "name"}},
				Rows:    []map[string]string{{"cluster": "alpha", "name": "shared"}},
			}})
		})
	}
}
