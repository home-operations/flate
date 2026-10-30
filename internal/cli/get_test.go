package cli

import (
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestKSView_Path(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, path, wantRow string
	}{
		{name: "omitted", wantRow: "./"},
		{name: "root", path: "./", wantRow: "./"},
		{name: "apps", path: "./apps", wantRow: "./apps"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			o := &manifest.Kustomization{Name: "apps", Namespace: "flux-system", Path: tc.path}
			before := o.Clone()
			row, doc := ksView().mapper(nil, o)
			assert.Equal(t, row["path"], tc.wantRow)
			assert.Equal(t, doc["path"], any(tc.path))
			if diff := cmp.Diff(before, o, cmp.AllowUnexported(manifest.Kustomization{})); diff != "" {
				t.Errorf("mapper mutated manifest (-want +got):\n%s", diff)
			}
		})
	}
}
