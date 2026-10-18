package helmchart

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/home-operations/flate/internal/testutil"
	repo "helm.sh/helm/v4/pkg/repo/v1"
)

func TestIndexFromBytes_MatchesHelmLoader(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, data string
		err        error
	}{
		{"entries", `apiVersion: v1
entries:
  chart:
    - name: chart
      version: 1.0.0
      urls: [chart.tgz]
    - version: 2.0.0
    - name: chart
      version: invalid
    - null
    - name: chart
      version: 3.0.0
      dependencies:
        - name: dependency
          version: 1.0.0
        - name: dependency
          version: 1.0.0
`, nil},
		{"json", `{"apiVersion":"v1","entries":{"chart":[{"name":"chart","version":"1.0.0"},null]}}`, nil},
		{"empty", "", repo.ErrEmptyIndexYaml},
		{"no api version", "entries: {}\n", repo.ErrNoAPIVersion},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(t, root, "index.yaml", tc.data)
			want, wantErr := repo.LoadIndexFile(filepath.Join(root, "index.yaml"))
			got, gotErr := indexFromBytes([]byte(tc.data))
			if !errors.Is(wantErr, tc.err) || !errors.Is(gotErr, tc.err) {
				t.Fatalf("upstream=%v FS=%v, want %v", wantErr, gotErr, tc.err)
			}
			if tc.err == nil {
				if diff := cmp.Diff(want, got); diff != "" {
					t.Fatalf("index (-want +got):\n%s", diff)
				}
			}
		})
	}
}
