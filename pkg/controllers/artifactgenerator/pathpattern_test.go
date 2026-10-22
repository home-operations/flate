package artifactgenerator

import (
	"context"
	"errors"
	"io/fs"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/google/go-cmp/cmp"
)

func TestExpand_CapturesAndIdentity(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, pattern, output string
		input                 fstest.MapFS
		want                  []request
		invalid               bool
	}{
		{"raw_paths_normalized_names_labels", "@repo/apps/{App}", "{App}", fstest.MapFS{"apps/Prod/file": {Data: []byte("x")}}, []request{{Output: output{Name: "prod", Copy: []operation{{From: "@repo/apps/Prod", To: "@artifact/Prod"}}}, Captures: map[string]string{"App": "prod"}}}, false},
		{"zero_matches", "@repo/absent/{App}", "{App}", fstest.MapFS{"apps/Prod/file": {}}, nil, false},
		{"files_not_directories", "@repo/{App}", "{App}", fstest.MapFS{"file": {}}, nil, false},
		{"root_excluded", "@repo/", "fixed", fstest.MapFS{"file": {}}, nil, false},
		{"duplicate_capture", "@repo/{App}/{App}", "{App}", nil, nil, true},
		{"invalid_capture", "@repo/{bad.name}", "fixed", nil, nil, true},
		{"unclosed_capture", "@repo/{App", "fixed", nil, nil, true},
		{"undefined_template_capture", "@repo/apps/{App}", "{missing}", fstest.MapFS{"apps/Prod/file": {}}, nil, true},
		{"invalid_capture_value", "@repo/apps/{App}", "fixed", fstest.MapFS{"apps/has space/file": {}}, nil, true},
		{"duplicate_expanded_name", "@repo/apps/{App}", "fixed", fstest.MapFS{"apps/A/file": {}, "apps/B/file": {}}, nil, true},
		{"normalized_name_collision", "@repo/apps/{App}", "{App}", fstest.MapFS{"apps/PROD/file": {}, "apps/prod/file": {}}, nil, true},
		{"invalid_expanded_name", "@repo/apps/{App}", "bad_{App}", fstest.MapFS{"apps/Prod/file": {}}, nil, true},
		{"unknown_pattern_alias", "@other/{App}", "fixed", nil, nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spec := specification{PathPattern: tc.pattern, Artifacts: []output{{Name: tc.output, Copy: []operation{{From: "@repo/apps/{App}", To: "@artifact/{App}"}}}}}
			got, err := expand(t.Context(), spec, map[string]fs.FS{"repo": tc.input})
			if (err != nil) != tc.invalid {
				t.Fatalf("expand() = %v; invalid=%v", err, tc.invalid)
			}
			if tc.name == "invalid_capture" && !strings.Contains(err.Error(), "invalid or repeated pathPattern capture") {
				t.Fatalf("capture diagnostic = %v", err)
			}
			if !tc.invalid {
				if diff := cmp.Diff(tc.want, got, cmp.AllowUnexported(request{}, output{}, operation{})); diff != "" {
					t.Fatal(diff)
				}
			}
		})
	}
}

func TestExpand_Canceled(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := expand(ctx, specification{PathPattern: "@repo/{App}"}, map[string]fs.FS{"repo": fstest.MapFS{"app/file": {}}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("expand() = %v", err)
	}
}
