package artifactgenerator

import (
	"context"
	"errors"
	"io/fs"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/pkg/manifest"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/home-operations/flate/internal/assert"
)

func TestExpand_CapturesAndIdentity(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, pattern, output string
		input                 fstest.MapFS
		want                  []request
		invalid               bool
	}{
		{
			"raw_paths_normalized_names_labels",
			"@repo/apps/{App}",
			"{App}",
			fstest.MapFS{"apps/Prod/file": {Data: []byte("x")}},
			[]request{
				{
					Output:   manifest.ArtifactGeneratorOutput{Name: "prod", Copy: []manifest.ArtifactGeneratorCopy{{From: "@repo/apps/Prod", To: "@artifact/Prod"}}},
					Captures: map[string]string{"App": "prod"},
				},
			},
			false,
		},
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
		t.Run(

			tc.name,

			func(t *testing.T) {
				t.Parallel()
				spec := manifest.ArtifactGeneratorSpec{
					PathPattern: tc.pattern,
					Artifacts: []manifest.ArtifactGeneratorOutput{
						{
							Name: tc.output,
							Copy: []manifest.ArtifactGeneratorCopy{{From: "@repo/apps/{App}", To: "@artifact/{App}"}},
						},
					},
				}
				got, err := expand(t.Context(), &spec, map[string]fs.FS{"repo": tc.input})
				assert.FatalIf(t, (err != nil) != tc.invalid, "expand() = %v; invalid=%v", err, tc.invalid)
				assert.FatalIf(t, tc.name == "invalid_capture" &&
					!strings.Contains(
						err.Error(),
						"invalid or repeated pathPattern capture",
					), "capture diagnostic = %v", err)
				if !tc.invalid {
					{
						diff := cmp.Diff(
							tc.want,
							got,
							cmp.AllowUnexported(
								request{},
								manifest.ArtifactGeneratorOutput{},
								manifest.ArtifactGeneratorCopy{},
							),
							cmpopts.IgnoreFields(request{}, "work"),
						)
						assert.FatalIf(t, diff != "", "%v", diff)
					}
				}
			},
		)
	}
}

func TestExpand_Canceled(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	{
		_, err := expand(
			ctx,
			&manifest.ArtifactGeneratorSpec{PathPattern: "@repo/{App}"},
			map[string]fs.FS{"repo": fstest.MapFS{"app/file": {}}},
		)
		assert.FatalIf(t, !errors.Is(err, context.Canceled), "expand() = %v", err)
	}
}
