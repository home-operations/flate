package manifest

import (
	"errors"
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"

	"github.com/home-operations/flate/internal/assert"
)

func TestCheckAPIVersion_ExactGroup(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		version string
		wantErr error
		invalid bool
	}{
		{version: SourceDomain + "/v1"},
		{version: SourceDomain + ".example.com/v1", wantErr: ErrInput},
		{version: "", wantErr: ErrInput},
		{version: SourceDomain + "/", wantErr: ErrInput, invalid: true},
		{version: SourceDomain + "/v1/extra", wantErr: ErrInput, invalid: true},
	} {
		t.Run(tc.version, func(t *testing.T) {
			t.Parallel()
			err := checkAPIVersion(map[string]any{"apiVersion": tc.version}, SourceDomain)
			{
				assert.ErrorIf(
					t,
					!errors.Is(err, tc.wantErr) || (tc.wantErr != nil && !errors.Is(err, ErrFlux)),
					"error = %v, want %v",
					err,
					tc.wantErr,
				)
			}
			_, invalid := errors.AsType[*field.Error](err)
			assert.Equal(t, invalid, tc.invalid)
			if tc.invalid {
				for _, kind := range []string{KindGitRepository, "Widget"} {
					doc := map[string]any{"apiVersion": tc.version, "kind": kind, "metadata": 7, "spec": "bad"}
					_, authoritative, changed, err := ParseDoc(doc, ParseDocOptions{DefaultNamespace: "apps"})
					invalid, ok := errors.AsType[*field.Error](err)
					{
						assert.FatalIf(
							t,
							!ok || invalid.Field != "apiVersion" || !errors.Is(err, ErrInput) ||
								!errors.Is(err, ErrFlux),
							"version error lost precedence or classification: %v",
							err,
						)
					}
					assert.Equal(t, changed, false)
					assert.Diff(t, authoritative, doc)
				}
			}
		})
	}
}
