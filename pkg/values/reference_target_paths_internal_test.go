package values

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestExpandValueReferences_MalformedTargetPathClassified(t *testing.T) {
	t.Parallel()
	for _, literal := range []bool{false, true} {
		t.Run(fmt.Sprintf("literal=%t", literal), func(t *testing.T) {
			t.Parallel()
			const path = "env[bad].value"
			cm := &manifest.ConfigMap{Name: "extra", Namespace: "default", Data: map[string]any{"value": "replacement"}}
			hr := &manifest.HelmRelease{
				Name:      "demo",
				Namespace: "default",
				Values:    map[string]any{"retained": "yes"},
				ValuesFrom: []manifest.ValuesReference{
					{
						Kind:       manifest.KindConfigMap,
						Name:       cm.Name,
						ValuesKey:  "value",
						TargetPath: path,
						Literal:    literal,
						Optional:   true,
					},
				},
			}
			_, err := ExpandValueReferences(hr, &sliceProvider{ConfigMaps: []*manifest.ConfigMap{cm}}, nil)
			{
				assert.FatalIf(t, !errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) ||
					!errors.Is(err, strconv.ErrSyntax), "target parser identities lost: %v", err)
			}
			cause, ok := errors.AsType[*strconv.NumError](err)
			{
				assert.FatalIf(t, !ok || cause.Num != "bad", "original index parser cause lost: %v", err)
			}
			for _, detail := range []string{`targetPath "` + path + `"`} {
				{
					assert.ErrorIf(t, !strings.Contains(err.Error(), detail), "error lacks %q: %v", detail, err)
				}
			}
			assert.Diff(t, hr.Values, map[string]any{"retained": "yes"})
		})
	}
}
