package helm

import (
	"context"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/source/cache"
)

func testTemplateCacheSameKeyReplacements(
	t *testing.T,
	inputs *templateCacheIntegrationInputs,
	alternateKey *cache.BlobKey,
	alternate map[string]any,
) {
	t.Helper()
	for _, replacement := range []string{"changed output", "corrupt payload", "{", `{"Output":"bWlzc2luZyBhbmFseXNpcw=="}`} {
		replacementDocs := []map[string]any{{"witness": replacement}}
		_, _, _, blobErr := (*inputs.cli).cache.Blob(
			t.Context(),
			(*alternateKey),
			&cache.Policy{},
			func(_ context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
				var errResult error
				switch replacement {
				case "corrupt payload":
					errResult = writeTestRecord(t.Context(),
						w,
						(*alternateKey),
						&renderRecord{Output: []byte("corrupt"), Analysis: inputs.loaded.analyze()},
					)
				case "changed output":
					errResult = writeTestRecord(t.Context(),
						w,
						(*alternateKey),
						&renderRecord{Output: encodedRender(t, replacementDocs), Analysis: inputs.loaded.analyze()},
					)
				default:
					_, errResult = io.WriteString(w, replacement)
				}
				return cache.ProducerResult{}, errResult
			},
		)
		assert.NoError(t, blobErr)

		out, receipt22, blobErr := testutil.RenderMaps(
			(*inputs.cli).Template(
				t.Context(),
				newHR(),
				alternate,
				RenderOptions{Options: Options{}, Sources: (*inputs.cliSources)},
			),
		)
		if blobErr == nil {
			{
				recordErr := (*inputs.cli).RecordRender(
					t.Context(),
					RenderAcceptance{
						Owner:   newHR().Named(),
						Receipt: receipt22,
						Records: emptyRenderInventory,
					},
				)
				assert.NoError(t, recordErr)
			}
		}

		switch replacement {
		case "changed output":
			assert.NoError(t, blobErr)
			assert.FatalIf(
				t,
				!reflect.DeepEqual(out, replacementDocs),
				"%s",
				"same-key replacement served stale output",
			)
		case "corrupt payload":
			assert.FatalIf(
				t,
				blobErr == nil || !strings.Contains(blobErr.Error(), "cache miss action") || len(out) != 0,
				"corrupt payload: %v, %v",
				out,
				blobErr,
			)
		case "{":
			assert.FatalIf(
				t,
				blobErr == nil || !strings.Contains(blobErr.Error(), "cache miss action") || len(out) != 0,
				"malformed replacement was concealed: %v",
				blobErr,
			)
		default:
			assert.FatalIf(
				t,
				blobErr == nil || !strings.Contains(blobErr.Error(), "cache miss action") || len(out) != 0,
				"missing analysis was accepted: %v",
				blobErr,
			)
		}
	}
}
