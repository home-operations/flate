package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/home-operations/flate/pkg/selection"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/output"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
	"github.com/home-operations/flate/pkg/store"
)

func TestRunSelected_RemainderAndPayloadErrors(t *testing.T) {
	t.Parallel()
	for _, test := range []bool{false, true} {
		t.Run(strconv.FormatBool(test), func(t *testing.T) {
			t.Parallel()
			o, err := completedOrchestrator(t)
			assert.NoError(t, err)
			id := testutil.KustomizationID("apps", "selected")
			o.Store().AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
			res := &orchestrator.Result{
				Failed: map[manifest.NamedResource]store.StatusInfo{
					id: {Status: store.StatusFailed, Message: "selected render failed"},
				},
			}
			resourceErr := &selection.FailuresError{Message: "aggregate render failure"}
			closeErr, writeErr := errors.New("shared cleanup failed"), errors.New("payload write failed")
			runs := []orchestrator.Rendered{{Orchestrator: o, Result: res, Err: resourceErr}}
			roots := []manifest.NamedResource{id}
			projectedResults0, projectionErrors0 := orchestrator.Select(
				runs,
				errors.Join(
					fmt.Errorf("head tree: %w", errors.Join(resourceErr, context.Canceled)),
					fmt.Errorf("cache close: %w", closeErr),
				),
				roots,
				nil,
			)
			sides, selectedErr, remainder := projectedResults0, projectionErrors0.Selected, projectionErrors0.Remaining
			assert.FatalIf(t, len(sides) != 1 || selectedErr == nil, "completed run selection: %v", selectedErr)
			var stdout, stderr bytes.Buffer
			_, payloadErr := failingWriter{err: writeErr}.Write([]byte("partial"))
			payload := selectedPayload{err: payloadErr}
			if test {
				payload.rosters = []output.TestReport{
					{Cases: []output.Case{{ID: id, Outcome: output.OutcomePassed}}, Passed: 1, Matched: 1},
				}
			}
			got := finishSelected(
				&selectedRequest{roots: roots, stdout: &stdout, stderr: &stderr, test: test},
				&selectedInvocation{
					groups: []selectedGroup{{cluster: ".", sides: sides, err: selectedErr, remainder: remainder}},
				},
				&payload,
			)
			assert.FatalIf(
				t,
				!errors.Is(got, closeErr) || !errors.Is(got, writeErr) || !errors.Is(got, context.Canceled) ||
					!errors.Is(got, selectedErr),
				"lost selected, cleanup or payload error: %v",
				got,
			)
			if _, reported := errors.AsType[reportedError](got); !reported {
				assert.NoError(t, (output.Encoder{Writer: &stderr}).Errors(got))
			}
			for _, message := range []string{"selected render failed", closeErr.Error(), writeErr.Error(), context.Canceled.Error()} {
				assert.Equal(t, strings.Count(stderr.String(), message), 1)
			}
		})
	}
}
