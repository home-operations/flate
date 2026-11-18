package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/home-operations/flate/pkg/source/cache"

	"github.com/home-operations/flate/pkg/selection"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/orchestrator"
	"github.com/home-operations/flate/internal/output"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/diff"
	"github.com/home-operations/flate/pkg/manifest"
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
			_, collectionErr := collectRendered(&selection.Result{Manifests: []manifest.DocumentBatch{{
				Identities: []manifest.NamedResource{{Kind: manifest.KindConfigMap, Name: "malformed"}},
			}}}, &buildFlags{})
			assert.Equal(t, errors.Is(collectionErr, manifest.ErrInput), true)
			payload := selectedPayload{err: errors.Join(payloadErr, collectionErr)}
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
			assert.Equal(t, errors.Is(got, collectionErr), true)
			assert.Equal(t, errors.Is(got, manifest.ErrInput), true)
			assert.Equal(t, errors.Is(got, manifest.ErrFlux), true)
			if _, reported := errors.AsType[reportedError](got); !reported {
				assert.NoError(t, (output.Encoder{Writer: &stderr}).Errors(got))
			}
			for _, message := range []string{"selected render failed", closeErr.Error(), writeErr.Error(), context.Canceled.Error(), collectionErr.Error()} {
				assert.Equal(t, strings.Count(stderr.String(), message), 1)
			}
		})
	}
}

type lifetimeWriter func([]byte) (int, error)

func (w lifetimeWriter) Write(data []byte) (int, error) { return w(data) }

func TestRunSelected_FinalConsumerLifetime(t *testing.T) {
	t.Parallel()
	for _, stage := range []string{"completed", "payload_error", "report_error", "baseline_error", "close_error"} {
		t.Run(stage, func(t *testing.T) {
			t.Parallel()
			testRunSelectedFinalConsumerLifetime(t, stage)
		})
	}
}

func testRunSelectedFinalConsumerLifetime(t *testing.T, stage string) {
	t.Helper()
	root, cacheDir := writeFixture(t), t.TempDir()
	flags := commonFlags{path: root, cacheDir: cacheDir, concurrency: 2}
	if stage == "baseline_error" {
		flags.base = "absent-revision"
	}
	failure := errors.New("final consumer failed")
	var data fs.FS
	var selected *selectedInvocation
	var diagnostics bytes.Buffer
	reads, reports := 0, 0
	read := func() {
		t.Helper()
		res, err := cache.Sweep(t.Context(), cacheDir, cache.SweepOpts{})
		assert.FatalIf(t, err == nil && len(res.Errors) == 0, "invocation closed before final consumer")
		if data != nil {
			body, readErr := fs.ReadFile(data, "content")
			assert.NoError(t, readErr)
			assert.Equal(t, string(body), "lazy payload")
			reads++
		}
	}
	stderr := lifetimeWriter(func(p []byte) (int, error) {
		if selected == nil || selected.owner.Config != nil {
			read()
		}
		reports++
		if stage == "report_error" {
			return 0, failure
		}
		return diagnostics.Write(p)
	})
	err := runSelected(t.Context(), selectedRequest{
		flags: flags, roots: flags.roots("", runnableKinds...), stdout: stderr, stderr: stderr,
	}, func(inv *selectedInvocation) selectedPayload {
		selected = inv
		if stage == "close_error" {
			closeCache := inv.owner.Config.Cache.Close
			release := reflect.ValueOf(inv.owner).Elem().FieldByName("releaseCache")
			// Private ownership injection MUST stay in the test without widening the production API.
			reflect.NewAt(release.Type(), release.Addr().UnsafePointer()).Elem().Set(reflect.ValueOf(func() error {
				assert.Equal(t, reads >= 4 && reports > 0, true)
				assert.NoError(t, inv.err)
				assert.NoError(t, inv.remainder)
				return errors.Join(closeCache(), failure)
			}))
		}
		var acquireErr error
		data, _, _, acquireErr = inv.owner.Config.Cache.Blob(t.Context(),
			cache.BlobKey{Content: stage}, &cache.Policy{},
			func(_ context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
				_, writeErr := io.WriteString(w, "lazy payload")
				return cache.ProducerResult{}, writeErr
			})
		assert.NoError(t, acquireErr)
		read()
		return selectedPayload{
			rosters: []output.TestReport{{Passed: 1, Matched: 1}},
			prepare: func([]diff.Names) { read() },
			write: func() error {
				read()
				if stage == "payload_error" {
					return failure
				}
				return nil
			},
		}
	})
	switch stage {
	case "completed":
		assert.NoError(t, err)
	case "baseline_error":
		assert.FatalIf(t, err == nil || selected != nil, "baseline failure lost ownership: %v", err)
	default:
		assert.Equal(t, errors.Is(err, failure), true)
	}
	assert.Equal(t, reports > 0, true)
	res, sweepErr := cache.Sweep(t.Context(), cacheDir, cache.SweepOpts{})
	assert.NoError(t, sweepErr)
	assert.Equal(t, len(res.Errors), 0)
	switch stage {
	case "baseline_error":
		return
	case "close_error":
		assert.Equal(t, errors.Is(selected.err, failure), true)
		assert.Equal(t, errors.Is(selected.remainder, failure), true)
		assert.Equal(t, errors.Is(selected.owner.Close(), failure), true)
		if _, reported := errors.AsType[reportedError](err); !reported {
			assert.NoError(t, (output.Encoder{Writer: &diagnostics}).Errors(err))
		}
		assert.Equal(t, strings.Count(diagnostics.String(), failure.Error()), 1)
		_, reported := errors.AsType[reportedError](err)
		assert.Equal(t, reported, true)
	default:
		assert.NoError(t, selected.owner.Close())
	}
	assert.Equal(t, reads >= 4, true)
	_, readErr := fs.ReadFile(data, "content")
	assert.Equal(t, errors.Is(readErr, fs.ErrClosed), true)
}
