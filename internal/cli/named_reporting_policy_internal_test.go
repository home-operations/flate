package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/home-operations/flate/pkg/selection"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
	"github.com/home-operations/flate/pkg/store"
)

func TestReportFailures_NamedReportingPolicy(t *testing.T) {
	t.Parallel()
	for _, named := range []bool{false, true} {
		t.Run(strconv.FormatBool(named), func(t *testing.T) {
			t.Parallel()
			o, err := completedOrchestrator(t)
			assert.NoError(t, err)
			id := testutil.KustomizationID("apps", "failed")
			o.Store().AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
			res := &orchestrator.Result{
				Failed: map[manifest.NamedResource]store.StatusInfo{
					id: {Status: store.StatusFailed, Message: "render failed"},
				},
			}
			query := manifest.NamedResource{Group: id.Group, Kind: id.Kind}
			if named {
				query.Name = id.Name
			}
			projectedResults0, projectionErrors0 := orchestrator.Select(
				[]orchestrator.Rendered{{Orchestrator: o, Result: res}},
				&selection.FailuresError{Message: "raw reconcile failures"},
				[]manifest.NamedResource{query},
				nil,
			)
			selectedResults, selectedErr, remainder := projectedResults0, projectionErrors0.Selected, projectionErrors0.Remaining
			assert.FatalIf(t, len(selectedResults) != 1 || selectedErr == nil, "selection failed: %v", selectedErr)
			var out bytes.Buffer
			extra := fmt.Errorf("reconcile: %w", context.Canceled)
			got := finishSelected(
				&selectedRequest{roots: []manifest.NamedResource{query}, stderr: &out},
				&selectedInvocation{
					groups: []selectedGroup{
						{
							cluster:   ".",
							sides:     selectedResults,
							err:       errors.Join(selectedErr, extra),
							remainder: errors.Join(remainder, extra),
						},
					},
				},
				&selectedPayload{},
			)
			assert.FatalIf(t, !errors.Is(got, context.Canceled), "lost non-resource error: %v", got)
			assert.ErrorIf(
				t,
				!strings.Contains(out.String(), "flate error: reconcile: context canceled"),
				"non-resource error missing from report: %s",
				&out,
			)
			writeErr := errors.New("report write failed")
			got = finishSelected(
				&selectedRequest{roots: []manifest.NamedResource{query}, stderr: failingWriter{err: writeErr}},
				&selectedInvocation{
					groups: []selectedGroup{
						{cluster: ".", sides: selectedResults, err: selectedErr, remainder: remainder},
					},
				},
				&selectedPayload{},
			)
			assert.Equal(t, errors.Is(got, writeErr), true)
		})
	}
}

//nolint:paralleltest // test changes the process environment or working directory.
func TestRun_GetNamed_UnrelatedFailureAndLabels(t *testing.T) {
	preservePersistenceNoticeState(t)
	renderPersistenceDisabledReason = persistenceReason
	for _, tc := range []struct {
		kind, name, label string
		visible           bool
	}{
		{kind: "ks", name: "matching", label: "area=selected", visible: true},
		{kind: "ks", name: "mismatching", label: "area=other"},
		{kind: "ks", name: "missing", label: "absent="},
		{kind: "ks", name: "empty", label: "empty=", visible: true},
		{kind: "hr", name: "matching", label: "area=selected", visible: true},
	} {
		t.Run(tc.kind+"/"+tc.name, func(t *testing.T) {
			root := writeNamedScopeFixture(t)
			file := "cluster/ks.yaml"
			if tc.kind == "hr" {
				file = "cluster/hr.yaml"
			}
			body, err := os.ReadFile(filepath.Join(root, file))
			if err != nil {
				t.Fatal(err)
			}
			body = []byte(
				strings.Replace(string(body), "namespace: ", "labels: {area: selected, empty: ''}, namespace: ", 1),
			)
			testutil.WriteFile(t, root, file, string(body))
			args := []string{
				"get",
				tc.kind,
				"selected",
				"--path",
				filepath.Join(root, "cluster"),
				"--cache-dir",
				t.TempDir(),
				"--concurrency",
				"2",
				"-o",
				"name",
				"-l",
				tc.label,
			}
			runCLIResult := runCLI(t, args...)
			stdout, stderr, code := runCLIResult.stdout, runCLIResult.stderr, runCLIResult.code
			if code != 0 || (stdout != "") != tc.visible || !strings.Contains(stderr, persistenceMessage) ||
				strings.Contains(stderr, "unrelated") ||
				strings.Contains(stderr, "outside") {
				t.Fatalf("healthy named get: exit=%d stdout=%q stderr=%s", code, stdout, stderr)
			}
			want := ""
			if tc.visible {
				want = "selected\n"
			}
			assert.Equal(t, stdout, want)
			appendScopeSpec(t, root, file, "  dependsOn: [{name: missing, namespace: other}]\n")
			runCLIResult2 := runCLI(t, args...)
			stderr, code = runCLIResult2.stderr, runCLIResult2.code
			if code != 1 || !strings.Contains(stderr, "missing") {
				t.Fatalf("label hid named failure: exit=%d stderr=%s", code, stderr)
			}
		})
	}
}
