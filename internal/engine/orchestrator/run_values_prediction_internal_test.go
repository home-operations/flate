package orchestrator

import (
	"strconv"
	"testing"

	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/manifest"
)

//nolint:paralleltest // The orchestrator helper changes the process-global egress guard.
func TestCollectResult_HelmValuesKey(t *testing.T) {
	for _, tc := range []struct {
		name, key         string
		optional, changed bool
		want              store.Outcome
	}{
		{name: "missing required key", key: "other.yaml", want: store.OutcomeFailed},
		{name: "readable required key", key: "values.yaml", want: store.OutcomePassed},
		{name: "missing optional key", key: "other.yaml", optional: true, want: store.OutcomePassed},
		{name: "changed-only missing required key", key: "other.yaml", changed: true, want: store.OutcomeFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			testutil.WriteFile(t, root, "kustomization.yaml", "resources: [release.yaml]\n")
			testutil.WriteFile(t, root, "release.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: reader, namespace: flux-system}
spec:
  chart:
    spec:
      chart: chart
      sourceRef: {kind: GitRepository, name: flux-system}
  valuesFrom:
    - {kind: ConfigMap, name: input, valuesKey: values.yaml, optional: `+strconv.FormatBool(tc.optional)+`}
`)
			testutil.WriteFile(t, root, "chart/Chart.yaml", "apiVersion: v2\nname: fixture\nversion: 1.0.0\n")
			testutil.WriteFile(t, root, "chart/templates/output.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: output}
data: {greeting: {{ .Values.greeting | quote }}}
`)
			input := &manifest.ConfigMap{
				Name: "input", Namespace: "flux-system", Data: map[string]any{tc.key: "greeting: exact"},
			}
			newRun := func() *Orchestrator {
				t.Helper()
				tree := Tree{Path: root, RepoRoot: root}
				if tc.changed {
					tree.ExternalChanges = detectChanges(t, []string{"release.yaml"}, []string{"release.yaml"})
				}
				o, err := newTestOrchestrator(t, tree, Config{
					CacheDir: t.TempDir(), Concurrency: 2, Selection: selection.Request{Changed: tc.changed},
				})
				assert.NoError(t, err)
				assert.NoError(t, o.bootstrap(t.Context()))
				o.store.AddObject(input)
				return o
			}
			reader := testutil.HelmReleaseID("flux-system", "reader")
			execution := newRun()
			reconcileEntryForTest(t, execution)
			executed := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{
				Tasks: execution.tasks, Store: execution.store, ID: reader, ReconcileNode: execution.hrc.ReconcileNode,
			})
			assert.FatalIf(t, executed.Outcome() != tc.want, "execution status = %v, want %s", executed, tc.want)
			if tc.want == store.OutcomeFailed {
				const want = `valuesFrom ConfigMap flux-system/input key "values.yaml" not found`
				assert.FatalIf(t, executed.Message != want, "execution failure = %q, want %q", executed.Message, want)
			}

			prediction := newRun()
			entry := prediction.entryBinding.Producer
			assert.FatalIf(t, !entry.ReportHidden(), "prediction requires a synthetic entry, got %s", entry)
			parent := manifest.NewEntryKustomization(manifest.BootstrapSourceID, ".", manifest.NamedResource{})
			assert.Equal(t, parent.Named(), entry)
			prediction.store.AddObject(parent)
			prediction.store.UpdateStatus(entry, store.StatusInfo{Status: store.StatusFailed, Message: "entry failed"})
			assert.Equal(t, prediction.store.FailedResources()[entry].Outcome(), store.OutcomeFailed)
			warnings := prediction.store.Warnings()
			result, err := prediction.collectResult(t.Context(), nil)
			assert.NoError(t, err)
			t.Cleanup(result.Close)
			_, hasStatus := prediction.store.GetStatus(reader)
			assert.Equal(t, hasStatus, false)
			assert.Equal(t, prediction.store.HasInventory(reader), false)
			assert.Diff(t, prediction.store.Warnings(), warnings)
			predicted, failed := result.Failed[reader]
			if tc.want == store.OutcomePassed {
				assert.FatalIf(t, failed, "readable key predicted a failure: %v", predicted)
				return
			}
			assert.FatalIf(t, !failed, "result collection missed the execution failure: %s", executed.Message)
			assert.Equal(t, predicted.Outcome(), executed.Outcome())
			assert.Equal(t, predicted.Message, executed.Message)
			assert.Diff(t, result.Blocked[reader], executed.BlockedBy())
		})
	}
}
