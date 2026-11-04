package values

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

//nolint:paralleltest // The test calls a helper that changes process-global state.
//nolint:paralleltest // This test changes process-global state.
func TestExpandPostBuildSubstituteReference_DiagnosticLogContext(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	owner := manifest.NamedResource{
		Group:     "fluxcd.controlplane.io",
		Kind:      "FluxInstance",
		Namespace: "flux-system",
		Name:      "sync",
	}
	entry := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "clusters/test", owner).WithNamespace("sources").(*manifest.Kustomization)
	entry.PostBuild = &kustomizev1.PostBuild{
		SubstituteFrom: []manifest.SubstituteReference{
			{Kind: "Unsupported", Name: "ignored"},
			{Kind: manifest.KindConfigMap, Name: "missing"},
		},
	}
	{
		err := ExpandPostBuildSubstituteReference(entry, NewStoreProvider(store.New(), true))
		assert.NoError(t, err)
	}
	count := 0
	for line := range bytes.SplitSeq(bytes.TrimSpace(logs.Bytes()), []byte("\n")) {
		var record struct {
			ID string `json:"id"`
		}
		{
			err := json.Unmarshal(line, &record)
			assert.NoError(t, err)
		}
		{
			assert.FatalIf(
				t,
				record.ID != manifest.DiagnosticFor(entry.Named(), entry).String(),
				"substitution debug identity=%q",
				record.ID,
			)
		}
		count++
	}
	{
		assert.FatalIf(t, count != 2, "debug records=%d", count)
	}
}
