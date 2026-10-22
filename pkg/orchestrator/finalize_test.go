package orchestrator

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestJoinTreeErrors_Sides(t *testing.T) {
	t.Parallel()
	base, head := errors.New("base failure"), errors.New("head failure")
	for _, tt := range []struct {
		name       string
		base, head error
		message    string
	}{
		{name: "neither"},
		{name: "base", base: base, message: "base tree: base failure"},
		{name: "head", head: head, message: "head tree: head failure"},
		{name: "both", base: base, head: head,
			message: "both trees had reconcile failures\nbase tree: base failure\nhead tree: head failure"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := joinTreeErrors(tt.base, tt.head)
			if tt.message == "" {
				assert.Equal(t, got, nil)
				return
			}
			if got == nil {
				t.Fatal("missing tree failure")
			}
			assert.Equal(t, got.Error(), tt.message)
			assert.Equal(t, errors.Is(got, base), tt.base != nil)
			assert.Equal(t, errors.Is(got, head), tt.head != nil)
		})
	}
}

func TestLogResourceFailures_Deterministic(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	a, b := testutil.KustomizationID("apps", "a"), testutil.KustomizationID("apps", "b")
	hr := testutil.HelmReleaseID("apps", "z")
	o := &Orchestrator{index: &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		hr: {File: "release.yaml"},
		a:  {File: "a.yaml"},
	}}}
	type event struct {
		Message string `json:"msg"`
		ID      string `json:"id"`
		Reason  string `json:"reason"`
		File    string `json:"file"`
	}
	want := []event{
		{Message: "orchestrator: resource failed", ID: hr.String(), Reason: "invalid input", File: "release.yaml"},
		{Message: "orchestrator: resource failed", ID: a.String(), Reason: "invalid input", File: "a.yaml"},
		{Message: "orchestrator: resource failed", ID: b.String(), Reason: "invalid input"},
	}
	for _, ids := range [][]manifest.NamedResource{{b, a, hr}, {hr, a, b}} {
		logs.Reset()
		failed := make(map[manifest.NamedResource]store.StatusInfo)
		for _, id := range ids {
			failed[id] = store.StatusInfo{Status: store.StatusFailed, Message: fmt.Errorf("%w: invalid input", manifest.ErrInput).Error()}
		}
		o.logResourceFailures(failed)
		var got []event
		for line := range bytes.Lines(logs.Bytes()) {
			var e event
			assert.NoError(t, json.Unmarshal(line, &e))
			got = append(got, e)
		}
		assert.Diff(t, got, want)
		assert.Equal(t, failed[a].String(), fmt.Errorf("%w: invalid input", manifest.ErrInput).Error())
	}
}
