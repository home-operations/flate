package cli

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/selection"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRun_ClusterReport_KindCollisionInventory(t *testing.T) {
	t.Parallel()
	widget := manifest.NamedResource{Group: "one.example", Kind: "Widget", Namespace: "ns", Name: "same"}
	other := widget
	other.Group = "two.example"
	gadget := manifest.NamedResource{Group: "one.example", Kind: "Gadget", Namespace: "ns", Name: "root"}
	victims := make([]manifest.NamedResource, 7)
	for i := range victims {
		victims[i] = manifest.NamedResource{Kind: "Leaf", Namespace: "ns", Name: fmt.Sprintf("leaf%d", i)}
	}
	victims[6] = manifest.NamedResource{Group: "hidden.example", Kind: "Gadget", Name: "hidden"}
	warning := manifest.Warning{
		Resource: other, Category: "Inventory", Count: 3,
		Message: "Widget.one.example/ns/opaque stays unchanged", Detail: []string{"Gadget.hidden.example/ns/reference"},
	}
	alpha := &selection.Result{
		Failed: map[manifest.NamedResource]store.StatusInfo{
			widget: {
				Status:  store.StatusFailed,
				Message: "widget failed",
			},
			gadget: {Status: store.StatusFailed, Message: "gadget failed"},
		},
		Causes: map[manifest.NamedResource][]manifest.NamedResource{widget: nil, gadget: victims},
	}
	omega := &selection.Result{Warnings: []manifest.Warning{warning}}
	var first string
	for run := range 2 {
		var stderr bytes.Buffer
		assert.NoError(t, finishSelected(
			&selectedRequest{stderr: &stderr, stdout: io.Discard},
			&selectedInvocation{
				groups: []selectedGroup{
					{cluster: "alpha", sides: []*selection.Result{alpha}},
					{cluster: "omega", sides: []*selection.Result{omega}},
				},
			},
			&selectedPayload{},
		))
		out := stderr.String()
		for _, want := range []string{
			"Widget.one.example  alpha: ns/same", "Widget.two.example omega: ns/same: " + warning.Message,
			"Gadget  alpha: ns/root", "alpha: ns/leaf5 +1 more", warning.Detail[0], "(×3)",
		} {
			assert.FatalIf(t, !strings.Contains(out, want), "missing %q: %s", want, out)
		}
		assert.FatalIf(t, strings.Contains(out, "Gadget.one.example") || strings.Contains(out, "hidden.example  ") ||
			strings.Contains(out, "Cluster:"), "hidden subjects changed the inventory: %s", out)
		if run == 0 {
			first = out
		} else {
			assert.Equal(t, out, first)
		}
	}
	assert.Diff(t, omega.Warnings, []manifest.Warning{warning})
}
