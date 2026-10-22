package depwait

import (
	"testing"

	"github.com/home-operations/flate/pkg/schedule"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
)

func TestAvailable_OptionalMaterializerAbsence(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                                                           string
		status                                                         store.Status
		level                                                          manifest.DrainLevel
		optional, allowed, generated, offline, private, data, producer bool
		want                                                           schedule.Kind
		omit                                                           bool
	}{
		{name: "optional absent", optional: true, want: schedule.Terminal, omit: true},
		{name: "optional pending", optional: true, producer: true, status: store.StatusPending, want: schedule.Blocked},
		{name: "optional pending cascade", optional: true, producer: true, status: store.StatusPending, level: manifest.DrainCascade, want: schedule.Blocked},
		{name: "optional exhausted", optional: true, producer: true, status: store.StatusPending, level: manifest.DrainForce, want: schedule.Terminal, omit: true},
		{name: "optional failed", optional: true, producer: true, status: store.StatusFailed, want: schedule.Terminal, omit: true},
		{name: "optional finished absent", optional: true, producer: true, status: store.StatusReady, want: schedule.Terminal, omit: true},
		{name: "required failed", producer: true, status: store.StatusFailed, want: schedule.DependencyFailed},
		{name: "required pending", producer: true, status: store.StatusPending, want: schedule.Blocked},
		{name: "required available", producer: true, status: store.StatusReady, data: true, want: schedule.Terminal},
		{name: "failed materializer with available data", producer: true, status: store.StatusFailed, data: true, want: schedule.Terminal},
		{name: "generated absent", generated: true, want: schedule.Terminal, omit: true},
		{name: "required unknown", want: schedule.Blocked},
		{name: "required unknown cascade", level: manifest.DrainCascade, want: schedule.DependencyFailed},
		{name: "allowed unknown", allowed: true, want: schedule.Terminal, omit: true},
		{name: "allowed settled absence", allowed: true, level: manifest.DrainCascade, want: schedule.Terminal, omit: true},
		{name: "private absence", private: true, want: schedule.Terminal, omit: true},
		{name: "optional file waiting", optional: true, offline: true, want: schedule.Blocked},
		{name: "optional file absent", optional: true, offline: true, level: manifest.DrainCascade, want: schedule.Terminal, omit: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := store.New()
			owner := testutil.KustomizationID("apps", "producer")
			input := values.Input{Generated: tc.generated, Offline: tc.offline, Private: tc.private}
			if tc.producer {
				s.AddObject(&manifest.Kustomization{Name: owner.Name, Namespace: owner.Namespace})
				s.UpdateStatus(owner, store.StatusInfo{Status: tc.status, Reason: "", Message: "materializer failed"})
				input.Materializers = []manifest.NamedResource{owner}
			}
			id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings"}
			if tc.data {
				input.Data = &manifest.ConfigMap{Name: id.Name, Namespace: id.Namespace, Data: map[string]any{"VALUE": "present"}}
				s.AddObject(input.Data.(*manifest.ConfigMap))
			}
			var reads store.ReadSet
			parent := testutil.KustomizationID("apps", "reader")
			reads.Init(s, parent, nil)
			w := Waiter{Store: &reads, Parent: parent}
			got := w.Available(t.Context(), DataDependency{NamedResource: id, Input: input, Optional: tc.optional, AllowMissing: tc.allowed}, tc.level)
			if got.Kind != tc.want || got.Omit != tc.omit {
				t.Fatalf("availability=%+v, want kind=%v omit=%t", got, tc.want, tc.omit)
			}
			if tc.want == schedule.Blocked && len(got.Blocked) != 1 {
				t.Fatalf("blocked inputs=%v", got.Blocked)
			}
			for required := range reads.Required() {
				t.Fatalf("availability made %s a required failure cause", required)
			}
			if got.Kind == schedule.DependencyFailed && got.Message != "dependency not found" {
				t.Fatalf("materializer failure became target cause: %+v", got)
			}
		})
	}
}

func TestAvailable_RequiredMaterializerCycle(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                            string
		transitive, optional, unrelated bool
	}{
		{name: "direct"}, {name: "transitive", transitive: true},
		{name: "optional", optional: true}, {name: "unrelated", unrelated: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			parent := testutil.KustomizationID("apps", "consumer")
			owner := testutil.KustomizationID("apps", "producer")
			cause := parent
			if tc.transitive {
				cause = testutil.KustomizationID("apps", "middle")
			}
			if tc.unrelated {
				cause = testutil.KustomizationID("apps", "other")
			}
			st.AddObject(&manifest.Kustomization{Name: owner.Name, Namespace: owner.Namespace})
			st.UpdateFailure(owner, &manifest.DependencyFailedError{Parent: owner, Failed: []manifest.NamedResource{cause}})
			if tc.transitive {
				st.UpdateFailure(cause, &manifest.DependencyFailedError{Parent: cause, Failed: []manifest.NamedResource{parent}})
			}
			id := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "settings", Namespace: "apps"}
			data := &manifest.ConfigMap{Name: id.Name, Namespace: id.Namespace, Data: map[string]any{"VALUE": "raw"}}
			var reads store.ReadSet
			reads.Init(st, parent, nil)
			w := Waiter{Store: &reads, Parent: parent}
			got := w.Available(t.Context(), DataDependency{NamedResource: id, Optional: tc.optional,
				Input: values.Input{Data: data, Materializers: []manifest.NamedResource{owner}}}, manifest.DrainForce)
			want := schedule.DependencyFailed
			if tc.optional || tc.unrelated {
				want = schedule.Terminal
			}
			if got.Kind != want || (len(got.Failed) != 0) != (want == schedule.DependencyFailed) {
				t.Fatalf("availability=%+v, want %v", got, want)
			}
		})
	}
}
