package store

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestEmission_RejectedRenderReplay(t *testing.T) {
	t.Parallel()
	for _, boundary := range []string{"publication", "checkpoint"} {
		t.Run(boundary, func(t *testing.T) {
			t.Parallel()
			st := New()
			owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
			source := &manifest.GitRepository{Name: "source", Namespace: "apps", URL: "https://first.test/repo"}
			st.AddObjects([]manifest.BaseManifest{owner, source})
			var reads ReadSet
			reads.Init(st, owner.Named(), nil)
			defer reads.Close()
			reads.GetObject(source.Named())
			_, _, attempt := reads.GetEmission(owner.Named())
			child := &manifest.ConfigMap{Name: "output", Namespace: "apps"}
			attempt.Children = []Child{{Obj: child}}
			attempt.Complete, attempt.Fingerprint = true, manifest.Fingerprint("render-input")
			replacement := new(*source)
			replacement.URL = "https://second.test/repo"
			st.AddObject(replacement)
			assert.Equal(t, reads.Valid(), false)
			if boundary == "publication" {
				st.RecordEmitted(owner.Named(), &attempt, nil)
			}
			attempt.Finish()
			assert.Equal(t, st.GetObject(child.Named()) == nil, true)
			assert.Equal(t, attempt.Footprint() == nil, true)
			var fresh ReadSet
			fresh.Init(st, owner.Named(), nil)
			defer fresh.Close()
			fresh.GetObject(source.Named())
			_, _, next := fresh.GetEmission(owner.Named())
			defer next.Finish()
			assert.Equal(t, len(next.Children), 0)
			_, changed := next.Replay(manifest.Fingerprint("different-input"))
			assert.Equal(t, changed, false)
			output, replay := next.Replay(attempt.Fingerprint)
			assert.Equal(t, replay, true)
			assert.Equal(t, len(output.Children), 1)
			next.Children, next.Complete, next.Fingerprint = output.Children, true, output.Fingerprint
			st.RecordEmitted(owner.Named(), &next, nil)
			assert.Equal(t, st.GetObject(child.Named()) == child, true)
			assert.Equal(t, next.Footprint() != nil, true)
		})
	}
}
