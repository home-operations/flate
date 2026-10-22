package store

import (
	"encoding/json"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestStore_ApplyingStatus(t *testing.T) {
	t.Parallel()
	s := New()
	obj := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
	s.AddObject(obj)
	var events []StatusInfo
	s.AddListener(EventStatusUpdated, func(_ manifest.NamedResource, payload any) {
		events = append(events, payload.(StatusInfo))
	}, false)
	s.SetCondition(obj.Named(), readyCondition(StatusPending, "", "applying 0 objects"))
	s.UpdateStatus(obj.Named(), ApplyingStatus(0))
	first, _ := s.GetStatus(obj.Named())
	assert.Equal(t, first, ApplyingStatus(0))
	assert.Equal(t, len(events), 1)
	_, conditions := s.Snapshot(obj.Named(), true)
	assert.Equal(t, conditions.Conditions[0].Message, "applying 0 objects")
	s.SetCondition(obj.Named(), conditions.Conditions[0])
	assert.Equal(t, len(events), 1)
	s.UpdateStatus(obj.Named(), ApplyingStatus(1234))
	current, _ := s.GetStatus(obj.Named())
	assert.Equal(t, current.String(), "applying 1234 objects")
	assert.Equal(t, len(events), 2)
	assert.Equal(t, events[0].String(), "applying 0 objects")
	assert.Equal(t, first.String(), "applying 0 objects")
	assert.Equal(t, conditions.Conditions[0].Message, "applying 0 objects")
	_, changed := s.Snapshot(obj.Named(), true)
	assert.Equal(t, changed.Conditions[0].Message, "applying 1234 objects")
	encoded, err := json.Marshal(current)
	assert.NoError(t, err)
	var decoded struct{ Message string }
	assert.NoError(t, json.Unmarshal(encoded, &decoded))
	assert.Equal(t, decoded.Message, "applying 1234 objects")
	healthy := readyCondition(StatusReady, "", "healthy")
	healthy.Type = ConditionHealthy
	s.SetCondition(obj.Named(), healthy)
	afterHealthy, _ := s.GetStatus(obj.Named())
	assert.Equal(t, afterHealthy, current)
	var replay StatusInfo
	u := s.AddListener(EventStatusUpdated, func(_ manifest.NamedResource, payload any) {
		replay = payload.(StatusInfo)
	}, true)
	u()
	assert.Equal(t, replay, current)
	s.UpdateStatus(obj.Named(), StatusInfo{Status: StatusReady, Reason: "", Message: "done"})
	assert.Equal(t, current.String(), "applying 1234 objects")
	assert.Equal(t, events[len(events)-1].String(), "done")
}
