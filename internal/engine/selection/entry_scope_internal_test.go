package selection

import (
	"maps"
	"testing"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestEntryScope_Same(t *testing.T) {
	t.Parallel()
	st := store.New()
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
	expiredSupport := new(store.ReadSet)
	expiredSupport.Init(st, owner.Named(), nil)
	expiredSupport.GetDeclaration(owner.Named())
	t.Cleanup(expiredSupport.Retire)
	st.AddObject(owner)

	scope := EntryScope{
		Preparation: new(store.ReadSet),
		Support:     new(store.ReadSet),
		Source:      new(store.SourceArtifact),
		Children:    map[manifest.NamedResource]bool{testutil.KustomizationID("apps", "inside"): true},
	}
	differentPreparation := scope
	differentPreparation.Preparation = new(store.ReadSet)
	differentSource := scope
	differentSource.Source = new(store.SourceArtifact)
	differentChildren := scope
	differentChildren.Children = map[manifest.NamedResource]bool{testutil.KustomizationID("apps", "outside"): true}
	expired := scope
	expired.Support = expiredSupport
	equivalent := scope
	equivalent.Children = maps.Clone(scope.Children)
	assert.Equal(t, expired.Current(), false)

	for _, tc := range []struct {
		name string
		prev *EntryScope
		next *EntryScope
		want bool
	}{
		{name: "nil receiver", next: &scope},
		{name: "nil argument", prev: &scope},
		{name: "different preparation", prev: &scope, next: &differentPreparation},
		{name: "different source", prev: &scope, next: &differentSource},
		{name: "different children", prev: &scope, next: &differentChildren},
		{name: "expired support", prev: &expired, next: &scope},
		{name: "equivalent current scopes", prev: &scope, next: &equivalent, want: true},
		{name: "equivalent pending scopes", prev: &EntryScope{}, next: &EntryScope{}, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.prev.Same(tc.next), tc.want)
		})
	}
}
