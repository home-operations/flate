package external_test

import (
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func testReadSet(obj manifest.BaseManifest) *store.ReadSet {
	s := store.New()
	s.AddObject(obj)
	reads := new(store.ReadSet)
	reads.Init(s, obj.Named(), nil)
	return reads
}
