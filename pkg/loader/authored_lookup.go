package loader

import (
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func (d *document) indexAuthoredObjects() {
	d.lookup = make(map[manifest.NamedResource]store.Child, len(d.objects))
	{
		childValues2 := d.objects
		for childIndex2 := range childValues2 {
			child := childValues2[childIndex2]
			obj := child.Obj
			if obj == nil || child.outcome.Err != nil {
				continue
			}
			if d.lookup[obj.Named()].Obj == nil {
				d.lookup[obj.Named()] = child.Child
			}
		}
	}
}
