package kustomize

import (
	"io/fs"
	"path/filepath"
	"testing"
	"testing/fstest"
)

// recordAgainst replays a build's reads against disk into a fresh readSet — the
// recorder side, mirroring what the instrumented overlay does during a render.
func recordAgainst(root string, lower fs.FS, files, dirs, probed []string) readSetSnapshot {
	rs := newReadSet(root)
	for _, rel := range files {
		abs := filepath.Join(root, rel)
		b, err := fs.ReadFile(lower, rel)
		rs.recordFile(abs, b, err)
	}
	for _, rel := range dirs {
		abs := filepath.Join(root, rel)
		e, _ := fs.ReadDir(lower, rel)
		rs.recordDir(abs, entryNames(e))
	}
	for _, rel := range probed {
		abs := filepath.Join(root, rel)
		info, err := fs.Stat(lower, rel)
		rs.recordNode(abs, diskKind(err == nil, err == nil && info.IsDir()))
	}
	return rs.snapshot()
}

func TestReadSet_HitAndInvalidation(t *testing.T) {
	const root = "/src/app"
	base := func() fstest.MapFS {
		return fstest.MapFS{
			"kustomization.yaml": {Data: []byte("resources: [a.yaml]\n")},
			"a.yaml":             {Data: []byte("kind: ConfigMap\n")},
			"b.yaml":             {Data: []byte("kind: Secret\n")},
		}
	}
	// A build that read the kustomization + a.yaml, listed the dir, and probed a
	// (missing) optional component.
	snap := recordAgainst(root, base(),
		[]string{"kustomization.yaml", "a.yaml"},
		[]string{"."},
		[]string{"comp"},
	)

	t.Run("unchanged tree is a hit", func(t *testing.T) {
		if !snap.stillValidFS(base()) {
			t.Error("identical tree must validate (cache hit)")
		}
	})

	t.Run("editing a recorded file misses", func(t *testing.T) {
		d := base()
		d["a.yaml"] = &fstest.MapFile{Data: []byte("kind: ConfigMap\ndata: {x: y}\n")}
		if snap.stillValidFS(d) {
			t.Error("an edit to a recorded input must invalidate (cache miss)")
		}
	})

	t.Run("editing an UNREAD file stays a hit (granularity)", func(t *testing.T) {
		d := base()
		d["b.yaml"] = &fstest.MapFile{Data: []byte("kind: Secret\nstringData: {k: v}\n")}
		if !snap.stillValidFS(d) {
			t.Error("b.yaml was never read by this build; editing it must NOT invalidate")
		}
	})

	t.Run("adding a file to the listed dir misses", func(t *testing.T) {
		d := base()
		d["c.yaml"] = &fstest.MapFile{Data: []byte("kind: Service\n")}
		if snap.stillValidFS(d) {
			t.Error("a new file in the auto-scanned dir must invalidate")
		}
	})

	t.Run("deleting a recorded file misses", func(t *testing.T) {
		d := base()
		delete(d, "a.yaml")
		if snap.stillValidFS(d) {
			t.Error("deleting a recorded input must invalidate")
		}
	})

	t.Run("a probed-absent component appearing misses", func(t *testing.T) {
		d := base()
		d["comp/kustomization.yaml"] = &fstest.MapFile{}
		if snap.stillValidFS(d) {
			t.Error("an optional component that materialized must invalidate")
		}
	})
}
