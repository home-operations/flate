package kustomize

import (
	"io/fs"
	"path/filepath"
	"testing"
)

// fakeDisk is a minimal diskReader over in-memory maps keyed by absolute path.
type fakeDisk struct {
	files map[string][]byte
	dirs  map[string][]string
}

func (f fakeDisk) ReadFile(p string) ([]byte, error) {
	b, ok := f.files[p]
	if !ok {
		return nil, fs.ErrNotExist
	}
	return b, nil
}

func (f fakeDisk) ReadDir(p string) ([]string, error) {
	e, ok := f.dirs[p]
	if !ok {
		return nil, fs.ErrNotExist
	}
	return e, nil
}

func (f fakeDisk) Exists(p string) bool {
	if _, ok := f.files[p]; ok {
		return true
	}
	_, ok := f.dirs[p]
	return ok
}

func (f fakeDisk) IsDir(p string) bool { _, ok := f.dirs[p]; return ok }

// recordAgainst replays a build's reads against disk into a fresh readSet — the
// recorder side, mirroring what the instrumented overlay does during a render.
func recordAgainst(root string, disk fakeDisk, files, dirs, probed []string) readSetSnapshot {
	rs := newReadSet(root)
	for _, rel := range files {
		abs := filepath.Join(root, rel)
		b, err := disk.ReadFile(abs)
		rs.recordFile(abs, b, err)
	}
	for _, rel := range dirs {
		abs := filepath.Join(root, rel)
		e, _ := disk.ReadDir(abs)
		rs.recordDir(abs, e)
	}
	for _, rel := range probed {
		abs := filepath.Join(root, rel)
		rs.recordNode(abs, diskKind(disk.Exists(abs), disk.IsDir(abs)))
	}
	return rs.snapshot()
}

func TestReadSet_HitAndInvalidation(t *testing.T) {
	const root = "/src/app"
	base := func() fakeDisk {
		return fakeDisk{
			files: map[string][]byte{
				"/src/app/kustomization.yaml": []byte("resources: [a.yaml]\n"),
				"/src/app/a.yaml":             []byte("kind: ConfigMap\n"),
				"/src/app/b.yaml":             []byte("kind: Secret\n"),
			},
			dirs: map[string][]string{"/src/app": {"kustomization.yaml", "a.yaml", "b.yaml"}},
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
		if !snap.stillValid(base(), root) {
			t.Error("identical tree must validate (cache hit)")
		}
	})

	t.Run("editing a recorded file misses", func(t *testing.T) {
		d := base()
		d.files["/src/app/a.yaml"] = []byte("kind: ConfigMap\ndata: {x: y}\n")
		if snap.stillValid(d, root) {
			t.Error("an edit to a recorded input must invalidate (cache miss)")
		}
	})

	t.Run("editing an UNREAD file stays a hit (granularity)", func(t *testing.T) {
		d := base()
		d.files["/src/app/b.yaml"] = []byte("kind: Secret\nstringData: {k: v}\n")
		if !snap.stillValid(d, root) {
			t.Error("b.yaml was never read by this build; editing it must NOT invalidate")
		}
	})

	t.Run("adding a file to the listed dir misses", func(t *testing.T) {
		d := base()
		d.dirs["/src/app"] = []string{"kustomization.yaml", "a.yaml", "b.yaml", "c.yaml"}
		d.files["/src/app/c.yaml"] = []byte("kind: Service\n")
		if snap.stillValid(d, root) {
			t.Error("a new file in the auto-scanned dir must invalidate")
		}
	})

	t.Run("deleting a recorded file misses", func(t *testing.T) {
		d := base()
		delete(d.files, "/src/app/a.yaml")
		if snap.stillValid(d, root) {
			t.Error("deleting a recorded input must invalidate")
		}
	})

	t.Run("a probed-absent component appearing misses", func(t *testing.T) {
		d := base()
		d.dirs["/src/app/comp"] = []string{"kustomization.yaml"}
		if snap.stillValid(d, root) {
			t.Error("an optional component that materialized must invalidate")
		}
	})
}
