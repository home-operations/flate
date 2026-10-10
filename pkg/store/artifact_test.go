package store

import (
	"io/fs"
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
)

type hookFS struct{ open func(string) (fs.File, error) }

func (h *hookFS) Open(name string) (fs.File, error) { return h.open(name) }

func TestSetArtifact_PayloadComparedByKey(t *testing.T) {
	s := New()
	id := manifest.NamedResource{Kind: manifest.KindGitRepository, Namespace: "n", Name: "a"}
	events := 0
	s.AddListener(EventArtifactUpdated, func(_ manifest.NamedResource, _ any) { events++ }, false)
	mk := func(key string) *SourceArtifact {
		return &SourceArtifact{LocalPath: "label", Key: key, FS: &hookFS{open: func(string) (fs.File, error) { return nil, fs.ErrNotExist }}}
	}
	s.SetArtifact(id, mk("pin"))
	s.SetArtifact(id, mk("pin"))
	if events != 1 {
		t.Fatalf("equal Key re-fired the artifact event: %d", events)
	}
	s.SetArtifact(id, mk("other"))
	if events != 2 {
		t.Fatalf("changed Key did not fire: %d", events)
	}
}
