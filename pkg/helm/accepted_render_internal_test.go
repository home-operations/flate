package helm

import (
	"io/fs"
	"sync/atomic"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

const acceptedRenderAnnotation = "flate.accepted-render"

type historyCountingFS struct {
	fs.FS

	opens atomic.Int64
}

func (f *historyCountingFS) Open(name string) (fs.File, error) {
	f.opens.Add(1)
	return f.FS.Open(name)
}

type historyCountingSources struct {
	sourceReader

	calls atomic.Int64
}

//nolint:ireturn // The upstream interface contract requires this return type.
func (s *historyCountingSources) GetArtifact(id manifest.NamedResource, read store.ArtifactRead) store.Artifact {
	s.calls.Add(1)
	return s.sourceReader.GetArtifact(id, read)
}
