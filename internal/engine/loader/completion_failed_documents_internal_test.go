package loader

import (
	"errors"
	"io"
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/testutil/assert"
)

func TestCompleteDeclarations_ReleasesFailedDocuments(t *testing.T) {
	t.Parallel()
	cause := errors.New("document read failed")
	reads := 0
	input := &evidenceFailureFS{FS: fstest.MapFS{"input.yaml": {}}, read: func(p []byte) (int, error) {
		reads++
		if reads == 1 {
			return copy(p, "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: partial}\n"), cause
		}
		return 0, io.EOF
	}}
	l, _ := completionFixture(t, input)
	l.documents.mountSources(l.Store, nil, l.Index)
	file := filepath.Join(l.SourceRoot, "input.yaml")
	_, err := l.documents.completeFile(t.Context(), file)
	assert.Equal(t, errors.Is(err, cause), true)
	objects := l.documents.cache[file].objects
	assert.Equal(t, len(objects) > 0, true)
	for i := range objects {
		assert.Equal(t, len(objects[i].Doc), 0)
		assert.Equal(t, len(objects[i].outcome.Document), 0)
	}
	assert.Equal(t, input.closed.Load(), int32(1))
}
