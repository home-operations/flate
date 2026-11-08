package loader

import (
	"encoding/json"
	"errors"
	"io/fs"
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestDocumentReader_DeferredCatalogErrorRetainsFailure(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	l := cachedFixtureLoader(t, root, fstest.MapFS{
		"object.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: object}\n")},
	}, nil)
	defer l.Close()
	file := filepath.Join(root, "object.yaml")
	delete(l.documents.cache, file)
	state := &inputCache{
		input: l.documents.artifacts[root], digests: map[string]string{"object.yaml": "digest"},
		types: map[string]fs.FileMode{"object.yaml": 0}, files: map[inputCoordinate]inputEvidence{},
	}
	frame := inputFrame{Files: []cachedInput{{
		Name: "object.yaml", Digest: "digest", Parsed: true, Coverage: manifest.RequiredDocuments,
		Objects: []cachedOccurrence{{Catalog: json.RawMessage(`{"Identity":{"Kind":"invalid"}}`)}},
	}}}
	assert.NoError(t, l.restoreFrameFiles(state, &frame))
	_, err := l.documents.fileObjects(file, manifest.ParseDocOptions{})
	assert.FatalIf(t, !errors.Is(err, manifest.ErrFlux), "deferred catalog error lost: %v", err)
}
