package oci

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"io/fs"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/source"
	"github.com/home-operations/flate/internal/store"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
)

func revalidationPayload(t *testing.T, payload []byte, operation *string) []byte {
	t.Helper()
	if (*operation) == sourcev1.OCILayerExtract {
		newReaderGz, err := gzip.NewReader(bytes.NewReader(payload))
		assert.NoError(t, err)

		payload, err = io.ReadAll(newReaderGz)
		{
			joinErr := errors.Join(err, newReaderGz.Close())
			assert.NoError(t, joinErr)
		}
	}
	return payload
}

func checkRevalidationIdentity(t *testing.T, phase string, identity *string, got string) {
	t.Helper()
	switch phase {
	case "initial":
		*identity = got
	case "changed":
		assert.FatalIf(t, got == *identity, "%s", "changed manifest retained content identity")
		*identity = got
	default:
		assert.FatalIf(t, got != *identity, "%s", "revalidation changed content identity")
	}
}

type revalidationOutput struct {
	operation string
	phase     string
	v1        *fakeOCIArtifact
	v2        *fakeOCIArtifact
	art       *store.SourceArtifact
}

func checkRevalidationOutput(t *testing.T, output revalidationOutput) {
	t.Helper()
	if output.operation == sourcev1.OCILayerExtract {
		want := "version: v1\n"
		if output.phase == "changed" || output.phase == "missing" || output.phase == "truncated" {
			want = "version: v2\n"
		}
		{
			got := mustReadFile(t, output.art.FS, "app.yaml")
			assert.FatalIf(t, got != want, "%s output=%q", output.phase, got)
		}
	} else {
		want := output.v1.layer
		if output.phase == "changed" || output.phase == "missing" || output.phase == "truncated" {
			want = output.v2.layer
		}
		got, err := fs.ReadFile(output.art.FS, source.OCICopiedLayerFilename)
		assert.FatalIf(t, err != nil || !bytes.Equal(got, want), "%s copied layer error=%v", output.phase, err)
	}
}
