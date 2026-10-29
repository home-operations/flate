package kustomize

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func TestRender_BuildDiagnostic(t *testing.T) {
	for _, tc := range []struct {
		name      string
		documents *DocumentsInput
	}{
		{name: "directory"},
		{name: "documents", documents: &DocumentsInput{Documents: []byte(cmYAML("proof"))}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			previous := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
			t.Cleanup(func() { slog.SetDefault(previous) })
			root := testutil.WriteTree(t, map[string]string{
				"kustomization.yaml": existingKustomization("cm.yaml"),
				"cm.yaml":            cmYAML("proof"),
			})
			cache := newTestTreeCache(t, t.TempDir())
			_, _, err := Render(t.Context(), cache, RenderRequest{Source: localSource(t, root), SubPath: ".", Documents: tc.documents}, sourcecache.RetryConfig{})
			assert.NoError(t, err)
			if count := strings.Count(logs.String(), `msg="kustomize: build"`); count != 1 {
				t.Fatalf("one uncached render logged %d builds:\n%s", count, logs.String())
			}
		})
	}
}
