package diff

import (
	"reflect"
	"strings"
	"testing"
	"unsafe"

	"github.com/home-operations/flate/pkg/manifest"
)

//nolint:paralleltest // Allocation measurement changes process-wide scheduling.
func TestDocuments_BorrowsPayload(t *testing.T) {
	payload := strings.Repeat("immutable payload", 1<<20)
	doc := map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "large"},
		"data": map[string]any{"payload": payload},
	}
	batches := []manifest.DocumentBatch{{Documents: []map[string]any{doc}}}
	projected := documents(batches)
	if reflect.ValueOf(projected[0].Manifest).Pointer() != reflect.ValueOf(doc).Pointer() {
		t.Fatal("projection copied the completed document")
	}
	borrowed := projected[0].Manifest["data"].(map[string]any)["payload"].(string)
	if unsafe.StringData(borrowed) != unsafe.StringData(payload) {
		t.Fatal("projection copied the immutable payload")
	}
	allocs := testing.AllocsPerRun(2, func() {
		if got := documents(batches); len(got) != 1 {
			t.Fatal("projection lost the document")
		}
	})
	if allocs != 1 {
		t.Fatalf("projection allocations = %v, want one document slice and zero payload copies or decodes", allocs)
	}
}
