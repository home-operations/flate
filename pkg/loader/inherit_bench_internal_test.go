package loader

import (
	"fmt"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/store"
)

func BenchmarkProjectAuthored_SharedBootstrapPath(b *testing.B) {
	root := b.TempDir()
	testutil.WriteFile(b, root, "app/kustomization.yaml", "resources: []\n")
	var docs strings.Builder
	for i := range 64 {
		if i != 0 {
			docs.WriteString("---\n")
		}
		docs.WriteString(testutil.KustomizationYAML(fmt.Sprintf("peer-%d", i), "apps", "  path: ./app\n"))
	}
	testutil.WriteFile(b, root, "app/peers.yaml", docs.String())
	l := New(store.New())
	l.SourceRoot = root
	if _, err := l.Load(b.Context(), root); err != nil {
		b.Fatal(err)
	}
	l.Admit(root)
	if len(l.kustomizations()) != 64 || len(l.Kustomizations()) != 0 {
		b.Fatal("inactive bootstrap evidence missing or runnable")
	}
	b.ReportAllocs()
	for b.Loop() {
		if l.projectAuthored(root) {
			b.Fatal("unchanged projection changed")
		}
	}
}
