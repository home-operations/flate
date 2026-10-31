package helm

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

type renderGate func() string

func (renderGate) MarshalJSON() ([]byte, error) { return []byte(`"rendered"`), nil }

func (g renderGate) String() string { return g() }

func TestTemplate_ConcurrentUncacheableRender(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
	testutil.WriteFile(t, dir, "mychart/crds/bad.yaml", "apiVersion: [\n")
	testutil.WriteFile(t, dir, "mychart/templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: '{{ .Values.gate }}'}\n")
	cli := NewClient(cachetest.New(t, "", sourcecache.New))
	hr := newHR()
	hr.Install = &helmv2.Install{DisableSchemaValidation: true}
	sources := localChartResolver(t, "chart-repo", "flux-system", dir)
	synctest.Test(t, func(t *testing.T) {
		entered, release := make(chan struct{}), make(chan struct{})
		var once sync.Once
		var calls atomic.Int32
		gate := renderGate(func() string {
			calls.Add(1)
			once.Do(func() { close(entered); <-release })
			return "rendered"
		})
		if prepared, err := cli.prepareTemplate(t.Context(), hr, map[string]any{"gate": gate}, RenderOptions{Sources: sources}); err != nil || prepared.key == "" {
			t.Fatalf("render gate has no cache identity: %v", err)
		}
		done := make(chan error, 2)
		render := func() {
			_, _, err := testutil.RenderMaps(cli.Template(t.Context(), hr, map[string]any{"gate": gate}, RenderOptions{Sources: sources}))
			done <- err
		}
		go render()
		select {
		case <-entered:
		case err := <-done:
			t.Fatalf("render completed before its template gate: %v", err)
		}
		go render()
		synctest.Wait()
		close(release)
		for range 2 {
			if err := <-done; !errors.Is(err, manifest.ErrInput) {
				t.Errorf("overlapping render lost malformed CRD failure: %v", err)
			}
		}
		if calls.Load() != 2 {
			t.Errorf("uncacheable render calls=%d, want 2", calls.Load())
		}
	})
}
