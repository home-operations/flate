package helm

import (
	"context"
	"encoding/hex"
	"io/fs"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestTemplate_UnchangedSourceBytes(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindGitRepository, manifest.KindHelmChart, manifest.KindOCIRepository} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			owner := cachetest.New(t, "", cache.New)
			client := NewClient(owner, nil)
			hr := newHR()
			hr.Chart.Source.Kind = kind
			files := fstest.MapFS{
				"Chart.yaml": {
					Data: []byte(
						"apiVersion: v2\nname: mychart\nversion: 1.0.0\ndependencies:\n- name: child\n  version: 1.0.0\n  repository: file://../unread\n",
					),
				},
				"templates/cm.yaml": {
					Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: parent}\ndata: {value: stable}\n"),
				},
				"charts/child/Chart.yaml": {Data: []byte("apiVersion: v2\nname: child\nversion: 1.0.0\n")},
				"charts/child/templates/cm.yaml": {
					Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: child}\ndata: {value: stable}\n"),
				},
			}
			var input fs.FS
			if kind == manifest.KindGitRepository {
				directory := fstest.MapFS{}
				for name, file := range files {
					directory["mychart/"+name] = file
				}
				input = directory
			} else {
				archive := chartArchive(t, files)
				generation, _, _, err := owner.Blob(
					t.Context(),
					cache.BlobKey{Content: kind},
					&cache.Policy{},
					func(_ context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
						_, err := w.Write(archive)
						return cache.ProducerResult{}, err
					},
				)
				assert.NoError(t, err)
				input = cache.NameBlob("chart.tgz", generation)
			}
			st := store.New()
			st.SetArtifact(hr.ChartSource(), &store.SourceArtifact{Kind: kind, FS: input})
			var reads store.ReadSet
			reads.Init(st, hr.Named(), nil)
			defer reads.Close()
			var previous []byte
			for run := range 2 {
				docs, _, err := testutil.RenderMaps(
					client.Template(t.Context(), hr, nil, RenderOptions{Sources: NewSources(&reads, nil)}),
				)
				assert.NoError(t, err)
				assert.Equal(t, len(docs), 2)
				assert.Equal(t, hasRenderField(docs, "name", "child"), true)
				assert.Equal(t, hasRenderField(docs, "name", "parent"), true)
				output := encodedRender(t, docs)
				if run == 1 {
					assert.Diff(t, output, previous)
				}
				previous = output
			}
			t.Logf("%s parity_output=%s", kind, hex.EncodeToString(previous))
		})
	}
}
