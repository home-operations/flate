package helm

import (
	"bytes"
	"compress/gzip"
	"io"
	"strings"
	"testing"
	"testing/fstest"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	"github.com/fluxcd/pkg/apis/kustomize"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestTemplate_EntryPrecondition(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		file     string
		content  string
		values   map[string]any
		patch    bool
		archive  bool
		header   bool
		possible bool
	}{
		{name: "complete negative"},
		{name: "escaped version", file: "templates/entry.yaml", content: `apiVersion: "kustomize.toolkit.fluxcd.io/v\x31"`, possible: true},
		{name: "metadata", file: "Chart.yaml", content: "apiVersion: v2\nname: mychart\nversion: 1.0.0\nannotations: {api: fluxcd.controlplane.io}\n", possible: true},
		{name: "schema", file: "values.schema.json", content: `{"description":"kustomize.toolkit.fluxcd.io"}`, possible: true},
		{name: "ordinary file", file: "entry.txt", content: manifest.FluxOperatorDomain, possible: true},
		{name: "defaults", file: "values.yaml", content: "api: " + manifest.FluxKustomizeDomain, possible: true},
		{name: "source values", file: "selected.yaml", content: "api: " + manifest.FluxOperatorDomain, possible: true},
		{name: "resolved values", values: map[string]any{"api": manifest.FluxKustomizeDomain}, possible: true},
		{name: "patch", patch: true, possible: true},
		{name: "compressed dependency", file: "charts/child/templates/entry.yaml", content: manifest.FluxOperatorDomain, archive: true, possible: true},
		{name: "dependency", file: "charts/child/templates/entry.yaml", content: manifest.FluxOperatorDomain, possible: true},
		{name: "compressed template", file: "templates/entry.yaml", content: manifest.FluxKustomizeDomain, archive: true, possible: true},
		{name: "archive header alone", archive: true, header: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			input := fstest.MapFS{
				"Chart.yaml":             {Data: []byte("apiVersion: v2\nname: mychart\nversion: 1.0.0\n")},
				"templates/failure.yaml": {Data: []byte(`{{ fail "Helm body ran" }}`)},
			}
			hr := newHR()
			hr.Chart.Name = "."
			if tc.file != "" {
				input[tc.file] = &fstest.MapFile{Data: []byte(tc.content)}
			}
			if tc.name == "source values" {
				hr.ChartValuesFiles = []string{tc.file}
			}
			if strings.HasPrefix(tc.file, "charts/child/") {
				input["charts/child/Chart.yaml"] = &fstest.MapFile{
					Data: []byte("apiVersion: v2\nname: child\nversion: 1.0.0\n"),
				}
			}
			if tc.patch {
				hr.PostRenderers = []helmv2.PostRenderer{
					{Kustomize: &helmv2.Kustomize{Patches: []kustomize.Patch{{Patch: manifest.FluxOperatorDomain}}}},
				}
			}
			if tc.archive {
				payload := chartArchive(t, input)
				if tc.header {
					reader, err := gzip.NewReader(bytes.NewReader(payload))
					assert.NoError(t, err)
					plain, err := io.ReadAll(reader)
					assert.NoError(t, err)
					assert.NoError(t, reader.Close())
					var compressed bytes.Buffer
					writer := gzip.NewWriter(&compressed)
					writer.Comment = manifest.FluxKustomizeDomain
					_, err = writer.Write(plain)
					assert.NoError(t, err)
					assert.NoError(t, writer.Close())
					payload = compressed.Bytes()
				}
				input = fstest.MapFS{"chart.tgz": {Data: payload}}
				hr.Chart.Name = "chart.tgz"
			}
			st := store.New()
			st.SetArtifact(hr.Chart.Source, &store.SourceArtifact{FS: input})
			cli := NewClient(cachetest.New(t, "", cache.New))
			calls := 0
			docs, receipt, err := cli.Template(t.Context(), hr, tc.values, RenderOptions{
				Sources: NewSources(st),
				Admit: func(admission RenderAdmission) (bool, error) {
					possible := admission.Possible
					calls++
					assert.Equal(t, possible, tc.possible)
					return true, nil
				},
			})
			assert.NoError(t, err)
			assert.Equal(t, calls, 1)
			assert.Equal(t, len(docs), 0)
			assert.Equal(t, receipt, (*manifest.RenderReceipt)(nil))
		})
	}
}
