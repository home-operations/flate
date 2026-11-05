package loader

import (
	"strconv"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestParsedInputs_DeclarationFingerprints(t *testing.T) {
	t.Parallel()
	for _, conflict := range []bool{false, true} {
		t.Run(strconv.FormatBool(conflict), func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			body := "apiVersion: v1\n" +
				"kind: ConfigMap\n" +
				"metadata: {name: data, namespace: apps, labels: {kustomize.toolkit.fluxcd.io/name: owner}}\n" +
				"data: {KEY: 'ENC[AES256_GCM,data:original,iv:iv,tag:tag,type:str]'}\n"
			documents, err := manifest.DecodeDocs(strings.NewReader(body), manifest.IncidentalDocuments)
			assert.NoError(t, err)
			want := manifest.Fingerprint(manifest.WithoutOwnershipLabels(documents[0].Document))
			if conflict {
				body += "---\n" + strings.ReplaceAll(body, "data:original", "data:different") + "---\n" + body
			}
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID,
				&store.SourceArtifact{
					LocalPath: root,
					FS:        fstest.MapFS{"objects.yaml": {Data: []byte(body)}},
				})
			l := New(st)
			l.SourceRoot = root
			l.Options.Selection.Changed = true
			_, err = l.Load(t.Context(), root, manifest.NamedResource{}, nil)
			assert.NoError(t, err)
			assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
			id := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "data", Namespace: "apps"}
			record, found := l.Index.Lookup(id)
			assert.Equal(t, found, true)
			if conflict {
				want = ""
			}
			assert.Equal(t, record.Declared, want)
			assert.NoError(t,
				(&documentReader{compact: true}).prepareStream(strings.NewReader(body),
					manifest.ParseDocOptions{},
					func(object parsedObject) {
						assert.NoError(t, object.outcome.Err)
						assert.Equal(
							t,
							object.Doc["data"].(map[string]any)["KEY"],
							any(nil),
						)
						assert.FatalIf(t, object.declared == "" ||
							object.declared == manifest.Fingerprint(
								manifest.WithoutOwnershipLabels(object.Doc),
							), "%s", "preparation lost the original declaration fingerprint")
					}))
		})
	}
}
