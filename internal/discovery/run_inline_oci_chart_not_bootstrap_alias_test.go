package discovery_test

import (
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/discovery"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRun_InlineOCIChartNotBootstrapAlias(t *testing.T) {
	t.Parallel()
	for _, scan := range []string{".", "entry"} {
		for _, kind := range []string{manifest.KindGitRepository, manifest.KindOCIRepository, manifest.KindBucket} {
			t.Run(scan+"/"+kind, func(t *testing.T) {
				t.Parallel()
				root := t.TempDir()
				testutil.WriteFile(t, root, "entry/release.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: release, namespace: apps}
spec:
  interval: 10m
  chart:
    kind: OCIRepository
    spec: {url: "oci://registry.example.test/chart"}
`)
				testutil.WriteFile(
					t,
					root,
					"entry/ordinary.yaml",
					testutil.KustomizationYAML(
						"ordinary",
						"apps",
						"  path: ./apps\n  sourceRef: {kind: "+kind+", name: ordinary}\n",
					),
				)
				testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [cm.yaml]\n")
				testutil.WriteFile(
					t,
					root,
					"apps/cm.yaml",
					"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: input}\n",
				)
				st := store.New()
				result, err := discovery.Run(
					t.Context(),
					discovery.Config{Trees: testTrees(t), Path: filepath.Join(root, scan), RepoRoot: root, Store: st},
				)
				assert.NoError(t, err)
				assert.FatalIf(
					t,
					result.Index.Records[testutil.HelmReleaseID("apps", "release")].Declaration == nil,
					"%v",
					"inline release was not admitted",
				)
				generated := manifest.NamedResource{
					Group:     manifest.SourceDomain,
					Kind:      manifest.KindOCIRepository,
					Namespace: "apps",
					Name:      "apps-release",
				}
				assert.FatalIf(
					t,
					st.GetObject(generated) != nil || st.GetArtifact(generated, store.ArtifactContent) != nil,
					"%v",
					"ungenerated chart source became a bootstrap alias",
				)
				ordinary := manifest.NamedResource{
					Group:     manifest.SourceDomain,
					Kind:      kind,
					Namespace: "apps",
					Name:      "ordinary",
				}
				artifact, ok := st.GetArtifact(ordinary, store.ArtifactContent).(*store.SourceArtifact)
				if kind == manifest.KindGitRepository {
					assert.FatalIf(
						t,
						!ok || !artifact.InputAlias || st.GetObject(ordinary) == nil,
						"Git source was not aliased: %+v",
						artifact,
					)
				} else {
					assert.FatalIf(
						t,
						ok || st.GetObject(ordinary) != nil,
						"missing non-Git source was aliased: %+v",
						artifact,
					)
				}
			})
		}
	}
}
