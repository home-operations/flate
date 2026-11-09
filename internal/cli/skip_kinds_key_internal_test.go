package cli

import (
	"strings"
	"testing"
	"testing/fstest"

	"github.com/spf13/pflag"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestBindCommon_SkipKindsRenderKey(t *testing.T) {
	t.Parallel()
	key := func(value string) string {
		var flags commonFlags
		fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
		bindCommon(fs, &flags)
		assert.NoError(t, fs.Parse([]string{"--skip-kinds=" + value}))
		cfg := buildOrchCfg(&flags)
		docs, err := manifest.DecodeDocs(
			strings.NewReader("apiVersion: apps/v1\nkind: Deployment\nmetadata: {name: retained}\n"),
			manifest.RequiredDocuments,
		)
		assert.NoError(t, err)
		req := kustomize.RenderRequest{
			Source:  &store.SourceArtifact{FS: fstest.MapFS{}, Content: "fixed-source"},
			Spec:    &manifest.Kustomization{Name: "fixture", Path: "."},
			SubPath: ".", SkipKinds: cfg.HelmOptions.SkipKinds,
		}
		shape := kustomize.PrepareShape(&req, docs)
		defer shape.Retire()
		result := manifest.Fingerprint(shape.Content())
		assert.FatalIf(t, result == "", "%s", "empty shaping key")
		return result
	}
	for _, tc := range []struct {
		name, first, second string
		different           bool
	}{
		{name: "ordinary order", first: "Secret,ConfigMap", second: "ConfigMap,Secret"},
		{name: "ordinary duplicate", first: "ConfigMap,Secret", second: "Secret,ConfigMap,Secret"},
		{name: "mixed order", first: "Secret,CustomResourceDefinition,ConfigMap", second: "ConfigMap,Secret,CustomResourceDefinition"},
		{name: "repeated CRD", first: "Secret,CustomResourceDefinition", second: "CustomResourceDefinition,Secret,CustomResourceDefinition"},
		{name: "request example", first: "Secret,CustomResourceDefinition", second: "CustomResourceDefinition,Secret"},
		{name: "different set", first: "Secret,CustomResourceDefinition,ConfigMap", second: "Secret,CustomResourceDefinition", different: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			first, second := key(tc.first), key(tc.second)
			assert.ErrorIf(
				t,
				(first != second) != tc.different,
				"shaping keys for %q and %q: %s, %s; different=%t",
				tc.first,
				tc.second,
				first,
				second,
				tc.different,
			)
		})
	}
}
