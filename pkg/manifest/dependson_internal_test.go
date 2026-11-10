package manifest

import (
	"fmt"
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func TestParseDoc_DependsOnNames(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"", "${PREFIX:-}"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			doc := mustYAML(
				t,
				fmt.Sprintf(
					"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: consumer}\nspec:\n  dependsOn: [{name: %q}]\n",
					name,
				),
			)
			_, err := ParseDoc(doc, ParseDocOptions{})
			assert.FatalIf(t, (err != nil) != (name == ""), "authored dependency %q: err=%v", name, err)
		})
	}
}
