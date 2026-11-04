package artifactgenerator

import (
	"fmt"
	"io/fs"
	"testing"

	"github.com/home-operations/flate/internal/assert"

	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func TestArtifactGenerator_GeneratedWorkloads(t *testing.T) {
	t.Parallel()
	for _, count := range []int{1, 100} {
		for _, backing := range []string{"memory", "persistent"} {
			t.Run(fmt.Sprintf("%d/%s", count, backing), func(t *testing.T) {
				t.Parallel()
				testArtifactGeneratorGeneratedWorkloadsCase(
					t,
					testArtifactGeneratorGeneratedWorkloadsCaseInputs{count: &count, backing: &backing},
				)
			},
			)
		}
	}
}

type testArtifactGeneratorGeneratedWorkloadsCaseInputs struct {
	count   *int
	backing *string
}

func testArtifactGeneratorGeneratedWorkloadsCase(
	t *testing.T,
	testCaseInput testArtifactGeneratorGeneratedWorkloadsCaseInputs,
) {
	t.Helper()

	obj, doc, input := generatedWorkload(t, (*testCaseInput.count))
	root := ""
	if (*testCaseInput.backing) == "persistent" {
		root = t.TempDir()
	}
	owner, err := cache.New(t.Context(), root)
	{
		assert.FatalIf(t, err != nil, "%v", err)
	}
	defer func() {
		{
			localErr := owner.Close()
			assert.ErrorIf(t, localErr != nil, "%v", localErr)
		}
	}()
	var identities []string
	phases := []string{"cold", "warm"}
	if root != "" {
		phases = append(phases, "reopened")
	}
	for _, phase := range phases {
		if phase == "reopened" {
			{
				localErr := owner.Close()
				assert.FatalIf(t, localErr != nil, "%v", localErr)
			}
			owner, err = cache.New(t.Context(), root)
			{
				assert.FatalIf(t, err != nil, "%v", err)
			}
		}
		input.opens.Store(0)
		input.bytes.Store(0)
		st := reconcileWorkload(t, owner, store.Child{Obj: obj, Doc: doc}, input)
		emission := st.GetArtifact(obj.Named(), store.ArtifactContent).(*store.Emission)
		if !emission.Complete || len(emission.Children) != (*testCaseInput.count) {
			t.Fatalf("%s inventory = %d, complete=%t", phase, len(emission.Children), emission.Complete)
		}
		for i, child := range emission.Children {
			artifact := st.GetArtifact(child.Obj.Named(), store.ArtifactContent).(*store.SourceArtifact)
			data, err := fs.ReadFile(artifact.FS, "input")
			{
				assert.FatalIf(
					t,
					err != nil || string(data) != "selected\n",
					"%s output %d = %q, %v",
					phase,
					i,
					data,
					err,
				)
			}
			identity := artifact.Key + "/" + artifact.Digest
			if phase == "cold" {
				identities = append(identities, identity)
			} else {
				assert.FatalIf(t, identity != identities[i], "warm identity changed for output %d", i)
			}
		}
		want := int64((*testCaseInput.count))
		if phase != "cold" {
			want = 0
		}
		if input.opens.Load() != want || input.bytes.Load() != want*9 {
			t.Fatalf(
				"%s input work = %d opens / %d bytes, want %d / %d",
				phase,
				input.opens.Load(),
				input.bytes.Load(),
				want,
				want*9,
			)
		}
		t.Logf(
			"outputs=%d backing=%s phase=%s input_opens=%d input_bytes=%d readable_payload_bytes=%d",
			(*testCaseInput.count),
			(*testCaseInput.backing),
			phase,
			input.opens.Load(),
			input.bytes.Load(),
			(*testCaseInput.count)*
				9,
		)
	}
}
