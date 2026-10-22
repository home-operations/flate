package artifactgenerator

import (
	"fmt"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"io/fs"
	"testing"
)

func TestArtifactGenerator_GeneratedWorkloads(t *testing.T) {
	t.Parallel()
	for _, count := range []int{1, 100} {
		for _, backing := range []string{"memory", "persistent"} {
			t.Run(fmt.Sprintf("%d/%s", count, backing), func(t *testing.T) {
				t.Parallel()
				obj, doc, input := generatedWorkload(t, count)
				root := ""
				if backing == "persistent" {
					root = t.TempDir()
				}
				owner, err := cache.New(t.Context(), root)
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := owner.Close(); err != nil {
						t.Error(err)
					}
				}()
				var identities []string
				phases := []string{"cold", "warm"}
				if root != "" {
					phases = append(phases, "reopened")
				}
				for _, phase := range phases {
					if phase == "reopened" {
						if err := owner.Close(); err != nil {
							t.Fatal(err)
						}
						owner, err = cache.New(t.Context(), root)
						if err != nil {
							t.Fatal(err)
						}
					}
					input.opens.Store(0)
					input.bytes.Store(0)
					st := reconcileWorkload(t, owner, store.Child{Obj: obj, Doc: doc}, input)
					emission := st.GetArtifact(obj.Named()).(*store.Emission)
					if !emission.Complete || len(emission.Children) != count {
						t.Fatalf("%s inventory = %d, complete=%t", phase, len(emission.Children), emission.Complete)
					}
					for i, child := range emission.Children {
						artifact := st.GetArtifact(child.Obj.Named()).(*store.SourceArtifact)
						data, err := fs.ReadFile(artifact.FS, "input")
						if err != nil || string(data) != "selected\n" {
							t.Fatalf("%s output %d = %q, %v", phase, i, data, err)
						}
						identity := artifact.Key + "/" + artifact.Digest
						if phase == "cold" {
							identities = append(identities, identity)
						} else if identity != identities[i] {
							t.Fatalf("warm identity changed for output %d", i)
						}
					}
					want := int64(count)
					if phase != "cold" {
						want = 0
					}
					if input.opens.Load() != want || input.bytes.Load() != want*9 {
						t.Fatalf("%s input work = %d opens / %d bytes, want %d / %d", phase, input.opens.Load(), input.bytes.Load(), want, want*9)
					}
					t.Logf("outputs=%d backing=%s phase=%s input_opens=%d input_bytes=%d readable_payload_bytes=%d", count, backing, phase, input.opens.Load(), input.bytes.Load(), count*9)
				}
			})
		}
	}
}
