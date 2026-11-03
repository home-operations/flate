package kustomize

import (
	"fmt"
	"io/fs"
	"os"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func testRenderConcurrentPrivateOverlaysCase(
	t *testing.T,
	testCaseInput *testRenderConcurrentPrivateOverlaysCaseInputs,
) {
	t.Helper()
	cache := newTestTreeCache(t, t.TempDir())
	start, done := make(chan struct{}), make(chan struct{})
	var renders, lookups, lookupReady sync.WaitGroup
	lookupReady.Add((*testCaseInput.workers))
	var activeRenders atomic.Int32
	iterations := make([]int, (*testCaseInput.workers))
	for n := range *testCaseInput.workers {
		renders.Go(func() {
			<-start
			lookupReady.Wait()
			original := parsedKS(t, ksDoc(map[string]any{"postBuild": map[string]any{}}))
			projected := original.Clone()
			projected.NamePrefix = fmt.Sprintf("render%d-", n)
			projected.TargetNamespace = fmt.Sprintf("namespace%d", n)
			projected.PostBuild.Substitute = map[string]string{"SLOT": strconv.Itoa(n)}
			spec, err := Prepare(projected, nil)
			if err != nil {
				t.Error(err)
				return
			}
			activeRenders.Add(1)
			defer activeRenders.Add(-1)
			out, _, err := testutil.RenderMaps(
				Render(
					t.Context(),
					cache,
					RenderRequest{Source: (*testCaseInput.artifact), Label: "/absent", SubPath: ".", Spec: spec},
					sourcecache.RetryConfig{},
				),
			)
			{
				assert.ErrorIf(
					t,
					err != nil || !hasRenderField(out, "name", fmt.Sprintf("render%d-shared", n)) ||
						!hasRenderField(
							out,
							"marker",
							"patched",
						) || !hasRenderField(out, "payload", "remote") ||
						!hasRenderField(
							out,
							"namespace",
							fmt.Sprintf("namespace%d", n),
						) || spec.PostBuild.Substitute["SLOT"] != strconv.Itoa(n) ||
						original.TargetNamespace != "" || len(original.PostBuild.Substitute) != 0,
					"private output = %s, %v",
					out,
					err,
				)
			}
		})
		lookups.Go(func() {
			<-start
			for batch := 0; ; batch++ {
				select {
				case <-done:
					return
				default:
				}
				duringRender := activeRenders.Load() != 0
				for _, tc := range *testCaseInput.scopeCases {
					doc := map[string]any{
						"apiVersion": tc.version,
						"kind":       tc.kind,
						"metadata":   map[string]any{"name": "scope"},
					}
					obj, authoritative, changed, err := manifest.ParseDoc(
						doc,
						manifest.ParseDocOptions{DefaultNamespace: "apps"},
					)
					if err != nil {
						t.Error(err)
						return
					}
					_, ns := manifest.DocMetadata(authoritative)
					if obj.Named().Namespace != tc.namespace || ns != tc.namespace ||
						changed != (tc.namespace != "") {
						t.Errorf(
							"%s %s: identity=%s document=%q changed=%t",
							tc.version,
							tc.kind,
							obj.Named(),
							ns,
							changed,
						)
						return
					}
					r := &manifest.ResourceSet{Namespace: "apps"}
					r.DependsOn = []fluxopv1.Dependency{
						{APIVersion: tc.version, Kind: tc.kind, Name: "scope"},
					}
					for ref := range manifest.References(r, manifest.ReferenceRelations) {
						if ref.Namespace != tc.namespace {
							t.Errorf(
								"%s %s dependency namespace=%q, want %q",
								tc.version,
								tc.kind,
								ref.Namespace,
								tc.namespace,
							)
							return
						}
					}
				}
				if batch == 0 {
					lookupReady.Done()
				}
				if duringRender && activeRenders.Load() != 0 {
					iterations[n]++
				}
			}
		})
	}
	close(start)
	renders.Wait()
	close(done)
	lookups.Wait()
	for n, count := range iterations {
		t.Logf("lookup worker %d: %d complete scope batches during render workers", n, count)
		{
			assert.ErrorIf(t, count == 0, "lookup worker %d did no scope work during render workers", n)
		}
	}
	for path, want := range *testCaseInput.files {
		got, err := fs.ReadFile((*testCaseInput.artifact).FS, path)
		{
			assert.FatalIf(
				t,
				err != nil || string(got) != want,
				"shared source %s mutated: %s, %v",
				path,
				got,
				err,
			)
		}
	}
}

func testRenderInputOriginsCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	files := map[string]string{
		"base/cm.yaml":            cmYAML("base"),
		"base/kustomization.yaml": existingKustomization("./cm.yaml"),
		"app/kustomization.yaml": existingKustomization(
			"../base",
			"./local.yaml",
		),
		"app/local.yaml":            cmYAML("local"),
		"generated/a space+@é.yaml": cmYAML("exotic"),
	}
	root := testutil.WriteTree(t, files)
	lower := memoryTree(t, files)
	spec := ksDoc(map[string]any{"namePrefix": "merged-"})
	want, _, err := testutil.RenderMaps(
		Render(
			t.Context(),
			newTestTreeCache(t, ""),
			RenderRequest{
				Source:      localSource(t, root),
				Label:       root,
				ApplyIgnore: false,
				SubPath:     (*testCaseInput),
				Spec:        parsedKS(t, spec),
			},
			sourcecache.RetryConfig{},
		),
	)
	{
		assert.NoError(t, err)
	}
	cache := newTestTreeCache(t, t.TempDir())
	source := &store.SourceArtifact{FS: lower, Key: "pin+rules+options+auth", LocalPath: root}
	for _, state := range []string{"cold", "warm", "host-absent"} {
		if state == "host-absent" {
			{
				err := os.RemoveAll(root)
				assert.NoError(t, err)
			}
		}
		got, _, err := testutil.RenderMaps(
			Render(
				t.Context(),
				cache,
				RenderRequest{Source: source, SubPath: (*testCaseInput), Spec: parsedKS(t, spec)},
				sourcecache.RetryConfig{},
			),
		)
		{
			assert.FatalIf(
				t,
				err != nil || !reflect.DeepEqual(got, want),
				"%s memory output differs: %s, %v",
				state,
				got,
				err,
			)
		}
	}
	if (*testCaseInput) == "app" {
		got, err := fs.ReadFile(lower, "app/kustomization.yaml")
		{
			assert.FatalIf(
				t,
				err != nil || string(got) != files["app/kustomization.yaml"],
				"%s",
				"generated manifest mutated source",
			)
		}
	}
}
