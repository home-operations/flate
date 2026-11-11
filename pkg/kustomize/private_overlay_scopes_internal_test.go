package kustomize

import (
	"fmt"
	"io/fs"
	"strconv"
	"sync"
	"testing"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func testRenderConcurrentPrivateOverlaysCase(
	t *testing.T,
	testCaseInput *testRenderConcurrentPrivateOverlaysCaseInputs,
) {
	t.Helper()
	cache := newTestTreeCache(t, t.TempDir())
	start, done := make(chan struct{}), make(chan struct{})
	var renders, lookups, renderStarted, lookupBatchDone sync.WaitGroup
	renderStarted.Add((*testCaseInput.workers))
	lookupBatchDone.Add((*testCaseInput.workers))
	for n := range *testCaseInput.workers {
		renders.Go(func() {
			<-start
			defer lookupBatchDone.Wait()
			renderStarted.Done()
			original := parsedKS(t, ksDoc(map[string]any{"postBuild": map[string]any{}}))
			projected := original.Clone()
			projected.NamePrefix = fmt.Sprintf("render%d-", n)
			projected.TargetNamespace = fmt.Sprintf("namespace%d", n)
			projected.PostBuild.Substitute = map[string]string{"SLOT": strconv.Itoa(n)}
			spec, err := Prepare(projected, nil, depwait.NewMissingPolicy(nil))
			if err != nil {
				t.Error(err)
				return
			}
			out, _, err := testutil.RenderMaps(
				Render(
					t.Context(),
					cache,
					RenderRequest{Source: (*testCaseInput.artifact), Label: "/absent", SubPath: ".", Spec: spec},
					sourcecache.RetryConfig{},
				),
			)
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
		})
		lookups.Go(func() {
			runPrivateOverlayLookup(t, &privateOverlayLookupInputs{
				scenario: &testCaseInput, start: &start, done: &done,
				renderStarted: &renderStarted, lookupBatchDone: &lookupBatchDone,
			})
		})
	}
	close(start)
	renders.Wait()
	close(done)
	lookups.Wait()
	for path, want := range *testCaseInput.files {
		got, err := fs.ReadFile((*testCaseInput.artifact).FS, path)
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

type privateOverlayLookupInputs struct {
	scenario        **testRenderConcurrentPrivateOverlaysCaseInputs
	start           *chan struct{}
	done            *chan struct{}
	renderStarted   *sync.WaitGroup
	lookupBatchDone *sync.WaitGroup
}

func runPrivateOverlayLookup(t *testing.T, inputs *privateOverlayLookupInputs) {
	t.Helper()

	<-(*inputs.start)
	inputs.renderStarted.Wait()
	ok := checkPrivateOverlayScopes(t, inputs.scenario)
	inputs.lookupBatchDone.Done()
	if !ok {
		return
	}
	for {
		select {
		case <-(*inputs.done):
			return
		default:
		}
		if !checkPrivateOverlayScopes(t, inputs.scenario) {
			return
		}
	}
}

func checkPrivateOverlayScopes(t *testing.T, scenario **testRenderConcurrentPrivateOverlaysCaseInputs) bool {
	t.Helper()

	for _, tc := range *(*scenario).scopeCases {
		doc := map[string]any{
			"apiVersion": tc.version,
			"kind":       tc.kind,
			"metadata":   map[string]any{"name": "scope"},
		}
		parsed, err := manifest.ParseDoc(
			doc,
			manifest.ParseDocOptions{DefaultNamespace: "apps"},
		)
		obj, authoritative, changed := parsed.Object, parsed.Document, parsed.Changed
		if err != nil {
			t.Error(err)
			return false
		}
		metadata := manifest.DocMetadata(authoritative)
		ns := metadata.Namespace
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
			return false
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
				return false
			}
		}
	}
	return true
}
