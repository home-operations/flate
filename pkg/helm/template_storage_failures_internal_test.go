package helm

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/opencontainers/go-digest"
	"oras.land/oras-go/v2/errdef"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
)

func TestTemplateCache_StorageFailures(t *testing.T) {
	t.Parallel()
	for _, fault := range []string{"lookup", "read", "publication", "construction"} {
		t.Run(fault, func(t *testing.T) {
			t.Parallel()
			testTemplateCacheStorageFailuresCase(t, &fault)
		},
		)
	}
}

func testTemplateCacheStorageFailuresCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	if (*testCaseInput) != "construction" && !renderPersistenceEligible(t) {
		t.Skip("storage faults require an eligible build")
	}
	chartDir := t.TempDir()
	testutil.WriteFile(t, chartDir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
	testutil.WriteFile(
		t,
		chartDir,
		"mychart/templates/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: fresh}\n",
	)
	if (*testCaseInput) == "construction" {
		testutil.WriteFile(
			t,
			chartDir,
			"mychart/crds/a-valid.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: prefix}\n",
		)
		testutil.WriteFile(t, chartDir, "mychart/crds/b-invalid.yaml", "apiVersion: [\n")
	}
	root := t.TempDir()
	owner := cachetest.New(t, root, cache.New)
	cli := NewClient(owner)
	cliSources := localChartResolver(t, "chart-repo", "flux-system", chartDir)
	hr := newHR()
	if (*testCaseInput) == "lookup" || (*testCaseInput) == "read" {
		testTemplateCacheStorageFailuresCaseLoop(t, struct {
			cli        **Client
			hr         **manifest.HelmRelease
			cliSources *Sources
		}{cli: &cli, hr: &hr, cliSources: &cliSources})
		d, err := cli.describeChart(t.Context(), hr, cliSources)
		assert.NoError(t, err)
		assert.FatalIf(t, d.record == nil, "%s", "lookup fault fixture did not warm a typed record")
		if (*testCaseInput) == "read" {
			r, _, _, err := owner.Blob(
				t.Context(),
				cache.BlobKey{
					Content: d.record.key,
					Render:  cache.RenderAccepted,
				},
				&cache.Policy{MaxAge: cache.Forever},
				nil,
			)
			assert.NoError(t, err)
			data, err := fs.ReadFile(r, "content")
			assert.NoError(t, err)

			sum := digest.FromBytes(data)
			blob := filepath.Join(root, "oci", "blobs", sum.Algorithm().String(), sum.Encoded())
			assert.NoError(t, os.Remove(blob))
			assert.NoError(t, os.Mkdir(blob, 0o750))
			cli = NewClient(owner)
			cliSources = localChartResolver(t, "chart-repo", "flux-system", chartDir)
		}
	}
	if (*testCaseInput) != "read" {
		assert.NoError(t, os.RemoveAll(root))
	}
	for range 2 {
		out, receipt31, err := testutil.RenderMaps(
			cli.Template(t.Context(), hr, nil, RenderOptions{Sources: cliSources}),
		)
		if err == nil {
			err = cli.RecordRender(
				t.Context(),
				RenderAcceptance{
					Owner:   hr.Named(),
					Receipt: receipt31,
					Records: emptyRenderInventory,
					Sources: []manifest.SourceEvidence{},
				},
			)
		}

		if (*testCaseInput) == "read" {
			{
				pathErr, ok := errors.AsType[*fs.PathError](err)
				assert.FatalIf(t, !ok || pathErr.Op != "read", "read fault did not precede decoding: %v", err)
			}
		}
		assert.FatalIf(
			t, (*testCaseInput) ==
				"construction" && !errors.Is(err, manifest.ErrInput), "construction did not precede publication: %v",
			err,
		)
		wantDocuments := 0
		if (*testCaseInput) == "publication" {
			wantDocuments = 1
		}
		assert.FatalIf(t, len(out) != wantDocuments || !errors.Is(err, manifest.ErrFlux) ||
			errors.Is(
				err,
				errdef.ErrNotFound,
			), "%s fault returned output %q, error %v", (*testCaseInput), out, err)
	}
}

func testTemplateCacheStorageFailuresCaseLoop(t *testing.T, loopInputs struct {
	cli        **Client
	hr         **manifest.HelmRelease
	cliSources *Sources
},
) {
	t.Helper()

	for range 2 {
		_, receipt30, err := testutil.RenderMaps(
			(*loopInputs.cli).Template(
				t.Context(),
				(*loopInputs.hr),
				nil,
				RenderOptions{Options: Options{}, Sources: (*loopInputs.cliSources)},
			),
		)
		if err == nil {
			{
				recordErr := (*loopInputs.cli).RecordRender(
					t.Context(),
					RenderAcceptance{
						Owner:   (*loopInputs.hr).Named(),
						Receipt: receipt30,
						Records: emptyRenderInventory,
						Sources: []manifest.SourceEvidence{},
					},
				)
				assert.NoError(t, recordErr)
			}
		}

		assert.NoError(t, err)
	}
}
