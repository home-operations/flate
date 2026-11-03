package helm

import (
	"context"
	"io"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func testTemplateCacheTemplateIntegrationCase3(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	root := ""
	if (*testCaseInput) != "memory" {
		root = t.TempDir()
	}
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
	testutil.WriteFile(
		t,
		dir,
		"mychart/templates/configmap.yaml",
		`{{ if .Values.fail }}{{ fail "cache miss action" }}{{ end }}
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ .Release.Name }}-cm
data:
  greeting: {{ .Values.greeting }}
extra: {sequence: [{value: original}]}
`,
	)
	owner := cachetest.New(t, root, cache.New)
	cli := NewClient(owner)
	reads := map[string]int{}
	input := os.DirFS(dir)
	chartRoot := ""
	if (*testCaseInput) == "acquired" {
		chartRoot = t.TempDir()
		input = acquireChartFS(t, cachetest.New(t, chartRoot, cache.New), input)
	} else {
		base := input
		input = chartOpenFS{FS: base, open: func(name string) (fs.File, error) {
			reads[name]++
			return base.Open(name)
		}}
	}
	st := store.New()
	st.SetArtifact(newHR().ChartSource(), &store.SourceArtifact{FS: input})
	resolver := NewSources(st)
	cliSources := resolver
	first, receipt13, err := testutil.RenderMaps(
		cli.Template(
			t.Context(),
			newHR(),
			map[string]any{"greeting": "hi"},
			RenderOptions{Options: Options{}, Sources: cliSources},
		),
	)
	if err == nil {
		{
			recordErr := cli.RecordRender(
				t.Context(),
				RenderAcceptance{Owner: newHR().Named(), Receipt: receipt13, Records: emptyRenderInventory},
			)
			assert.NoError(t, recordErr)
		}
	}

	{
		assert.NoError(t, err)
	}
	canonical, err := loadDecodedChart(t.Context(), cli, newHR(), cliSources)
	assert.NoError(t, err)
	beforeReads := reads["mychart/templates/configmap.yaml"]
	second, receipt14, err := testutil.RenderMaps(
		cli.Template(
			t.Context(),
			newHR(),
			map[string]any{"greeting": "hi"},
			RenderOptions{Options: Options{}, Sources: cliSources},
		),
	)
	if err == nil {
		{
			recordErr := cli.RecordRender(
				t.Context(),
				RenderAcceptance{Owner: newHR().Named(), Receipt: receipt14, Records: emptyRenderInventory},
			)
			assert.NoError(t, recordErr)
		}
	}

	{
		assert.NoError(t, err)
	}
	assert.Diff(t, second, first)
	admittedKey := ""
	blocked, receipt15, err := testutil.RenderMaps(
		cli.Template(t.Context(), newHR(), map[string]any{"greeting": "hi"}, RenderOptions{
			Sources: cliSources,
			Admit: func(key string) (bool, error) {
				admittedKey = key
				return true, nil
			},
		}),
	)
	if err == nil {
		{
			recordErr := cli.RecordRender(
				t.Context(),
				RenderAcceptance{Owner: newHR().Named(), Receipt: receipt15, Records: emptyRenderInventory},
			)
			assert.NoError(t, recordErr)
		}
	}

	assert.NoError(t, err)
	assert.Diff(t, blocked, []map[string]any(nil))
	{
		assert.FatalIf(t, admittedKey == "", "%s", "completed output bypassed admission")
	}
	if (*testCaseInput) != "acquired" {
		assert.Equal(t, reads["mychart/templates/configmap.yaml"], beforeReads+2)
	}
	warm, err := loadDecodedChart(t.Context(), cli, newHR(), cliSources)
	assert.NoError(t, err)
	assert.Equal(t, warm.chart == canonical.chart, true)
	assert.Equal(t, warm.fingerprint, canonical.fingerprint)
	third, receipt16, err := testutil.RenderMaps(
		cli.Template(
			t.Context(),
			newHR(),
			map[string]any{"greeting": "different"},
			RenderOptions{Options: Options{}, Sources: cliSources},
		),
	)
	if err == nil {
		{
			recordErr := cli.RecordRender(
				t.Context(),
				RenderAcceptance{Owner: newHR().Named(), Receipt: receipt16, Records: emptyRenderInventory},
			)
			assert.NoError(t, recordErr)
		}
	}

	{
		assert.NoError(t, err)
	}
	assert.Equal(t, reflect.DeepEqual(third, first), false)
	loaded, err := loadDecodedChart(t.Context(), cli, newHR(), cliSources)
	{
		assert.NoError(t, err)
	}
	values := map[string]any{"greeting": "hi", "fail": true}
	loaded.values, loaded.valuesErr = cli.observeValuesFiles(
		t.Context(), &loaded, newHR().ChartValuesFiles,
		newHR().IgnoreMissingValuesFiles,
	)
	assert.NoError(t, loaded.valuesErr)
	key := cache.BlobKey{
		Content: computeTemplateKey(&loaded, values, Options{}, newHR()),
		Render:  cache.RenderAccepted,
	}
	_, _, _, err = owner.Blob(
		t.Context(),
		key,
		cache.Policy{MaxAge: 0},
		func(_ context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
			writeTestRecordErr := writeTestRecord(
				w,
				key,
				&renderRecord{Output: encodedRender(t, first), Analysis: loaded.analyze()},
			)
			return cache.ProducerResult{}, writeTestRecordErr
		},
	)
	{
		assert.NoError(t, err)
	}

	{
		closeErr := owner.Close()
		assert.NoError(t, closeErr)
	}
	cli = NewClient(cachetest.New(t, root, cache.New))
	cliSources = resolver
	loaded, err = loadDecodedChart(t.Context(), cli, newHR(), cliSources)
	assert.NoError(t, err)
	out, receipt17, err := testutil.RenderMaps(
		cli.Template(t.Context(), newHR(), values, RenderOptions{Options: Options{}, Sources: cliSources}),
	)
	if err == nil {
		{
			recordErr := cli.RecordRender(
				t.Context(),
				RenderAcceptance{Owner: newHR().Named(), Receipt: receipt17, Records: emptyRenderInventory},
			)
			assert.NoError(t, recordErr)
		}
	}

	if root != "" && renderPersistenceEligible(t) {
		t.Log("verified persisted completed-output hit")
		{
			assert.FatalIf(
				t,
				err != nil || !reflect.DeepEqual(out, first),
				"completed hit: output=%q, error=%v",
				out,
				err,
			)
		}
		out[0]["data"].(map[string]any)["greeting"] = "mutated"
		out[0]["extra"].(map[string]any)["sequence"].([]any)[0].(map[string]any)["value"] = "mutated"
		again, receipt18, renderMapsErr := testutil.RenderMaps(
			cli.Template(t.Context(), newHR(), values, RenderOptions{Sources: cliSources}),
		)
		if renderMapsErr == nil {
			{
				recordErr := cli.RecordRender(
					t.Context(),
					RenderAcceptance{Owner: newHR().Named(), Receipt: receipt18, Records: emptyRenderInventory},
				)
				assert.NoError(t, recordErr)
			}
		}

		assert.NoError(t, renderMapsErr)
		assert.Diff(t, again, first)
		fresh := NewClient(cachetest.New(t, root, cache.New))
		var receipt19 *manifest.RenderReceipt
		again, receipt19, renderMapsErr = testutil.RenderMaps(
			fresh.Template(t.Context(), newHR(), values, RenderOptions{Sources: cliSources}),
		)
		if renderMapsErr == nil {
			{
				recordErr := fresh.RecordRender(
					t.Context(),
					RenderAcceptance{Owner: newHR().Named(), Receipt: receipt19, Records: emptyRenderInventory},
				)
				assert.NoError(t, recordErr)
			}
		}

		assert.NoError(t, renderMapsErr)
		assert.Diff(t, again, first)
		initial, renderMapsErr := cli.describeChart(t.Context(), newHR(), cliSources)
		assert.NoError(t, renderMapsErr)
		var receipt20 *manifest.RenderReceipt
		_, receipt20, renderMapsErr = testutil.RenderMaps(
			cli.Template(t.Context(), newHR(), values, RenderOptions{Options: Options{}, Sources: cliSources}),
		)
		if renderMapsErr == nil {
			{
				recordErr := cli.RecordRender(
					t.Context(),
					RenderAcceptance{Owner: newHR().Named(), Receipt: receipt20, Records: emptyRenderInventory},
				)
				assert.NoError(t, recordErr)
			}
		}

		assert.NoError(t, renderMapsErr)
		repeated, renderMapsErr := cli.describeChart(t.Context(), newHR(), cliSources)
		assert.NoError(t, renderMapsErr)
		{
			assert.FatalIf(
				t,
				initial.record == nil || repeated.record != initial.record,
				"%s",
				"repeated hit did not share its typed record",
			)
		}
		alternate := maps.Clone(values)
		alternate["variant"] = true
		loaded.values, loaded.valuesErr = cli.observeValuesFiles(
			t.Context(), &loaded, newHR().ChartValuesFiles,
			newHR().IgnoreMissingValuesFiles,
		)
		assert.NoError(t, loaded.valuesErr)
		alternateKey := cache.BlobKey{
			Content: computeTemplateKey(&loaded, alternate, Options{}, newHR()),
			Render:  cache.RenderAccepted,
		}
		r, _, _, renderMapsErr := cli.cache.Blob(
			t.Context(),
			alternateKey,
			cache.Policy{},
			func(_ context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
				writeTestRecordErr2 := writeTestRecord(w, alternateKey, initial.record)
				return cache.ProducerResult{}, writeTestRecordErr2
			},
		)
		assert.NoError(t, renderMapsErr)
		assert.Equal(t, cache.AcquisitionIdentity(r) != initial.record.identity, true)

		var receipt21 *manifest.RenderReceipt
		_, receipt21, renderMapsErr = testutil.RenderMaps(
			cli.Template(
				t.Context(),
				newHR(),
				alternate,
				RenderOptions{Options: Options{}, Sources: cliSources},
			),
		)
		if renderMapsErr == nil {
			{
				recordErr := cli.RecordRender(
					t.Context(),
					RenderAcceptance{Owner: newHR().Named(), Receipt: receipt21, Records: emptyRenderInventory},
				)
				assert.NoError(t, recordErr)
			}
		}

		assert.NoError(t, renderMapsErr)
		distinct, renderMapsErr := cli.describeChart(t.Context(), newHR(), cliSources)
		assert.NoError(t, renderMapsErr)
		{
			assert.FatalIf(
				t,
				distinct.record == initial.record || distinct.record.key != alternateKey.Content,
				"%s",
				"distinct release key reused a typed record",
			)
		}
		for _, replacement := range []string{"changed output", "corrupt payload", "{", `{"Output":"bWlzc2luZyBhbmFseXNpcw=="}`} {
			replacementDocs := []map[string]any{{"witness": replacement}}
			_, _, _, blobErr := cli.cache.Blob(
				t.Context(),
				alternateKey,
				cache.Policy{},
				func(_ context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
					var errResult error
					switch replacement {
					case "corrupt payload":
						errResult = writeTestRecord(
							w,
							alternateKey,
							&renderRecord{Output: []byte("corrupt"), Analysis: loaded.analyze()},
						)
					case "changed output":
						errResult = writeTestRecord(
							w,
							alternateKey,
							&renderRecord{Output: encodedRender(t, replacementDocs), Analysis: loaded.analyze()},
						)
					default:
						_, errResult = io.WriteString(w, replacement)
					}
					return cache.ProducerResult{}, errResult
				},
			)
			assert.NoError(t, blobErr)

			out, receipt22, blobErr := testutil.RenderMaps(
				cli.Template(
					t.Context(),
					newHR(),
					alternate,
					RenderOptions{Options: Options{}, Sources: cliSources},
				),
			)
			if blobErr == nil {
				{
					recordErr := cli.RecordRender(
						t.Context(),
						RenderAcceptance{
							Owner:   newHR().Named(),
							Receipt: receipt22,
							Records: emptyRenderInventory,
						},
					)
					assert.NoError(t, recordErr)
				}
			}

			switch replacement {
			case "changed output":
				assert.NoError(t, blobErr)
				{
					assert.FatalIf(
						t,
						!reflect.DeepEqual(out, replacementDocs),
						"%s",
						"same-key replacement served stale output",
					)
				}
			case "corrupt payload":
				{
					assert.FatalIf(
						t,
						blobErr == nil || !strings.Contains(blobErr.Error(), "cache miss action") || len(out) != 0,
						"corrupt payload: %v, %v",
						out,
						blobErr,
					)
				}
			case "{":
				{
					assert.FatalIf(
						t,
						blobErr == nil || !strings.Contains(blobErr.Error(), "cache miss action") || len(out) != 0,
						"malformed replacement was concealed: %v",
						blobErr,
					)
				}
			default:
				{
					assert.FatalIf(
						t,
						blobErr == nil || !strings.Contains(blobErr.Error(), "cache miss action") || len(out) != 0,
						"missing analysis was accepted: %v",
						blobErr,
					)
				}
			}
		}
		recoveryValues := map[string]any{"greeting": "hi"}
		recoveryKey := cache.BlobKey{
			Content: computeTemplateKey(&loaded, recoveryValues, Options{}, newHR()),
			Render:  cache.RenderAccepted,
		}
		_, _, _, renderMapsErr = cli.cache.Blob(
			t.Context(),
			recoveryKey,
			cache.Policy{},
			func(_ context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
				writeTestRecordErr3 := writeTestRecord(
					w,
					recoveryKey,
					&renderRecord{Output: []byte("corrupt"), Analysis: loaded.analyze()},
				)
				return cache.ProducerResult{}, writeTestRecordErr3
			},
		)
		assert.NoError(t, renderMapsErr)

		beforeRecovery, renderMapsErr := cli.describeChart(t.Context(), newHR(), cliSources)
		assert.NoError(t, renderMapsErr)
		recovered, receipt23, renderMapsErr := testutil.RenderMaps(
			cli.Template(t.Context(), newHR(), recoveryValues, RenderOptions{Sources: cliSources}),
		)
		if renderMapsErr == nil {
			{
				recordErr := cli.RecordRender(
					t.Context(),
					RenderAcceptance{Owner: newHR().Named(), Receipt: receipt23, Records: emptyRenderInventory},
				)
				assert.NoError(t, recordErr)
			}
		}

		assert.NoError(t, renderMapsErr)
		assert.Diff(t, recovered, first)
		afterRecovery, renderMapsErr := cli.describeChart(t.Context(), newHR(), cliSources)
		assert.NoError(t, renderMapsErr)
		{
			assert.FatalIf(
				t,
				afterRecovery.record == beforeRecovery.record || afterRecovery.record == nil,
				"%s",
				"recovered generation did not replace the typed record",
			)
		}
		recoveredOutput, renderMapsErr := testutil.DecodeRender(afterRecovery.record.Output)
		assert.NoError(t, renderMapsErr)
		assert.Diff(t, recoveredOutput, first)
		r, _, _, renderMapsErr = cli.cache.Blob(t.Context(), recoveryKey, cache.Policy{MaxAge: cache.Forever}, nil)
		assert.NoError(t, renderMapsErr)
		var replacement renderRecord
		assert.NoError(t, readTestRecord(r, &replacement))

		decoded, renderMapsErr := testutil.DecodeRender(replacement.Output)
		assert.NoError(t, renderMapsErr)
		assert.Diff(t, decoded, first)
		var receipt24 *manifest.RenderReceipt
		again, receipt24, renderMapsErr = testutil.RenderMaps(
			cli.Template(t.Context(), newHR(), recoveryValues, RenderOptions{Sources: cliSources}),
		)
		if renderMapsErr == nil {
			{
				recordErr := cli.RecordRender(
					t.Context(),
					RenderAcceptance{Owner: newHR().Named(), Receipt: receipt24, Records: emptyRenderInventory},
				)
				assert.NoError(t, recordErr)
			}
		}

		assert.NoError(t, renderMapsErr)
		assert.Diff(t, again, first)
		reopened := NewClient(cachetest.New(t, root, cache.New))
		var receipt25 *manifest.RenderReceipt
		again, receipt25, renderMapsErr = testutil.RenderMaps(
			reopened.Template(t.Context(), newHR(), recoveryValues, RenderOptions{Sources: cliSources}),
		)
		if renderMapsErr == nil {
			{
				recordErr := reopened.RecordRender(
					t.Context(),
					RenderAcceptance{Owner: newHR().Named(), Receipt: receipt25, Records: emptyRenderInventory},
				)
				assert.NoError(t, recordErr)
			}
		}

		assert.NoError(t, renderMapsErr)
		assert.Diff(t, again, first)
		t.Log("verified corrupt payload recovery, replacement, subsequent and reopened hits")
	} else {
		assert.FatalIf(
			t,
			err == nil || !strings.Contains(err.Error(), "cache miss action"),
			"ineligible output did not bypass persistence: %v",
			err,
		)
	}
	warm, err = loadDecodedChart(t.Context(), cli, newHR(), cliSources)
	assert.NoError(t, err)
	assert.Equal(t, warm.chart == loaded.chart, true)
	assert.Equal(t, warm.fingerprint, loaded.fingerprint)
	values["greeting"] = "changed"
	_, receipt26, err := testutil.RenderMaps(
		cli.Template(t.Context(), newHR(), values, RenderOptions{Options: Options{}, Sources: cliSources}),
	)
	if err == nil {
		{
			recordErr := cli.RecordRender(
				t.Context(),
				RenderAcceptance{Owner: newHR().Named(), Receipt: receipt26, Records: emptyRenderInventory},
			)
			assert.NoError(t, recordErr)
		}
	}
	{
		assert.FatalIf(
			t,
			err == nil || !strings.Contains(err.Error(), "cache miss action"),
			"changed input did not miss completed output: %v",
			err,
		)
	}
	testTemplateCacheTemplateIntegrationCase3Loop(t, struct {
		testCaseInput **string
		chartRoot     *string
		input         *fs.FS
		cli           **Client
		cliSources    *Sources
		first         *[]map[string]any
	}{testCaseInput: &testCaseInput, chartRoot: &chartRoot, input: &input, cli: &cli, cliSources: &cliSources, first: &first})
}

func testTemplateCacheTemplateIntegrationCase3Loop(t *testing.T, loopInputs struct {
	testCaseInput **string
	chartRoot     *string
	input         *fs.FS
	cli           **Client
	cliSources    *Sources
	first         *[]map[string]any
},
) {
	t.Helper()

	if (*(*loopInputs.testCaseInput)) == "acquired" {
		assert.NoError(t, os.RemoveAll(filepath.Join((*loopInputs.chartRoot), "oci", "blobs")))
		_, err := fs.ReadFile((*loopInputs.input), "mychart/templates/configmap.yaml")
		{
			assert.FatalIf(t, err == nil, "%s", "backing removal did not prevent chart reads")
		}
		repeated, receipt27, err := testutil.RenderMaps(
			(*loopInputs.cli).Template(
				t.Context(),
				newHR(),
				map[string]any{"greeting": "hi"},
				RenderOptions{Sources: (*loopInputs.cliSources)},
			),
		)
		if err == nil {
			{
				recordErr := (*loopInputs.cli).RecordRender(
					t.Context(),
					RenderAcceptance{Owner: newHR().Named(), Receipt: receipt27, Records: emptyRenderInventory},
				)
				assert.NoError(t, recordErr)
			}
		}

		assert.NoError(t, err)
		assert.Diff(t, repeated, (*loopInputs.first))
	}
}
