package orchestrator

import (
	"testing"
	"testing/fstest"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/admissiontest"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func testSelectSingleRunOmitsExternalDisclosuresCase(
	t *testing.T,
	testCaseInput *struct {
		name string

		reason         store.StatusReason
		message        string
		requestedName  string
		namespace      string
		emptyNamespace bool
		local          bool
		nonParticipant bool
		inferred       bool
		labels         bool
		indexed        bool
		failedParent   bool
		want           bool
	},
) {
	t.Helper()

	namespace := "apps"
	if testCaseInput.emptyNamespace {
		namespace = ""
	}
	source := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindOCIRepository,
		Namespace: "sources",
		Name:      "source",
	}
	var obj manifest.BaseManifest = &manifest.Kustomization{
		Name:      "external",
		Namespace: namespace,
		SourceRef: kustomizev1.CrossNamespaceSourceReference{
			Kind:      source.Kind,
			Namespace: source.Namespace,
			Name:      source.Name,
		},
	}
	if testCaseInput.nonParticipant {
		obj = &manifest.HelmRelease{
			Name:      "external",
			Namespace: namespace,
			Chart:     manifest.HelmChart{Source: source},
			HelmReleaseSpec: helmv2.HelmReleaseSpec{
				Chart: &helmv2.HelmChartTemplate{
					Spec: helmv2.HelmChartTemplateSpec{
						Chart:   "chart",
						Version: "",
						SourceRef: &helmv2.CrossNamespaceObjectReference{
							Kind:       source.Kind,
							Name:       source.Name,
							Namespace:  source.Namespace,
							APIVersion: "",
						},
					},
				},
			},
		}
	}
	run, other := selectedRun(&Result{}), selectedRun(&Result{})
	run.repoRoot = t.TempDir()
	run.store.AddObject(&manifest.OCIRepository{Name: source.Name, Namespace: source.Namespace})
	status := store.StatusInfo{
		Status:  store.StatusReady,
		Reason:  testCaseInput.reason,
		Message: testCaseInput.message,
	}
	if testCaseInput.indexed {
		entry := testutil.KustomizationID("apps", "entry")
		run.index = indexForTest(map[manifest.NamedResource]string{obj.Named(): "external.yaml"})
		record := run.index.Records[obj.Named()]
		record.Parent, record.Declaration = entry, obj
		run.index.Records[obj.Named()] = record
		if testCaseInput.failedParent {
			status = store.StatusInfo{Status: store.StatusFailed, Message: "producer failed"}
		}
		run.store.UpdateStatus(entry, status)
	} else {
		run.store.AddObject(obj)
		run.store.UpdateStatus(obj.Named(), status)
	}
	if testCaseInput.local {
		run.store.SetArtifact(
			source,
			&store.SourceArtifact{LocalPath: run.repoRoot, FS: fstest.MapFS{"file": {}}},
		)
	}
	if testCaseInput.inferred {
		kept := &manifest.Kustomization{Name: "kept", Namespace: "kept"}
		run.store.AddObject(kept)
		run.index = indexForTest(
			map[manifest.NamedResource]string{kept.Named(): "kept.yaml", obj.Named(): "external.yaml"},
		)
		run.loader = admissiontest.New(
			t,
			admissiontest.Config{
				Changes:   change.NewSet([]string{"kept.yaml"}),
				Index:     run.index,
				Inventory: run.store,
				Root:      "",
			},
		)
		admissiontest.Commit(t, run.loader, nil)
		run.filter = run.loader.Options.Filter
	}
	var labels map[string]string
	if testCaseInput.labels {
		labels = map[string]string{"app": "excluded"}
	}
	testSelectSingleRunOmitsExternalDisclosuresCaseTail(
		t,
		&testSelectSingleRunOmitsExternalDisclosuresCaseTailInputs{
			obj:           &obj,
			testCaseInput: &testCaseInput,
			run:           &run,
			labels:        &labels,
			other:         &other,
			source:        &source,
		},
	)
}

func testSelectSingleRunOmitsExternalDisclosuresCaseTail(
	t *testing.T,
	tailCaseInput *testSelectSingleRunOmitsExternalDisclosuresCaseTailInputs,
) {
	t.Helper()
	root := []manifest.NamedResource{
		{
			Group:     (*tailCaseInput.obj).Named().Group,
			Kind:      (*tailCaseInput.obj).Named().Kind,
			Name:      (*tailCaseInput.testCaseInput).requestedName,
			Namespace: (*tailCaseInput.testCaseInput).namespace,
		},
	}
	selected, err, _ := Select([]Rendered{(*tailCaseInput.run)}, nil, root, (*tailCaseInput.labels))
	if err != nil || len(selected[0].External) != 0 {
		t.Fatalf("single-run external disclosures: %+v %v", selected[0].External, err)
	}
	for externalSide, runs := range [][]Rendered{{(*tailCaseInput.run), (*tailCaseInput.other)}, {(*tailCaseInput.other), (*tailCaseInput.run)}} {
		selected, err, _ = Select(runs, nil, root, (*tailCaseInput.labels))
		{
			assert.FatalIf(t, err != nil, "%v", err)
		}
		for i, res := range selected {
			var want []manifest.Warning
			if (*tailCaseInput.testCaseInput).want && i == externalSide {
				want = []manifest.Warning{
					{
						Resource: (*tailCaseInput.obj).Named(),
						Subject:  manifest.DiagnosticFor((*tailCaseInput.obj).Named(), (*tailCaseInput.obj)),
						Related:  []manifest.NamedResource{(*tailCaseInput.source)},
						Category: manifest.WarnExternalSource,
						Message:  "external source " + tailCaseInput.source.String() + " is outside the local tree; not rendered",
					},
				}
			}
			assert.Diff(t, res.External, want)
			if ((*tailCaseInput.testCaseInput).inferred || (*tailCaseInput.testCaseInput).labels) &&
				len(res.Objects) != 0 {
				t.Fatalf("output filter admitted external object: %+v", res.Objects)
			}
		}
	}
}

type testSelectSingleRunOmitsExternalDisclosuresCaseTailInputs struct {
	obj           *manifest.BaseManifest
	testCaseInput **struct {
		name           string
		reason         store.StatusReason
		message        string
		requestedName  string
		namespace      string
		emptyNamespace bool
		local          bool
		nonParticipant bool
		inferred       bool
		labels         bool
		indexed        bool
		failedParent   bool
		want           bool
	}
	run    *Rendered
	labels *map[string]string
	other  *Rendered
	source *manifest.NamedResource
}
