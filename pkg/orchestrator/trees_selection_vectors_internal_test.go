package orchestrator

import (
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func selectionPreparationKey(owner, source manifest.NamedResource) manifest.WorkKey {
	return manifest.WorkKey{
		Tree: manifest.TreeID{
			Cluster: ".",
			Side:    manifest.TreeHead,
		},
		Owner:  owner,
		Source: source,
		Phase:  manifest.WorkPreparation,
	}
}

func preparationCounts(attempts, files, readers, keys uint64) manifest.WorkCounts {
	return manifest.WorkCounts{
		Attempts:       attempts,
		Completed:      attempts,
		OwnerPrepares:  attempts,
		FileEntries:    files,
		ReaderEdges:    readers,
		RawKeyPrepares: keys,
	}
}

func coldSelectionPreparations() map[string]map[manifest.WorkKey]manifest.WorkCounts {
	a, b := testutil.KustomizationID("apps", "a"), testutil.KustomizationID("apps", "b")
	hr := testutil.HelmReleaseID("apps", "release")
	rs := manifest.NamedResource{
		Group:     manifest.FluxOperatorDomain,
		Kind:      manifest.KindResourceSet,
		Namespace: "apps",
		Name:      "set",
	}
	alias := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "apps",
		Name:      "cluster",
	}
	chart := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindHelmChart,
		Namespace: "apps",
		Name:      "apps-release",
	}
	entry := testutil.KustomizationID("\x00", "flux")
	return map[string]map[manifest.WorkKey]manifest.WorkCounts{
		"full/cold": {
			selectionPreparationKey(manifest.NamedResource{}, alias): preparationCounts(
				0,
				12,
				8,
				0,
			),
			selectionPreparationKey(manifest.NamedResource{}, manifest.BootstrapSourceID): preparationCounts(
				3,
				18,
				15,
				2,
			),
			selectionPreparationKey(alias, alias): preparationCounts(
				2,
				6,
				4,
				0,
			),
			selectionPreparationKey(hr, chart): preparationCounts(
				2,
				0,
				0,
				3,
			),
			selectionPreparationKey(entry, manifest.BootstrapSourceID): preparationCounts(
				1,
				0,
				0,
				3,
			),
			selectionPreparationKey(a, alias): preparationCounts(
				1,
				0,
				0,
				3,
			),
			selectionPreparationKey(b, alias): preparationCounts(
				1,
				0,
				0,
				3,
			),
			selectionPreparationKey(rs, manifest.BootstrapSourceID): preparationCounts(
				0,
				0,
				0,
				1,
			),
		},
		"helmrelease/cold": {
			selectionPreparationKey(manifest.NamedResource{}, alias): preparationCounts(
				0,
				12,
				8,
				0,
			),
			selectionPreparationKey(manifest.NamedResource{}, manifest.BootstrapSourceID): preparationCounts(
				6,
				12,
				10,
				2,
			),
			selectionPreparationKey(alias, alias): preparationCounts(
				3,
				6,
				4,
				0,
			),
			selectionPreparationKey(manifest.BootstrapSourceID, manifest.BootstrapSourceID): preparationCounts(
				1,
				0,
				0,
				0,
			),
			selectionPreparationKey(chart, chart): preparationCounts(
				1,
				0,
				0,
				0,
			),
			selectionPreparationKey(hr, chart): preparationCounts(
				5,
				0,
				0,
				3,
			),
			selectionPreparationKey(entry, manifest.BootstrapSourceID): preparationCounts(
				2,
				14,
				0,
				3,
			),
		},
		"kustomization/cold": {
			selectionPreparationKey(manifest.NamedResource{}, alias): preparationCounts(
				0,
				12,
				8,
				0,
			),
			selectionPreparationKey(manifest.NamedResource{}, manifest.BootstrapSourceID): preparationCounts(
				6,
				12,
				10,
				2,
			),
			selectionPreparationKey(alias, alias): preparationCounts(
				3,
				6,
				4,
				0,
			),
			selectionPreparationKey(manifest.BootstrapSourceID, manifest.BootstrapSourceID): preparationCounts(
				1,
				0,
				0,
				0,
			),
			selectionPreparationKey(entry, manifest.BootstrapSourceID): preparationCounts(
				2,
				7,
				0,
				3,
			),
			selectionPreparationKey(a, alias): preparationCounts(
				3,
				0,
				0,
				3,
			),
		},
		"namespace/cold": {
			selectionPreparationKey(manifest.NamedResource{}, alias): preparationCounts(
				0,
				12,
				8,
				0,
			),
			selectionPreparationKey(manifest.NamedResource{}, manifest.BootstrapSourceID): preparationCounts(
				6,
				18,
				21,
				2,
			),
			selectionPreparationKey(alias, alias): preparationCounts(
				3,
				6,
				4,
				0,
			),
			selectionPreparationKey(manifest.BootstrapSourceID, manifest.BootstrapSourceID): preparationCounts(
				1,
				0,
				0,
				0,
			),
			selectionPreparationKey(chart, chart): preparationCounts(
				1,
				0,
				0,
				0,
			),
			selectionPreparationKey(hr, chart): preparationCounts(
				5,
				0,
				0,
				3,
			),
			selectionPreparationKey(entry, manifest.BootstrapSourceID): preparationCounts(
				2,
				0,
				0,
				3,
			),
			selectionPreparationKey(a, alias): preparationCounts(
				3,
				0,
				0,
				3,
			),
			selectionPreparationKey(b, alias): preparationCounts(
				3,
				0,
				0,
				3,
			),
			selectionPreparationKey(rs, manifest.BootstrapSourceID): preparationCounts(
				2,
				0,
				0,
				1,
			),
		},
		"optional-key/cold": {
			selectionPreparationKey(manifest.NamedResource{}, alias): preparationCounts(
				0,
				12,
				8,
				0,
			),
			selectionPreparationKey(manifest.NamedResource{}, manifest.BootstrapSourceID): preparationCounts(
				6,
				12,
				10,
				2,
			),
			selectionPreparationKey(alias, alias): preparationCounts(
				3,
				6,
				4,
				0,
			),
			selectionPreparationKey(manifest.BootstrapSourceID, manifest.BootstrapSourceID): preparationCounts(
				1,
				0,
				0,
				0,
			),
			selectionPreparationKey(chart, chart): preparationCounts(
				1,
				0,
				0,
				0,
			),
			selectionPreparationKey(hr, chart): preparationCounts(
				5,
				0,
				0,
				3,
			),
			selectionPreparationKey(entry, manifest.BootstrapSourceID): preparationCounts(
				2,
				14,
				0,
				3,
			),
		},
		"optional-object/cold": {
			selectionPreparationKey(manifest.NamedResource{}, alias): preparationCounts(
				0,
				12,
				8,
				0,
			),
			selectionPreparationKey(manifest.NamedResource{}, manifest.BootstrapSourceID): preparationCounts(
				6,
				12,
				10,
				2,
			),
			selectionPreparationKey(alias, alias): preparationCounts(
				3,
				6,
				4,
				0,
			),
			selectionPreparationKey(manifest.BootstrapSourceID, manifest.BootstrapSourceID): preparationCounts(
				1,
				0,
				0,
				0,
			),
			selectionPreparationKey(chart, chart): preparationCounts(
				1,
				0,
				0,
				0,
			),
			selectionPreparationKey(hr, chart): preparationCounts(
				5,
				0,
				0,
				3,
			),
			selectionPreparationKey(entry, manifest.BootstrapSourceID): preparationCounts(
				2,
				14,
				0,
				3,
			),
		},
		"targetPath/cold": {
			selectionPreparationKey(manifest.NamedResource{}, alias): preparationCounts(
				0,
				12,
				8,
				0,
			),
			selectionPreparationKey(manifest.NamedResource{}, manifest.BootstrapSourceID): preparationCounts(
				6,
				12,
				10,
				2,
			),
			selectionPreparationKey(alias, alias): preparationCounts(
				3,
				6,
				4,
				0,
			),
			selectionPreparationKey(manifest.BootstrapSourceID, manifest.BootstrapSourceID): preparationCounts(
				1,
				0,
				0,
				0,
			),
			selectionPreparationKey(chart, chart): preparationCounts(
				1,
				0,
				0,
				0,
			),
			selectionPreparationKey(hr, chart): preparationCounts(
				5,
				0,
				0,
				3,
			),
			selectionPreparationKey(entry, manifest.BootstrapSourceID): preparationCounts(
				2,
				14,
				0,
				3,
			),
		},
	}
}

func hotSelectionPreparations() map[string]map[manifest.WorkKey]manifest.WorkCounts {
	a, b := testutil.KustomizationID("apps", "a"), testutil.KustomizationID("apps", "b")
	hr := testutil.HelmReleaseID("apps", "release")
	rs := manifest.NamedResource{
		Group:     manifest.FluxOperatorDomain,
		Kind:      manifest.KindResourceSet,
		Namespace: "apps",
		Name:      "set",
	}
	alias := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "apps",
		Name:      "cluster",
	}
	chart := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindHelmChart,
		Namespace: "apps",
		Name:      "apps-release",
	}
	entry := testutil.KustomizationID("\x00", "flux")
	return map[string]map[manifest.WorkKey]manifest.WorkCounts{
		"full/hot": {
			selectionPreparationKey(manifest.NamedResource{}, alias): preparationCounts(
				0,
				12,
				8,
				0,
			),
			selectionPreparationKey(manifest.NamedResource{}, manifest.BootstrapSourceID): preparationCounts(
				3,
				18,
				15,
				2,
			),
			selectionPreparationKey(alias, alias): preparationCounts(
				2,
				6,
				4,
				0,
			),
			selectionPreparationKey(hr, chart): preparationCounts(
				2,
				0,
				0,
				5,
			),
			selectionPreparationKey(entry, manifest.BootstrapSourceID): preparationCounts(
				1,
				0,
				0,
				6,
			),
			selectionPreparationKey(a, alias): preparationCounts(
				1,
				0,
				0,
				5,
			),
			selectionPreparationKey(b, alias): preparationCounts(
				1,
				0,
				0,
				5,
			),
			selectionPreparationKey(rs, manifest.BootstrapSourceID): preparationCounts(
				0,
				0,
				0,
				2,
			),
		},
		"helmrelease/hot": {
			selectionPreparationKey(manifest.NamedResource{}, alias): preparationCounts(
				0,
				12,
				8,
				0,
			),
			selectionPreparationKey(manifest.NamedResource{}, manifest.BootstrapSourceID): preparationCounts(
				6,
				12,
				10,
				2,
			),
			selectionPreparationKey(alias, alias): preparationCounts(
				3,
				6,
				4,
				0,
			),
			selectionPreparationKey(manifest.BootstrapSourceID, manifest.BootstrapSourceID): preparationCounts(
				1,
				0,
				0,
				0,
			),
			selectionPreparationKey(chart, chart): preparationCounts(
				1,
				0,
				0,
				0,
			),
			selectionPreparationKey(hr, chart): preparationCounts(
				5,
				0,
				0,
				5,
			),
			selectionPreparationKey(entry, manifest.BootstrapSourceID): preparationCounts(
				2,
				14,
				0,
				6,
			),
		},
		"kustomization/hot": {
			selectionPreparationKey(manifest.NamedResource{}, alias): preparationCounts(
				0,
				12,
				8,
				0,
			),
			selectionPreparationKey(manifest.NamedResource{}, manifest.BootstrapSourceID): preparationCounts(
				6,
				12,
				10,
				2,
			),
			selectionPreparationKey(alias, alias): preparationCounts(
				3,
				6,
				4,
				0,
			),
			selectionPreparationKey(manifest.BootstrapSourceID, manifest.BootstrapSourceID): preparationCounts(
				1,
				0,
				0,
				0,
			),
			selectionPreparationKey(entry, manifest.BootstrapSourceID): preparationCounts(
				2,
				7,
				0,
				6,
			),
			selectionPreparationKey(a, alias): preparationCounts(
				3,
				0,
				0,
				5,
			),
		},
		"namespace/hot": {
			selectionPreparationKey(manifest.NamedResource{}, alias): preparationCounts(
				0,
				12,
				8,
				0,
			),
			selectionPreparationKey(manifest.NamedResource{}, manifest.BootstrapSourceID): preparationCounts(
				6,
				18,
				21,
				2,
			),
			selectionPreparationKey(alias, alias): preparationCounts(
				3,
				6,
				4,
				0,
			),
			selectionPreparationKey(manifest.BootstrapSourceID, manifest.BootstrapSourceID): preparationCounts(
				1,
				0,
				0,
				0,
			),
			selectionPreparationKey(chart, chart): preparationCounts(
				1,
				0,
				0,
				0,
			),
			selectionPreparationKey(hr, chart): preparationCounts(
				5,
				0,
				0,
				5,
			),
			selectionPreparationKey(entry, manifest.BootstrapSourceID): preparationCounts(
				2,
				0,
				0,
				6,
			),
			selectionPreparationKey(a, alias): preparationCounts(
				3,
				0,
				0,
				5,
			),
			selectionPreparationKey(b, alias): preparationCounts(
				3,
				0,
				0,
				5,
			),
			selectionPreparationKey(rs, manifest.BootstrapSourceID): preparationCounts(
				2,
				0,
				0,
				2,
			),
		},
		"optional-key/hot": {
			selectionPreparationKey(manifest.NamedResource{}, alias): preparationCounts(
				0,
				12,
				8,
				0,
			),
			selectionPreparationKey(manifest.NamedResource{}, manifest.BootstrapSourceID): preparationCounts(
				6,
				12,
				10,
				2,
			),
			selectionPreparationKey(alias, alias): preparationCounts(
				3,
				6,
				4,
				0,
			),
			selectionPreparationKey(manifest.BootstrapSourceID, manifest.BootstrapSourceID): preparationCounts(
				1,
				0,
				0,
				0,
			),
			selectionPreparationKey(chart, chart): preparationCounts(
				1,
				0,
				0,
				0,
			),
			selectionPreparationKey(hr, chart): preparationCounts(
				5,
				0,
				0,
				5,
			),
			selectionPreparationKey(entry, manifest.BootstrapSourceID): preparationCounts(
				2,
				14,
				0,
				6,
			),
		},
		"optional-object/hot": {
			selectionPreparationKey(manifest.NamedResource{}, alias): preparationCounts(
				0,
				12,
				8,
				0,
			),
			selectionPreparationKey(manifest.NamedResource{}, manifest.BootstrapSourceID): preparationCounts(
				6,
				12,
				10,
				2,
			),
			selectionPreparationKey(alias, alias): preparationCounts(
				3,
				6,
				4,
				0,
			),
			selectionPreparationKey(manifest.BootstrapSourceID, manifest.BootstrapSourceID): preparationCounts(
				1,
				0,
				0,
				0,
			),
			selectionPreparationKey(chart, chart): preparationCounts(
				1,
				0,
				0,
				0,
			),
			selectionPreparationKey(hr, chart): preparationCounts(
				5,
				0,
				0,
				5,
			),
			selectionPreparationKey(entry, manifest.BootstrapSourceID): preparationCounts(
				2,
				14,
				0,
				6,
			),
		},
		"targetPath/hot": {
			selectionPreparationKey(manifest.NamedResource{}, alias): preparationCounts(
				0,
				12,
				8,
				0,
			),
			selectionPreparationKey(manifest.NamedResource{}, manifest.BootstrapSourceID): preparationCounts(
				6,
				12,
				10,
				2,
			),
			selectionPreparationKey(alias, alias): preparationCounts(
				3,
				6,
				4,
				0,
			),
			selectionPreparationKey(manifest.BootstrapSourceID, manifest.BootstrapSourceID): preparationCounts(
				1,
				0,
				0,
				0,
			),
			selectionPreparationKey(chart, chart): preparationCounts(
				1,
				0,
				0,
				0,
			),
			selectionPreparationKey(hr, chart): preparationCounts(
				5,
				0,
				0,
				5,
			),
			selectionPreparationKey(entry, manifest.BootstrapSourceID): preparationCounts(
				2,
				14,
				0,
				6,
			),
		},
	}
}
