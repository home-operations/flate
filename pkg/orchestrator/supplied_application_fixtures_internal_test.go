package orchestrator

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/change"
	"github.com/home-operations/flate/internal/loader"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func testSuppliedApplication(t *testing.T, kind string, restricted, covering bool) {
	t.Helper()
	root := t.TempDir()
	testutil.WriteFile(
		t,
		root,
		"entry/literal.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: literal, namespace: apps}\n",
	)
	path := "./elsewhere"
	if covering {
		path = "./entry"
	}
	body := testutil.KustomizationYAML("supplied", "apps", "  path: "+path+"\n")
	if kind == "FluxInstance" {
		repo := testutil.GitInit(t, root)
		hash := testutil.GitCommitAll(t,
			repo,
			"entry",
			&git.CommitOptions{Author: &object.Signature{
				Name:  "t",
				Email: "t@e",
				When: time.Unix(0,
					0),
			}})
		if err := repo.Storer.SetReference(
			plumbing.NewHashReference(plumbing.NewBranchReferenceName("main"), hash),
		); err != nil {
			t.Fatal(err)
		}
		body = "apiVersion: fluxcd.controlplane.io/v1\nkind: FluxInstance\nmetadata: {name: supplied," +
			" namespace: apps}\nspec:\n  sync: {name: self, kind: GitRepository, url: 'https://fix" +
			"ture.invalid/self', ref: refs/heads/main, path: " + path + "}\n"
	}
	inputs, err := loader.LoadDocuments(t.Context(), strings.NewReader(body), "supply.yaml")
	if err != nil {
		t.Fatal(err)
	}
	tree := Tree{
		ID: manifest.TreeID{
			Cluster: ".",
			Side:    manifest.TreeHead,
		},
		Path: filepath.Join(root,
			"entry"),
		RepoRoot: root,
		SelfURLs: []string{"https://fixture.invalid/self"},
	}
	if restricted {
		tree.ExternalChanges = change.NewSet([]string{"entry/literal.yaml"})
	}
	runs, err := renderTreesForTest(t.Context(), t, []Tree{tree}, Config{Concurrency: 2, Supply: inputs})
	if err != nil {
		t.Fatal(err)
	}
	assertSuppliedApplication(t, runs, inputs[0].Obj.Named(), covering)
}

func assertSuppliedApplication(t *testing.T, runs []Rendered, supplied manifest.NamedResource, covering bool) {
	t.Helper()
	assert.FatalIf(t, len(runs) != 1 || runs[0].Orchestrator == nil, "tree construction = %+v", runs)
	o, result := runs[0].Orchestrator, runs[0].Result
	if o.store.HasInventory(supplied) || o.store.GetArtifact(supplied, store.ArtifactContent) != nil {
		t.Fatal("supplied applier rendered")
	}
	if _, known := o.store.GetStatus(supplied); known {
		t.Fatal("supplied applier acquired status")
	}
	if covering {
		record, indexed := o.index.Lookup(supplied)
		if !indexed || record.Origin != manifest.SuppliedDeclaration || record.Declaration == nil {
			t.Fatalf("supplied application missing from discovery: indexed=%t record=%+v", indexed, record)
		}
		if names := configMapNames(result); len(names) != 1 || !names["literal"] || len(result.Manifests) != 1 {
			t.Fatalf("literal entry output = %v", names)
		}
		if !o.entryBinding.Producer.ReportHidden() || !o.store.HasInventory(o.entryBinding.Producer) ||
			len(result.Manifests) == 0 {
			t.Fatalf("literal root did not render: %+v", o.entryBinding)
		}
	} else if o.entryBinding.Producer != (manifest.NamedResource{}) {
		t.Fatal("noncovering evidence qualified entry")
	}
	projectedResults0, projectionErrors0 := Select(runs,
		nil,
		[]manifest.NamedResource{{
			Group: manifest.FluxKustomizeDomain,
			Kind:  manifest.KindKustomization,
			Name:  "supplied",
		}},
		nil)
	selections, selectedErr, remainder := projectedResults0, projectionErrors0.Selected, projectionErrors0.Remaining
	if selectedErr == nil && remainder != nil {
		t.Fatal(remainder)
	}
	if len(selections) != 0 &&
		(len(selections[0].Objects) != 0 || selections[0].Counts[manifest.KindKustomization] != 0) {
		t.Fatal("explicit selector reported supplied applier")
	}
}
