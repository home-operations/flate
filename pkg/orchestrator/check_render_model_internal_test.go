package orchestrator

import (
	"bytes"
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/home-operations/flate/pkg/schedule"

	"github.com/home-operations/flate/internal/assert"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/admissiontest"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/baseline"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func checkRenderModel(t *testing.T, fixture *renderModelCase) ([]byte, error) {
	t.Helper()
	if err := checkModelBounds(&fixture.modelCase); err != nil {
		return nil, err
	}
	beforeRoot, afterRoot := t.TempDir(), t.TempDir()
	materializeRenderModelSourcesResult1 := materializeRenderModelSources(t, fixture)
	serializedBefore, serializedAfter := materializeRenderModelSourcesResult1.before, materializeRenderModelSourcesResult1.after
	writeRenderModelWorld(t, beforeRoot, serializedBefore)
	writeRenderModelWorld(t, afterRoot, serializedAfter)
	owner, err := sourcecache.New(t.Context(), "")
	if err != nil {
		return nil, err
	}
	defer func() {
		{
			err := owner.Close()
			assert.ErrorIf(t, err != nil, "%v", err)
		}
	}()
	cfg := treeConfig{Concurrency: 2, Cache: owner}
	beforeScan := cmp.Or(fixture.beforeScan, fixture.scan)
	baseTree, headTree := Tree{
		RepoRoot: beforeRoot,
		Path:     filepath.Join(beforeRoot, beforeScan),
		SelfURLs: fixture.selfURLs,
	}, Tree{
		RepoRoot: afterRoot,
		Path:     filepath.Join(afterRoot, fixture.scan),
		SelfURLs: fixture.selfURLs,
	}
	if fixture.committed {
		prepareCommittedRenderModelResult2, prepareCommittedRenderModelResult2Err := cfg.prepareCommittedRenderModel(
			t, afterRoot, fixture.scan, [2]*modelWorld{&serializedBefore, &serializedAfter},
		)
		before, after, prepareErr := prepareCommittedRenderModelResult2.before, prepareCommittedRenderModelResult2.after, prepareCommittedRenderModelResult2Err
		if prepareErr != nil {
			return nil, prepareErr
		}
		baseTree, headTree = before, after
	}
	fullBefore, fullAfter := Rendered{}, Rendered{}
	for _, side := range []struct {
		root string
		scan string
		out  *Rendered
	}{{beforeRoot, beforeScan, &fullBefore}, {afterRoot, fixture.scan, &fullAfter}} {
		runs, renderErr := RenderTrees(
			t.Context(),
			[]Tree{
				{
					ID:       manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
					Path:     filepath.Join(side.root, side.scan),
					RepoRoot: side.root,
					SelfURLs: fixture.selfURLs,
				},
			},
			Config{Concurrency: 2},
		)
		*side.out = runs[0]
		side.out.Err = renderErr
	}
	baseTree.ID, headTree.ID = manifest.TreeID{
		Cluster: ".",
		Side:    manifest.TreeBase,
	}, manifest.TreeID{
		Cluster: ".",
		Side:    manifest.TreeHead,
	}
	baseTree.Input, baseTree.BaseInput, baseTree.ExternalChanges = cfg.BaseInput, cfg.Input, cfg.ExternalChanges
	headTree.Input, headTree.BaseInput, headTree.ExternalChanges = cfg.Input, cfg.BaseInput, cfg.ExternalChanges
	runs, pairErr := RenderTrees(t.Context(), []Tree{baseTree, headTree}, cfg.Config)
	old, current := runs[0], runs[1]
	selection := compareModelWorlds(fixture.before, fixture.after)
	var problems []error
	var evidence strings.Builder
	var reference, actual [2]modelObservation
	var observed [2]bool
	{
		sideValues := []struct {
			name          string
			world         modelWorld
			full, changed Rendered
			want          modelSelection
		}{
			{
				"before",
				fixture.before,
				fullBefore,
				old,
				selection.before,
			},
			{"after", fixture.after, fullAfter, current, selection.after},
		}
		for index := range sideValues {
			side := sideValues[index]

			full, _, fullErr := observeRenderModel(side.world, side.full, false)
			changed, admitted, changedErr := observeRenderModel(side.world, side.changed, true)
			fmt.Fprintf(
				&evidence,
				"%s expected affected=%v support=%v unavailable=%v deleted=%v actual=%v full-error=%v changed-error=%v full-observation-error=%v changed-observation-error=%v\n",
				side.name,
				modelIDs(side.want.affected),
				modelIDs(side.want.support),
				modelIDs(side.want.unavailable),
				modelIDs(side.want.deleted),
				modelIDs(admitted),
				side.full.Err,
				side.changed.Err,
				fullErr,
				changedErr,
			)
			if observeErr := errors.Join(fullErr, changedErr); observeErr != nil {
				problems = append(problems, fmt.Errorf("observe %s: %w", side.name, observeErr))
				continue
			}
			observed[index] = true
			membership := full
			membership.documents = slices.Clone(full.documents)
			if err := checkRenderModelMembership(expectedRenderModel(side.world, nil), membership); err != nil {
				problems = append(problems, fmt.Errorf("full-%s: %w", side.name, err))
			}
			writeRenderModelStatus(&evidence, side.name, admitted, &side.changed)
			fmt.Fprintf(
				&evidence,
				"%s full-documents=%+v changed-documents=%+v\n",
				side.name,
				full.documents,
				changed.documents,
			)
			work := side.want
			if err := compareModelSelection(work, admitted); err != nil {
				problems = append(problems, fmt.Errorf("changed-%s: %w", side.name, err))
			}
			reference[index] = projectRenderModel(full, side.want, side.world)
			actual[index] = changed
			if err := compareInitialRenderModelObservation(index, side.name, reference[index], changed); err != nil {
				problems = append(problems, err)
			}
			if err := checkDeletedRenderModelDocuments(side.name, side.want.deleted, changed.documents); err != nil {
				return nil, err
			}
		}
	}
	if err := compareRenderModelSides(observed, &reference, &actual); err != nil {
		problems = append(problems, err)
	}
	if err := checkRenderModelPairError(pairErr, old.Err, current.Err); err != nil {
		return nil, err
	}
	if len(problems) > 0 {
		return nil, fmt.Errorf("%w\n%s", errors.Join(problems...), evidence.String())
	}
	return marshalRenderModelManifests(old.Result.Manifests, current.Result.Manifests)
}

func failRenderModel(t *testing.T, input []byte, fixture *renderModelCase, err error) {
	t.Helper()
	var diagnostic strings.Builder
	fmt.Fprintf(
		&diagnostic,
		"input=%x\noperations=%+v\nunsupported=%v\nerror=%v\n",
		input,
		fixture.operations,
		fixture.unsupported,
		err,
	)
	selection := compareModelWorlds(fixture.before, fixture.after)
	for _, side := range []struct {
		name  string
		world modelWorld
		want  modelSelection
	}{{"before", fixture.before, selection.before}, {"after", fixture.after, selection.after}} {
		fmt.Fprintf(
			&diagnostic,
			"%s expected affected=%v support=%v unavailable=%v deleted=%v\n",
			side.name,
			modelIDs(side.want.affected),
			modelIDs(side.want.support),
			modelIDs(side.want.unavailable),
			modelIDs(side.want.deleted),
		)
		ids := slices.Collect(maps.Keys(side.world.producers))
		slices.SortFunc(ids, compareModelID)
		fmt.Fprintf(&diagnostic, "%s producers=%v\n", side.name, ids)
		keys := slices.Collect(maps.Keys(side.world.files))
		slices.SortFunc(keys, compareModelPath)
		for _, file := range keys {
			body := side.world.files[file].body
			if utf8.Valid(body) {
				fmt.Fprintf(&diagnostic, "%s:%s\n%s\n", file.source.binding, file.path, body)
			} else {
				fmt.Fprintf(&diagnostic, "%s:%s\nhex:%x\n", file.source.binding, file.path, body)
			}
		}
	}

	if root := os.Getenv("FLATE_E8_ARTIFACT_ROOT"); root != "" {
		digest := sha256.Sum256(input)
		dir := filepath.Join(root, "fuzz", hex.EncodeToString(digest[:8]))
		if writeErr := os.MkdirAll(dir, 0o750); writeErr != nil {
			t.Error(writeErr)
		} else {
			seedName := "e8-" + hex.EncodeToString(digest[:8])
			corpusDir := "pkg/orchestrator/testdata/fuzz/FuzzRenderTrees_IndependentModel"
			reproduction, marshalErr := json.MarshalIndent(map[string][]string{
				"mkdir_argv":        {"mkdir", "-p", corpusDir},
				"install_seed_argv": {"cp", filepath.Join(dir, "seed"), filepath.Join(corpusDir, seedName)},
				"test_argv": {
					"mise",
					"exec",
					"--",
					"go",
					"test",
					"-race",
					"./pkg/orchestrator",
					"-run",
					"^FuzzRenderTrees_IndependentModel$/" + seedName + "$",
					"-count=1",
				},
			}, "", "  ")
			assert.FatalIf(t, marshalErr != nil, "%v", marshalErr)
			for name, body := range map[string][]byte{
				"input": input, "worlds.txt": []byte(diagnostic.String()),
				"seed":           []byte(fmt.Sprintf("go test fuzz v1\n[]byte(%q)\n", input)),
				"reproduce.json": reproduction,
			} {
				{
					writeErr := os.WriteFile(filepath.Join(dir, name), body, 0o600)
					assert.ErrorIf(t, writeErr != nil, "%v", writeErr)
				}
			}
		}
	}
	t.Fatal(diagnostic.String())
}

type renderModelInput struct {
	name  string
	input []byte
}

func renderModelInputs() []renderModelInput {
	cases := make([]renderModelInput, 0, len(renderModelSeeds())+1)
	for i, name := range renderModelSeeds() {
		cases = append(cases, renderModelInput{name, []byte{byte(i), 0}})
	}
	cases = append(cases, renderModelInput{"negative-read-and-ordering-committed", []byte{3, 1}})
	for _, feature := range []struct {
		name          string
		seed, variant byte
	}{
		{"generator-input", 1, 4},
		{"chart-template", 1, 5},
		{"chart-template-directory", 1, 133},
		{"chart-values", 1, 6},
		{"inline-values", 1, 7},
		{"referenced-values", 1, 8},
		{"chart-dependency", 1, 9},
		{"shared-owner-removal", 2, 10},
		{"ancestor-patch", 1, 11},
		{"ignore-membership", 4, 12},
		{"orphan-transition", 0, 13},
		{"alias-pinned-dirty-collision", 1, 14},
		{"second-source-content", 1, 15},
		{"group-move", 1, 16},
		{"group-collision", 1, 17},
		{"late-data-support", 0, 18},
		{"optional-absent", 1, 19},
		{"required-absent", 1, 20},
		{"missing-crd", 1, 21},
		{"late-source-support", 0, 22},
		{"source-binding-move", 1, 23},
		{"root-move", 1, 24},
		{"owner-move", 0, 25},
		{"ancestor-injected-patch", 0, 26},
		{"two-sided-add", 4, 27},
		{"ignore-removal", 4, 28},
		{"emitted-namespace-move", 0, 29},
	} {
		cases = append(cases, renderModelInput{feature.name, []byte{feature.seed, feature.variant}})
	}
	for _, mutation := range []struct {
		name   string
		opcode byte
	}{
		{"same-size-edit", 0},
		{"comment-only", 1},
		{"rename-input", 2},
		{"name-move", 3},
		{"namespace-move", 4},
		{"path-move", 5},
		{"suspension", 6},
		{"unrelated-dirty-file", 7},
		{"owner-removal", 8},
	} {
		cases = append(cases, renderModelInput{mutation.name, []byte{1, 0, mutation.opcode, 0, 1}})
	}
	cases = append(cases,
		renderModelInput{"composed-input-and-identity", []byte{1, 0, 0, 0, 1, 2, 0, 2, 3, 0, 3}},
		renderModelInput{"composed-chart-inputs", []byte{1, 5, 0, 0, 1, 1, 0, 2}},
		renderModelInput{"composed-owner-removal-orphan", []byte("9,Y000000")},
		renderModelInput{"ignored-input-path-move", []byte{0xc9, 0x58, 0x32}},
		renderModelInput{"remove-all-owners", []byte{0, 0x28, 0x59, 0x30, 0x30}},
		renderModelInput{"required-input-namespace-move", []byte{1, 0x32, 0x30, 0x30, 0x30, 0x31, 0x30, 0x31, 0x30}},
		renderModelInput{"last-owner-local-chart", []byte{1, 5, 8, 1, 0}},
	)
	for i, name := range []string{"disjoint-growth", "source-bijection", "reverse-edit", "equivalent-placement"} {
		cases = append(cases, renderModelInput{"metamorphic-" + name, []byte{255, byte(i)}})
	}
	return cases
}

func TestRenderTrees_IndependentModel(t *testing.T) {
	t.Parallel()
	t.Run(
		"work_final_and_retired",
		func(t *testing.T) {
			t.Parallel()
			o := newOrchestrator(
				t.Context(), &Config{RecordWork: true},
				&Tree{ID: manifest.TreeID{Cluster: "cluster", Side: manifest.TreeHead}}, task.NewBounded(2),
			)
			defer o.stop()
			o.index = &manifest.DiscoveryIndex{}
			o.rsc.Configure(base.Options{})
			o.rsc.StartLifecycle()
			owner := &manifest.ResourceSet{
				Name:              "owner",
				Namespace:         "apps",
				ResourcesTemplate: "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output}\ndata: {value: first}\n",
			}
			o.store.AddObject(owner)
			first := o.rsc.ReconcileNode(t.Context(), owner.Named(), manifest.DrainNone, schedule.Result{})
			next := *owner
			next.ResourcesTemplate = strings.ReplaceAll(owner.ResourcesTemplate, "first", "final")
			o.store.AddObject(&next)
			assert.FatalIf(t, first.Valid(), "%v", "first execution retained superseded declaration authority")
			final := o.rsc.ReconcileNode(t.Context(), owner.Named(), manifest.DrainNone, schedule.Result{})
			first.Retire()
			assert.FatalIf(t, !final.Valid(), "%v", "final live execution is invalid")
			result, err := o.collectResult(nil)
			assert.FatalIf(t, err != nil, "%v", err)
			{
				got := workCounts(result.Work, owner.Named(), manifest.WorkEvaluation)
				assert.FatalIf(t, got !=
					(manifest.WorkCounts{Attempts: 2, Completed: 2}), "retired and final work=%+v", got)
			}
			_, _, output := o.store.GetEmission(owner.Named())
			defer output.Finish()
			assert.FatalIf(
				t,
				len(output.Manifests) != 1 || output.Manifests[0].Document["data"].(map[string]any)["value"] != "final",
				"accepted output=%+v",
				output.Manifests,
			)
			final.Close()
			{
				got := workCounts(result.Work, owner.Named(), manifest.WorkEvaluation)
				assert.FatalIf(t, got.Completed != 2, "result borrowed execution counters: %+v", got)
			}
		},
	)
	for _, tc := range renderModelInputs() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fixture := decodeRenderModelCase(tc.input)
			if _, err := checkRenderModel(t, &fixture); err != nil {
				failRenderModel(t, tc.input, &fixture, err)
			}
		})
	}
}

func TestIndependentModel_StaticRoots(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"removed-owner", "suspended-owner", "replacement-owner", "other-source-owner"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			testIndependentModelStaticRootsCase(t, &name)
		},
		)
	}
}

func TestIndependentModel_ObservationRejects(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, want string }{
		{"missing-side", "missing render side"},
		{"missing-artifact", "terminal artifact"},
		{"pending-producer", "terminal artifact"},
		{"incomplete-emission", "inventory incomplete"},
		{"extra-store-object", "unexpected store identity"},
		{"extra-store-producer", "unexpected store identity"},
		{"unselected-emission", "unselected producer has artifact"},
		{"unselected-output", "output from unselected"},
		{"ownerless-extra-document", "object membership"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testIndependentModelObservationRejectsCase(t, &tc)
		},
		)
	}
}

func TestIndependentModel_BaselineUnavailable(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"shallow", "missing-tree"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			fixture := decodeRenderModelCase([]byte{1, 0})
			root := t.TempDir()
			repo := testutil.GitInit(t, root)
			writeRenderModelWorld(t, root, fixture.before)
			baseInput := testutil.GitCommitAll(
				t,
				repo,
				"before",
				&gogit.CommitOptions{
					Author: &object.Signature{Name: "fixture", Email: "fixture@example.test", When: time.Unix(0, 0)},
				},
			)
			selected := baseInput.String()
			if mode == "shallow" {
				{
					err := repo.Storer.SetShallow([]plumbing.Hash{baseInput})
					assert.FatalIf(t, err != nil, "%v", err)
				}
				selected = ""
			} else {
				commit, err := repo.CommitObject(baseInput)
				assert.FatalIf(t, err != nil, "%v", err)
				oid := commit.TreeHash.String()
				{
					err := os.Remove(filepath.Join(root, ".git", "objects", oid[:2], oid[2:]))
					assert.FatalIf(t, err != nil, "%v", err)
				}
			}
			owner, err := sourcecache.New(t.Context(), "")
			assert.FatalIf(t, err != nil, "%v", err)
			t.Cleanup(func() {
				{
					localErr := owner.Close()
					assert.ErrorIf(t, localErr != nil, "%v", localErr)
				}
			})
			result, err := baseline.AutoResolve(t.Context(), filepath.Join(root, fixture.scan), selected, owner)
			assert.FatalIf(
				t,
				!errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux),
				"unavailable %s baseline accepted: result=%+v err=%v",
				mode,
				result,
				err,
			)
			assert.FatalIf(t, result != nil &&
				(result.BaseInput != nil && result.BaseInput.FS != nil || result.Source != nil && result.Source.FS != nil), "unavailable %s returned usable render inputs: %+v", mode, result)
			t.Logf("%s: baseline admission unavailable, no changed render claimed: %v", mode, err)
		})
	}
}

func renameRenderModelSource(w modelWorld, binding string) modelWorld {
	out := modelWorld{files: make(map[modelPath]modelFile), producers: make(map[modelID]modelProducer)}
	old := renderModelSource(w)
	next := old
	next.binding = binding
	rename := func(id modelID) modelID {
		if id.source == old {
			id.source = next
		}
		if id.kind == "GitRepository" && id.name == old.binding {
			id.name = binding
		}
		return id
	}
	for file, content := range w.files {
		file.source = next
		content.body = bytes.ReplaceAll(content.body, []byte("name: "+old.binding), []byte("name: "+binding))
		out.files[file] = content
	}
	for id, p := range cloneModelWorld(w).producers { //nolint:gocritic // Map iteration preserves the value snapshot without an extra lookup.
		p.declaration = bytes.ReplaceAll(p.declaration, []byte("name: "+old.binding), []byte("name: "+binding))
		for i := range p.reads {
			p.reads[i].file.source = next
		}
		for _, refs := range [][]modelID{p.content, p.ordering, p.sources} {
			for i := range refs {
				refs[i] = rename(refs[i])
			}
		}
		if p.emitter != nil {
			localID := rename(*p.emitter)
			p.emitter = &localID
		}
		for i := range p.documents {
			p.documents[i].id.modelID = rename(p.documents[i].id.modelID)
		}
		out.producers[rename(id)] = p
	}
	return out
}

func relocateRenderModel(w modelWorld) modelWorld {
	out := cloneModelWorld(w)
	out.files = make(map[modelPath]modelFile)
	for file, content := range w.files {
		file.path = "nested/" + file.path
		content.body = bytes.ReplaceAll(content.body, []byte("path: ./"), []byte("path: ./nested/"))
		out.files[file] = content
	}
	for id, p := range out.producers { //nolint:gocritic // Map iteration preserves the value snapshot without an extra lookup.
		p.declaration = bytes.ReplaceAll(p.declaration, []byte("path: ./"), []byte("path: ./nested/"))
		for i := range p.reads {
			p.reads[i].file.path = "nested/" + p.reads[i].file.path
		}
		out.producers[id] = p
	}
	return out
}

func testIndependentModelStaticRootsCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	before := newRenderModelWorld("cluster")
	owner := addRenderModelKS(before, "app", "flux-system", "apps/app")
	after := cloneModelWorld(before)
	p := after.producers[owner]
	delete(after.producers, owner)
	want := true
	switch *testCaseInput {
	case "suspended-owner":
		p.suspended = true
		after.producers[owner] = p
		want = false
	case "replacement-owner":
		next := owner
		next.name = "replacement"
		after.producers[next] = p
		want = false
	case "other-source-owner":
		next := owner
		next.source.binding = "other"
		for i := range p.reads {
			p.reads[i].file.source = next.source
		}
		after.producers[next] = p
	}
	got := renderModelStaticRoots(after, before)
	if want {
		assert.FatalIf(
			t,
			!reflect.DeepEqual(got, before.producers[owner].documents),
			"orphan inventory: want=%+v got=%+v",
			before.producers[owner].documents,
			got,
		)
	} else {
		assert.FatalIf(t, len(got) != 0, "owned package promoted: %+v", got)
	}
}

func testIndependentModelObservationRejectsCase(
	t *testing.T,
	testCaseInput *struct {
		name string

		want string
	},
) {
	t.Helper()

	world := newRenderModelWorld("cluster")
	id := addRenderModelKS(world, "app", "flux-system", "apps/app")
	obj := &manifest.Kustomization{Name: "app", Namespace: "flux-system"}
	side := Rendered{
		Orchestrator: &Orchestrator{store: store.New(), tasks: task.NewBounded(2)},
		Result:       &Result{},
	}
	side.store.AddObject(obj)
	side.store.UpdateStatus(obj.Named(), store.StatusInfo{Status: store.StatusReady})
	if testCaseInput.name != "missing-artifact" && testCaseInput.name != "incomplete-emission" && testCaseInput.
		name != "unselected-output" {
		side.store.RecordEmitted(obj.Named(), &store.Emission{Complete: true}, nil)
	}
	selected := false
	switch testCaseInput.name {
	case "missing-side":
		side = Rendered{}
	case "pending-producer":
		side.store.UpdateStatus(obj.Named(), store.StatusInfo{Status: store.StatusPending})
	case "incomplete-emission":
		side.store.RecordEmitted(obj.Named(), &store.Emission{}, nil)
	case "extra-store-object":
		side.store.AddObject(&manifest.ConfigMap{Name: "extra", Namespace: "flux-system"})
	case "extra-store-producer":
		side.store.AddObject(&manifest.Kustomization{Name: "extra", Namespace: "flux-system"})
	case "unselected-emission", "unselected-output":
		selected = true
		side.loader = admissiontest.New(
			t,
			admissiontest.Config{Changes: change.NewSet(nil), Index: nil, Inventory: nil, Root: ""},
		)
		admissiontest.Commit(t, side.loader, nil)
		side.filter = side.loader.Options.Filter
		if testCaseInput.name == "unselected-output" {
			side.Result.Manifests = []manifest.DocumentBatch{
				{
					Parent: obj,
					Documents: []map[string]any{
						{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "extra"}},
					},
				},
			}
		}
	case "ownerless-extra-document":
		side.Result.Manifests = []manifest.DocumentBatch{
			{
				Documents: []map[string]any{
					{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "extra"}},
				},
			},
		}
	}
	got, _, err := observeRenderModel(world, side, selected)
	if testCaseInput.name == "ownerless-extra-document" {
		assert.FatalIf(t, err != nil, "%v", err)
		err = compareModelObservation(
			modelObservation{producers: map[modelID]modelStatus{id: {terminal: true}}},
			got,
		)
	}
	assert.FatalIf(
		t,
		err == nil || !strings.Contains(err.Error(), testCaseInput.want),
		"%s accepted: want=%q got=%v",
		testCaseInput.name,
		testCaseInput.want,
		err,
	)
}

func removeAbsentRenderModelFiles(root string, present, other modelWorld) error {
	for file := range present.files {
		if file.source != renderModelSource(present) {
			continue
		}
		if _, ok := other.files[file]; !ok {
			if err := os.Remove(filepath.Join(root, file.path)); err != nil {
				return err
			}
		}
	}
	return nil
}

func marshalRenderModelManifests(before, after []manifest.DocumentBatch) ([]byte, error) {
	var canonical []any
	for _, batches := range [][]manifest.DocumentBatch{before, after} {
		for _, batch := range batches {
			if batch.Parent != nil && batch.Parent.Named().ReportHidden() {
				continue
			}
			var id manifest.NamedResource
			if batch.Parent != nil {
				id = batch.Parent.Named()
			}
			canonical = append(canonical, struct {
				ID        manifest.NamedResource
				Documents []map[string]any
			}{id, batch.Documents})
		}
	}
	return json.Marshal(canonical)
}

func checkDeletedRenderModelDocuments(name string, deleted modelSet, documents []modelDocument) error {
	for _, id := range modelIDs(deleted) {
		if name == "after" &&
			slices.ContainsFunc(documents, func(d modelDocument) bool { return d.id.modelID == id }) {
			return fmt.Errorf("deleted object remains: %s", id)
		}
	}
	return nil
}

func writeRenderModelStatus(evidence *strings.Builder, name string, admitted modelSet, changed *Rendered) {
	for _, id := range modelIDs(admitted) {
		info, _ := changed.store.GetStatus(namedRenderModelID(&id))
		fmt.Fprintf(
			evidence,
			"%s %s status=%+v artifact=%T\n",
			name,
			id,
			info,
			changed.store.GetArtifact(namedRenderModelID(&id), store.ArtifactContent),
		)
	}
}

func compareRenderModelSides(observed [2]bool, reference, actual *[2]modelObservation) error {
	if observed[0] && observed[1] {
		reference[1].deleted = modelDeletedDocuments(reference[0].documents, reference[1].documents)
		actual[1].deleted = modelDeletedDocuments(actual[0].documents, actual[1].documents)
	}
	if observed[1] {
		if err := compareModelObservation(reference[1], actual[1]); err != nil {
			return fmt.Errorf("changed-after: %w", err)
		}
	}
	return nil
}

func commitRenderModelSources(t *testing.T, root string, before, after modelWorld) (plumbing.Hash, error) {
	t.Helper()
	repo := testutil.GitInit(t, root)
	if err := removeAbsentRenderModelFiles(root, after, before); err != nil {
		return plumbing.ZeroHash, err
	}
	writeRenderModelWorld(t, root, before)
	hash := testutil.GitCommitAll(
		t,
		repo,
		"before",
		&gogit.CommitOptions{Author: &object.Signature{Name: "fixture", Email: "fixture@example.test"}},
	)
	if err := removeAbsentRenderModelFiles(root, before, after); err != nil {
		return plumbing.ZeroHash, err
	}
	writeRenderModelWorld(t, root, after)
	return hash, nil
}

func compareInitialRenderModelObservation(index int, name string, reference, changed modelObservation) error {
	if index == 0 {
		if err := compareModelObservation(reference, changed); err != nil {
			return fmt.Errorf("changed-%s: %w", name, err)
		}
	}
	return nil
}

type committedModel struct {
	before Tree
	after  Tree
}

func (c *treeConfig) prepareCommittedRenderModel(
	t *testing.T,
	root, scan string,
	worlds [2]*modelWorld,
) (committedModel, error) {
	t.Helper()
	hash, commitErr := commitRenderModelSources(t, root, *worlds[0], *worlds[1])
	if commitErr != nil {
		return committedModel{before: Tree{}, after: Tree{}}, commitErr
	}
	baseInput, resolveErr := baseline.AutoResolve(t.Context(), filepath.Join(root, scan), hash.String(), c.Cache)
	if resolveErr != nil {
		return committedModel{before: Tree{}, after: Tree{}}, resolveErr
	}
	c.BaseInput, c.Input, c.ExternalChanges = baseInput.BaseInput, baseInput.Source, baseInput.Changes
	return committedModel{
		before: Tree{
			RepoRoot: baseInput.BaseInput.LocalPath,
			Path:     baseInput.BaseScanPath,
			SelfURLs: baseInput.SelfURLs,
		},
		after: Tree{
			RepoRoot: baseInput.Source.LocalPath,
			Path:     baseInput.ScanPath,
			SelfURLs: baseInput.SelfURLs,
		},
	}, nil
}

func checkRenderModelPairError(pairErr, beforeErr, afterErr error) error {
	if (pairErr != nil) != (beforeErr != nil || afterErr != nil) {
		return fmt.Errorf("pair error lost side errors: %w", pairErr)
	}
	return nil
}
