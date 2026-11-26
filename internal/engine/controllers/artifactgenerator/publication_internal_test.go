package artifactgenerator

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	sourcewatcherv1 "github.com/fluxcd/source-watcher/api/v2/v1beta1"

	"github.com/home-operations/flate/internal/core/artifactsyntax"
	"github.com/home-operations/flate/internal/engine/depwait"
	"github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/source/gittree"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/engine/task"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestArtifactGenerator_ManagedPublication(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, skip string
		disk       bool
	}{
		{name: "memory"},
		{name: "disk", disk: true},
		{name: "skip_namespaces", skip: "Namespace"},
		{name: "skip_artifacts", skip: manifest.KindExternalArtifact},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var root string
			if tc.disk {
				root = t.TempDir()
			}
			owner, err := cache.New(t.Context(), root)
			assert.NoError(t, err)
			defer func() { assert.NoError(t, owner.Close()) }()
			for run := range 2 {
				if run != 0 && tc.disk {
					assert.NoError(t, owner.Close())
					owner, err = cache.New(t.Context(), root)
					assert.NoError(t, err)
				}
				input := fs.FS(fstest.MapFS{
					"one-meta": {
						Data: []byte("metadata: {labels: {team: first}, annotations: {notes: \"one\\ntwo\"}}"),
					},
					"two-meta":  {Data: []byte("metadata: {labels: {team: second}}")},
					"one-bytes": {Data: []byte("side: first")},
					"two-bytes": {Data: []byte("side: second")},
				})
				if run != 0 {
					input = &observedFS{FS: input, failPath: "one-meta"}
				}
				testManagedPublication(t, owner, input, tc.skip)
			}
		})
	}
	t.Run("admitted_identity", func(t *testing.T) {
		t.Parallel()
		st := store.New()
		c := New(st, task.NewBounded(2), nil)
		fixture := generatorForTest(t, nil)
		raw := fixture.object
		id := raw.Named()
		assert.Equal(t, c.Owns(id), true)
		foreign := id
		foreign.Group = "foreign.example"
		assert.Equal(t, c.Owns(foreign), false)
		st.AddObject(raw, fixture.document)
		ex := &execution{Controller: c}
		ex.run.Init(c.Controller, raw.Named())
		t.Cleanup(ex.run.Close)
		p := generatorPublication{execution: ex, id: raw.Named(), raw: raw, attempt: &store.Emission{}}
		first, err := (&manifest.ExternalArtifact{Name: "one"}).RenderDocument()
		assert.NoError(t, err)
		// Typed decoding accepts the capitalized key. Admission uses the authored key for its default.
		first["metadata"].(map[string]any)["Namespace"] = "other"
		second, err := (&manifest.ExternalArtifact{Name: "two", Namespace: "tenant"}).RenderDocument()
		assert.NoError(t, err)
		third, err := (&manifest.ExternalArtifact{Name: "one", Namespace: "tenant"}).RenderDocument()
		assert.NoError(t, err)
		docs := []manifest.DocOccurrence{
			{Document: first, Ordinal: 1}, {Document: second, Ordinal: 2}, {Document: third, Ordinal: 3},
		}
		const digest = "shared-digest"
		payloads := []*store.SourceArtifact{
			{Content: "first", Digest: digest, FS: fstest.MapFS{}},
			{Content: "second", Digest: digest, FS: fstest.MapFS{}},
			{Content: "first", Digest: digest, FS: fstest.MapFS{}},
		}
		assert.NoError(t, p.publish(t.Context(), docs, payloads, true))
		for i, id := range []manifest.NamedResource{
			managedArtifact("one", "apps"), managedArtifact("two", "tenant"), managedArtifact("one", "tenant"),
		} {
			assert.Equal(t, p.attempt.Children[i].Obj.Named(), id)
			got, ok := st.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact)
			assert.FatalIf(t, !ok, "no payload for %v", id)
			assert.Equal(t, got.Key, id.NamespacedName())
			assert.Equal(t, got.Content, payloads[i].Content)
			assert.Equal(t, got.Digest, digest)
			wantPath, _ := gittree.Label(gittree.LabelOptions{Root: "/", Key: digest, Source: id})
			assert.Equal(t, got.LocalPath, wantPath)
		}
		assert.Equal(t, payloads[0].LocalPath == payloads[2].LocalPath, false)
		assert.Equal(t, payloads[0].Key == payloads[2].Key, false)
	})
}

func TestArtifactGenerator_StaleSourceAfterGate(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		filesystem fs.FS
	}{
		{name: "stale source", filesystem: fstest.MapFS{}},
		{name: "descriptor without filesystem"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			fixture := generatorForTest(t, []any{managedOutput("bundle", "apps", "@repo/", "@artifact/")})
			raw := fixture.object
			spec, err := raw.ArtifactGeneratorSpec()
			assert.NoError(t, err)
			prepared, err := validateSpec(&spec, raw.Namespace)
			assert.NoError(t, err)
			source := &manifest.GitRepository{Name: "repo", Namespace: "apps"}
			credential := &manifest.ConfigMap{Name: "settings", Namespace: "apps"}
			st.AddObject(raw, fixture.document)
			st.AddObject(source)
			st.AddObject(credential)

			var sourceReads store.ReadSet
			sourceReads.Init(st, source.Named(), nil)
			t.Cleanup(sourceReads.Retire)
			sourceReads.GetObject(credential.Named())
			st.SetArtifact(
				source.Named(),
				&store.SourceArtifact{Content: "input", FS: tc.filesystem, Reads: &sourceReads},
			)
			controller := New(st, task.NewBounded(2), nil)
			ex := &execution{Controller: controller}
			ex.run.Init(controller.Controller, raw.Named())
			t.Cleanup(ex.run.Close)
			_, readable := ex.run.Reads.ReadableSourceArtifact(source.Named(), store.ArtifactContent)
			assert.Equal(t, readable, tc.filesystem != nil)

			if tc.filesystem != nil {
				updated := *credential
				updated.Data = map[string]any{"value": "updated"}
				controller.Tasks.Go(
					t.Context(),
					"replace source input",
					func(context.Context) { st.AddObject(&updated) },
				)
				controller.Tasks.BlockTillDone()
				assert.Equal(t, sourceReads.Valid(), false)
			}

			_, err = ex.generatorInputs(raw, &prepared)
			blocked, ok := errors.AsType[*depwait.BlockedError](err)
			assert.FatalIf(t, !ok, "expected a blocked dependency, got %v", err)
			assert.Diff(t, blocked.Deps, []manifest.NamedResource{source.Named()})
			assert.Equal(t, ex.run.Reads.Valid(), tc.filesystem == nil)
		})
	}
}

func testManagedPublication(t *testing.T, owner *cache.Cache, input fs.FS, skip string) {
	t.Helper()
	st := store.New()
	setManagedGeneratorSource(st, input)
	c := New(st, task.NewBounded(2), owner)
	if skip != "" {
		c.SkipKinds = []string{skip}
	}
	outputs := make([]any, 0, 2)
	for _, name := range []string{"one", "two"} {
		output := managedOutput(name, "tenant", "@repo/"+name+"-meta", "@artifact/cloudbuild.yaml")
		output["copy"] = append(output["copy"].([]any),
			map[string]any{"from": "@repo/" + name + "-bytes", "to": "@artifact/workload.yaml"},
			map[string]any{"from": "@repo/" + name + "-bytes", "to": "@artifact/appveyor.yml"},
		)
		output["inputsFrom"] = "@artifact/appveyor.yml"
		outputs = append(outputs, output)
	}
	const path = "@artifact/cloudbuild.yaml"
	gen := addManagedGenerator(
		t,
		st,
		managedGeneratorFixture{name: "generator", outputs: outputs, managed: map[string]any{
			"allowedNames": []any{"ns == 'tenant'"},
			"metadata": map[string]any{"fromSource": map[string]any{
				"path": path, "allowedLabels": map[string]any{"team": "true"},
				"allowedAnnotations": map[string]any{"notes": "true"},
			}},
		}},
	)
	declaration := st.GetObject(gen).(*manifest.RawObject)
	spec, err := declaration.ArtifactGeneratorSpec()
	assert.NoError(t, err)
	assert.Equal(t, spec.CommonMetadata == nil, true)

	runForTest(t, c, gen)
	status, _ := st.GetStatus(gen)
	assert.FatalIf(t, status.Status != store.StatusReady, "generator status = %+v", status)

	ns := manifest.NamedResource{Kind: "Namespace", Name: "tenant"}
	namespace := st.GetObject(ns)
	assert.FatalIf(t, namespace == nil, "no cluster-scoped Namespace")
	assert.Equal(t, namespace.Named(), ns)
	_, namespaceDoc, assessment := st.GetEmission(ns)
	defer assessment.Finish()
	metadata := namespaceDoc["metadata"].(map[string]any)
	assert.Diff(t, metadata["labels"], map[string]any{
		"team": "first", "app.kubernetes.io/managed-by": "source-watcher",
		"source.extensions.fluxcd.io/generator": "generator",
	})
	assert.Diff(t, metadata["annotations"], map[string]any{"notes": "one\ntwo"})
	assert.Equal(t, st.GetArtifact(ns, store.ArtifactContent) == nil, true)

	for _, name := range []string{"one", "two"} {
		id := managedArtifact(name, "tenant")
		side := "first"
		if name == "two" {
			side = "second"
		}
		payload, ok := st.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact)
		assert.FatalIf(t, !ok, "no payload for %v", id)
		assert.Equal(t, payload.Key, id.NamespacedName())
		data, err := fs.ReadFile(payload.FS, "workload.yaml")
		assert.NoError(t, err)
		assert.Equal(t, string(data), "side: "+side)
		for _, ignored := range []string{"cloudbuild.yaml", "appveyor.yml"} {
			_, err := fs.Stat(payload.FS, ignored)
			assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
		}
		child := st.GetObject(id).(*manifest.ExternalArtifact)
		assert.Equal(t, len(child.Annotations), 0)
		assert.Equal(t, string(child.ExportedInputs["side"].Raw), `"`+side+`"`)
	}
	emission := st.GetArtifact(gen, store.ArtifactContent).(*store.Emission)
	assert.Equal(t, emission.Complete && emission.Fingerprint != "", true)
	assert.Equal(t, len(emission.Children), 3)
	ordinals := make([]int, 0, len(emission.Manifests))
	for i := range emission.Manifests {
		doc := &emission.Manifests[i]
		ordinals = append(ordinals, doc.Ordinal)
	}
	switch skip {
	case "Namespace":
		assert.Diff(t, ordinals, []int{1, 2})
	case manifest.KindExternalArtifact:
		assert.Diff(t, ordinals, []int{0})
	default:
		assert.Diff(t, ordinals, []int{0, 1, 2})
	}
}

func TestArtifactGenerator_ManagedFailureOrder(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, allowed, selector, secondSource, want string
	}{
		{"staged_read", "true", "@artifact/cloudbuild.yaml", "@repo/second-file", `namespace reconciliation failed: failed to read namespace metadata file "@artifact/cloudbuild.yaml": read cloudbuild.yaml: invalid argument`},
		{"later_build", "true", "@artifact/cloudbuild.yaml", "@repo/{app}/workload.yaml", "evaluate second: source 'Second/workload.yaml' does not exist"},
		{"rejected_target", "false", "@artifact/cloudbuild.yaml", "@repo/second-file", `namespace "tenant" is not allowed by .spec.managedNamespaces.allowedNames`},
		{"conflicting_paths", "true", "@artifact/cloudbuild.yaml/{app}", "@repo/second-file", `namespace reconciliation failed: namespace metadata path "@artifact/cloudbuild.yaml/{app}" resolves to "@artifact/cloudbuild.yaml/First" and "@artifact/cloudbuild.yaml/Second" for different artifacts targeting namespace "tenant", it must only use namespace-level captures`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			owner, err := cache.New(t.Context(), "")
			assert.NoError(t, err)
			defer func() { assert.NoError(t, owner.Close()) }()
			c := New(st, task.NewBounded(2), owner)
			files := fstest.MapFS{
				"First/cloudbuild.yaml/First/item":   {Data: []byte("file")},
				"Second/cloudbuild.yaml/Second/item": {Data: []byte("file")},
				"second-file":                        {Data: []byte("second")},
				"First/workload.yaml":                {Data: []byte("first")},
			}
			setManagedGeneratorSource(st, files)
			output := managedOutput("{app}", "tenant", "@repo/{app}/cloudbuild.yaml", "@artifact/")
			second := output["copy"].([]any)
			second = append(second, map[string]any{"from": tc.secondSource, "to": "@artifact/workload.yaml"})
			output["copy"] = second
			id := addManagedGenerator(
				t,
				st,
				managedGeneratorFixture{
					name:    "generator",
					outputs: []any{output},
					pattern: "@repo/{app}",
					managed: map[string]any{
						"allowedNames": []any{tc.allowed},
						"metadata":     map[string]any{"fromSource": map[string]any{"path": tc.selector}},
					},
				},
			)
			runForTest(t, c, id)
			status, _ := st.GetStatus(id)
			assert.FatalIf(t, status.Status != store.StatusFailed || !strings.Contains(status.Message, tc.want),
				"generator status = %+v, want %q", status, tc.want)
			assert.Equal(t, st.GetObject(manifest.NamedResource{Kind: "Namespace", Name: "tenant"}) == nil, true)
			assert.Equal(t, st.GetObject(managedArtifact("first", "tenant")) == nil, true)
			assert.Equal(t, st.GetArtifact(managedArtifact("first", "tenant"), store.ArtifactContent) == nil, true)
		})
	}
}

func TestArtifactGenerator_InvalidNamespaceMetadataStrategy(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		metadata map[string]any
		index    int
		strategy string
	}{
		{
			name: "unknown_for_unmanaged_target",
			metadata: map[string]any{"from": []any{
				map[string]any{"namespace": "tenant", "strategy": sourcewatcherv1.MergeStrategy},
				map[string]any{"namespace": "other", "strategy": "Overide"},
			}},
			index:    1,
			strategy: "Overide",
		},
		{
			name: "empty_with_source",
			metadata: map[string]any{
				"from":       []any{map[string]any{"namespace": "tenant", "strategy": ""}},
				"fromSource": map[string]any{"path": "@repo/metadata.yaml"},
			},
		},
		{
			name: "omitted",
			metadata: map[string]any{"from": []any{
				map[string]any{"namespace": "tenant"},
			}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			owner, err := cache.New(t.Context(), "")
			assert.NoError(t, err)
			defer func() { assert.NoError(t, owner.Close()) }()
			c := New(st, task.NewBounded(2), owner)
			input := &observedFS{FS: fstest.MapFS{
				"file":          {Data: []byte("bytes")},
				"metadata.yaml": {Data: []byte("kind: NamespaceMetadata\n")},
			}}
			setManagedGeneratorSource(st, input)
			id := addManagedGenerator(t, st, managedGeneratorFixture{
				name:    "generator",
				outputs: []any{managedOutput("payload", "tenant", "@repo/file", "@artifact/file")},
				managed: map[string]any{
					"allowedNames": []any{"true"},
					"metadata":     tc.metadata,
				},
			})
			obj := st.GetObject(id).(*manifest.RawObject)
			var got error
			c.Tasks.Go(t.Context(), "generator", func(ctx context.Context) {
				run := &execution{Controller: c}
				run.run.Init(c.Controller, id)
				defer run.run.Close()
				got = run.reconcile(ctx, obj, manifest.DrainNone)
			})
			c.Tasks.BlockTillDone()
			assert.Equal(t, c.Tasks.Failures(), 0)
			assert.ErrorIf(t, !errors.Is(got, manifest.ErrInput), "reconcile error = %v, want ErrInput", got)
			assert.ErrorIf(t, !errors.Is(got, manifest.ErrFlux), "reconcile error = %v, want ErrFlux", got)
			want := fmt.Sprintf(
				"spec.managedNamespaces.metadata.from[%d].strategy: unsupported strategy %q (allowed: %s, %s, %s)",
				tc.index,
				tc.strategy,
				sourcewatcherv1.NamespaceMetadataResetStrategy,
				sourcewatcherv1.OverrideStrategy,
				sourcewatcherv1.MergeStrategy,
			)
			assert.ErrorIf(
				t,
				got == nil || !strings.Contains(got.Error(), want),
				"reconcile error = %v, want %q",
				got,
				want,
			)
			assert.Equal(t, input.opens.Load(), 0)
			payload := managedArtifact("payload", "tenant")
			assert.Equal(t, st.GetArtifact(payload, store.ArtifactContent) == nil, true)
			assert.Equal(t, st.GetObject(payload) == nil, true)
			assert.Equal(t, st.GetObject(manifest.NamedResource{Kind: "Namespace", Name: "tenant"}) == nil, true)
		})
	}
}

func TestArtifactGenerator_ManagedProducerConflict(t *testing.T) {
	t.Parallel()
	for _, order := range [][]string{{"a", "z"}, {"z", "a"}} {
		t.Run(strings.Join(order, "_"), func(t *testing.T) {
			t.Parallel()
			st := store.New()
			owner, err := cache.New(t.Context(), "")
			assert.NoError(t, err)
			defer func() { assert.NoError(t, owner.Close()) }()
			c := New(st, task.NewBounded(2), owner)
			setManagedGeneratorSource(st, fstest.MapFS{"file": {Data: []byte("bytes")}})
			for _, name := range order {
				id := addManagedGenerator(t, st, managedGeneratorFixture{
					name: name, outputs: []any{managedOutput(name, "tenant", "@repo/file", "@artifact/file")},
					managed: map[string]any{"allowedNames": []any{"true"}, "metadata": map[string]any{
						"from": []any{
							map[string]any{
								"namespace": "*",
								"strategy":  sourcewatcherv1.MergeStrategy,
								"labels":    map[string]any{"winner": name},
							},
						},
					}},
				})
				runForTest(t, c, id)
			}
			ns := manifest.NamedResource{Kind: "Namespace", Name: "tenant"}
			_, doc, assessment := st.GetEmission(ns)
			defer assessment.Finish()
			assert.Equal(t, doc["metadata"].(map[string]any)["labels"].(map[string]any)["winner"], "z")
			warnings := st.Warnings()
			assert.FatalIf(t, len(warnings) != 1, "warnings = %+v", warnings)
			assert.Equal(t, warnings[0].Category, manifest.WarnConflictingProducers)
			assert.Equal(t, warnings[0].Resource, ns)
			assert.Equal(t, warnings[0].Count, 1)
			assert.Equal(t, slices.Contains(warnings[0].Related, manifest.NamedResource{
				Group:     manifest.SourceExtensionsDomain,
				Kind:      manifest.KindArtifactGenerator,
				Namespace: "apps",
				Name:      "a",
			}), true)
		})
	}
}

func TestCaptureNamespaceMetadata_RetriesRead(t *testing.T) {
	t.Parallel()
	const path = "cloudbuild.yaml"
	cause := errors.New("staged read failed")
	files := fstest.MapFS{path: {Data: []byte("{}")}, "workload.yaml": {Data: []byte("kind: ConfigMap")}}
	fail := true
	base := stagedReadFS{MapFS: files, open: func(name string) error {
		if name == path && fail {
			return cause
		}
		return nil
	}}
	e := evaluation{}
	request := cache.Composition{
		Base:   cache.CompositionInput{FS: base, Content: "immutable"},
		Policy: "fixed",
		Captures: []cache.StagedCapture{
			{
				Purpose: metadataCapture,
				Path:    "@artifact/" + path,
				Read:    e.captureNamespaceMetadata(artifactsyntax.ParseSelector("@artifact/" + path)),
			},
		},
	}
	root := t.TempDir()
	owner, err := cache.New(t.Context(), root)
	assert.NoError(t, err)
	defer func() { assert.NoError(t, owner.Close()) }()
	var key string
	for _, phase := range []string{"fail", "retry", "memory_replay", "disk_replay"} {
		if phase == "disk_replay" {
			assert.NoError(t, owner.Close())
			owner, err = cache.New(t.Context(), root)
			assert.NoError(t, err)
		}
		output, descriptor, current, composeErr := owner.Compose(
			t.Context(),
			request,
			&cache.Policy{MaxAge: cache.Forever},
		)
		if phase == "fail" {
			_, typed := errors.AsType[*namespaceMetadataReadError](composeErr)
			assert.Equal(t, typed && errors.Is(composeErr, cause), true)
			assert.Equal(t, output == nil && current == "", true)
			fail = false
			continue
		}
		assert.NoError(t, composeErr)
		assert.Equal(t, descriptor.Annotations[cache.StagedCaptureAnnotation(metadataCapture)], `{"Data":"e30="}`)
		if phase == "retry" {
			key = current
			fail = true
		} else {
			assert.Equal(t, current, key)
		}
	}
}

type stagedReadFS struct {
	fstest.MapFS

	open func(string) error
}

func (f stagedReadFS) Open(name string) (fs.File, error) {
	if err := f.open(name); err != nil {
		return nil, err
	}
	return f.MapFS.Open(name)
}

func managedOutput(name, namespace, from, to string) map[string]any {
	return map[string]any{
		"name": name, "namespace": namespace,
		"copy": []any{map[string]any{"from": from, "to": to}},
	}
}

func managedArtifact(name, namespace string) manifest.NamedResource {
	return manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindExternalArtifact,
		Namespace: namespace,
		Name:      name,
	}
}

type managedGeneratorFixture struct {
	name, pattern string
	outputs       []any
	managed       map[string]any
}

func addManagedGenerator(t *testing.T, st *store.Store, fixture managedGeneratorFixture) manifest.NamedResource {
	t.Helper()
	doc := generatorForTest(t, fixture.outputs).document
	metadata := doc["metadata"].(map[string]any)
	metadata["name"], metadata["uid"] = fixture.name, fixture.name
	spec := doc["spec"].(map[string]any)
	spec["managedNamespaces"] = fixture.managed
	if fixture.pattern != "" {
		spec["pathPattern"] = fixture.pattern
	}
	parsed, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
	assert.NoError(t, err)
	st.AddObject(parsed.Object, parsed.Document)
	return parsed.Object.Named()
}

func setManagedGeneratorSource(st *store.Store, input fs.FS) {
	repo := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "apps",
		Name:      "repo",
	}
	st.SetArtifact(repo, &store.SourceArtifact{Key: "input", Content: "input", FS: input})
}
