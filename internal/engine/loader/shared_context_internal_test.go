package loader

import (
	"maps"
	"path/filepath"
	"slices"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"sigs.k8s.io/kustomize/kyaml/filesys"

	"github.com/home-operations/flate/internal/engine/change"
	"github.com/home-operations/flate/internal/engine/depwait"
	internalkustomize "github.com/home-operations/flate/internal/engine/kustomize/krusty"
	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestAdmission_SharedProducerNamespaces(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	st.SetArtifact(
		manifest.BootstrapSourceID,
		&store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: fstest.MapFS{
			"owners.yaml": {Data: []byte(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: selected, namespace: flux-system}
spec: {path: ./selected, targetNamespace: selected, components: [../shared]}
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: sibling, namespace: flux-system}
spec: {path: ./sibling, targetNamespace: sibling, components: [../shared]}
`)},
			"selected/kustomization.yaml": {Data: []byte("resources: [payload.yaml]\n")},
			"selected/payload.yaml": {
				Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: payload}\ndata: {value: changed}\n"),
			},
			"sibling/kustomization.yaml": {Data: []byte("resources: []\n")},
			"shared/kustomization.yaml": {
				Data: []byte(
					"apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nresources: [external.yaml]\n",
				),
			},
			"shared/external.yaml": {
				Data: []byte(
					"apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata: {name: credentials}\nspec: {target: {name: credentials}, dataFrom: [{extract: {key: credentials}}]}\n",
				),
			},
		}},
	)
	l := New(st)
	l.SourceRoot = root

	l.Options.Changes = detectChanges(t, []string{"selected/payload.yaml"}, []string{"selected/payload.yaml"})
	configureSelection(t, l, selection.Request{Changed: true})
	_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
	proposal, err := prepareSelection(
		t.Context(),
		t,
		l,
		selection.PrepareRequest{Changes: l.Options.Changes, Invocation: true},
	)
	assert.NoError(t, err)
	selected := manifest.NamedResource{
		Group:     manifest.FluxKustomizeDomain,
		Kind:      manifest.KindKustomization,
		Namespace: "flux-system",
		Name:      "selected",
	}
	sibling := selected
	sibling.Name = "sibling"
	assert.Equal(t, slices.Contains(proposal.Selected, selected), true)
	assert.Equal(t, slices.Contains(proposal.Needed, sibling), false)
}

func TestAdmission_MissingInputWaitsForKnownOwner(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                                        string
		complete, optional, generated, allowMissing bool
		production                                  string
	}{{name: "pending"}, {name: "complete", complete: true}, {name: "optional", complete: true, optional: true}, {name: "generated", complete: true, generated: true}, {name: "allowed", complete: true, allowMissing: true}, {name: "self", complete: true, production: "self"}, {name: "peer", complete: true, production: "peer"}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testAdmissionMissingInputWaitsForKnownOwnerCase(
				t,
				&tc,
			)
		},
		)
	}
}

func TestAdmission_DeclaredChartSourceNamespace(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	st.SetArtifact(
		manifest.BootstrapSourceID,
		&store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: fstest.MapFS{
			"flux/kustomization.yaml": {Data: []byte("namespace: apps\nresources: [owner.yaml]\n")},
			"flux/owner.yaml": {
				Data: []byte(
					"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: owner}\nspec: {path: ./app, targetNamespace: apps}\n",
				),
			},
			"app/kustomization.yaml": {Data: []byte("resources: [source.yaml, release.yaml]\n")},
			"app/source.yaml": {
				Data: []byte(
					"apiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata: {name: chart}\nspec: {url: oci://fixture.invalid/chart}\n",
				),
			},
			"app/release.yaml": {
				Data: []byte(
					"apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: consumer}\nspec: {chartRef: {kind: OCIRepository, name: chart}}\n",
				),
			},
		}},
	)
	opaque := &manifest.HelmRelease{Name: "opaque", Namespace: "apps"}
	st.AddObject(opaque)
	l := New(st)
	l.SourceRoot = root
	configureSelection(t, l, selection.Request{Changed: true})

	l.Options.Changes = detectChanges(t, []string{"app/release.yaml"}, []string{"app/release.yaml"})
	_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
	id := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindOCIRepository,
		Namespace: "apps",
		Name:      "chart",
	}
	record, known := l.Index.Lookup(id)
	if !known {
		t.Fatalf("declared chart source is missing: index=%+v", l.Index.Records)
	}
	assert.Equal(t, record.Authored.Namespace, "")
	proposal, err := prepareSelection(
		t.Context(),
		t,
		l,
		selection.PrepareRequest{Changes: l.Options.Changes, Invocation: true},
	)
	assert.NoError(t, err)
	assert.Equal(t, slices.Contains(proposal.Needed, id), true)
	assert.Equal(t, slices.Contains(proposal.Needed, opaque.Named()), false)
}

func testAdmissionMissingInputWaitsForKnownOwnerCase(
	t *testing.T,
	testCaseInput *struct {
		name         string
		complete     bool
		optional     bool
		generated    bool
		allowMissing bool
		production   string
	},
) {
	t.Helper()

	root := t.TempDir()
	st := store.New()
	st.SetArtifact(
		manifest.BootstrapSourceID,
		&store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: fstest.MapFS{}},
	)
	kind := manifest.KindConfigMap
	if testCaseInput.generated {
		kind = manifest.KindSecret
	}
	consumer := &manifest.HelmRelease{
		Name:       "consumer",
		Namespace:  "apps",
		ValuesFrom: []manifest.ValuesReference{{Kind: kind, Name: "missing", Optional: testCaseInput.optional}},
	}
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "./owned"}
	opaque := &manifest.HelmRelease{Name: "opaque", Namespace: "apps"}
	for _, object := range []manifest.BaseManifest{consumer, owner, opaque} {
		st.AddObject(object)
	}
	if testCaseInput.complete {
		output := &store.Emission{Complete: true}
		if testCaseInput.generated {
			output.Children = []store.Child{
				{
					Obj: &manifest.RawObject{
						APIVersion: "external-secrets.io/v1",
						Kind:       "ExternalSecret",
						Name:       "missing",
						Namespace:  "apps",
					},
				},
			}
		}
		st.RecordEmitted(owner.Named(), output, nil)
		st.UpdateStatus(owner.Named(), store.StatusInfo{Status: store.StatusReady})
	}
	l := New(st)
	l.SourceRoot = root

	if testCaseInput.allowMissing {
		l.Options.Missing = depwait.NewMissingPolicy([]string{kind})
	}
	configureSelection(t, l, selection.Request{Roots: []manifest.NamedResource{consumer.Named()}})
	l.Index = &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		consumer.Named(): {
			Parent:      owner.Named(),
			Productions: []manifest.Production{{Owner: owner.Named(), Source: manifest.BootstrapSourceID}},
		},
	}}
	if testCaseInput.production != "" {
		producer := owner.Named()
		if testCaseInput.production == "self" {
			producer = consumer.Named()
		}
		bare := manifest.NamedResource{Kind: kind, Name: "missing"}
		l.Index.Records[bare] = manifest.DiscoveryRecord{Productions: []manifest.Production{{
			Owner:  producer,
			Source: manifest.BootstrapSourceID, Authored: bare, File: "missing.yaml", Path: "missing.yaml",
		}}}
	}
	occurrence := new(occurrenceFacts).compactOccurrence(opaque, projectedProducerForTest(opaque))
	occurrence.Declaration.Source = root
	l.locations = map[manifest.NamedResource]authoredOccurrence{opaque.Named(): occurrence}
	l.Index.Records[opaque.Named()] = manifest.DiscoveryRecord{}
	proposal, err := prepareSelection(
		t.Context(),
		t,
		l,
		selection.PrepareRequest{Changes: l.Options.Changes, Invocation: true},
	)
	assert.NoError(t, err)
	assert.Equal(t, slices.Contains(proposal.Needed, owner.Named()), true)
	assert.Equal(
		t,
		slices.Contains(
			proposal.Needed,
			opaque.Named(),
		),
		testCaseInput.complete && !testCaseInput.optional && !testCaseInput.generated && !testCaseInput.allowMissing,
	)
	assert.Equal(t, slices.Contains(proposal.Selected, opaque.Named()), false)
	plain, err := prepareSelection(
		t.Context(),
		t,
		l,
		selection.PrepareRequest{Changes: l.Options.Changes, Generation: []store.Child{{Obj: consumer}}},
	)
	assert.NoError(t, err)
	assert.Equal(t, slices.Contains(plain.Needed, owner.Named()), true)
	wantOpaque := testCaseInput.complete && !testCaseInput.optional && !testCaseInput.generated &&
		!testCaseInput.allowMissing
	assert.Equal(t, slices.Contains(plain.Needed, opaque.Named()), wantOpaque)
	assertAdmissionCapturedMembership(t, l, consumer, plain)
}

func assertAdmissionCapturedMembership(
	t *testing.T,
	l *Loader,
	consumer manifest.BaseManifest,
	plain *selection.Admission,
) {
	t.Helper()
	var reads store.ReadSet
	owner := l.Index.Records[consumer.Named()].Parent
	reads.Init(l.Store, owner, nil)
	defer reads.Close()
	for range 2 {
		captured, err := prepareSelection(t.Context(), t, l, selection.PrepareRequest{
			Changes:    l.Options.Changes,
			Generation: []store.Child{{Obj: consumer}},
			Reads:      &reads,
		})
		assert.NoError(t, err)
		assert.Diff(t, captured.Needed, plain.Needed)
		assert.Diff(t, captured.Selected, plain.Selected)
		assert.Diff(t, captured.Dirty, plain.Dirty)
	}
	assert.Equal(t, reads.ID(), owner)
}

func TestFilter_RequiredSourceProducerDemand(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"full", "forward", "reverse"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			files := fstest.MapFS{
				"cluster/ks.yaml": {Data: []byte(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: cluster-apps, namespace: flux-system}
spec: {path: ./apps, sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}}
`)},
				"apps/default/kustomization.yaml": {
					Data: []byte(
						"namespace: default\nresources: [nextflux/ks.yaml]\ntransformers: [transformer.yaml]\n",
					),
				},
				"apps/default/nextflux/ks.yaml": {Data: []byte(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: nextflux}
spec: {path: ./apps/default/nextflux/app, sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}}
`)},
				"apps/flux-system/kustomization.yaml": {
					Data: []byte(
						"namespace: flux-system\nresources: [instance/ks.yaml]\ntransformers: [transformer.yaml]\n",
					),
				},
				"apps/flux-system/instance/ks.yaml": {Data: []byte(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: flux-instance}
spec: {path: ./apps/flux-system/instance/ks, sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}}
`)},
				"apps/other/kustomization.yaml": {
					Data: []byte("namespace: other\nresources: [ks.yaml]\ntransformers: [transformer.yaml]\n"),
				},
				"apps/other/ks.yaml": {Data: []byte(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: unrelated}
spec: {path: ./unrelated, sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}}
`)},
				"apps/default/nextflux/app/kustomization.yaml": {
					Data: []byte("namespace: default\nresources: [helmrelease.yaml]\n"),
				},
				"apps/default/nextflux/app/helmrelease.yaml": {Data: []byte(`apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: nextflux}
spec:
  chartRef: {kind: OCIRepository, name: app-template, namespace: flux-system}
  values: {revision: before}
`)},
				"apps/flux-system/instance/ks/kustomization.yaml": {Data: []byte("resources: [ocirepository.yaml]\n")},
				"apps/flux-system/instance/ks/ocirepository.yaml": {
					Data: []byte(`apiVersion: source.toolkit.fluxcd.io/v1
kind: OCIRepository
metadata: {name: app-template}
spec: {url: oci://fixture.invalid/app-template}
`),
				},
				"unrelated/kustomization.yaml": {Data: []byte("resources: [configmap.yaml]\n")},
				"unrelated/configmap.yaml": {
					Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unrelated}\n"),
				},
			}
			files["apps/default/transformer.yaml"] = &fstest.MapFile{Data: []byte(`apiVersion: builtin
kind: NamespaceTransformer
metadata: {name: apply-ns, namespace: default}
fieldSpecs:
  - {path: spec/targetNamespace, group: kustomize.toolkit.fluxcd.io, kind: Kustomization, create: true}
`)}
			files["apps/flux-system/transformer.yaml"] = &fstest.MapFile{Data: []byte(`apiVersion: builtin
kind: NamespaceTransformer
metadata: {name: apply-ns, namespace: flux-system}
fieldSpecs:
  - {path: spec/targetNamespace, group: kustomize.toolkit.fluxcd.io, kind: Kustomization, create: true}
`)}
			files["apps/other/transformer.yaml"] = &fstest.MapFile{Data: []byte(`apiVersion: builtin
kind: NamespaceTransformer
metadata: {name: apply-ns, namespace: other}
fieldSpecs:
  - {path: spec/targetNamespace, group: kustomize.toolkit.fluxcd.io, kind: Kustomization, create: true}
`)}
			other := make(fstest.MapFS, len(files))
			maps.Copy(other, files)
			readerFile := "apps/default/nextflux/app/helmrelease.yaml"
			other[readerFile] = &fstest.MapFile{Data: []byte(`apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: nextflux}
spec:
  chartRef: {kind: OCIRepository, name: app-template, namespace: flux-system}
  values: {revision: after}
`)}
			before, after := &store.SourceArtifact{
				Provider:  store.ProviderInput,
				LocalPath: root,
				FS:        files,
			}, &store.SourceArtifact{
				Provider:  store.ProviderInput,
				LocalPath: root,
				FS:        other,
			}
			pair := map[string][2]*store.SourceArtifact{
				"full": {before, after}, "forward": {before, after}, "reverse": {after, before},
			}[mode]
			before, after = pair[0], pair[1]
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, after)
			st.AddObject(&manifest.Kustomization{
				Name:      "cluster-apps",
				Namespace: "flux-system",
				Path:      "./apps",
				SourceRef: kustomizev1.CrossNamespaceSourceReference{
					Kind:      manifest.KindGitRepository,
					Name:      "flux-system",
					Namespace: "flux-system",
				},
			})
			l := New(st)
			defer l.Close()
			l.SourceRoot = root
			changes, err := change.Detect(t.Context(), before, after, change.DetectConfig{})
			assert.NoError(t, err)
			assert.Diff(t, changes.Paths(), []string{readerFile})
			l.Options.Changes = map[string]*change.Set{"forward": changes, "reverse": changes}[mode]
			configureSelection(t, l, selection.Request{Changed: mode != "full"})
			_, err = l.Load(t.Context(), filepath.Join(root, "cluster"), manifest.NamedResource{}, nil)
			assert.NoError(t, err)
			_, err = l.Load(t.Context(), filepath.Join(root, "apps"), manifest.NamedResource{
				Group:     manifest.FluxKustomizeDomain,
				Kind:      manifest.KindKustomization,
				Namespace: "flux-system",
				Name:      "cluster-apps",
			}, nil)
			assert.NoError(t, err)
			assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
			reader := manifest.NamedResource{
				Group:     manifest.HelmReleaseDomain,
				Kind:      manifest.KindHelmRelease,
				Namespace: "default",
				Name:      "nextflux",
			}
			source := manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindOCIRepository,
				Namespace: "flux-system",
				Name:      "app-template",
			}
			producer := manifest.NamedResource{
				Group:     manifest.FluxKustomizeDomain,
				Kind:      manifest.KindKustomization,
				Namespace: "flux-system",
				Name:      "flux-instance",
			}
			unrelated := producer
			unrelated.Name, unrelated.Namespace = "unrelated", "other"
			occurrence, known := l.Occurrence(reader)
			assert.Equal(t, known, true)
			assert.Equal(t, occurrence.Authored.Namespace, "")
			assert.Equal(t, occurrence.Identity, reader)
			assert.Equal(t, l.Index.Records[reader].Path, filepath.Join(root, filepath.FromSlash(readerFile)))
			authoredSource := source
			authoredSource.Namespace = ""
			record, known := l.Index.Lookup(authoredSource)
			assert.FatalIf(t, !known, "authored source declaration is missing")
			assert.Equal(t, record.Authored.Namespace, "")
			assert.Equal(t, slices.ContainsFunc(record.Productions, func(p manifest.Production) bool {
				return p.Owner == producer && p.Source == manifest.BootstrapSourceID &&
					p.File == "apps/flux-system/instance/ks/ocirepository.yaml"
			}), true)
			t.Logf("reader=%s source=%s producer=%s source record=%+v", reader, source, producer, record)
			child := l.IndexedObject(reader, l.Index.Records[reader])
			if mode != "full" {
				child, err = l.OccurrenceObject(t.Context(), &occurrence, after)
				assert.NoError(t, err)
			}
			assert.FatalIf(t, child.Obj == nil, "reader declaration is unavailable")
			references := slices.Collect(manifest.References(child.Obj, manifest.ReferenceRelations))
			assert.Equal(t, slices.ContainsFunc(references, func(ref manifest.Reference) bool {
				return ref.NamedResource == source
			}), true)
			assert.Equal(t, slices.ContainsFunc(references, func(ref manifest.Reference) bool {
				return ref.NamedResource == source && !ref.ContentInput()
			}), false)
			assert.NoError(t, l.PromoteOrphans(t.Context()))
			assert.Equal(t, st.GetObject(source) == nil, true)
			assert.Equal(t, selection.Root(rootEvidenceForTest(l, source, nil)).Independent, false)
			fs := filesys.MakeFsInMemory()
			for name, file := range files {
				target := filepath.Join(root, name)
				assert.NoError(t, fs.MkdirAll(filepath.Dir(target)))
				assert.NoError(t, fs.WriteFile(target, file.Data))
			}
			owners := make([]store.Child, 0, 3)
			for _, namespace := range []string{"default", "flux-system", "other"} {
				dir := filepath.Join("apps", namespace)
				internalkustomize.BuildMutex.Lock()
				result, err := internalkustomize.Build(fs, filepath.Join(root, dir), false)
				internalkustomize.BuildMutex.Unlock()
				assert.NoError(t, err)
				data, err := result.AsYaml()
				assert.NoError(t, err)
				owners = append(owners, builtObjectsForTest(t, string(data))...)
			}
			publish := func(owner manifest.NamedResource, generation []store.Child, requireProducer bool) {
				proposal, err := prepareSelection(t.Context(), t, l, selection.PrepareRequest{
					Owner: owner, Generation: generation,
				})
				assert.NoError(t, err)
				assert.ErrorIf(t, requireProducer && !slices.Contains(proposal.Needed, producer),
					"reader emitted: required source producer lacks execution demand: needed=%v", proposal.Needed)
				_, committed, err := l.commitSelection(t.Context(), t, proposal, selection.Execution)
				assert.NoError(t, err)
				assert.Equal(t, committed, true)
				st.RecordEmitted(owner, &store.Emission{Children: generation, Complete: true}, nil)
			}
			demand := func(phase string, requireProducer bool) {
				proposal, err := prepareSelection(t.Context(), t, l, selection.PrepareRequest{Invocation: true})
				assert.NoError(t, err)
				assert.ErrorIf(t, !slices.Contains(proposal.Needed, reader), "%s: reader lacks execution demand", phase)
				assert.ErrorIf(t, requireProducer && !slices.Contains(proposal.Needed, producer),
					"%s: required source producer lacks execution demand: needed=%v", phase, proposal.Needed)
				assert.Equal(t, slices.Contains(proposal.Needed, source), true)
				assert.Equal(t, slices.Contains(proposal.Needed, unrelated), false)
				assert.Equal(t, slices.Contains(proposal.Selected, unrelated), false)
				assert.Equal(t, slices.Contains(proposal.Selected, source), false)
				proposal.Retire()
			}
			if mode == "full" {
				publish = func(owner manifest.NamedResource, generation []store.Child, _ bool) {
					st.RecordEmitted(owner, &store.Emission{Children: generation, Complete: true}, nil)
				}
				demand = func(_ string, _ bool) {
					decision := l.Options.Session.Snapshot()
					assert.Equal(t, decision.Reconcile(reader), true)
					assert.Equal(t, decision.Reconcile(producer), true)
					assert.Equal(t, decision.Reconcile(source), true)
				}
			}
			demand("indexed", true)
			st.RecordEmitted(producer, &store.Emission{Complete: false}, nil)
			demand("partial", true)
			st.RecordEmitted(manifest.NamedResource{
				Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization,
				Namespace: "flux-system", Name: "cluster-apps",
			}, &store.Emission{Children: owners, Complete: true}, nil)
			demand("owners emitted", true)
			if mode != "full" {
				readerOwner := manifest.NamedResource{
					Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization,
					Namespace: "default", Name: "nextflux",
				}
				child.Productions = l.Index.Records[reader].Productions
				publish(readerOwner, []store.Child{child}, true)
			}
			demand("reader emitted", true)
			producerIndex := slices.IndexFunc(
				owners,
				func(child store.Child) bool { return child.Obj.Named() == producer },
			)
			assert.FatalIf(t, producerIndex < 0, "built source producer declaration is unavailable")
			producerChild := owners[producerIndex]
			assert.Equal(t, producerChild.Obj.EmissionPolicy().Namespace, producer.Namespace)
			docs := builtObjectsForTest(t, string(files["apps/flux-system/instance/ks/ocirepository.yaml"].Data))
			parsed := ParseChildren(producerChild.Obj, []manifest.DocOccurrence{{Document: docs[0].Doc}},
				manifest.ParseDocOptions{}, nil)
			assert.Equal(t, len(parsed.Failures), 0)
			assert.FatalIf(t, len(parsed.Children) != 1, "source emission children=%d", len(parsed.Children))
			output := parsed.Children[0]
			assert.Equal(t, output.Obj.Named(), source)
			output.Productions = record.Productions
			publish(producer, []store.Child{output}, false)
			st.UpdateStatus(producer, store.StatusInfo{Status: store.StatusReady})
			assert.Equal(t, st.GetObject(source) != nil, true)
			demand("emitted", false)
		})
	}
}
