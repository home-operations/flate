package orchestrator

import (
	"bytes"
	"fmt"
	"maps"
	"slices"
	"strings"
)

func extendRenderModel(f *renderModelCase, seed int, variant byte) {
	if variant < 4 {
		return
	}
	source := renderModelSource(f.before)
	app := modelID{
		source:    source,
		group:     "kustomize.toolkit.fluxcd.io",
		kind:      "Kustomization",
		namespace: "flux-system",
		name:      "app",
	}
	if variant == 14 || variant == 15 {
		other := newRenderModelWorld("other")
		pinned := addRenderModelKS(other, "pinned", "flux-system", "apps/app")
		coordinate := pinned.source
		coordinate.revision, coordinate.root = "pinned", "artifact"
		for _, w := range []modelWorld{f.before, f.after} {
			for file, content := range other.files {
				file.source = coordinate
				if strings.HasPrefix(file.path, "flux/") {
					file.source = source
				}
				content.body = bytes.ReplaceAll(content.body, []byte("value: old"), []byte("value: pin"))
				w.files[file] = content
			}
			p := other.producers[pinned]
			p.reads = slices.Clone(p.reads)
			for i := range p.reads {
				p.reads[i].file.source = coordinate
			}
			p.sources = []modelID{renderModelSourceID(coordinate)}
			p.documents = []modelDocument{
				{
					id: modelObjectID{
						modelID: modelID{source: coordinate, kind: "ConfigMap", name: "pinned-output"},
						version: "v1",
					},
					sdk: map[string]any{
						"apiVersion": "v1",
						"kind":       "ConfigMap",
						"metadata": map[string]any{
							"name": "pinned-output",
							"labels": map[string]any{
								"kustomize.toolkit.fluxcd.io/name":      "pinned",
								"kustomize.toolkit.fluxcd.io/namespace": "flux-system",
							},
						},
						"data": map[string]any{"value": "pin"},
					},
				},
			}
			id := pinned
			id.source = coordinate
			w.producers[id] = p
			declaration := putRenderModelFile(
				w,
				"flux/source-other.yaml",
				"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: other, namespace: flux-system}\nspec: {url: 'file:///model-other', ref: {commit: before}}\n",
			)
			w.producers[renderModelSourceID(coordinate)] = modelProducer{reads: []modelRead{{file: declaration}}}
		}
		if variant == 15 {
			file := modelPath{source: source, path: "apps/app/cm.yaml"}
			f.after.files[file] = f.before.files[file]
			input := modelPath{source: coordinate, path: "apps/app/cm.yaml"}
			writeRenderModelFile(
				f,
				input,
				bytes.ReplaceAll(f.after.files[input].body, []byte("value: pin"), []byte("value: new")),
			)
			declaration := modelPath{source: source, path: "flux/source-other.yaml"}
			writeRenderModelFile(
				f,
				declaration,
				bytes.ReplaceAll(f.after.files[declaration].body, []byte("commit: before"), []byte("commit: after")),
			)
			id := pinned
			id.source = coordinate
			p := f.after.producers[id]
			p.documents = slices.Clone(p.documents)
			doc := maps.Clone(p.documents[0].sdk.(map[string]any))
			doc["data"] = map[string]any{"value": "new"}
			p.documents[0].sdk = doc
			f.after.producers[id] = p
		}
		return
	}
	if returned := extendRenderModelSegment(&f, &variant, &source, &app); returned {
		return
	}
	extendRenderModelTail(
		&extendRenderModelTailInputs{variant: &variant, source: &source, f: &f, app: &app, seed: &seed},
	)
}

func extendRenderModelTail(tailCaseInput *extendRenderModelTailInputs) {
	switch *tailCaseInput.variant {
	case 16:
		file := modelPath{source: (*tailCaseInput.source), path: "apps/app/cm.yaml"}
		writeRenderModelFile(
			(*tailCaseInput.f),
			file,
			bytes.Replace(
				(*tailCaseInput.f).after.files[file].body,
				[]byte("apiVersion: v1"),
				[]byte("apiVersion: example.test/v1"),
				1,
			),
		)
		p := (*tailCaseInput.f).after.producers[(*tailCaseInput.app)]
		p.documents = slices.Clone(p.documents)
		p.documents[0].id.group = "example.test"
		(*tailCaseInput.f).
			after.producers[(*tailCaseInput.app)] = p
	case 17:
		for _, w := range []modelWorld{(*tailCaseInput.f).before, (*tailCaseInput.f).after} {
			file := putRenderModelFile(
				w,
				"apps/app/foreign.yaml",
				"apiVersion: example.test/v1\nkind: ConfigMap\nmetadata: {name: app-output}\nspec: {value: foreign}\n",
			)
			putRenderModelFile(w, "apps/app/kustomization.yaml", "resources: [cm.yaml, foreign.yaml]\n")
			p := w.producers[(*tailCaseInput.app)]
			p.reads = append(p.reads, modelRead{file: file})
			p.documents = append(
				p.documents,
				modelDocument{
					id: modelObjectID{
						modelID: modelID{
							source: (*tailCaseInput.source),
							group:  "example.test",
							kind:   "ConfigMap",
							name:   "app-output",
						},
						version: "v1",
					},
				},
			)
			w.producers[(*tailCaseInput.app)] = p
		}
	case 18:
		tailCaseInput.addGeneratedValues()
	case 19:
		for _, w := range []modelWorld{(*tailCaseInput.f).before, (*tailCaseInput.f).after} {
			changeRenderModelDeclaration(
				w, tailCaseInput.app, "  path:",
				"  postBuild:\n    substituteFrom: [{kind: ConfigMap, name: absent, optional: true}]\n  path:",
			)
			p := w.producers[(*tailCaseInput.app)]
			p.reads = append(
				p.reads,
				modelRead{file: modelPath{source: (*tailCaseInput.source), path: "flux/absent.yaml"}, optional: true},
			)
			w.producers[(*tailCaseInput.app)] = p
		}
	case 20:
		for _, w := range []modelWorld{(*tailCaseInput.f).before, (*tailCaseInput.f).after} {
			changeRenderModelDeclaration(
				w, tailCaseInput.app, "  path:",
				"  postBuild:\n    substituteFrom: [{kind: ConfigMap, name: absent}]\n  path:",
			)
			p := w.producers[(*tailCaseInput.app)]
			p.reads = append(
				p.reads,
				modelRead{file: modelPath{source: (*tailCaseInput.source), path: "flux/absent.yaml"}},
			)
			p.content = []modelID{
				{source: (*tailCaseInput.source), kind: "ConfigMap", namespace: "flux-system", name: "absent"},
			}
			p.documents = nil
			w.producers[(*tailCaseInput.app)] = p
		}
	case 22:
		tailCaseInput.addLateSourceAlias()
	case 23:
		tailCaseInput.changeSourceBinding()
	case 24:
		(*tailCaseInput.f).
			committed = false
		(*tailCaseInput.f).
			after = relocateRenderModel((*tailCaseInput.f).after)
		(*tailCaseInput.f).
			beforeScan, (*tailCaseInput.f).scan = (*tailCaseInput.f).scan, "nested/"+(*tailCaseInput.f).scan
	case 25:
		old := modelID{
			source:    (*tailCaseInput.source),
			group:     tailCaseInput.app.group,
			kind:      tailCaseInput.app.kind,
			namespace: tailCaseInput.app.namespace,
			name:      "parent",
		}
		next := old
		next.name = "new-owner"
		p := (*tailCaseInput.f).after.producers[old]
		p.declaration = bytes.Replace(p.declaration, []byte("  name: parent\n"), []byte("  name: new-owner\n"), 1)
		(*tailCaseInput.f).
			after.producers[old] = p
		rekeyRenderModel((*tailCaseInput.f).after, old, next)
		delete(
			(*tailCaseInput.f).after.files,
			modelPath{source: (*tailCaseInput.source), path: "flux/parent-flux-system.yaml"},
		)
		writeRenderModelFile(
			(*tailCaseInput.f),
			modelPath{source: (*tailCaseInput.source), path: "flux/new-owner-flux-system.yaml"},
			p.declaration,
		)
	case 26:
		for i, w := range []modelWorld{(*tailCaseInput.f).before, (*tailCaseInput.f).after} {
			value := []string{"old", "new"}[i]
			putRenderModelFile(
				w,
				"parents/kustomization.yaml",
				"namespace: flux-system\nresources: [child.yaml]\npatches:\n- target: {kind: Kustomization, name: app}\n  patch: |-\n    - op: add\n      path: /spec/postBuild\n      value:\n        substitute: {EXTRA: "+value+"}\n",
			)
			putRenderModelFile(
				w,
				"apps/app/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: app-output}\ndata: {value: '${EXTRA}'}\n",
			)
			p := w.producers[(*tailCaseInput.app)]
			p.declaration = append(p.declaration, []byte("  postBuild: {substitute: {EXTRA: "+value+"}}\n")...)
			w.producers[(*tailCaseInput.app)] = p
		}
	case 27:
		(*tailCaseInput.f).
			before, (*tailCaseInput.f).after = (*tailCaseInput.f).after, (*tailCaseInput.f).before
	case 28:
		extendRenderModel((*tailCaseInput.f), (*tailCaseInput.seed), 12)
		(*tailCaseInput.f).
			before, (*tailCaseInput.f).after = (*tailCaseInput.f).after, (*tailCaseInput.f).before
	case 29:
		parent := modelID{
			source:    (*tailCaseInput.source),
			group:     tailCaseInput.app.group,
			kind:      tailCaseInput.app.kind,
			namespace: tailCaseInput.app.namespace,
			name:      "parent",
		}
		file := modelPath{source: (*tailCaseInput.source), path: "parents/kustomization.yaml"}
		writeRenderModelFile(
			(*tailCaseInput.f),
			file,
			bytes.Replace(
				(*tailCaseInput.f).after.files[file].body,
				[]byte("namespace: flux-system"),
				[]byte("namespace: ns-1"),
				1,
			),
		)
		p := (*tailCaseInput.f).after.producers[(*tailCaseInput.app)]
		p.declaration = bytes.Replace(
			p.declaration,
			[]byte("  namespace: flux-system\n"),
			[]byte("  namespace: ns-1\n"),
			1,
		)
		(*tailCaseInput.f).
			after.producers[(*tailCaseInput.app)] = p
		next := (*tailCaseInput.app)
		next.namespace = "ns-1"
		rekeyRenderModel((*tailCaseInput.f).after, (*tailCaseInput.app), next)
		pp := (*tailCaseInput.f).after.producers[parent]
		pp.documents = slices.Clone(pp.documents)
		pp.documents[0].id.namespace = "ns-1"
		(*tailCaseInput.f).
			after.producers[parent] = pp
	case 21:
		id := (*tailCaseInput.app)
		id.group, id.kind = "fluxcd.controlplane.io", "ResourceSet"
		crd := modelID{
			source: (*tailCaseInput.source),
			group:  "apiextensions.k8s.io",
			kind:   "CustomResourceDefinition",
			name:   "widgets.example.test",
		}
		for i, w := range []modelWorld{(*tailCaseInput.f).before, (*tailCaseInput.f).after} {
			delete(w.producers, (*tailCaseInput.app))
			delete(w.files, modelPath{source: (*tailCaseInput.source), path: "flux/app-flux-system.yaml"})
			declaration := fmt.Sprintf(
				"apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSet\nmetadata: {name: app, namespace: flux-system, annotations: {revision: '%d'}}\nspec:\n  dependsOn: [{apiVersion: apiextensions.k8s.io/v1, kind: CustomResourceDefinition, name: widgets.example.test}]\n  resources: []\n",
				i,
			)
			putRenderModelFile(w, "flux/rs.yaml", declaration)
			w.producers[id] = modelProducer{declaration: []byte(declaration), ordering: []modelID{crd}}
		}
	case 4:
		for _, w := range []modelWorld{(*tailCaseInput.f).before, (*tailCaseInput.f).after} {
			p := w.producers[(*tailCaseInput.app)]
			delete(w.files, p.reads[1].file)
			input := putRenderModelFile(w, "apps/app/value.txt", "old\n")
			putRenderModelFile(
				w,
				"apps/app/kustomization.yaml",
				"generatorOptions: {disableNameSuffixHash: true}\nconfigMapGenerator:\n- name: app-output\n  files: [value.txt]\n",
			)
			p.reads[1].file = input
			w.producers[(*tailCaseInput.app)] = p
		}
		writeRenderModelFile(
			(*tailCaseInput.f),
			modelPath{source: (*tailCaseInput.source), path: "apps/app/value.txt"},
			[]byte("new\n"),
		)
	case 10:
		mutateRenderModel((*tailCaseInput.f), 8, 1, 0)
	case 11:
		for _, w := range []modelWorld{(*tailCaseInput.f).before, (*tailCaseInput.f).after} {
			p := w.producers[(*tailCaseInput.app)]
			patch := putRenderModelFile(
				w,
				"shared/patch.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: app-output}\ndata: {patched: old}\n",
			)
			putRenderModelFile(
				w,
				"apps/app/kustomization.yaml",
				"resources: [cm.yaml]\npatches: [{path: ../../shared/patch.yaml}]\n",
			)
			p.reads = append(p.reads, modelRead{file: patch})
			w.producers[(*tailCaseInput.app)] = p
		}
		writeRenderModelFile((*tailCaseInput.f), modelPath{source: (*tailCaseInput.source), path: "shared/patch.yaml"},
			[]byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: app-output}\ndata: {patched: new}\n"),
		)
	case 12:
		for _, w := range []modelWorld{(*tailCaseInput.f).before, (*tailCaseInput.f).after} {
			p := w.producers[(*tailCaseInput.app)]
			p.reads = p.reads[:2]
			p.documents = p.documents[:1]
			w.producers[(*tailCaseInput.app)] = p
			delete(w.files, modelPath{source: (*tailCaseInput.source), path: "apps/app/removed.yaml"})
			putRenderModelFile(w, "apps/app/kustomization.yaml", "resources: [cm.yaml]\n")
		}
		file := modelPath{source: (*tailCaseInput.source), path: "apps/app/cm.yaml"}
		content := (*tailCaseInput.f).before.files[file]
		content.included = false
		(*tailCaseInput.f).
			before.files[file] = content
		putRenderModelFile((*tailCaseInput.f).before, ".sourceignore", "apps/app/cm.yaml\n")
		putRenderModelFile((*tailCaseInput.f).after, ".sourceignore", "")
	case 13:
		if (*tailCaseInput.seed) == 0 {
			p := (*tailCaseInput.f).after.producers[(*tailCaseInput.app)]
			p.emitter = nil
			(*tailCaseInput.f).
				after.producers[(*tailCaseInput.app)] = p
			putRenderModelFile((*tailCaseInput.f).after, "flux/app-flux-system.yaml", string(p.declaration))
			parent := modelID{
				source:    (*tailCaseInput.source),
				group:     tailCaseInput.app.group,
				kind:      tailCaseInput.app.kind,
				namespace: tailCaseInput.app.namespace,
				name:      "parent",
			}
			delete((*tailCaseInput.f).after.producers, parent)
			delete(
				(*tailCaseInput.f).after.files,
				modelPath{source: (*tailCaseInput.source), path: "flux/parent-flux-system.yaml"},
			)
			delete(
				(*tailCaseInput.f).after.files,
				modelPath{source: (*tailCaseInput.source), path: "parents/kustomization.yaml"},
			)
			delete(
				(*tailCaseInput.f).after.files,
				modelPath{source: (*tailCaseInput.source), path: "parents/child.yaml"},
			)
		}
	}
}

func writeRenderModelFile(f *renderModelCase, file modelPath, body []byte) {
	f.after.files[file] = modelFile{body: body, included: renderModelFileIncluded(f.after, file)}
}

func renderModelFileIncluded(w modelWorld, file modelPath) bool {
	ignore := w.files[modelPath{source: file.source, path: ".sourceignore"}]
	return !slices.Contains(strings.Split(string(ignore.body), "\n"), file.path)
}

//nolint:gocritic // Value parameters preserve inlining and the private next snapshot retained by emitter records.
func rekeyRenderModel(w modelWorld, old, next modelID) {
	producer := w.producers[old]
	delete(w.producers, old)
	w.producers[next] = producer
	for id, p := range w.producers { //nolint:gocritic // Map iteration preserves the value snapshot without an extra lookup.
		for _, refs := range [][]modelID{p.content, p.ordering, p.sources} {
			for i := range refs {
				if refs[i] == old {
					refs[i] = next
				}
			}
		}
		if p.emitter != nil && *p.emitter == old {
			p.emitter = &next
		}
		w.producers[id] = p
	}
}

type extendRenderModelTailInputs struct {
	variant *byte

	source *modelSource

	f **renderModelCase

	app *modelID

	seed *int
}

func extendRenderModelSegment(f **renderModelCase, variant *byte, source *modelSource, app *modelID) bool {
	if (*variant) >= 5 && (*variant) <= 9 {
		for _, w := range []modelWorld{(*f).before, (*f).after} {
			delete(w.files, modelPath{source: (*source), path: "flux/app-flux-system.yaml"})
			delete(w.files, modelPath{source: (*source), path: "apps/app/kustomization.yaml"})
			delete(w.files, modelPath{source: (*source), path: "apps/app/cm.yaml"})
			delete(w.producers, (*app))
			hr := (*app)
			hr.kind, hr.group = "HelmRelease", "helm.toolkit.fluxcd.io"
			declaration := "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: app, namespace: flux-system}\nspec:\n  interval: 10m\n  chart:\n    spec:\n      chart: charts/app\n      sourceRef: {kind: GitRepository, name: cluster, namespace: flux-system}\n"
			putRenderModelFile(w, "flux/app-flux-system.yaml", declaration)
			chart := putRenderModelFile(w, "charts/app/Chart.yaml", "apiVersion: v2\nname: app\nversion: 1.0.0\n")
			values := putRenderModelFile(w, "charts/app/values.yaml", "value: old\n")
			template := putRenderModelFile(
				w,
				"charts/app/templates/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: app-output, namespace: flux-system}\ndata: {value: {{ .Values.value | quote }}}\n",
			)
			w.producers[hr] = modelProducer{
				declaration: []byte(declaration),
				reads:       []modelRead{{file: chart}, {file: values}, {file: template}},
				sources:     []modelID{renderModelSourceID((*source))},
				documents: []modelDocument{
					{
						id: modelObjectID{
							modelID: modelID{
								source:    (*source),
								kind:      "ConfigMap",
								namespace: "flux-system",
								name:      "app-output",
							},
							version:          "v1",
							namespacePresent: true,
						},
					},
				},
			}
		}
		hr := (*app)
		hr.kind, hr.group = "HelmRelease", "helm.toolkit.fluxcd.io"
		switch *variant {
		case 5:
			file := modelPath{source: (*source), path: "charts/app/templates/cm.yaml"}
			writeRenderModelFile(
				(*f),
				file,
				bytes.Replace(
					(*f).before.files[file].body,
					[]byte(".Values.value"),
					[]byte(".Values.value | upper"),
					1,
				),
			)
		case 6:
			file := modelPath{source: (*source), path: "charts/app/values.yaml"}
			writeRenderModelFile((*f), file, []byte("value: new\n"))
		case 7:
			for _, w := range []modelWorld{(*f).before, (*f).after} {
				p := w.producers[hr]
				p.declaration = append(p.declaration, []byte("  values: {value: old}\n")...)
				w.producers[hr] = p
				putRenderModelFile(w, "flux/app-flux-system.yaml", string(p.declaration))
			}
			changeRenderModelDeclaration((*f).after, &hr, "value: old", "value: new")
		case 8:
			for _, w := range []modelWorld{(*f).before, (*f).after} {
				file := putRenderModelFile(
					w,
					"flux/values.yaml",
					"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: values, namespace: flux-system}\ndata:\n  values.yaml: |\n    value: old\n",
				)
				p := w.producers[hr]
				p.reads = append(p.reads, modelRead{file: file})
				p.declaration = append(p.declaration, []byte("  valuesFrom: [{kind: ConfigMap, name: values}]\n")...)
				w.producers[hr] = p
				putRenderModelFile(w, "flux/app-flux-system.yaml", string(p.declaration))
			}
			file := modelPath{source: (*source), path: "flux/values.yaml"}
			writeRenderModelFile(
				(*f),
				file,
				bytes.Replace((*f).before.files[file].body, []byte("value: old"), []byte("value: new"), 1),
			)
		case 9:
			file := modelPath{source: (*source), path: "charts/app/charts/child-1.0.0.tgz"}
			for _, w := range []modelWorld{(*f).before, (*f).after} {
				p := w.producers[hr]
				putRenderModelFile(
					w,
					"charts/app/Chart.yaml",
					"apiVersion: v2\nname: app\nversion: 1.0.0\ndependencies: [{name: child, version: 1.0.0, repository: 'file://charts/child'}]\n",
				)
				w.files[file] = modelFile{body: renderModelSubchart("old"), included: true}
				p.reads = append(p.reads, modelRead{file: file})
				p.documents = append(
					p.documents,
					modelDocument{
						id: modelObjectID{
							modelID: modelID{
								source:    (*source),
								kind:      "ConfigMap",
								namespace: "flux-system",
								name:      "child-output",
							},
							version:          "v1",
							namespacePresent: true,
						},
					},
				)
				w.producers[hr] = p
			}
			writeRenderModelFile((*f), file, renderModelSubchart("new"))
		}
		return true
	}
	return false
}

func (s *extendRenderModelTailInputs) addGeneratedValues() {
	for _, w := range []modelWorld{(*s.f).before, (*s.f).after} {
		data := addRenderModelKS(w, "z-data", "flux-system", "data")
		putRenderModelFile(
			w,
			"data/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: generated-values, namespace: flux-system}\ndata: {VALUE: old}\n",
		)
		d := w.producers[data]
		d.documents = []modelDocument{
			{
				id: modelObjectID{
					modelID: modelID{
						source:    (*s.source),
						kind:      "ConfigMap",
						namespace: "flux-system",
						name:      "generated-values",
					},
					version:          "v1",
					namespacePresent: true,
				},
			},
		}
		w.producers[data] = d
		p := w.producers[(*s.app)]
		p.content = []modelID{data}
		p.declaration = bytes.Replace(
			p.declaration,
			[]byte("  path:"),
			[]byte("  postBuild:\n    substituteFrom: [{kind: ConfigMap, name: generated-values}]\n  path:"),
			1,
		)
		w.producers[(*s.app)] = p
		putRenderModelFile(
			w,
			"parents/child.yaml",
			strings.Replace(string(p.declaration), "  namespace: flux-system\n", "", 1),
		)
		putRenderModelFile(
			w,
			"apps/app/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: app-output}\ndata: {value: '${VALUE}'}\n",
		)
	}
	file := modelPath{source: (*s.source), path: "data/cm.yaml"}
	writeRenderModelFile((*s.f), file,
		bytes.ReplaceAll((*s.f).after.files[file].body, []byte("VALUE: old"), []byte("VALUE: new")),
	)
}

func (s *extendRenderModelTailInputs) addLateSourceAlias() {
	(*s.f).
		selfURLs = []string{"git://fixture.invalid/cluster"}
	other := (*s.source)
	other.binding = "late"
	parent := modelID{
		source:    (*s.source),
		group:     s.app.group,
		kind:      s.app.kind,
		namespace: s.app.namespace,
		name:      "parent",
	}
	for _, w := range []modelWorld{(*s.f).before, (*s.f).after} {
		p := w.producers[(*s.app)]
		p.declaration = bytes.ReplaceAll(
			p.declaration,
			[]byte("name: cluster, namespace: flux-system"),
			[]byte("name: late, namespace: flux-system"),
		)
		p.reads = slices.Clone(p.reads)
		for i := range p.reads {
			file := p.reads[i].file
			p.reads[i].file.source = other
			w.files[p.reads[i].file] = w.files[file]
		}
		p.sources = []modelID{renderModelSourceID(other)}
		p.documents = slices.Clone(p.documents)
		for i := range p.documents {
			p.documents[i].id.source = other
		}
		id := (*s.app)
		id.source = other
		delete(w.producers, (*s.app))
		w.producers[id] = p
		putRenderModelFile(
			w,
			"parents/child.yaml",
			strings.Replace(string(p.declaration), "  namespace: flux-system\n", "", 1),
		)
		putRenderModelFile(
			w,
			"parents/kustomization.yaml",
			"namespace: flux-system\nresources: [child.yaml, source.yaml]\n",
		)
		declaration := putRenderModelFile(
			w,
			"parents/source.yaml",
			"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: late, namespace: flux-system}\nspec: {url: 'git://fixture.invalid/cluster'}\n",
		)
		pp := w.producers[parent]
		pp.reads = append(pp.reads, modelRead{file: declaration})
		object := renderModelSourceID(other)
		object.source = (*s.source)
		pp.documents = append(
			pp.documents,
			modelDocument{id: modelObjectID{modelID: object, version: "v1", namespacePresent: true}},
		)
		w.producers[parent] = pp
		w.producers[renderModelSourceID(other)] = modelProducer{emitter: &parent}
	}
}

func (s *extendRenderModelTailInputs) changeSourceBinding() {
	other := (*s.source)
	other.binding = "other"
	p := (*s.f).after.producers[(*s.app)]
	p.declaration = bytes.ReplaceAll(
		p.declaration,
		[]byte("name: cluster, namespace: flux-system"),
		[]byte("name: other, namespace: flux-system"),
	)
	p.reads = slices.Clone(p.reads)
	for i := range p.reads {
		file := p.reads[i].file
		p.reads[i].file.source = other
		(*s.f).
			after.files[p.reads[i].file] = (*s.f).after.files[file]
	}
	p.sources = []modelID{renderModelSourceID(other)}
	p.documents = slices.Clone(p.documents)
	for i := range p.documents {
		p.documents[i].id.source = other
	}
	id := (*s.app)
	id.source = other
	delete((*s.f).after.producers, (*s.app))
	(*s.f).
		after.producers[id] = p
	(*s.f).
		after.producers[renderModelSourceID(other)] = modelProducer{}
	writeRenderModelFile(
		(*s.f),
		modelPath{source: (*s.source), path: "flux/app-flux-system.yaml"},
		p.declaration,
	)
}
