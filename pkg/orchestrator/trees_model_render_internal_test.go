package orchestrator

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"errors"
	"maps"
	"slices"
	"strings"

	"github.com/home-operations/flate/internal/testutil"
)

func renderModelSeeds() []string {
	return []string{
		"late-child-support", "source-qualified-path", "orphan-and-shared-input",
		"negative-read-and-ordering", "two-sided-delete-ignore", "shallow-and-unavailable", "empty-output",
	}
}

type renderModelCase struct {
	modelCase

	committed  bool
	scan       string
	beforeScan string
	selfURLs   []string
}

func renderModelSource(w modelWorld) modelSource {
	for _, id := range modelIDs(modelProducerIDs(w)) {
		if id.kind == "GitRepository" {
			return id.source
		}
	}
	panic("model source missing")
}

func renderModelSourceID(source modelSource) modelID {
	return modelID{
		source:    source,
		group:     "source.toolkit.fluxcd.io",
		kind:      "GitRepository",
		namespace: "flux-system",
		name:      source.binding,
	}
}

func newRenderModelWorld(binding string) modelWorld {
	source := modelSource{binding: binding, revision: "working", root: "repo"}
	return modelWorld{
		files:     map[modelPath]modelFile{{source: source, path: "flux"}: {included: true, directory: true}},
		producers: map[modelID]modelProducer{renderModelSourceID(source): {alias: true}},
	}
}

func putRenderModelFile(w modelWorld, name, body string) modelPath {
	file := modelPath{source: renderModelSource(w), path: name}
	w.files[file] = modelFile{body: []byte(body), included: true}
	return file
}

func addRenderModelKS(w modelWorld, name, namespace, directory string) modelID {
	source := renderModelSource(w)
	id := modelID{
		source:    source,
		group:     "kustomize.toolkit.fluxcd.io",
		kind:      "Kustomization",
		namespace: namespace,
		name:      name,
	}
	declaration := testutil.KustomizationYAML(
		name,
		namespace,
		"  path: ./"+directory+"\n  sourceRef: {kind: GitRepository, name: "+source.binding+", namespace: flux-system}\n",
	)
	putRenderModelFile(w, "flux/"+name+"-"+namespace+".yaml", declaration)
	input := putRenderModelFile(w, directory+"/kustomization.yaml", "resources: [cm.yaml]\n")
	body := putRenderModelFile(
		w,
		directory+"/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: "+name+"-output}\ndata: {value: old}\n",
	)
	w.producers[id] = modelProducer{
		declaration: []byte(
			declaration,
		),
		reads:   []modelRead{{file: input}, {file: body}},
		sources: []modelID{renderModelSourceID(source)},
		documents: []modelDocument{
			{
				id: modelObjectID{
					modelID: modelID{source: source, kind: "ConfigMap", name: name + "-output"},
					version: "v1",
				},
			},
		},
	}
	return id
}

func changeRenderModelDeclaration(w modelWorld, id *modelID, from, to string) {
	producer := w.producers[*id]
	producer.declaration = []byte(strings.Replace(string(producer.declaration), from, to, 1))
	w.producers[*id] = producer
	putRenderModelFile(w, "flux/"+id.name+"-"+id.namespace+".yaml", string(producer.declaration))
}

func decodeRenderModelCase(data []byte) renderModelCase {
	if len(data) > 0 && data[0] == 255 {
		return decodeMetamorphicRenderModelCase(data)
	}
	r := modelBytes{data: data}
	seed, variant := int(r.next())%len(renderModelSeeds()), r.next()
	directory := variant&128 != 0
	variant = (variant & 127) % 30
	if variant >= 4 {
		switch variant {
		case 10:
			seed = 2
		case 12, 27, 28:
			seed = 4
		case 13, 18, 22, 25, 26, 29:
			seed = 0
		default:
			seed = 1
		}
	}
	before := newRenderModelWorld("cluster")
	app := addRenderModelKS(before, "app", "flux-system", "apps/app")
	stable := addRenderModelKS(before, "stable", "flux-system", "apps/stable")
	file := modelPath{source: app.source, path: "apps/app/cm.yaml"}
	operations := []modelOperation{
		{
			kind:     "write",
			file:     file,
			body:     bytes.Replace(before.files[file].body, []byte("old"), []byte("new"), 1),
			included: true,
		},
	}
	result := renderModelCase{scan: "flux"}
	decodeRenderModelCaseSegment(
		&decodeRenderModelCaseSegmentInputs{
			seed:       &seed,
			variant:    &variant,
			before:     &before,
			app:        &app,
			stable:     &stable,
			file:       &file,
			operations: &operations,
			result:     &result,
		},
	)
	result.before = before
	result.operations = operations
	applyModelOperationsResult1 := applyModelOperations(before, operations)
	result.after, result.unsupported = applyModelOperationsResult1.world, applyModelOperationsResult1.unsupported
	if seed == 4 {
		putRenderModelFile(result.after, "apps/app/kustomization.yaml", "resources: [cm.yaml]\n")
		p := result.after.producers[app]
		p.reads, p.documents = p.reads[:2], p.documents[:1]
		result.after.producers[app] = p
	}
	if seed == 5 {
		p := result.after.producers[app]
		p.documents = []modelDocument{
			{
				id: modelObjectID{
					modelID: modelID{source: app.source, kind: "ConfigMap", name: "recovered"},
					version: "v1",
				},
			},
		}
		result.after.producers[app] = p
	}
	if variant == 1 && seed == 1 {
		changeRenderModelDeclaration(result.after, &app, "  path:", "  suspend: true\n  path:")
		p := result.after.producers[app]
		p.suspended = true
		result.after.producers[app] = p
	}
	extendRenderModel(&result, seed, variant)
	if directory {
		result.committed = false
	}
	for steps := 0; r.at < len(r.data) && steps < 24 && result.beforeScan == ""; steps++ {
		opcode, target, value := r.next(), r.next(), r.next()
		previous := cloneModelWorld(result.after)
		mutateRenderModel(&result, opcode, target, value)
		if len(modelWorldOperations(result.before, result.after)) > 24 {
			result.after = previous
			result.unsupported["operation-limit"]++
		}
	}
	if r.at < len(r.data) {
		category := "operation-limit"
		if result.beforeScan != "" {
			category = "root-placement-operations"
		}
		result.unsupported[category] += min(24, (len(r.data)-r.at+2)/3)
	}
	result.operations = modelWorldOperations(result.before, result.after)
	applyModelOperationsResult2 := applyModelOperations(result.before, result.operations)
	result.after, _ = applyModelOperationsResult2.world, applyModelOperationsResult2.unsupported
	result.unsupported["ResourceSet-frozen-semantics"]++
	return result
}

func renderModelSubchart(value string) []byte {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	archive := tar.NewWriter(gz)
	files := map[string]string{
		"child/Chart.yaml":        "apiVersion: v2\nname: child\nversion: 1.0.0\n",
		"child/templates/cm.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: child-output, namespace: flux-system}\ndata: {value: " + value + "}\n",
	}
	for _, name := range slices.Sorted(maps.Keys(files)) {
		body := files[name]
		if err := archive.WriteHeader(&tar.Header{Name: name, Mode: 0o600, Size: int64(len(body))}); err != nil {
			panic(err)
		}
		if _, err := archive.Write([]byte(body)); err != nil {
			panic(err)
		}
	}
	if err := errors.Join(archive.Close(), gz.Close()); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

type decodeRenderModelCaseSegmentInputs struct {
	seed       *int
	variant    *byte
	before     *modelWorld
	app        *modelID
	stable     *modelID
	file       *modelPath
	operations *[]modelOperation
	result     *renderModelCase
}

func decodeRenderModelCaseSegment(statementInput *decodeRenderModelCaseSegmentInputs) {
	switch *statementInput.seed {
	case 0:
		parent := addRenderModelKS((*statementInput.before), "parent", "flux-system", "parents")
		child := statementInput.before.producers[(*statementInput.app)]
		child.emitter = &parent
		statementInput.before.producers[(*statementInput.app)] = child
		delete(
			statementInput.before.files,
			modelPath{source: statementInput.app.source, path: "flux/app-flux-system.yaml"},
		)
		putRenderModelFile(
			(*statementInput.before),
			"parents/kustomization.yaml",
			"namespace: flux-system\nresources: [child.yaml]\n",
		)
		putRenderModelFile(
			(*statementInput.before),
			"parents/child.yaml",
			strings.Replace(string(child.declaration), "  namespace: flux-system\n", "", 1),
		)
		p := statementInput.before.producers[parent]
		p.reads = []modelRead{
			{file: modelPath{source: statementInput.app.source, path: "parents/kustomization.yaml"}},
			{file: modelPath{source: statementInput.app.source, path: "parents/child.yaml"}},
		}
		p.documents = []modelDocument{
			{id: modelObjectID{modelID: (*statementInput.app), version: "v1", namespacePresent: true}},
		}
		statementInput.before.producers[parent] = p
		delete(statementInput.before.files, modelPath{source: statementInput.app.source, path: "parents/cm.yaml"})
	case 1:
		statementInput.result.committed = true
		putRenderModelFile((*statementInput.before), "unrelated.txt", "committed\n")
	case 2:
		shared := putRenderModelFile(
			(*statementInput.before),
			"shared/component/kustomization.yaml",
			"apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\ncommonAnnotations: {shared: old}\n",
		)
		for _, id := range []modelID{(*statementInput.app), (*statementInput.stable)} {
			p := statementInput.before.producers[id]
			input := p.reads[0].file
			putRenderModelFile(
				(*statementInput.before),
				input.path,
				"resources: [cm.yaml]\ncomponents: [../../shared/component]\n",
			)
			p.reads = append(p.reads, modelRead{file: shared})
			statementInput.before.producers[id] = p
		}
		(*statementInput.operations) = []modelOperation{
			{
				kind:     "write",
				file:     shared,
				body:     bytes.Replace(statementInput.before.files[shared].body, []byte("old"), []byte("new"), 1),
				included: true,
			},
		}
	case 3:
		statementInput.result.committed = (*statementInput.variant) == 1
		optional := modelPath{source: statementInput.app.source, path: "flux/optional.yaml"}
		changeRenderModelDeclaration(
			(*statementInput.before),
			statementInput.app,
			"  path:",
			"  postBuild:\n    substituteFrom: [{kind: ConfigMap, name: optional, optional: true}]\n  path:",
		)
		p := statementInput.before.producers[(*statementInput.app)]
		p.reads = append(p.reads, modelRead{file: optional, optional: true})
		statementInput.before.producers[(*statementInput.app)] = p
		putRenderModelFile(
			(*statementInput.before),
			statementInput.file.path,
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: app-output}\ndata: {value: '${VALUE:=old}'}\n",
		)
		changeRenderModelDeclaration(
			(*statementInput.before),
			statementInput.stable,
			"  path:",
			"  dependsOn: [{name: app}]\n  path:",
		)
		s := statementInput.before.producers[(*statementInput.stable)]
		s.ordering = []modelID{(*statementInput.app)}
		statementInput.before.producers[(*statementInput.stable)] = s
		(*statementInput.operations) = slices.Grow([]modelOperation{
			{
				kind: "write",
				file: optional,
				body: []byte(
					"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: optional, namespace: flux-system}\ndata: {VALUE: new}\n",
				),
				included: true,
			},
		}, 2-1)
	case 4:
		statementInput.result.committed = true
		p := statementInput.before.producers[(*statementInput.app)]
		p.reads = append(
			p.reads,
			modelRead{file: modelPath{source: statementInput.app.source, path: "apps/app/removed.yaml"}},
		)
		p.documents = append(
			p.documents,
			modelDocument{
				id: modelObjectID{
					modelID: modelID{source: statementInput.app.source, kind: "ConfigMap", name: "removed"},
					version: "v1",
				},
			},
		)
		statementInput.before.producers[(*statementInput.app)] = p
		putRenderModelFile(
			(*statementInput.before),
			"apps/app/removed.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: removed}\n",
		)
		putRenderModelFile(
			(*statementInput.before),
			"apps/app/kustomization.yaml",
			"resources: [cm.yaml, removed.yaml]\n",
		)
		(*statementInput.operations) = append(
			(*statementInput.operations),
			modelOperation{
				kind: "delete-file",
				file: modelPath{source: statementInput.app.source, path: "apps/app/removed.yaml"},
			},
		)
	case 5:
		p := statementInput.before.producers[(*statementInput.app)]
		missing := modelPath{source: statementInput.app.source, path: "apps/app/missing.yaml"}
		p.reads = append(p.reads, modelRead{file: missing})
		p.documents = nil
		statementInput.before.producers[(*statementInput.app)] = p
		putRenderModelFile((*statementInput.before), "apps/app/kustomization.yaml", "resources: [missing.yaml]\n")
		(*statementInput.operations) = []modelOperation{
			{
				kind:     "write",
				file:     missing,
				body:     []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: recovered}\n"),
				included: true,
			},
		}
	case 6:
		putRenderModelFile((*statementInput.before), "apps/app/kustomization.yaml", "resources: []\n")
		delete(statementInput.before.files, (*statementInput.file))
		p := statementInput.before.producers[(*statementInput.app)]
		p.reads, p.documents = p.reads[:1], nil
		statementInput.before.producers[(*statementInput.app)] = p
		input := modelPath{source: statementInput.app.source, path: "apps/app/kustomization.yaml"}
		(*statementInput.operations) = []modelOperation{
			{kind: "write", file: input, body: []byte("resources: []\n# changed empty producer\n"), included: true},
		}
	}
}

func decodeMetamorphicRenderModelCase(data []byte) renderModelCase {
	control := decodeRenderModelCase([]byte{1, 0})
	var kind byte
	if len(data) > 1 {
		kind = data[1] % 4
	}
	switch kind {
	case 0:
		addRenderModelKS(control.before, "disjoint", "flux-system", "apps/disjoint")
		addRenderModelKS(control.after, "disjoint", "flux-system", "apps/disjoint")
	case 1:
		control.before = renameRenderModelSource(control.before, "renamed-source")
		control.after = renameRenderModelSource(control.after, "renamed-source")
	case 2:
		control.before, control.after = control.after, control.before
	case 3:
		control.before, control.after = relocateRenderModel(control.before), relocateRenderModel(control.after)
		control.scan = "nested/flux"
	}
	if len(data) > 2 {
		control.unsupported["metamorphic-operations"] = min(24, (len(data)-2+2)/3)
	}
	control.operations = modelWorldOperations(control.before, control.after)
	applyModelOperationsResult3 := applyModelOperations(control.before, control.operations)
	control.after, _ = applyModelOperationsResult3.world, applyModelOperationsResult3.unsupported
	return control
}
