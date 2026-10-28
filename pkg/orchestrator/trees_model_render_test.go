package orchestrator

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path"
	"path/filepath"
	"reflect"

	"go.yaml.in/yaml/v3"
	"slices"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/baseline"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

var renderModelSeeds = []string{
	"late-child-support", "source-qualified-path", "orphan-and-shared-input",
	"negative-read-and-ordering", "two-sided-delete-ignore", "shallow-and-unavailable", "empty-output",
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
	return modelID{source: source, group: "source.toolkit.fluxcd.io", kind: "GitRepository", namespace: "flux-system", name: source.binding}
}

func newRenderModelWorld(binding string) modelWorld {
	source := modelSource{binding: binding, revision: "working", root: "repo"}
	return modelWorld{files: map[modelPath]modelFile{{source: source, path: "flux"}: {included: true, directory: true}}, producers: map[modelID]modelProducer{renderModelSourceID(source): {alias: true}}}
}

func putRenderModelFile(w modelWorld, name, body string) modelPath {
	file := modelPath{source: renderModelSource(w), path: name}
	w.files[file] = modelFile{body: []byte(body), included: true}
	return file
}

func addRenderModelKS(w modelWorld, name, namespace, directory string) modelID {
	source := renderModelSource(w)
	id := modelID{source: source, group: "kustomize.toolkit.fluxcd.io", kind: "Kustomization", namespace: namespace, name: name}
	declaration := testutil.KustomizationYAML(name, namespace, "  path: ./"+directory+"\n  sourceRef: {kind: GitRepository, name: "+source.binding+", namespace: flux-system}\n")
	putRenderModelFile(w, "flux/"+name+"-"+namespace+".yaml", declaration)
	input := putRenderModelFile(w, directory+"/kustomization.yaml", "resources: [cm.yaml]\n")
	body := putRenderModelFile(w, directory+"/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: "+name+"-output}\ndata: {value: old}\n")
	w.producers[id] = modelProducer{declaration: []byte(declaration), reads: []modelRead{{file: input}, {file: body}}, sources: []modelID{renderModelSourceID(source)},
		documents: []modelDocument{{id: modelObjectID{modelID: modelID{source: source, kind: "ConfigMap", name: name + "-output"}, version: "v1"}}}}
	return id
}

func changeRenderModelDeclaration(w modelWorld, id modelID, from, to string) {
	producer := w.producers[id]
	producer.declaration = []byte(strings.Replace(string(producer.declaration), from, to, 1))
	w.producers[id] = producer
	putRenderModelFile(w, "flux/"+id.name+"-"+id.namespace+".yaml", string(producer.declaration))
}

func decodeRenderModelCase(data []byte) renderModelCase {
	if len(data) > 0 && data[0] == 255 {
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
		control.after, _ = applyModelOperations(control.before, control.operations)
		return control
	}
	r := modelBytes{data: data}
	seed, variant := int(r.next())%len(renderModelSeeds), r.next()
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
	operations := []modelOperation{{kind: "write", file: file, body: bytes.Replace(before.files[file].body, []byte("old"), []byte("new"), 1), included: true}}
	result := renderModelCase{scan: "flux"}
	switch seed {
	case 0:
		parent := addRenderModelKS(before, "parent", "flux-system", "parents")
		child := before.producers[app]
		child.emitter = &parent
		before.producers[app] = child
		delete(before.files, modelPath{source: app.source, path: "flux/app-flux-system.yaml"})
		putRenderModelFile(before, "parents/kustomization.yaml", "namespace: flux-system\nresources: [child.yaml]\n")
		putRenderModelFile(before, "parents/child.yaml", strings.Replace(string(child.declaration), "  namespace: flux-system\n", "", 1))
		p := before.producers[parent]
		p.reads = []modelRead{{file: modelPath{source: app.source, path: "parents/kustomization.yaml"}}, {file: modelPath{source: app.source, path: "parents/child.yaml"}}}
		p.documents = []modelDocument{{id: modelObjectID{modelID: app, version: "v1", namespacePresent: true}}}
		before.producers[parent] = p
		delete(before.files, modelPath{source: app.source, path: "parents/cm.yaml"})
	case 1:
		result.committed = true
		putRenderModelFile(before, "unrelated.txt", "committed\n")
	case 2:
		shared := putRenderModelFile(before, "shared/component/kustomization.yaml", "apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\ncommonAnnotations: {shared: old}\n")
		for _, id := range []modelID{app, stable} {
			p := before.producers[id]
			input := p.reads[0].file
			putRenderModelFile(before, input.path, "resources: [cm.yaml]\ncomponents: [../../shared/component]\n")
			p.reads = append(p.reads, modelRead{file: shared})
			before.producers[id] = p
		}
		operations = []modelOperation{{kind: "write", file: shared, body: bytes.Replace(before.files[shared].body, []byte("old"), []byte("new"), 1), included: true}}
	case 3:
		result.committed = variant == 1
		optional := modelPath{source: app.source, path: "flux/optional.yaml"}
		changeRenderModelDeclaration(before, app, "  path:", "  postBuild:\n    substituteFrom: [{kind: ConfigMap, name: optional, optional: true}]\n  path:")
		p := before.producers[app]
		p.reads = append(p.reads, modelRead{file: optional, optional: true})
		before.producers[app] = p
		putRenderModelFile(before, file.path, "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: app-output}\ndata: {value: '${VALUE:=old}'}\n")
		changeRenderModelDeclaration(before, stable, "  path:", "  dependsOn: [{name: app}]\n  path:")
		s := before.producers[stable]
		s.ordering = []modelID{app}
		before.producers[stable] = s
		operations = []modelOperation{{kind: "write", file: optional, body: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: optional, namespace: flux-system}\ndata: {VALUE: new}\n"), included: true}}
	case 4:
		result.committed = true
		p := before.producers[app]
		p.reads = append(p.reads, modelRead{file: modelPath{source: app.source, path: "apps/app/removed.yaml"}})
		p.documents = append(p.documents, modelDocument{id: modelObjectID{modelID: modelID{source: app.source, kind: "ConfigMap", name: "removed"}, version: "v1"}})
		before.producers[app] = p
		putRenderModelFile(before, "apps/app/removed.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: removed}\n")
		putRenderModelFile(before, "apps/app/kustomization.yaml", "resources: [cm.yaml, removed.yaml]\n")
		operations = append(operations, modelOperation{kind: "delete-file", file: modelPath{source: app.source, path: "apps/app/removed.yaml"}})
	case 5:
		p := before.producers[app]
		missing := modelPath{source: app.source, path: "apps/app/missing.yaml"}
		p.reads = append(p.reads, modelRead{file: missing})
		p.documents = nil
		before.producers[app] = p
		putRenderModelFile(before, "apps/app/kustomization.yaml", "resources: [missing.yaml]\n")
		operations = []modelOperation{{kind: "write", file: missing, body: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: recovered}\n"), included: true}}
	case 6:
		putRenderModelFile(before, "apps/app/kustomization.yaml", "resources: []\n")
		delete(before.files, file)
		p := before.producers[app]
		p.reads, p.documents = p.reads[:1], nil
		before.producers[app] = p
		input := modelPath{source: app.source, path: "apps/app/kustomization.yaml"}
		operations = []modelOperation{{kind: "write", file: input, body: []byte("resources: []\n# changed empty producer\n"), included: true}}
	}
	result.before = before
	result.operations = operations
	result.after, result.unsupported = applyModelOperations(before, operations)
	if seed == 4 {
		putRenderModelFile(result.after, "apps/app/kustomization.yaml", "resources: [cm.yaml]\n")
		p := result.after.producers[app]
		p.reads, p.documents = p.reads[:2], p.documents[:1]
		result.after.producers[app] = p
	}
	if seed == 5 {
		p := result.after.producers[app]
		p.documents = []modelDocument{{id: modelObjectID{modelID: modelID{source: app.source, kind: "ConfigMap", name: "recovered"}, version: "v1"}}}
		result.after.producers[app] = p
	}
	if variant == 1 && seed == 1 {
		changeRenderModelDeclaration(result.after, app, "  path:", "  suspend: true\n  path:")
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
	result.after, _ = applyModelOperations(result.before, result.operations)
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

func extendRenderModel(f *renderModelCase, seed int, variant byte) {
	if variant < 4 {
		return
	}
	source := renderModelSource(f.before)
	app := modelID{source: source, group: "kustomize.toolkit.fluxcd.io", kind: "Kustomization", namespace: "flux-system", name: "app"}
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
			p.documents = []modelDocument{{id: modelObjectID{modelID: modelID{source: coordinate, kind: "ConfigMap", name: "pinned-output"}, version: "v1"}, sdk: map[string]any{
				"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "pinned-output", "labels": map[string]any{"kustomize.toolkit.fluxcd.io/name": "pinned", "kustomize.toolkit.fluxcd.io/namespace": "flux-system"}}, "data": map[string]any{"value": "pin"},
			}}}
			id := pinned
			id.source = coordinate
			w.producers[id] = p
			declaration := putRenderModelFile(w, "flux/source-other.yaml", "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: other, namespace: flux-system}\nspec: {url: 'file:///model-other', ref: {commit: before}}\n")
			w.producers[renderModelSourceID(coordinate)] = modelProducer{reads: []modelRead{{file: declaration}}}
		}
		if variant == 15 {
			file := modelPath{source: source, path: "apps/app/cm.yaml"}
			f.after.files[file] = f.before.files[file]
			input := modelPath{source: coordinate, path: "apps/app/cm.yaml"}
			writeRenderModelFile(f, input, bytes.ReplaceAll(f.after.files[input].body, []byte("value: pin"), []byte("value: new")))
			declaration := modelPath{source: source, path: "flux/source-other.yaml"}
			writeRenderModelFile(f, declaration, bytes.ReplaceAll(f.after.files[declaration].body, []byte("commit: before"), []byte("commit: after")))
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
	if variant >= 5 && variant <= 9 {
		for _, w := range []modelWorld{f.before, f.after} {
			delete(w.files, modelPath{source: source, path: "flux/app-flux-system.yaml"})
			delete(w.files, modelPath{source: source, path: "apps/app/kustomization.yaml"})
			delete(w.files, modelPath{source: source, path: "apps/app/cm.yaml"})
			delete(w.producers, app)
			hr := app
			hr.kind, hr.group = "HelmRelease", "helm.toolkit.fluxcd.io"
			declaration := "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: app, namespace: flux-system}\nspec:\n  interval: 10m\n  chart:\n    spec:\n      chart: charts/app\n      sourceRef: {kind: GitRepository, name: cluster, namespace: flux-system}\n"
			putRenderModelFile(w, "flux/app-flux-system.yaml", declaration)
			chart := putRenderModelFile(w, "charts/app/Chart.yaml", "apiVersion: v2\nname: app\nversion: 1.0.0\n")
			values := putRenderModelFile(w, "charts/app/values.yaml", "value: old\n")
			template := putRenderModelFile(w, "charts/app/templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: app-output, namespace: flux-system}\ndata: {value: {{ .Values.value | quote }}}\n")
			w.producers[hr] = modelProducer{declaration: []byte(declaration), reads: []modelRead{{file: chart}, {file: values}, {file: template}}, sources: []modelID{renderModelSourceID(source)}, documents: []modelDocument{{id: modelObjectID{modelID: modelID{source: source, kind: "ConfigMap", namespace: "flux-system", name: "app-output"}, version: "v1", namespacePresent: true}}}}
		}
		hr := app
		hr.kind, hr.group = "HelmRelease", "helm.toolkit.fluxcd.io"
		switch variant {
		case 5:
			file := modelPath{source: source, path: "charts/app/templates/cm.yaml"}
			writeRenderModelFile(f, file, bytes.Replace(f.before.files[file].body, []byte(".Values.value"), []byte(".Values.value | upper"), 1))
		case 6:
			file := modelPath{source: source, path: "charts/app/values.yaml"}
			writeRenderModelFile(f, file, []byte("value: new\n"))
		case 7:
			for _, w := range []modelWorld{f.before, f.after} {
				p := w.producers[hr]
				p.declaration = append(p.declaration, []byte("  values: {value: old}\n")...)
				w.producers[hr] = p
				putRenderModelFile(w, "flux/app-flux-system.yaml", string(p.declaration))
			}
			changeRenderModelDeclaration(f.after, hr, "value: old", "value: new")
		case 8:
			for _, w := range []modelWorld{f.before, f.after} {
				file := putRenderModelFile(w, "flux/values.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: values, namespace: flux-system}\ndata:\n  values.yaml: |\n    value: old\n")
				p := w.producers[hr]
				p.reads = append(p.reads, modelRead{file: file})
				p.declaration = append(p.declaration, []byte("  valuesFrom: [{kind: ConfigMap, name: values}]\n")...)
				w.producers[hr] = p
				putRenderModelFile(w, "flux/app-flux-system.yaml", string(p.declaration))
			}
			file := modelPath{source: source, path: "flux/values.yaml"}
			writeRenderModelFile(f, file, bytes.Replace(f.before.files[file].body, []byte("value: old"), []byte("value: new"), 1))
		case 9:
			file := modelPath{source: source, path: "charts/app/charts/child-1.0.0.tgz"}
			for _, w := range []modelWorld{f.before, f.after} {
				p := w.producers[hr]
				putRenderModelFile(w, "charts/app/Chart.yaml", "apiVersion: v2\nname: app\nversion: 1.0.0\ndependencies: [{name: child, version: 1.0.0, repository: 'file://charts/child'}]\n")
				w.files[file] = modelFile{body: renderModelSubchart("old"), included: true}
				p.reads = append(p.reads, modelRead{file: file})
				p.documents = append(p.documents, modelDocument{id: modelObjectID{modelID: modelID{source: source, kind: "ConfigMap", namespace: "flux-system", name: "child-output"}, version: "v1", namespacePresent: true}})
				w.producers[hr] = p
			}
			writeRenderModelFile(f, file, renderModelSubchart("new"))
		}
		return
	}
	switch variant {
	case 16:
		file := modelPath{source: source, path: "apps/app/cm.yaml"}
		writeRenderModelFile(f, file, bytes.Replace(f.after.files[file].body, []byte("apiVersion: v1"), []byte("apiVersion: example.test/v1"), 1))
		p := f.after.producers[app]
		p.documents = slices.Clone(p.documents)
		p.documents[0].id.group = "example.test"
		f.after.producers[app] = p
	case 17:
		for _, w := range []modelWorld{f.before, f.after} {
			file := putRenderModelFile(w, "apps/app/foreign.yaml", "apiVersion: example.test/v1\nkind: ConfigMap\nmetadata: {name: app-output}\nspec: {value: foreign}\n")
			putRenderModelFile(w, "apps/app/kustomization.yaml", "resources: [cm.yaml, foreign.yaml]\n")
			p := w.producers[app]
			p.reads = append(p.reads, modelRead{file: file})
			p.documents = append(p.documents, modelDocument{id: modelObjectID{modelID: modelID{source: source, group: "example.test", kind: "ConfigMap", name: "app-output"}, version: "v1"}})
			w.producers[app] = p
		}
	case 18:
		for _, w := range []modelWorld{f.before, f.after} {
			data := addRenderModelKS(w, "z-data", "flux-system", "data")
			putRenderModelFile(w, "data/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: generated-values, namespace: flux-system}\ndata: {VALUE: old}\n")
			d := w.producers[data]
			d.documents = []modelDocument{{id: modelObjectID{modelID: modelID{source: source, kind: "ConfigMap", namespace: "flux-system", name: "generated-values"}, version: "v1", namespacePresent: true}}}
			w.producers[data] = d
			p := w.producers[app]
			p.content = []modelID{data}
			p.declaration = bytes.Replace(p.declaration, []byte("  path:"), []byte("  postBuild:\n    substituteFrom: [{kind: ConfigMap, name: generated-values}]\n  path:"), 1)
			w.producers[app] = p
			putRenderModelFile(w, "parents/child.yaml", strings.Replace(string(p.declaration), "  namespace: flux-system\n", "", 1))
			putRenderModelFile(w, "apps/app/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: app-output}\ndata: {value: '${VALUE}'}\n")
		}
		file := modelPath{source: source, path: "data/cm.yaml"}
		writeRenderModelFile(f, file, bytes.ReplaceAll(f.after.files[file].body, []byte("VALUE: old"), []byte("VALUE: new")))
	case 19:
		for _, w := range []modelWorld{f.before, f.after} {
			changeRenderModelDeclaration(w, app, "  path:", "  postBuild:\n    substituteFrom: [{kind: ConfigMap, name: absent, optional: true}]\n  path:")
			p := w.producers[app]
			p.reads = append(p.reads, modelRead{file: modelPath{source: source, path: "flux/absent.yaml"}, optional: true})
			w.producers[app] = p
		}
	case 20:
		for _, w := range []modelWorld{f.before, f.after} {
			changeRenderModelDeclaration(w, app, "  path:", "  postBuild:\n    substituteFrom: [{kind: ConfigMap, name: absent}]\n  path:")
			p := w.producers[app]
			p.reads = append(p.reads, modelRead{file: modelPath{source: source, path: "flux/absent.yaml"}})
			p.content = []modelID{{source: source, kind: "ConfigMap", namespace: "flux-system", name: "absent"}}
			p.documents = nil
			w.producers[app] = p
		}
	case 22:
		f.selfURLs = []string{"git://fixture.invalid/cluster"}
		other := source
		other.binding = "late"
		parent := modelID{source: source, group: app.group, kind: app.kind, namespace: app.namespace, name: "parent"}
		for _, w := range []modelWorld{f.before, f.after} {
			p := w.producers[app]
			p.declaration = bytes.ReplaceAll(p.declaration, []byte("name: cluster, namespace: flux-system"), []byte("name: late, namespace: flux-system"))
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
			id := app
			id.source = other
			delete(w.producers, app)
			w.producers[id] = p
			putRenderModelFile(w, "parents/child.yaml", strings.Replace(string(p.declaration), "  namespace: flux-system\n", "", 1))
			putRenderModelFile(w, "parents/kustomization.yaml", "namespace: flux-system\nresources: [child.yaml, source.yaml]\n")
			declaration := putRenderModelFile(w, "parents/source.yaml", "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: late, namespace: flux-system}\nspec: {url: 'git://fixture.invalid/cluster'}\n")
			pp := w.producers[parent]
			pp.reads = append(pp.reads, modelRead{file: declaration})
			object := renderModelSourceID(other)
			object.source = source
			pp.documents = append(pp.documents, modelDocument{id: modelObjectID{modelID: object, version: "v1", namespacePresent: true}})
			w.producers[parent] = pp
			w.producers[renderModelSourceID(other)] = modelProducer{emitter: &parent}
		}
	case 23:
		other := source
		other.binding = "other"
		p := f.after.producers[app]
		p.declaration = bytes.ReplaceAll(p.declaration, []byte("name: cluster, namespace: flux-system"), []byte("name: other, namespace: flux-system"))
		p.reads = slices.Clone(p.reads)
		for i := range p.reads {
			file := p.reads[i].file
			p.reads[i].file.source = other
			f.after.files[p.reads[i].file] = f.after.files[file]
		}
		p.sources = []modelID{renderModelSourceID(other)}
		p.documents = slices.Clone(p.documents)
		for i := range p.documents {
			p.documents[i].id.source = other
		}
		id := app
		id.source = other
		delete(f.after.producers, app)
		f.after.producers[id] = p
		f.after.producers[renderModelSourceID(other)] = modelProducer{}
		writeRenderModelFile(f, modelPath{source: source, path: "flux/app-flux-system.yaml"}, p.declaration)
	case 24:
		f.committed = false
		f.after = relocateRenderModel(f.after)
		f.beforeScan, f.scan = f.scan, "nested/"+f.scan
	case 25:
		old := modelID{source: source, group: app.group, kind: app.kind, namespace: app.namespace, name: "parent"}
		next := old
		next.name = "new-owner"
		p := f.after.producers[old]
		p.declaration = bytes.Replace(p.declaration, []byte("  name: parent\n"), []byte("  name: new-owner\n"), 1)
		f.after.producers[old] = p
		rekeyRenderModel(f.after, old, next)
		delete(f.after.files, modelPath{source: source, path: "flux/parent-flux-system.yaml"})
		writeRenderModelFile(f, modelPath{source: source, path: "flux/new-owner-flux-system.yaml"}, p.declaration)
	case 26:
		for i, w := range []modelWorld{f.before, f.after} {
			value := []string{"old", "new"}[i]
			putRenderModelFile(w, "parents/kustomization.yaml", "namespace: flux-system\nresources: [child.yaml]\npatches:\n- target: {kind: Kustomization, name: app}\n  patch: |-\n    - op: add\n      path: /spec/postBuild\n      value:\n        substitute: {EXTRA: "+value+"}\n")
			putRenderModelFile(w, "apps/app/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: app-output}\ndata: {value: '${EXTRA}'}\n")
			p := w.producers[app]
			p.declaration = append(p.declaration, []byte("  postBuild: {substitute: {EXTRA: "+value+"}}\n")...)
			w.producers[app] = p
		}
	case 27:
		f.before, f.after = f.after, f.before
	case 28:
		extendRenderModel(f, seed, 12)
		f.before, f.after = f.after, f.before
	case 29:
		parent := modelID{source: source, group: app.group, kind: app.kind, namespace: app.namespace, name: "parent"}
		file := modelPath{source: source, path: "parents/kustomization.yaml"}
		writeRenderModelFile(f, file, bytes.Replace(f.after.files[file].body, []byte("namespace: flux-system"), []byte("namespace: ns-1"), 1))
		p := f.after.producers[app]
		p.declaration = bytes.Replace(p.declaration, []byte("  namespace: flux-system\n"), []byte("  namespace: ns-1\n"), 1)
		f.after.producers[app] = p
		next := app
		next.namespace = "ns-1"
		rekeyRenderModel(f.after, app, next)
		pp := f.after.producers[parent]
		pp.documents = slices.Clone(pp.documents)
		pp.documents[0].id.namespace = "ns-1"
		f.after.producers[parent] = pp
	case 21:
		id := app
		id.group, id.kind = "fluxcd.controlplane.io", "ResourceSet"
		crd := modelID{source: source, group: "apiextensions.k8s.io", kind: "CustomResourceDefinition", name: "widgets.example.test"}
		for i, w := range []modelWorld{f.before, f.after} {
			delete(w.producers, app)
			delete(w.files, modelPath{source: source, path: "flux/app-flux-system.yaml"})
			declaration := fmt.Sprintf("apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSet\nmetadata: {name: app, namespace: flux-system, annotations: {revision: '%d'}}\nspec:\n  dependsOn: [{apiVersion: apiextensions.k8s.io/v1, kind: CustomResourceDefinition, name: widgets.example.test}]\n  resources: []\n", i)
			putRenderModelFile(w, "flux/rs.yaml", declaration)
			w.producers[id] = modelProducer{declaration: []byte(declaration), ordering: []modelID{crd}}
		}
	case 4:
		for _, w := range []modelWorld{f.before, f.after} {
			p := w.producers[app]
			delete(w.files, p.reads[1].file)
			input := putRenderModelFile(w, "apps/app/value.txt", "old\n")
			putRenderModelFile(w, "apps/app/kustomization.yaml", "generatorOptions: {disableNameSuffixHash: true}\nconfigMapGenerator:\n- name: app-output\n  files: [value.txt]\n")
			p.reads[1].file = input
			w.producers[app] = p
		}
		writeRenderModelFile(f, modelPath{source: source, path: "apps/app/value.txt"}, []byte("new\n"))
	case 10:
		mutateRenderModel(f, 8, 1, 0)
	case 11:
		for _, w := range []modelWorld{f.before, f.after} {
			p := w.producers[app]
			patch := putRenderModelFile(w, "shared/patch.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: app-output}\ndata: {patched: old}\n")
			putRenderModelFile(w, "apps/app/kustomization.yaml", "resources: [cm.yaml]\npatches: [{path: ../../shared/patch.yaml}]\n")
			p.reads = append(p.reads, modelRead{file: patch})
			w.producers[app] = p
		}
		writeRenderModelFile(f, modelPath{source: source, path: "shared/patch.yaml"}, []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: app-output}\ndata: {patched: new}\n"))
	case 12:
		for _, w := range []modelWorld{f.before, f.after} {
			p := w.producers[app]
			p.reads = p.reads[:2]
			p.documents = p.documents[:1]
			w.producers[app] = p
			delete(w.files, modelPath{source: source, path: "apps/app/removed.yaml"})
			putRenderModelFile(w, "apps/app/kustomization.yaml", "resources: [cm.yaml]\n")
		}
		file := modelPath{source: source, path: "apps/app/cm.yaml"}
		content := f.before.files[file]
		content.included = false
		f.before.files[file] = content
		putRenderModelFile(f.before, ".sourceignore", "apps/app/cm.yaml\n")
		putRenderModelFile(f.after, ".sourceignore", "")
	case 13:
		if seed == 0 {
			p := f.after.producers[app]
			p.emitter = nil
			f.after.producers[app] = p
			putRenderModelFile(f.after, "flux/app-flux-system.yaml", string(p.declaration))
			parent := modelID{source: source, group: app.group, kind: app.kind, namespace: app.namespace, name: "parent"}
			delete(f.after.producers, parent)
			delete(f.after.files, modelPath{source: source, path: "flux/parent-flux-system.yaml"})
			delete(f.after.files, modelPath{source: source, path: "parents/kustomization.yaml"})
			delete(f.after.files, modelPath{source: source, path: "parents/child.yaml"})
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

func rekeyRenderModel(w modelWorld, old, next modelID) {
	producer := w.producers[old]
	delete(w.producers, old)
	w.producers[next] = producer
	for id, p := range w.producers {
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

func mutateRenderModel(f *renderModelCase, opcode, target, value byte) {
	ids := modelIDs(modelProducerIDs(f.after))
	ids = slices.DeleteFunc(ids, func(id modelID) bool {
		p := f.after.producers[id]
		kind := opcode % 9
		chartEdit := id.kind == "HelmRelease" && (kind == 0 || kind == 1 || kind == 2 || kind == 7 || kind == 8)
		if id.kind != "Kustomization" && !chartEdit || id.source != renderModelSource(f.after) || p.emitter != nil || len(p.reads) == 0 {
			return true
		}
		for _, consumer := range f.after.producers {
			if slices.Contains(consumer.content, id) || slices.Contains(consumer.ordering, id) || consumer.emitter != nil && *consumer.emitter == id {
				return true
			}
		}
		return false
	})
	if len(ids) == 0 {
		return
	}
	id := ids[int(target)%len(ids)]
	p := f.after.producers[id]
	declarationFile := modelPath{source: id.source, path: "flux/" + id.name + "-" + id.namespace + ".yaml"}
	input := p.reads[0].file
	switch opcode % 9 {
	case 0:
		for _, read := range p.reads {
			file, ok := f.after.files[read.file]
			if !ok || !utf8.Valid(file.body) || !bytes.Contains(file.body, []byte("old")) && !bytes.Contains(file.body, []byte("new")) {
				continue
			}
			body := bytes.ReplaceAll(file.body, []byte("old"), fmt.Appendf(nil, "v%02x", value))
			body = bytes.ReplaceAll(body, []byte("new"), fmt.Appendf(nil, "v%02x", value))
			writeRenderModelFile(f, read.file, body)
			break
		}
	case 1:
		body := append(slices.Clone(f.after.files[input].body), fmt.Appendf(nil, "# input-%02x\n", value)...)
		if len(body) <= 4096 {
			writeRenderModelFile(f, input, body)
		}
	case 2:
		for i, read := range p.reads[1:] {
			file, ok := f.after.files[read.file]
			if !ok || path.Base(read.file.path) != "cm.yaml" {
				continue
			}
			next := read.file
			next.path = path.Join(path.Dir(next.path), fmt.Sprintf("renamed-%02x.yaml", value))
			delete(f.after.files, read.file)
			file.included = renderModelFileIncluded(f.after, next)
			f.after.files[next] = file
			p.reads[i+1].file = next
			f.after.producers[id] = p
			writeRenderModelFile(f, input, bytes.ReplaceAll(f.after.files[input].body, []byte("cm.yaml"), []byte(path.Base(next.path))))
			break
		}
	case 3, 4:
		next := id
		if opcode%9 == 3 {
			next.name = fmt.Sprintf("renamed-%02x", value)
		} else {
			next.namespace = []string{"flux-system", "ns-1", "ns-2"}[value%3]
		}
		if next == id || f.after.producers[next].declaration != nil {
			return
		}
		for _, dep := range p.ordering {
			p.declaration = bytes.ReplaceAll(p.declaration, []byte("[{name: "+dep.name+"}]"), []byte("[{name: "+dep.name+", namespace: "+dep.namespace+"}]"))
		}
		p.declaration = bytes.Replace(p.declaration, []byte("  name: "+id.name+"\n"), []byte("  name: "+next.name+"\n"), 1)
		p.declaration = bytes.Replace(p.declaration, []byte("  namespace: "+id.namespace+"\n"), []byte("  namespace: "+next.namespace+"\n"), 1)
		for i, dep := range p.content {
			if dep.namespace == id.namespace && (dep.kind == "ConfigMap" || dep.kind == "Secret") {
				p.content[i].namespace = next.namespace
			}
		}
		delete(f.after.files, declarationFile)
		f.after.producers[id] = p
		rekeyRenderModel(f.after, id, next)
		writeRenderModelFile(f, modelPath{source: id.source, path: "flux/" + next.name + "-" + next.namespace + ".yaml"}, slices.Clone(p.declaration))
		for other, dependency := range f.after.producers {
			if !slices.Contains(dependency.ordering, next) {
				continue
			}
			dependency.declaration = bytes.ReplaceAll(dependency.declaration, []byte("[{name: "+id.name+"}]"), []byte("[{name: "+next.name+", namespace: "+next.namespace+"}]"))
			f.after.producers[other] = dependency
			writeRenderModelFile(f, modelPath{source: other.source, path: "flux/" + other.name + "-" + other.namespace + ".yaml"}, slices.Clone(dependency.declaration))
		}
	case 5:
		if len(p.reads) != 2 || path.Base(p.reads[1].file.path) != "cm.yaml" {
			f.unsupported["path-move-with-external-reads"]++
			return
		}
		directory := path.Dir(input.path)
		nextDirectory := fmt.Sprintf("moved-%02x", value)
		for file, content := range maps.Clone(f.after.files) {
			if file.source != id.source || !strings.HasPrefix(file.path, directory+"/") {
				continue
			}
			next := file
			next.path = nextDirectory + strings.TrimPrefix(file.path, directory)
			delete(f.after.files, file)
			content.included = renderModelFileIncluded(f.after, next)
			f.after.files[next] = content
			for i := range p.reads {
				if p.reads[i].file == file {
					p.reads[i].file = next
				}
			}
		}
		p.declaration = bytes.Replace(p.declaration, []byte("path: ./"+directory), []byte("path: ./"+nextDirectory), 1)
		f.after.producers[id] = p
		writeRenderModelFile(f, declarationFile, slices.Clone(p.declaration))
	case 6:
		p.suspended = true
		if !bytes.Contains(p.declaration, []byte("suspend: true")) {
			p.declaration = bytes.Replace(p.declaration, []byte("  path:"), []byte("  suspend: true\n  path:"), 1)
		}
		f.after.producers[id] = p
		writeRenderModelFile(f, declarationFile, slices.Clone(p.declaration))
	case 7:
		file := modelPath{source: id.source, path: fmt.Sprintf("ignored-%02x.txt", value)}
		if len(f.after.files) < 48 {
			writeRenderModelFile(f, file, []byte("unconsumed\n"))
		}
	case 8:
		delete(f.after.files, declarationFile)
		delete(f.after.producers, id)
	}
}

func renderModelStaticRoots(w, other modelWorld) []modelDocument {
	packages := make(map[modelPath][]modelDocument)
	owned := make(map[modelPath]bool)
	for id, p := range other.producers {
		if id.kind == "Kustomization" && len(p.reads) > 0 {
			packages[p.reads[0].file] = p.documents
		}
	}
	for id, p := range w.producers {
		if id.kind == "Kustomization" && len(p.reads) > 0 {
			owned[p.reads[0].file] = true
		}
	}
	var documents []modelDocument
	for _, input := range slices.SortedFunc(maps.Keys(packages), compareModelPath) {
		if !owned[input] && w.files[input].included {
			documents = append(documents, packages[input]...)
		}
	}
	return documents
}

func writeRenderModelWorld(t *testing.T, root string, w modelWorld) {
	t.Helper()
	keys := slices.Collect(maps.Keys(w.files))
	slices.SortFunc(keys, compareModelPath)
	for _, file := range keys {
		if file.source != renderModelSource(w) {
			continue
		}
		content := w.files[file]
		if content.directory {
			if err := os.MkdirAll(filepath.Join(root, file.path), 0o750); err != nil {
				t.Fatal(err)
			}
			continue
		}
		testutil.WriteFile(t, root, file.path, string(content.body))
	}
}

func namedRenderModelID(id modelID) manifest.NamedResource {
	return manifest.NamedResource{Group: id.group, Kind: id.kind, Namespace: id.namespace, Name: id.name}
}

func actualRenderModelID(w modelWorld, side Rendered, id manifest.NamedResource) modelID {
	source := renderModelSource(w)
	binding := ""
	if obj := side.store.GetObject(id); obj != nil {
		if consumer, ok := obj.(interface{ SourceID() manifest.NamedResource }); ok {
			binding = consumer.SourceID().Name
		} else if id.Kind == "GitRepository" {
			binding = id.Name
		}
	}
	if binding != "" {
		source.binding = binding
		for candidate := range w.files {
			if candidate.source.binding == binding {
				source = candidate.source
				break
			}
		}
	}
	return modelID{source: source, group: id.Group, kind: id.Kind, namespace: id.Namespace, name: id.Name}
}

func renderModelDocument(source modelSource, doc map[string]any) modelDocument {
	metadata, _ := doc["metadata"].(map[string]any)
	name, _ := metadata["name"].(string)
	namespace, nsPresent := metadata["namespace"].(string)
	api, _ := doc["apiVersion"].(string)
	group, version, qualified := strings.Cut(api, "/")
	if !qualified {
		group, version = "", api
	}
	kind, _ := doc["kind"].(string)
	return modelDocument{id: modelObjectID{modelID: modelID{source: source, group: group, kind: kind, namespace: namespace, name: name}, version: version, namespacePresent: nsPresent}, sdk: doc}
}

func renderModelUniverse(w modelWorld) (modelSet, error) {
	ids := modelProducerIDs(w)
	for _, doc := range w.static {
		ids[doc.id.modelID] = true
	}
	for _, p := range w.producers {
		for _, doc := range p.documents {
			ids[doc.id.modelID] = true
		}
	}
	for file, content := range w.files {
		if !content.included || !strings.HasPrefix(file.path, "flux/") || !strings.HasSuffix(file.path, ".yaml") {
			continue
		}
		decoder := yaml.NewDecoder(bytes.NewReader(content.body))
		for {
			var doc map[string]any
			if err := decoder.Decode(&doc); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return nil, fmt.Errorf("model declaration %s: %w", file.path, err)
			}
			if doc["kind"] != nil {
				id := renderModelDocument(file.source, doc).id.modelID
				ids[id] = true
				if id.group == "helm.toolkit.fluxcd.io" && id.kind == "HelmRelease" {
					spec, _ := doc["spec"].(map[string]any)
					chart, _ := spec["chart"].(map[string]any)
					template, _ := chart["spec"].(map[string]any)
					ref, _ := template["sourceRef"].(map[string]any)
					namespace, _ := ref["namespace"].(string)
					if namespace == "" {
						namespace = id.namespace
					}
					ids[modelID{source: id.source, group: "source.toolkit.fluxcd.io", kind: "HelmChart", namespace: namespace, name: id.namespace + "-" + id.name}] = true
				}
			}
		}
	}
	return ids, nil
}

func observeRenderModel(w modelWorld, side Rendered, selected bool) (modelObservation, modelSet, error) {
	out := modelObservation{producers: make(map[modelID]modelStatus)}
	admitted := make(modelSet)
	if side.Orchestrator == nil || side.Result == nil {
		return out, admitted, errors.Join(errors.New("missing render side"), side.Err)
	}
	side.tasks.BlockTillDone()
	if failures := side.tasks.Failures(); failures != 0 {
		return out, admitted, fmt.Errorf("task panics: %d", failures)
	}
	if side.Err != nil {
		if !errors.Is(side.Err, manifest.ErrFlux) {
			return out, admitted, fmt.Errorf("unclassified side error: %w", side.Err)
		}
		out.errorClass = "flux"
	}
	universe, err := renderModelUniverse(w)
	if err != nil {
		return out, admitted, err
	}
	for _, obj := range side.store.ListObjects("", "") {
		id := obj.Named()
		if id == manifest.BootstrapSourceID {
			continue
		}
		key := actualRenderModelID(w, side, id)
		known := false
		for expected := range universe {
			if namedRenderModelID(expected) == id {
				known = true
				break
			}
		}
		if !known {
			return out, admitted, fmt.Errorf("unexpected store identity: %s", key)
		}
		if side.store.ReportHidden(id) || !id.KindInfo().Runnable {
			continue
		}
		if selected && !side.filter.ShouldReconcile(id) {
			if artifact := side.store.GetArtifact(id, store.ArtifactContent); artifact != nil {
				if _, source := artifact.(*store.SourceArtifact); !source {
					return out, admitted, fmt.Errorf("unselected producer has artifact: %s", key)
				}
			}
			continue
		}
		admitted[key] = true
		info, ok := side.store.GetStatus(id)
		state := modelStatus{terminal: ok && info.Status != store.StatusPending, blockers: make(modelSet)}
		switch info.Outcome() {
		case store.OutcomeFailed:
			state.failure = "missing-input"
		case store.OutcomeBlocked:
			state.failure = "blocked"
		case store.OutcomeSuspended:
			state.failure = "suspended"
		}
		for _, blocker := range side.Result.Blocked[id] {
			state.blockers[actualRenderModelID(w, side, blocker)] = true
		}
		if state.failure == "" && !obj.Suspended() {
			artifact := side.store.GetArtifact(id, store.ArtifactContent)
			if artifact == nil || info.Outcome() != store.OutcomePassed {
				return out, admitted, fmt.Errorf("admitted producer lacks terminal artifact: %s status=%+v artifact=%T", key, info, artifact)
			}
			if emission, ok := artifact.(*store.Emission); ok && !emission.Complete {
				return out, admitted, fmt.Errorf("producer inventory incomplete: %s", key)
			}
		}
		out.producers[key] = state
	}
	for _, batch := range side.Result.Manifests {
		source := renderModelSource(w)
		if batch.Parent != nil {
			key := actualRenderModelID(w, side, batch.Parent.Named())
			if _, ok := out.producers[key]; !ok {
				return out, admitted, fmt.Errorf("output from unselected or unknown producer: %s", key)
			}
			source = key.source
		}
		for _, doc := range batch.Documents {
			out.documents = append(out.documents, renderModelDocument(source, doc))
		}
	}
	return out, admitted, nil
}

func expectedRenderModel(w modelWorld, selection *modelSelection) modelObservation {
	evaluation := evaluateModel(w)
	out := modelObservation{producers: make(map[modelID]modelStatus)}
	ids := slices.Collect(maps.Keys(evaluation.status))
	slices.SortFunc(ids, compareModelID)
	for _, id := range ids {
		if selection != nil && !selection.affected[id] && !selection.support[id] {
			continue
		}
		state := evaluation.status[id]
		out.producers[id] = state
		if state.failure != "" && state.failure != "suspended" {
			out.errorClass = "flux"
		}
		if state.failure == "" {
			out.documents = append(out.documents, w.producers[id].documents...)
		}
	}
	return out
}

func checkRenderModelMembership(want, got modelObservation) error {
	for i := range got.documents {
		if !slices.ContainsFunc(want.documents, func(d modelDocument) bool { return d.id == got.documents[i].id && d.sdk != nil }) {
			got.documents[i].sdk = nil
		}
	}
	return compareModelObservation(want, got)
}

func projectRenderModel(full modelObservation, selection modelSelection, w modelWorld) modelObservation {
	out := modelObservation{producers: make(map[modelID]modelStatus)}
	for id, state := range full.producers {
		if selection.affected[id] || selection.support[id] {
			out.producers[id] = state
			if state.failure != "" && state.failure != "suspended" {
				out.errorClass = "flux"
			}
		}
	}
	for _, doc := range full.documents {
		for id := range out.producers {
			if slices.ContainsFunc(w.producers[id].documents, func(d modelDocument) bool { return d.id == doc.id }) {
				out.documents = append(out.documents, doc)
				break
			}
		}
	}
	return out
}

func materializeRenderModelSources(t *testing.T, fixture renderModelCase) (modelWorld, modelWorld) {
	t.Helper()
	before, after := cloneModelWorld(fixture.before), cloneModelWorld(fixture.after)
	primary := renderModelSource(before)
	sources := map[modelSource]bool{}
	for _, world := range []modelWorld{before, after} {
		for file := range world.files {
			if file.source != primary {
				sources[file.source] = true
			}
		}
	}
	for coordinate := range sources {
		if coordinate.revision == "working" {
			continue
		}
		root := t.TempDir()
		repo := testutil.GitInit(t, root)
		var previous map[string]modelFile
		var revision string
		for _, world := range []modelWorld{before, after} {
			files := map[string]modelFile{}
			for file, content := range world.files {
				if file.source == coordinate {
					files[file.path] = content
				}
			}
			if !reflect.DeepEqual(previous, files) {
				wt, err := repo.Worktree()
				if err != nil {
					t.Fatal(err)
				}
				for name := range previous {
					if _, exists := files[name]; !exists {
						if _, err := wt.Remove(name); err != nil {
							t.Fatal(err)
						}
					}
				}
				for name, file := range files {
					testutil.WriteFile(t, root, name, string(file.body))
				}
				revision = testutil.GitCommitAll(t, repo, "source snapshot", &gogit.CommitOptions{Author: &object.Signature{Name: "fixture", Email: "fixture@example.test", When: time.Unix(0, 0)}}).String()
				previous = files
			}
			file := modelPath{source: primary, path: "flux/source-" + coordinate.binding + ".yaml"}
			declaration := world.files[file]
			declaration.body = []byte(fmt.Sprintf("apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: %s, namespace: flux-system}\nspec: {url: 'file://%s', ref: {commit: %s}}\n", coordinate.binding, filepath.ToSlash(root), revision))
			world.files[file] = declaration
		}
		testutil.WriteFile(t, root, "apps/app/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: dirty-decoy}\ndata: {value: dirty}\n")
	}
	return before, after
}

func checkRenderModel(t *testing.T, fixture renderModelCase) ([]byte, error) {
	t.Helper()
	if err := checkModelBounds(fixture.modelCase); err != nil {
		return nil, err
	}
	beforeRoot, afterRoot := t.TempDir(), t.TempDir()
	serializedBefore, serializedAfter := materializeRenderModelSources(t, fixture)
	writeRenderModelWorld(t, beforeRoot, serializedBefore)
	writeRenderModelWorld(t, afterRoot, serializedAfter)
	owner, err := sourcecache.New(t.Context(), "")
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	}()
	cfg := treeConfig{Concurrency: 2, Cache: owner}
	beforeScan := fixture.beforeScan
	if beforeScan == "" {
		beforeScan = fixture.scan
	}
	baseTree, headTree := Tree{RepoRoot: beforeRoot, Path: filepath.Join(beforeRoot, beforeScan), SelfURLs: fixture.selfURLs}, Tree{RepoRoot: afterRoot, Path: filepath.Join(afterRoot, fixture.scan), SelfURLs: fixture.selfURLs}
	if fixture.committed {
		repo := testutil.GitInit(t, afterRoot)
		for file := range serializedAfter.files {
			if file.source != renderModelSource(serializedAfter) {
				continue
			}
			if _, ok := serializedBefore.files[file]; !ok {
				if err := os.Remove(filepath.Join(afterRoot, file.path)); err != nil {
					return nil, err
				}
			}
		}
		writeRenderModelWorld(t, afterRoot, serializedBefore)
		hash := testutil.GitCommitAll(t, repo, "before", &gogit.CommitOptions{Author: &object.Signature{Name: "fixture", Email: "fixture@example.test"}})
		for file := range serializedBefore.files {
			if file.source != renderModelSource(serializedBefore) {
				continue
			}
			if _, ok := serializedAfter.files[file]; !ok {
				if err := os.Remove(filepath.Join(afterRoot, file.path)); err != nil {
					return nil, err
				}
			}
		}
		writeRenderModelWorld(t, afterRoot, serializedAfter)
		base, resolveErr := baseline.AutoResolve(t.Context(), filepath.Join(afterRoot, fixture.scan), hash.String(), owner)
		if resolveErr != nil {
			return nil, resolveErr
		}
		cfg.BaseInput, cfg.Input, cfg.ExternalChanges = base.BaseInput, base.Source, base.Changes
		baseTree = Tree{RepoRoot: base.BaseInput.LocalPath, Path: base.BaseScanPath, SelfURLs: base.SelfURLs}
		headTree = Tree{RepoRoot: base.Source.LocalPath, Path: base.ScanPath, SelfURLs: base.SelfURLs}
	}
	fullBefore, fullAfter := Rendered{}, Rendered{}
	for _, side := range []struct {
		root string
		scan string
		out  *Rendered
	}{{beforeRoot, beforeScan, &fullBefore}, {afterRoot, fixture.scan, &fullAfter}} {
		runs, renderErr := RenderTrees(t.Context(), []Tree{{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}, Path: filepath.Join(side.root, side.scan), RepoRoot: side.root, SelfURLs: fixture.selfURLs}}, cfg.Config)
		*side.out = runs[0]
		side.out.Err = renderErr
	}
	baseTree.ID, headTree.ID = manifest.TreeID{Cluster: ".", Side: manifest.TreeBase}, manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}
	baseTree.Input, baseTree.BaseInput, baseTree.ExternalChanges = cfg.BaseInput, cfg.Input, cfg.ExternalChanges
	headTree.Input, headTree.BaseInput, headTree.ExternalChanges = cfg.Input, cfg.BaseInput, cfg.ExternalChanges
	runs, pairErr := RenderTrees(t.Context(), []Tree{baseTree, headTree}, cfg.Config)
	old, current := runs[0], runs[1]
	selection := compareModelWorlds(fixture.before, fixture.after)
	var problems []error
	var evidence strings.Builder
	var reference, actual [2]modelObservation
	var observed [2]bool
	for index, side := range []struct {
		name          string
		world         modelWorld
		full, changed Rendered
		want          modelSelection
	}{
		{"before", fixture.before, fullBefore, old, selection.before}, {"after", fixture.after, fullAfter, current, selection.after},
	} {
		full, _, fullErr := observeRenderModel(side.world, side.full, false)
		changed, admitted, changedErr := observeRenderModel(side.world, side.changed, true)
		fmt.Fprintf(&evidence, "%s expected affected=%v support=%v unavailable=%v deleted=%v actual=%v full-error=%v changed-error=%v full-observation-error=%v changed-observation-error=%v\n",
			side.name, modelIDs(side.want.affected), modelIDs(side.want.support), modelIDs(side.want.unavailable), modelIDs(side.want.deleted), modelIDs(admitted), side.full.Err, side.changed.Err, fullErr, changedErr)
		if fullErr != nil || changedErr != nil {
			problems = append(problems, fmt.Errorf("observe %s: %w", side.name, errors.Join(fullErr, changedErr)))
			continue
		}
		observed[index] = true
		membership := full
		membership.documents = slices.Clone(full.documents)
		if err := checkRenderModelMembership(expectedRenderModel(side.world, nil), membership); err != nil {
			problems = append(problems, fmt.Errorf("full-%s: %w", side.name, err))
		}
		for _, id := range modelIDs(admitted) {
			info, _ := side.changed.store.GetStatus(namedRenderModelID(id))
			fmt.Fprintf(&evidence, "%s %s status=%+v artifact=%T\n", side.name, id, info, side.changed.store.GetArtifact(namedRenderModelID(id), store.ArtifactContent))
		}
		fmt.Fprintf(&evidence, "%s full-documents=%+v changed-documents=%+v\n", side.name, full.documents, changed.documents)
		if err := compareModelSelection(side.want, admitted); err != nil {
			problems = append(problems, fmt.Errorf("changed-%s: %w", side.name, err))
		}
		reference[index] = projectRenderModel(full, side.want, side.world)
		actual[index] = changed
		if index == 0 {
			if err := compareModelObservation(reference[index], changed); err != nil {
				problems = append(problems, fmt.Errorf("changed-%s: %w", side.name, err))
			}
		}
		for _, id := range modelIDs(side.want.deleted) {
			if side.name == "after" && slices.ContainsFunc(changed.documents, func(d modelDocument) bool { return d.id.modelID == id }) {
				return nil, fmt.Errorf("deleted object remains: %s", id)
			}
		}
	}
	if observed[0] && observed[1] {
		reference[1].deleted = modelDeletedDocuments(reference[0].documents, reference[1].documents)
		actual[1].deleted = modelDeletedDocuments(actual[0].documents, actual[1].documents)
	}
	if observed[1] {
		if err := compareModelObservation(reference[1], actual[1]); err != nil {
			problems = append(problems, fmt.Errorf("changed-after: %w", err))
		}
	}
	if (pairErr != nil) != (old.Err != nil || current.Err != nil) {
		return nil, fmt.Errorf("pair error lost side errors: %w", pairErr)
	}
	if len(problems) > 0 {
		return nil, fmt.Errorf("%w\n%s", errors.Join(problems...), evidence.String())
	}
	var canonical []any
	for _, batches := range [][]manifest.DocumentBatch{old.Result.Manifests, current.Result.Manifests} {
		for _, batch := range batches {
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

func failRenderModel(t *testing.T, input []byte, fixture renderModelCase, err error) {
	t.Helper()
	var diagnostic strings.Builder
	fmt.Fprintf(&diagnostic, "input=%x\noperations=%+v\nunsupported=%v\nerror=%v\n", input, fixture.operations, fixture.unsupported, err)
	selection := compareModelWorlds(fixture.before, fixture.after)
	for _, side := range []struct {
		name  string
		world modelWorld
		want  modelSelection
	}{{"before", fixture.before, selection.before}, {"after", fixture.after, selection.after}} {
		fmt.Fprintf(&diagnostic, "%s expected affected=%v support=%v unavailable=%v deleted=%v\n", side.name, modelIDs(side.want.affected), modelIDs(side.want.support), modelIDs(side.want.unavailable), modelIDs(side.want.deleted))
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
				"test_argv":         {"mise", "exec", "--", "go", "test", "-race", "./pkg/orchestrator", "-run", "^FuzzRenderTrees_IndependentModel$/" + seedName + "$", "-count=1"},
			}, "", "  ")
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			for name, body := range map[string][]byte{
				"input": input, "worlds.txt": []byte(diagnostic.String()),
				"seed":           []byte(fmt.Sprintf("go test fuzz v1\n[]byte(%q)\n", input)),
				"reproduce.json": reproduction,
			} {
				if writeErr := os.WriteFile(filepath.Join(dir, name), body, 0o600); writeErr != nil {
					t.Error(writeErr)
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
	cases := make([]renderModelInput, 0, len(renderModelSeeds)+1)
	for i, name := range renderModelSeeds {
		cases = append(cases, renderModelInput{name, []byte{byte(i), 0}})
	}
	cases = append(cases, renderModelInput{"negative-read-and-ordering-committed", []byte{3, 1}})
	for _, feature := range []struct {
		name          string
		seed, variant byte
	}{
		{"generator-input", 1, 4}, {"chart-template", 1, 5}, {"chart-template-directory", 1, 133}, {"chart-values", 1, 6},
		{"inline-values", 1, 7}, {"referenced-values", 1, 8}, {"chart-dependency", 1, 9},
		{"shared-owner-removal", 2, 10}, {"ancestor-patch", 1, 11},
		{"ignore-membership", 4, 12}, {"orphan-transition", 0, 13},
		{"alias-pinned-dirty-collision", 1, 14}, {"second-source-content", 1, 15},
		{"group-move", 1, 16}, {"group-collision", 1, 17}, {"late-data-support", 0, 18},
		{"optional-absent", 1, 19}, {"required-absent", 1, 20}, {"missing-crd", 1, 21}, {"late-source-support", 0, 22},
		{"source-binding-move", 1, 23}, {"root-move", 1, 24}, {"owner-move", 0, 25}, {"ancestor-injected-patch", 0, 26},
		{"two-sided-add", 4, 27}, {"ignore-removal", 4, 28}, {"emitted-namespace-move", 0, 29},
	} {
		cases = append(cases, renderModelInput{feature.name, []byte{feature.seed, feature.variant}})
	}
	for _, mutation := range []struct {
		name   string
		opcode byte
	}{
		{"same-size-edit", 0}, {"comment-only", 1}, {"rename-input", 2},
		{"name-move", 3}, {"namespace-move", 4}, {"path-move", 5},
		{"suspension", 6}, {"unrelated-dirty-file", 7}, {"owner-removal", 8},
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
	for _, tc := range renderModelInputs() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fixture := decodeRenderModelCase(tc.input)
			if _, err := checkRenderModel(t, fixture); err != nil {
				failRenderModel(t, tc.input, fixture, err)
			}
		})
	}
}

func TestIndependentModel_StaticRoots(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"removed-owner", "suspended-owner", "replacement-owner", "other-source-owner"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			before := newRenderModelWorld("cluster")
			owner := addRenderModelKS(before, "app", "flux-system", "apps/app")
			after := cloneModelWorld(before)
			p := after.producers[owner]
			delete(after.producers, owner)
			want := true
			switch name {
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
				if !reflect.DeepEqual(got, before.producers[owner].documents) {
					t.Fatalf("orphan inventory: want=%+v got=%+v", before.producers[owner].documents, got)
				}
			} else if len(got) != 0 {
				t.Fatalf("owned package promoted: %+v", got)
			}
		})
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
			world := newRenderModelWorld("cluster")
			id := addRenderModelKS(world, "app", "flux-system", "apps/app")
			obj := &manifest.Kustomization{Name: "app", Namespace: "flux-system"}
			side := Rendered{Orchestrator: &Orchestrator{store: store.New(), tasks: task.NewBounded(2)}, Result: &Result{}}
			side.store.AddObject(obj)
			side.store.UpdateStatus(obj.Named(), store.StatusInfo{Status: store.StatusReady})
			if tc.name != "missing-artifact" && tc.name != "incomplete-emission" && tc.name != "unselected-output" {
				side.store.RecordEmitted(obj.Named(), &store.Emission{Complete: true}, nil)
			}
			selected := false
			switch tc.name {
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
				side.filter = change.NewFilter(change.NewSet(nil), nil, nil, "")
				if tc.name == "unselected-output" {
					side.Result.Manifests = []manifest.DocumentBatch{{Parent: obj, Documents: []map[string]any{{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "extra"}}}}}
				}
			case "ownerless-extra-document":
				side.Result.Manifests = []manifest.DocumentBatch{{Documents: []map[string]any{{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "extra"}}}}}
			}
			got, _, err := observeRenderModel(world, side, selected)
			if tc.name == "ownerless-extra-document" {
				if err != nil {
					t.Fatal(err)
				}
				err = compareModelObservation(modelObservation{producers: map[modelID]modelStatus{id: {terminal: true}}}, got)
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("%s accepted: want=%q got=%v", tc.name, tc.want, err)
			}
		})
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
			base := testutil.GitCommitAll(t, repo, "before", &gogit.CommitOptions{Author: &object.Signature{Name: "fixture", Email: "fixture@example.test", When: time.Unix(0, 0)}})
			selected := base.String()
			if mode == "shallow" {
				if err := repo.Storer.SetShallow([]plumbing.Hash{base}); err != nil {
					t.Fatal(err)
				}
				selected = ""
			} else {
				commit, err := repo.CommitObject(base)
				if err != nil {
					t.Fatal(err)
				}
				oid := commit.TreeHash.String()
				if err := os.Remove(filepath.Join(root, ".git", "objects", oid[:2], oid[2:])); err != nil {
					t.Fatal(err)
				}
			}
			owner, err := sourcecache.New(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := owner.Close(); err != nil {
					t.Error(err)
				}
			})
			result, err := baseline.AutoResolve(t.Context(), filepath.Join(root, fixture.scan), selected, owner)
			if !errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) {
				t.Fatalf("unavailable %s baseline accepted: result=%+v err=%v", mode, result, err)
			}
			if result != nil && (result.BaseInput != nil && result.BaseInput.FS != nil || result.Source != nil && result.Source.FS != nil) {
				t.Fatalf("unavailable %s returned usable render inputs: %+v", mode, result)
			}
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
	for id, p := range cloneModelWorld(w).producers {
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
			id := rename(*p.emitter)
			p.emitter = &id
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
	for id, p := range out.producers {
		p.declaration = bytes.ReplaceAll(p.declaration, []byte("path: ./"), []byte("path: ./nested/"))
		for i := range p.reads {
			p.reads[i].file.path = "nested/" + p.reads[i].file.path
		}
		out.producers[id] = p
	}
	return out
}

func TestRenderTrees_IndependentModel_Metamorphic(t *testing.T) {
	t.Parallel()
	fixture := decodeRenderModelCase([]byte{1, 0})
	first, err := checkRenderModel(t, fixture)
	if err != nil {
		failRenderModel(t, []byte{1, 0}, fixture, err)
	}
	second, err := checkRenderModel(t, fixture)
	if err != nil {
		failRenderModel(t, []byte{1, 0}, fixture, err)
	}
	if !bytes.Equal(first, second) {
		t.Fatalf("two successful runs differ:\n%s\n%s", first, second)
	}
	for index, name := range []string{"disjoint-growth", "source-bijection", "reverse-edit", "equivalent-placement"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			encoded := []byte{255, byte(index)}
			control := decodeRenderModelCase(encoded)
			switch name {
			case "disjoint-growth":
				want, got := compareModelWorlds(fixture.before, fixture.after), compareModelWorlds(control.before, control.after)
				if !reflect.DeepEqual(want, got) {
					t.Fatalf("disjoint growth changed expected membership: want=%+v got=%+v", want, got)
				}
			case "reverse-edit":
				want, got := compareModelWorlds(fixture.before, fixture.after), compareModelWorlds(control.before, control.after)
				if !maps.Equal(want.before.affected, got.after.affected) || !maps.Equal(want.after.affected, got.before.affected) {
					t.Fatal("reversing an edit changed its symmetric affected membership")
				}
			}
			output, err := checkRenderModel(t, control)
			if err != nil {
				failRenderModel(t, encoded, control, err)
			}
			if name != "reverse-edit" && !bytes.Equal(first, output) {
				t.Fatalf("%s changed equivalent selected output:\n%s\n%s", name, first, output)
			}
		})
	}
}

func FuzzRenderTrees_IndependentModel(f *testing.F) {
	for _, tc := range renderModelInputs() {
		f.Add(tc.input)
	}
	f.Fuzz(func(t *testing.T, input []byte) {
		fixture := decodeRenderModelCase(input)
		if _, err := checkRenderModel(t, fixture); err != nil {
			failRenderModel(t, input, fixture, err)
		}
	})
}
