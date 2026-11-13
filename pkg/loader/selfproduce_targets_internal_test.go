package loader

import (
	"slices"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestSelfProduceBuilder_SubstitutedTargets(t *testing.T) {
	t.Parallel()
	for _, tc := range []substitutedTargetCase{
		{name: "inline", inline: true, known: true},
		{name: "readable ConfigMap", input: manifest.KindConfigMap, readable: true, known: true},
		{name: "readable Secret", input: manifest.KindSecret, readable: true, known: true},
		{name: "inherited reference", input: manifest.KindConfigMap, readable: true, known: true, inherited: true},
		{name: "unknown", input: manifest.KindConfigMap},
		{name: "unknown with default", input: manifest.KindSecret, defaultName: true},
		{name: "inline over unknown", input: manifest.KindSecret, inline: true, known: true},
		{name: "literal declaration", inline: true, declaration: "declared", known: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testSubstitutedTargetCase(t, &tc)
		})
	}
}

func TestSelfProduceBuilder_SubstitutionExclusions(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, field, target string
		known               bool
	}{
		{name: "enabled", target: "${BUCKET}", known: true},
		{name: "enabled literal target", target: "shared", known: true},
		{name: "annotation", field: "annotations", target: "${BUCKET}"},
		{name: "label", field: "labels", target: "${BUCKET}"},
		{name: "annotation literal target", field: "annotations", target: "shared", known: true},
		{name: "label literal target", field: "labels", target: "shared", known: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			l := New(st)
			l.Options.Missing = depwait.NewMissingPolicy(nil)
			owner := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
			owner.PostBuild = &kustomizev1.PostBuild{Substitute: map[string]string{"BUCKET": "shared"}}
			st.AddObject(owner)
			l.Index.Records[owner.Named()] = manifest.DiscoveryRecord{Declaration: owner}
			metadata := map[string]any{"name": "${BUCKET}"}
			if tc.field != "" {
				metadata[tc.field] = map[string]any{manifest.SubstituteAnnotationKey: "disabled"}
			}
			occurrence := manifest.DocOccurrence{Document: map[string]any{
				"apiVersion": "external-secrets.io/v1", "kind": "ExternalSecret", "metadata": metadata,
				"spec": map[string]any{"target": map[string]any{"name": tc.target}},
			}}
			reader := documentReader{}
			child := reader.prepareStreamObject(manifest.ParseDocOptions{}, &occurrence)
			assert.NoError(t, child.outcome.Err)
			builder := selfProduceBuilder{idx: l.Index, store: st}
			recorder := productionRecorder{visit: graphVisit{
				owner: owner.Named(), source: manifest.BootstrapSourceID,
				graphContext: graphContext{namespace: "apps"},
			}}
			recorder.record(&builder, l, &child, "v1")
			assert.NoError(t, builder.finalizeTargets(t.Context(), l, nil))
			target := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "shared"}
			record, _ := l.Index.Lookup(target)
			assert.Equal(t, len(record.Productions) != 0, tc.known)
			for _, production := range record.Productions {
				assert.Equal(t, production.Owner, owner.Named())
				if tc.field != "" {
					assert.Equal(t, production.Declaration, manifest.NamedResource{})
				} else {
					assert.Equal(t, production.Declaration.Name, "shared")
					assert.Equal(t, production.Declaration.Namespace, "apps")
				}
			}
		})
	}
}

type substitutedTargetCase struct {
	name, input, declaration string
	inline, readable, known  bool
	defaultName, inherited   bool
}

func testSubstitutedTargetCase(t *testing.T, tc *substitutedTargetCase) {
	t.Helper()
	st := store.New()
	l := New(st)
	l.Options.Missing = depwait.NewMissingPolicy(nil)
	owner := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	owner.PostBuild = &kustomizev1.PostBuild{}
	if tc.inline {
		owner.PostBuild.Substitute = map[string]string{"BUCKET": "shared"}
	}
	if tc.input != "" {
		owner.PostBuild.SubstituteFrom = []manifest.SubstituteReference{{Kind: tc.input, Name: "settings"}}
	}
	var parent manifest.NamedResource
	if tc.inherited {
		root := &manifest.Kustomization{Name: "root", Namespace: "apps"}
		root.PostBuild = &kustomizev1.PostBuild{Substitute: map[string]string{"SETTINGS": "settings"}}
		st.AddObject(root)
		l.Index.Records[root.Named()] = manifest.DiscoveryRecord{Declaration: root}
		parent = root.Named()
		owner.PostBuild.SubstituteFrom[0].Name = "${SETTINGS}"
	}
	if tc.readable {
		id := manifest.NamedResource{Kind: tc.input, Namespace: "apps", Name: "settings"}
		if tc.input == manifest.KindSecret {
			st.AddObject(manifest.NewSecret(id, nil, map[string]any{"BUCKET": "shared"}))
		} else {
			st.AddObject(manifest.NewConfigMap(id, map[string]any{"BUCKET": "shared"}))
		}
	}
	name := "${BUCKET}"
	if tc.defaultName {
		name = "${BUCKET:-shared}"
	}
	declaration := name
	if tc.declaration != "" {
		declaration = tc.declaration
	}
	external := &manifest.RawObject{
		APIVersion: "external-secrets.io/v1", Kind: "ExternalSecret", Name: declaration,
		Spec: map[string]any{"target": map[string]any{"name": name}},
	}
	unused := owner.Clone()
	unused.Name, unused.TargetNamespace = "unused", "apps"
	unused.PostBuild = &kustomizev1.PostBuild{Substitute: map[string]string{"BUCKET": "unused"}}
	other := owner.Clone()
	other.Name, other.TargetNamespace = "other-namespace", "other"
	other.PostBuild = &kustomizev1.PostBuild{Substitute: map[string]string{"BUCKET": "shared"}}
	owner.TargetNamespace = "apps"
	builder := selfProduceBuilder{idx: l.Index, store: st}
	for _, producer := range []*manifest.Kustomization{owner, unused, other} {
		st.AddObject(producer)
		l.Index.Records[producer.Named()] = manifest.DiscoveryRecord{Declaration: producer}
		if producer == owner {
			record := l.Index.Records[producer.Named()]
			record.Parent = parent
			l.Index.Records[producer.Named()] = record
		}
		occurrence := compactOccurrence(external, manifest.TemplateProjection{})
		builder.recordProducer(&producerDeclaration{
			fact: &occurrence, authored: external.Named(),
			identity: manifest.Resolve(external.Named(), producer.TargetNamespace, ""),
			visit: graphVisit{
				owner:        producer.Named(),
				source:       manifest.BootstrapSourceID,
				coordinate:   "component/external.yaml",
				graphContext: graphContext{namespace: producer.TargetNamespace},
			},
		}, nil)
	}
	assert.NoError(t, builder.finalizeTargets(t.Context(), l, nil))
	if tc.inherited {
		assert.Equal(t, owner.PostBuild.SubstituteFrom[0].Name, "${SETTINGS}")
	}
	for id := range l.Index.Records {
		assert.Equal(t, manifest.HasEnvsubstReference(id.Name), false)
		assert.Equal(t, id.Name != "", true)
	}
	target := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "shared"}
	record, _ := l.Index.Lookup(target)
	assert.Equal(t, slices.ContainsFunc(record.Productions, func(p manifest.Production) bool {
		return p.Owner == owner.Named()
	}), tc.known)
	for i := range record.Productions {
		production := &record.Productions[i]
		assert.Equal(t, production.Owner, owner.Named())
		assert.Equal(t, production.Source, manifest.BootstrapSourceID)
		assert.Equal(t, production.Declaration.Namespace, "apps")
		assert.Equal(t, manifest.HasEnvsubstReference(production.Declaration.Name), false)
	}
	for _, expected := range []struct{ namespace, name, owner string }{
		{"apps", "unused", "unused"}, {"other", "shared", "other-namespace"},
	} {
		target.Namespace, target.Name = expected.namespace, expected.name
		record, _ = l.Index.Lookup(target)
		assert.FatalIf(
			t,
			len(record.Productions) != 1,
			"target %s has productions %+v",
			target,
			record.Productions,
		)
		assert.Equal(t, record.Productions[0].Owner.Name, expected.owner)
	}
}
