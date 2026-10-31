package manifest

import (
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/home-operations/flate/internal/assert"
)

func TestTemplateInputs_StaticReferences(t *testing.T) {
	t.Parallel()
	doc := `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: child}
spec:
  path: ./apps/child
  sourceRef: {kind: GitRepository, name: content}
  postBuild:
    substituteFrom: [{kind: ConfigMap, name: settings}]
`
	for _, tc := range []struct {
		name, content string
		count         int
	}{
		{"concrete", doc, 2},
		{"list", "apiVersion: v1\nkind: List\nitems:\n  - " + strings.ReplaceAll(strings.TrimSuffix(doc, "\n"), "\n", "\n    ") + "\n", 2},
		{"list alias", "apiVersion: v1\nx-kind: &list List\nkind: *list\nitems:\n  - " + strings.ReplaceAll(strings.TrimSuffix(doc, "\n"), "\n", "\n    ") + "\n", 2},
		{"list merge", "defaults: &defaults {apiVersion: v1, kind: List}\n<<: *defaults\nitems:\n  - " + strings.ReplaceAll(strings.TrimSuffix(doc, "\n"), "\n", "\n    ") + "\n", 2},
		{"alias", strings.ReplaceAll(strings.ReplaceAll(doc, "metadata: {name: child}", "metadata: {name: &identity child}"), "name: content", "name: *identity"), 2},
		{"constant template", "{{ print " + strconv.Quote(doc) + " }}", 2},
		{"explicit namespace", strings.ReplaceAll(doc, "metadata: {name: child}", "metadata: {name: child, namespace: foreign}"), 2},
		{"dynamic source", strings.ReplaceAll(doc, "name: content", "name: '<< inputs.source >>'"), 1},
		{"dynamic path", strings.ReplaceAll(doc, "./apps/child", "'{{ .Values.path }}'"), 2},
		{"known before opaque", doc + "---\n{{ include \"unknown\" . }}\n", 2},
		{"known after opaque", "{{ include \"unknown\" . }}\n---\n" + doc, 2},
		{"malformed", "{{ include \"content\" . }}", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			refs := ProjectTemplate([]byte(tc.content), "apps").Inputs
			assert.Equal(t, len(refs), tc.count)
			for _, input := range refs {
				ref := input.Reference
				namespace := "apps"
				if tc.name == "explicit namespace" {
					namespace = "foreign"
					assert.Equal(t, ref.NamespaceDefault, false)
				}
				assert.Equal(t, ref.Namespace, namespace)
				assert.Equal(t, ref.Gate, GateStage(0))
				if ref.Use == "source" {
					if tc.name == "alias" {
						assert.Equal(t, ref.Name, "child")
					}
					if tc.name == "dynamic path" {
						assert.Equal(t, len(ref.Files), 0)
					} else {
						assert.Diff(t, ref.Files, []string{"apps/child"})
					}
				}
			}
		})
	}
}

func TestProducerProjections_ConcurrentContent(t *testing.T) {
	t.Parallel()
	projections := NewProducerProjections()
	defer projections.Clear()
	const content = "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: shared}\n"
	var objects [2]BaseManifest
	var failures [2]error
	var callers sync.WaitGroup
	for i := range objects {
		callers.Go(func() {
			set := &ResourceSet{Name: "set", Namespace: "apps"}
			set.ResourcesTemplate = content
			projection, err := projections.Project(t.Context(), set)
			failures[i] = err
			if len(projection.Objects) != 0 {
				objects[i] = projection.Objects[0]
			}
		})
	}
	callers.Wait()
	for _, err := range failures {
		assert.NoError(t, err)
	}
	if objects[0] == nil || objects[0] != objects[1] {
		t.Fatal("equal content did not share the computed immutable object")
	}
}

func TestProducerProjections_DifferentContentDoesNotWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		projections := NewProducerProjections()
		defer projections.Clear()
		first := &ResourceSet{Name: "first", Namespace: "apps"}
		first.ResourcesTemplate = "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: first}\n"
		second := *first
		second.Name = "second"
		second.ResourcesTemplate = strings.ReplaceAll(first.ResourcesTemplate, "first", "second")
		release, err := projections.locks.Acquire(t.Context(), Fingerprint(first))
		assert.NoError(t, err)
		waiting := make(chan error, 1)
		independent := make(chan error, 1)
		go func() {
			_, err := projections.Project(t.Context(), first)
			waiting <- err
		}()
		go func() {
			_, err := projections.Project(t.Context(), &second)
			independent <- err
		}()
		synctest.Wait()
		select {
		case err := <-independent:
			assert.NoError(t, err)
		default:
			t.Fatal("different producer content waited on a held key")
		}
		select {
		case <-waiting:
			t.Fatal("the held content key did not block its caller")
		default:
		}
		release()
		assert.NoError(t, <-waiting)
	})
}
