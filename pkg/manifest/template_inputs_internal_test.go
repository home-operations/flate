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
		{"opaque payload", doc + "data: {value: {{ .Values.VALUE | quote }}}\n", 2},
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
			testTemplateInputsStaticReferencesCase(t, &tc)
		},
		)
	}
	t.Run("first evaluated child replaces draft", func(t *testing.T) {
		t.Parallel()
		testTemplateInputsStaticReferencesFirstEvaluatedChildReplacesDraft(
			t,
			&doc,
		)
	},
	)
}

func TestProducerProjections_ConcurrentContent(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"producer", "template"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			testProducerProjectionsConcurrentContentCase(
				t,
				&mode,
			)
		},
		)
	}
}

//nolint:paralleltest // Subtests share a synctest bubble and cannot run in parallel.
func TestProducerProjections_DifferentContentDoesNotWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		projections := NewProducerProjections()
		defer projections.Clear()
		first := &ResourceSet{Name: "first", Namespace: "apps"}
		first.ResourcesTemplate = "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: first}\n"
		second := *first
		second.Name = "second"
		second.ResourcesTemplate = strings.ReplaceAll(first.ResourcesTemplate, "first", "second")
		_, err := projections.ProjectTemplate(t.Context(), nil, "")
		assert.NoError(t, err)
		release, err := projections.cache.Load().locks.Acquire(t.Context(), Fingerprint(first))
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

func testTemplateInputsStaticReferencesCase(t *testing.T, testCaseInput *struct {
	name    string
	content string
	count   int
},
) {
	t.Helper()

	projection, err := NewProducerProjections().ProjectTemplate(t.Context(), []byte(testCaseInput.content), "apps")
	assert.NoError(t, err)
	refs := projection.Inputs
	assert.Equal(t, len(refs), testCaseInput.count)
	{
		inputValues := refs
		for inputIndex := range inputValues {
			input := inputValues[inputIndex]
			ref := input.Reference
			namespace := "apps"
			if testCaseInput.name == "explicit namespace" {
				namespace = "foreign"
				assert.Equal(t, ref.NamespaceDefault, false)
			}
			assert.Equal(t, ref.Namespace, namespace)
			assert.Equal(t, ref.Gate, GateStage(0))
			if ref.Use == "source" {
				assert.Equal(t, input.UnresolvedPath, testCaseInput.name == "dynamic path")
				if testCaseInput.name == "alias" {
					assert.Equal(t, ref.Name, "child")
				}
				if testCaseInput.name == "dynamic path" {
					assert.Equal(t, len(ref.Files), 0)
				} else {
					assert.Diff(t, ref.Files, []string{"apps/child"})
				}
			}
		}
	}
}

func testTemplateInputsStaticReferencesFirstEvaluatedChildReplacesDraft(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	parsed, err := ParseDoc(map[string]any{
		"apiVersion": FluxOperatorDomain + "/v1", "kind": KindResourceSet,
		"metadata": map[string]any{"name": "factory", "namespace": "apps"},
		"spec": map[string]any{
			"inputs": []any{
				map[string]any{"path": "first", "settings": "accepted"},
				map[string]any{"path": "second", "settings": "rejected"},
			},
			"resourcesTemplate": strings.ReplaceAll(
				strings.ReplaceAll((*testCaseInput), "./apps/child", "'<< inputs.path >>'"),
				"name: settings",
				"name: '<< inputs.settings >>'",
			),
		},
	}, ParseDocOptions{})
	object := parsed.Object
	assert.NoError(t, err)
	projection := ProjectProducer(object)
	assert.Equal(t, len(projection.Objects), 1)
	assert.Equal(t, projection.Objects[0].(*Kustomization).Path, "first")
	assert.Equal(t, len(projection.Targets), 1)
	assert.Equal(t, len(projection.Inputs), 2)
	{
		inputValues2 := projection.Inputs
		for inputIndex2 := range inputValues2 {
			input := inputValues2[inputIndex2]
			if input.Reference.Use == "source" {
				assert.Diff(t, input.Reference.Files, []string{"first"})
			} else {
				assert.Equal(t, input.Reference.Name, "accepted")
			}
		}
	}
}

func testProducerProjectionsConcurrentContentCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()
	const content = "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: shared}\ndata: {value: '{{ .Values.VALUE }}'}\n"

	projections := NewProducerProjections()
	defer projections.Clear()
	project := func() (TemplateProjection, error) {
		if (*testCaseInput) == "template" {
			return projections.ProjectTemplate(t.Context(), []byte(content), "apps")
		}
		set := &ResourceSet{Name: "set", Namespace: "apps"}
		set.ResourcesTemplate = content
		return projections.Project(t.Context(), set)
	}
	var results [2]TemplateProjection
	var failures [2]error
	var callers sync.WaitGroup
	start := make(chan struct{})
	for i := range results {
		callers.Go(func() {
			<-start
			results[i], failures[i] = project()
		})
	}
	close(start)
	callers.Wait()
	for _, err := range failures {
		assert.NoError(t, err)
	}
	for _, projection := range results {
		assert.Equal(t, len(projection.Targets), 1)
	}
	assert.FatalIf(
		t,
		&results[0].Targets[0] != &results[1].Targets[0],
		"%s",
		"equal content did not share immutable projection facts",
	)
	assert.FatalIf(t, (*testCaseInput) == "producer" &&
		(results[0].Objects[0] == nil || results[0].Objects[0] != results[1].Objects[0]), "%s", "equal content did not share the computed immutable object")
	projections.Clear()
	fresh, err := project()
	assert.NoError(t, err)
	assert.Equal(t, len(fresh.Targets), 1)
	assert.FatalIf(
		t,
		&fresh.Targets[0] == &results[0].Targets[0],
		"%s",
		"cleared content reused prior projection facts",
	)

	if (*testCaseInput) == "template" {
		other, err := projections.ProjectTemplate(t.Context(), []byte(content), "other")
		assert.NoError(t, err)
		assert.Equal(t, other.Targets[0].Namespace, "other")
		changed, err := projections.ProjectTemplate(
			t.Context(),
			[]byte(strings.ReplaceAll(content, "shared", "changed")),
			"apps",
		)
		assert.NoError(t, err)
		assert.Equal(t, changed.Targets[0].Name, "changed")
	}
}
