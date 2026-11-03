package manifest

import (
	"errors"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func testParseDocDispatchCase(t *testing.T, testCaseInput *testParseDocDispatchCaseInputs) {
	t.Helper()

	doc := mustYAML(t, testCaseInput.tc.yaml)
	md := doc["metadata"].(map[string]any)
	delete(md, "namespace")
	if testCaseInput.ns.authored != "" {
		md["namespace"] = testCaseInput.ns.authored
	}
	before := DeepCopyMap(doc)
	obj, authoritative, changed, err := ParseDoc(
		doc,
		ParseDocOptions{WipeSecrets: true, DefaultNamespace: testCaseInput.ns.fallback},
	)
	assert.NoError(t, err)
	want := testCaseInput.ns.authored
	if want == "" && !testCaseInput.tc.cluster {
		want = testCaseInput.ns.fallback
	}
	assert.Equal(t, obj.Named().Kind, testCaseInput.tc.kind)
	assert.Equal(t, obj.Named().Group, apiGroup(DocAPIVersion(doc)))
	assert.Equal(t, obj.Named().Namespace, want)
	defaulted := testCaseInput.ns.authored == "" && want != ""
	assert.Equal(t, changed, defaulted)
	assert.Diff(t, doc, before)
	if defaulted {
		before["metadata"].(map[string]any)["namespace"] = want
	}
	assert.Diff(t, authoritative, before)
	replayed, replayDoc, replayChanged, err := ParseDoc(
		authoritative,
		ParseDocOptions{WipeSecrets: true, DefaultNamespace: testCaseInput.ns.fallback},
	)
	assert.NoError(t, err)
	assert.Equal(t, replayed.Named(), obj.Named())
	assert.Equal(t, replayChanged, false)
	assert.Diff(t, replayDoc, authoritative)
	if raw, ok := obj.(*RawObject); ok {
		info := KindOf(raw)
		assert.Equal(t, info.Group, apiGroup(DocAPIVersion(doc)))
		assert.Equal(t, info.Tracked || info.Runnable || info.Status || info.Discovery || info.Test, false)
		spec, _ := doc["spec"].(map[string]any)
		assert.Diff(t, raw.Spec, spec)
	}
}

func testParseDocMissingFieldsCase(t *testing.T, testCaseInput *struct {
	name     string
	metadata any
	absent   bool
	raw      string
	core     string
	typed    string
},
) {
	t.Helper()

	for _, admission := range []struct{ kind, version, want string }{
		{"Widget", "example.test/v1", testCaseInput.raw},
		{KindConfigMap, "v1", testCaseInput.core},
		{KindHelmRelease, HelmReleaseDomain + "/v2", testCaseInput.typed},
	} {
		t.Run(admission.kind, func(t *testing.T) {
			t.Parallel()
			doc := map[string]any{"apiVersion": admission.version, "kind": admission.kind}
			if !testCaseInput.absent {
				doc["metadata"] = testCaseInput.metadata
			}
			before := DeepCopyMap(doc)
			obj, authoritative, changed, err := ParseDoc(doc, ParseDocOptions{DefaultNamespace: "apps"})
			if admission.want == "" {
				assert.NoError(t, err)
				assert.Equal(t, obj.Named().Namespace, "apps")
				assert.Equal(t, changed, true)
			} else {
				{
					assert.FatalIf(t, !errors.Is(err, ErrInput) || !errors.Is(err, ErrFlux) ||
						!strings.Contains(err.Error(), admission.want), "error = %v, want %q with ErrInput/ErrFlux", err, admission.want)
				}
				assert.Equal(t, changed, false)
				assert.Diff(t, authoritative, before)
			}
			assert.Diff(t, doc, before)
		})
	}
}

func testParseDocMissingFieldsTypedSpecErrorPrecedesMissingMetadata(t *testing.T) {
	t.Helper()

	doc := map[string]any{"apiVersion": HelmReleaseDomain + "/v2", "kind": KindHelmRelease, "spec": "bad"}
	_, authoritative, changed, err := ParseDoc(doc, ParseDocOptions{DefaultNamespace: "apps"})
	{
		assert.FatalIf(t, !errors.Is(err, ErrInput) || !errors.Is(err, ErrFlux) ||
			!strings.Contains(
				err.Error(),
				"HelmRelease decode: json: cannot unmarshal string",
			), "malformed spec lost decode precedence: %v", err)
	}
	assert.Equal(t, changed, false)
	assert.Diff(t, authoritative, doc)
}
