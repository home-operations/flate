package scope_test

import (
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

func e2EChildKustomizationIdentityNameCase(t *testing.T, flags *[]string, tc **struct {
	name      string
	target    string
	authored  string
	namespace string
}, capturedInputs struct {
	root        *string
	rawBodies   *[]string
	order       *[2]int
	rawVersions *[]string
	removed     *int
},
) {
	t.Helper()

	renderRaw := func() map[string]string {
		t.Helper()
		out, stderr, code := harness.RunCLIBuffers(
			append([]string{"build", "all", "-o", "yaml"}, (*flags)...)...)
		assert.FatalIf(t, code != 0, "build exit=%d stderr=%s", code, stderr)
		raw := make(map[string]string)
		for body := range strings.SplitSeq(out, "---\n") {
			docs, err := testutil.SplitDocs([]byte(body))
			assert.NoError(t, err)
			for _, doc := range docs {
				version := manifest.DocAPIVersion(doc)
				name, ns := manifest.DocMetadata(doc)
				if manifest.DocKind(doc) != manifest.KindKustomization || name != "child" ||
					version == "kustomize.toolkit.fluxcd.io/v1" {
					continue
				}
				{
					_, duplicate := raw[version]
					assert.FatalIf(
						t,
						duplicate || ns != (*tc).namespace,
						"duplicate or wrong namespace for raw %s: %s",
						version,
						body,
					)
				}
				raw[version] = body
			}
		}
		return raw
	}
	testutil.WriteFile(
		t,
		(*capturedInputs.root),
		"apps/raw.yaml",
		(*capturedInputs.rawBodies)[capturedInputs.order[0]]+"---\n"+(*capturedInputs.rawBodies)[capturedInputs.order[1]],
	)
	before := renderRaw()
	assert.FatalIf(
		t,
		len(before) != 2 || before[(*capturedInputs.rawVersions)[0]] == "" ||
			before[(*capturedInputs.rawVersions)[1]] == "",
		"raw groups did not coexist: %v",
		before,
	)
	remaining := 1 - (*capturedInputs.removed)
	version := (*capturedInputs.rawVersions)[remaining]
	testutil.WriteFile(t, (*capturedInputs.root), "apps/raw.yaml", (*capturedInputs.rawBodies)[remaining])
	after := renderRaw()
	assert.FatalIf(
		t,
		len(after) != 1 || after[version] != before[version],
		"withdrawal changed surviving bytes or retained stale output: before=%v after=%v",
		before,
		after,
	)
	nextVersion := strings.TrimSuffix(version, "/v1") + "/v2"
	testutil.WriteFile(
		t,
		(*capturedInputs.root),
		"apps/raw.yaml",
		strings.Replace((*capturedInputs.rawBodies)[remaining], version, nextVersion, 1),
	)
	versioned := renderRaw()
	assert.FatalIf(t, len(versioned) != 1 ||
		versioned[nextVersion] != strings.Replace(
			before[version],
			version,
			nextVersion,
			1,
		), "version-only change split identity or changed content: before=%v after=%v", before, versioned)
}

func e2EChildKustomizationIdentityNameCase2(t *testing.T, flags *[]string, tc **struct {
	name      string
	target    string
	authored  string
	namespace string
}, capturedInputs struct {
	root        *string
	rawBodies   *[]string
	order       *[2]int
	rawVersions *[]string
	removed     *int
},
) {
	t.Helper()

	e2EChildKustomizationIdentityNameCase(t, flags, tc, struct {
		root        *string
		rawBodies   *[]string
		order       *[2]int
		rawVersions *[]string
		removed     *int
	}{root: capturedInputs.root, rawBodies: capturedInputs.rawBodies, order: capturedInputs.order, rawVersions: capturedInputs.rawVersions, removed: capturedInputs.removed})
}
