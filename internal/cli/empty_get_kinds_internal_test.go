package cli

import (
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/output"
	"github.com/home-operations/flate/internal/testutil"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_GetAll_EmptyKinds(t *testing.T) {
	for _, kinds := range []string{"ks", "hr", "empty"} {
		t.Run(kinds, func(t *testing.T) { runGetAllEmptyKindsKinds(t, &kinds) })
	}
}

func runGetAllEmptyKindsKinds(t *testing.T, kinds *string) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, "kustomization.yaml", "resources: [objects.yaml]\n")
	body := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: empty}\n"
	want := ""
	switch *kinds {
	case "ks":
		body = testutil.KustomizationYAML("only", "flux-system", "  suspend: true\n  path: ./absent\n")
		want = "Kustomization/only\n"
	case "hr":
		body = "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: only, namespace: apps}\nspec: {suspend: true, chart: {spec: {chart: demo, version: 1.0.0, sourceRef: {kind: HelmRepository, name: charts}}}}\n"
		want = "HelmRelease/only\n"
	}
	testutil.WriteFile(t, root, "objects.yaml", body)
	for _, format := range []string{"", "table", "name", "json", "yaml"} {
		t.Run("format="+format, func(t *testing.T) {
			runGetAllEmptyKindsKindsCase(t, &root, &format, struct {
				want  *string
				kinds **string
			}{want: &want, kinds: &kinds})
		})
	}
}

func runGetAllEmptyKindsKindsCase(t *testing.T, root *string, format *string, capturedInputs struct {
	want  *string
	kinds **string
},
) {
	t.Helper()

	var all string
	runGetAllEmptyKindsKindsCaseLoop(t, struct {
		root           **string
		format         **string
		all            *string
		capturedInputs *struct {
			want  *string
			kinds **string
		}
	}{root: &root, format: &format, all: &all, capturedInputs: &capturedInputs})
}

func runGetAllEmptyKindsKindsCaseLoop(t *testing.T, loopInputs struct {
	root           **string
	format         **string
	all            *string
	capturedInputs *struct {
		want  *string
		kinds **string
	}
},
) {
	t.Helper()

	for _, command := range []string{"get all", "get"} {
		args := append(
			strings.Fields(command),
			"--path",
			(*(*loopInputs.root)),
			"--cache-dir",
			t.TempDir(),
			"--concurrency",
			"2",
		)
		if (*(*loopInputs.format)) != "" {
			args = append(args, "-o", (*(*loopInputs.format)))
		}
		out, stderr, code := runClusterInputs(t, args...)
		assert.FatalIf(t, code != 0, "exit=%d stdout=%s stderr=%s", code, out, stderr)
		if command == "get all" {
			(*loopInputs.all) = out
		} else {
			assert.Equal(t, out, (*loopInputs.all))
		}
		switch *(*loopInputs.format) {
		case "name":
			assert.Equal(t, out, (*loopInputs.capturedInputs.want))
		case "json", "yaml":
			groups := decodeGetDocuments(t, out, (*(*loopInputs.format)), []string{"."})
			runGetAllEmptyKindsKindsCaseLoopLoop(t, struct {
				loopInputs *struct {
					root           **string
					format         **string
					all            *string
					capturedInputs *struct {
						want  *string
						kinds **string
					}
				}
				groups *[]output.DocumentGroup
				out    *string
			}{loopInputs: &loopInputs, groups: &groups, out: &out})
		default:
			switch {
			case (*(*loopInputs.capturedInputs.kinds)) == "empty":

				assert.Equal(t, out, "")

			case strings.Contains(out, "\n\n") || !strings.Contains(out, strings.TrimSpace((*loopInputs.capturedInputs.want))):

				t.Fatalf("empty kind left a section: %q", out)
			}
		}
	}
}

func runGetAllEmptyKindsKindsCaseLoopLoop(t *testing.T, loopInputs struct {
	loopInputs *struct {
		root           **string
		format         **string
		all            *string
		capturedInputs *struct {
			want  *string
			kinds **string
		}
	}
	groups *[]output.DocumentGroup
	out    *string
},
) {
	t.Helper()

	if (*(*loopInputs.loopInputs.capturedInputs.kinds)) == "empty" {
		assert.Equal(t, len((*loopInputs.groups)[0].Documents), 0)
		if (*(*loopInputs.loopInputs.format)) == "json" {
			assert.Equal(t, (*loopInputs.out), "[]\n")
		} else {
			assert.Equal(t, (*loopInputs.out), "")
		}
	} else {
		assert.Equal(t, len((*loopInputs.groups)[0].Documents), 1)
		assert.Equal(
			t,
			(*loopInputs.groups)[0].Documents[0]["kind"].(string)+"/"+(*loopInputs.groups)[0].Documents[0]["name"].(string)+"\n",
			(*loopInputs.loopInputs.capturedInputs.want),
		)
	}
}
