package helm

import (
	"strings"
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"

	sourcecache "github.com/home-operations/flate/internal/source/cache"
)

func TestTemplate_ActionSchemaPolicy(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
	testutil.WriteFile(t, dir, "mychart/templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: demo}\n")
	testutil.WriteFile(
		t,
		dir,
		"mychart/values.schema.json",
		`{"type":"object","properties":{"name":{"type":"string","pattern":"^valid$"}},"required":["name"]}`,
	)
	for _, tc := range []struct {
		name                               string
		install, upgrade, absent, wantSkip bool
	}{
		{name: "install validation disabled, upgrade enabled", install: true, wantSkip: true},
		{name: "install validation enabled, upgrade disabled", upgrade: true},
		{name: "nil install"},
		{name: "absent required value", absent: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testTemplateActionSchemaPolicyCase1(t, &tc, &dir)
		})
	}
}

func testTemplateActionSchemaPolicyCase1(t *testing.T, tc *struct {
	name     string
	install  bool
	upgrade  bool
	absent   bool
	wantSkip bool
}, dir *string,
) {
	t.Helper()

	cli := NewClient(cachetest.New(t, "", sourcecache.New))

	hr := newHR()
	hr.DisableSchemaValidation = tc.install
	if tc.install {
		hr.Install = &helmv2.Install{DisableSchemaValidation: true}
	}
	if tc.upgrade {
		hr.Upgrade = &helmv2.Upgrade{DisableSchemaValidation: true}
	}
	opts := Options{}
	values := map[string]any{"name": "invalid"}
	if tc.absent {
		delete(values, "name")
	}
	original := hr.Clone()
	warm := hr.Clone()
	warm.DisableSchemaValidation = true
	warm.Upgrade = &helmv2.Upgrade{DisableSchemaValidation: true}
	if _, _, err := testutil.RenderMaps(
		cli.Template(
			t.Context(),
			warm,
			values,
			RenderOptions{Options: opts, Sources: localChartResolver(t, "chart-repo", "flux-system", (*dir))},
		),
	); err != nil {
		t.Fatalf("warm cache: %v", err)
	}
	for range 2 {
		_, _, err := testutil.RenderMaps(
			cli.Template(
				t.Context(),
				hr,
				values,
				RenderOptions{Options: opts, Sources: localChartResolver(t, "chart-repo", "flux-system", (*dir))},
			),
		)
		assert.Diff(t, hr.HelmReleaseSpec, original.HelmReleaseSpec)
		assert.Equal(t, hr.DisableSchemaValidation, original.DisableSchemaValidation)
		if tc.wantSkip {
			assert.FatalIf(t, err != nil, "%s", err)

			continue
		}
		assert.FatalIf(t, err == nil, "%s", "invalid schema accepted after warmed skipped render")

		if tc.absent {
			assert.FatalIf(t, !strings.Contains(err.Error(), "name"), "%s", err)

			continue
		}
		assert.Equal(
			t,
			err.Error(),
			"helm template: values don't meet the specifications of the schema(s) in the following chart(s):\nmychart:\n- at '/name': 'invalid' does not match pattern '^valid$'\n",
		)
	}
}

func TestTemplate_SchemaErrorOrder(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
	testutil.WriteFile(t, dir, "mychart/templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: demo}\n")
	testutil.WriteFile(t, dir, "mychart/values.schema.json", `{
  "type":"object",
  "properties":{
    "alpha":{"type":"string"},
    "middle":{"type":"string"},
    "zeta":{"type":"string"},
    "controllers":{"type":"object","properties":{
      "3ds":{"allOf":[{"properties":{"timeZone":{"type":"string"},"enabled":{"type":"boolean"}}}]},
      "ds":{"allOf":[{"properties":{"timeZone":{"type":"string"},"enabled":{"type":"boolean"}}}]},
      "omega":{"allOf":[{"properties":{"timeZone":{"type":"string"},"enabled":{"type":"boolean"}}}]}
    }}
  }
}`)
	cli := NewClient(cachetest.New(t, "", sourcecache.New))
	hr := newHR()
	values := map[string]any{
		"alpha": nil, "middle": nil, "zeta": nil,
		"controllers": map[string]any{
			"3ds":   map[string]any{"timeZone": nil, "enabled": nil},
			"ds":    map[string]any{"timeZone": nil, "enabled": nil},
			"omega": map[string]any{"timeZone": nil, "enabled": nil},
		},
	}
	input := RenderOptions{Sources: localChartResolver(t, "chart-repo", "flux-system", dir)}
	_, _, first := testutil.RenderMaps(cli.Template(t.Context(), hr, values, input))
	_, _, second := testutil.RenderMaps(cli.Template(t.Context(), hr, values, input))

	assert.FatalIf(t, first == nil || second == nil, "schema failures: %v; %v", first, second)

	const want = `helm template: values don't meet the specifications of the schema(s) in the following chart(s):
mychart:
- at '/alpha': got null, want string
- at '/controllers': validation failed
  - at '/controllers/3ds': 'allOf' failed
    - at '/controllers/3ds': validation failed
      - at '/controllers/3ds/enabled': got null, want boolean
      - at '/controllers/3ds/timeZone': got null, want string
  - at '/controllers/ds': 'allOf' failed
    - at '/controllers/ds': validation failed
      - at '/controllers/ds/enabled': got null, want boolean
      - at '/controllers/ds/timeZone': got null, want string
  - at '/controllers/omega': 'allOf' failed
    - at '/controllers/omega': validation failed
      - at '/controllers/omega/enabled': got null, want boolean
      - at '/controllers/omega/timeZone': got null, want string
- at '/middle': got null, want string
- at '/zeta': got null, want string
`
	assert.Equal(t, first.Error(), want)
	assert.Equal(t, second.Error(), want)
	assert.Equal(t, first.Error(), second.Error())
}

func TestTemplate_SchemaPropertyOrder(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
	testutil.WriteFile(t, dir, "mychart/templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: demo}\n")
	testutil.WriteFile(t, dir, "mychart/values.schema.json", `{"type":"object","additionalProperties":false}`)
	cli := NewClient(cachetest.New(t, "", sourcecache.New))
	hr := newHR()
	values := map[string]any{"zulu": true, "alpha": true, "mu": true, "beta": true, "theta": true, "omega": true}
	input := RenderOptions{Sources: localChartResolver(t, "chart-repo", "flux-system", dir)}
	_, _, first := testutil.RenderMaps(cli.Template(t.Context(), hr, values, input))
	_, _, second := testutil.RenderMaps(cli.Template(t.Context(), hr, values, input))

	assert.FatalIf(t, first == nil || second == nil, "schema failures: %v; %v", first, second)

	const want = `helm template: values don't meet the specifications of the schema(s) in the following chart(s):
mychart:
- at '': additional properties 'alpha', 'beta', 'mu', 'omega', 'theta', 'zulu' not allowed
`
	assert.Equal(t, first.Error(), want)
	assert.Equal(t, second.Error(), want)
	assert.Equal(t, first.Error(), second.Error())
}
