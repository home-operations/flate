package cli

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func substitutionFixture(t *testing.T, postBuild, expression string) string {
	t.Helper()
	root := t.TempDir()
	testutil.GitInit(t, root)
	testutil.WriteFile(t, root, "kustomization.yaml", "resources: [flux.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"flux.yaml",
		testutil.KustomizationYAML("consumer", "apps", "  interval: 1m\n  path: ./app\n")+postBuild,
	)
	testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"app/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: rendered, namespace: apps}\ndata:\n  value: '"+expression+"'\n",
	)
	return root
}

func substitutionInput(t *testing.T, body string) string {
	t.Helper()
	root := t.TempDir()
	testutil.WriteFile(t, root, "values.yaml", body)
	return filepath.Join(root, "values.yaml")
}

func substitutionReference(kind string, optional bool) string {
	return fmt.Sprintf(
		"  postBuild:\n    substituteFrom:\n      - kind: %s\n        name: external\n        optional: %v\n",
		kind,
		optional,
	)
}

func substitutionOutput(t *testing.T, output string) string {
	t.Helper()
	docs, err := testutil.SplitDocs([]byte(output))
	assert.NoError(t, err)
	assert.FatalIf(
		t,
		len(docs) != 2,
		"expected rendered ConfigMap and applied Kustomization, got %d documents: %s",
		len(docs),
		output,
	)
	declaration := docs[1]
	result1 := manifest.DocMetadata(declaration)
	name, namespace := result1.Name, result1.Namespace
	spec, _ := declaration["spec"].(map[string]any)
	assert.FatalIf(
		t,
		manifest.DocKind(declaration) != manifest.KindKustomization || name != "consumer" || namespace != "apps" ||
			spec["path"] != "./app" ||
			spec["interval"] != "1m",
		"unexpected applied consumer declaration: %v",
		declaration,
	)
	result2 := manifest.DocMetadata(docs[0])
	name, namespace = result2.Name, result2.Namespace
	assert.FatalIf(t, manifest.DocKind(docs[0]) != manifest.KindConfigMap || name != "rendered" || namespace != "apps",
		"unexpected emitted input: %v", docs[0])

	value, _ := docs[0]["data"].(map[string]any)["value"].(string)
	return value
}

const (
	externalCM     = "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: external, namespace: apps}\ndata: {VALUE: supplied}\n"
	externalSecret = "apiVersion: v1\nkind: Secret\nmetadata: {name: external, namespace: apps}\nstringData: {VALUE: supplied}\n"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_ExternalSubstitution(t *testing.T) {
	cases := []struct {
		name, postBuild, input, want string
		fail, strict                 bool
	}{
		{name: "required absent", postBuild: substitutionReference("ConfigMap", false), fail: true},
		{
			name:      "required supplied",
			postBuild: substitutionReference("ConfigMap", false),
			input:     externalCM,
			want:      "supplied",
			strict:    true,
		},
		{name: "wrong kind", postBuild: substitutionReference("ConfigMap", false), input: externalSecret, fail: true},
		{
			name:      "wrong namespace",
			postBuild: substitutionReference("ConfigMap", false),
			input:     strings.Replace(externalCM, "namespace: apps", "namespace: other", 1),
			fail:      true,
		},
		{
			name:      "wrong name",
			postBuild: substitutionReference("ConfigMap", false),
			input:     strings.Replace(externalCM, "name: external", "name: other", 1),
			fail:      true,
		},
		{name: "optional absent", postBuild: substitutionReference("ConfigMap", true), want: "${VALUE}"},
		{
			name:      "optional supplied",
			postBuild: substitutionReference("ConfigMap", true),
			input:     externalCM,
			want:      "supplied",
		},
		{
			name:      "secret stringData",
			postBuild: substitutionReference("Secret", false),
			input:     externalSecret,
			want:      "supplied",
			strict:    true,
		},
		{
			name:      "secret data",
			postBuild: substitutionReference("Secret", false),
			input: strings.Replace(
				externalSecret,
				"stringData: {VALUE: supplied}",
				"data: {VALUE: c3VwcGxpZWQ=}",
				1,
			),
			want: "supplied",
		},
		{
			name:      "secret ciphertext empty",
			postBuild: substitutionReference("Secret", false),
			input: strings.Replace(
				externalSecret,
				"supplied",
				"'ENC[AES256_GCM,data:YWJj,iv:ZGVm,tag:Z2hp,type:str]'",
				1,
			) + "sops: {encrypted_regex: data}\n",
			want: "${VALUE}",
		},
		{name: "missing unsupplied secret", postBuild: substitutionReference("Secret", false), fail: true},
		{
			name:      "inline beats reference",
			postBuild: substitutionReference("ConfigMap", false) + "    substitute: {VALUE: inline}\n",
			input:     externalCM,
			want:      "inline",
			strict:    true,
		},
		{
			name:      "later reference wins",
			postBuild: substitutionReference("ConfigMap", false) + "      - kind: Secret\n        name: external\n",
			input:     externalCM + "---\n" + strings.Replace(externalSecret, "supplied", "second", 1),
			want:      "second",
			strict:    true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			testRunExternalSubstitutionCase1(t, &tc)
		})
	}
}

func testRunExternalSubstitutionCase1(t *testing.T, tc *struct {
	name      string
	postBuild string
	input     string
	want      string
	fail      bool
	strict    bool
},
) {
	t.Helper()
	root := substitutionFixture(t, tc.postBuild, "${VALUE}")
	args := []string{
		"build",
		"all",
		"--path",
		root,
		"--concurrency",
		"2",
		"--cache-dir",
		t.TempDir(),
		"--skip-kinds",
		"CustomResourceDefinition",
	}
	if tc.input != "" {
		args = append(args, "--supply", substitutionInput(t, tc.input))
	}
	if tc.strict {
		args = append(args, "--strict-substitutions")
	}
	migratedResult1 := runCLI(t, args...)
	out, diagnostic, code := migratedResult1.stdout, migratedResult1.stderr, migratedResult1.code
	assert.FatalIf(t, (code != 0) != tc.fail,
		"exit=%d: %s", code, diagnostic)

	if tc.fail {
		assert.FatalIf(t, !strings.Contains(diagnostic, "ConfigMap") && !strings.Contains(diagnostic, "Secret") ||
			!strings.Contains(diagnostic, "apps/external"),
			"missing reference diagnostic: %s", diagnostic)

		return
	}
	if got := substitutionOutput(t, out); got != tc.want {
		t.Fatalf("got %q, want %q", got, tc.want)
	}
}

func TestRun_SubstitutionFilesAndDuplicates(t *testing.T) {
	root := substitutionFixture(t, substitutionReference("ConfigMap", false), "${VALUE}-${OLD:-gone}")
	cwd := t.TempDir()
	t.Chdir(cwd)
	testutil.WriteFile(
		t,
		cwd,
		"first,values.yaml",
		strings.Replace(externalCM, "data: {VALUE: supplied}", "data: {VALUE: first, OLD: old}", 1),
	)
	testutil.WriteFile(
		t,
		cwd,
		"last.yaml",
		"---\nnull\n---\n"+strings.Replace(
			externalCM,
			"supplied",
			"second",
			1,
		)+"---\n"+strings.Replace(
			externalCM,
			"supplied",
			"last",
			1,
		),
	)
	migratedResult2 := runCLI(
		t,
		"build",
		"all",
		"--path",
		root,
		"--cache-dir",
		t.TempDir(),
		"--concurrency",
		"4",
		"--supply",
		"first,values.yaml",
		"--supply",
		"last.yaml",
	)
	out, diagnostic, code := migratedResult2.stdout, migratedResult2.stderr, migratedResult2.code

	assert.FatalIf(t, code != 0, "%v", diagnostic)
	{
		got := substitutionOutput(t, out)
		assert.FatalIf(t, got != "last-gone", "duplicate objects merged or ordered incorrectly: %q", got)
	}
}

func TestRun_SubstitutionEnvironment(t *testing.T) {
	input := substitutionInput(t, externalCM)
	second := substitutionInput(t, strings.Replace(externalCM, "supplied", "last", 1))
	for _, tc := range []struct {
		name, files, strict, want string
		flags                     []string
	}{
		{name: "colon file list", files: input + ":" + second, strict: "true", want: "last"},
		{name: "file flags override invalid env", files: ":", strict: "true", want: "supplied", flags: []string{"--supply", input}},
		{name: "false flag overrides invalid bool env", files: input, strict: "invalid", want: "supplied", flags: []string{"--strict-substitutions=false"}},
		{name: "false flag overrides true env", files: input, strict: "true", want: "supplied", flags: []string{"--strict-substitutions=false"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("FLATE_SUPPLY", tc.files)
			t.Setenv("FLATE_STRICT_SUBSTITUTIONS", tc.strict)
			if tc.strict == "" {
				t.Setenv("FLATE_STRICT_SUBSTITUTIONS", "false")
			}
			root := substitutionFixture(t, substitutionReference("ConfigMap", false), "${VALUE}")
			args := append(
				[]string{"build", "all", "--path", root, "--cache-dir", t.TempDir(), "--concurrency", "2"},
				tc.flags...)
			migratedResult3 := runCLI(t, args...)
			out, diagnostic, code := migratedResult3.stdout, migratedResult3.stderr, migratedResult3.code
			assert.FatalIf(t, code != 0, "%v", diagnostic)
			{
				got := substitutionOutput(t, out)
				assert.FatalIf(t, got != tc.want, "got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestSupplyEnv_EmptyPath(t *testing.T) {
	t.Parallel()
	for _, input := range []string{":", "file:", ":file"} {
		{
			_, err := supplyEnv(input)
			assert.FatalIf(t, !errors.Is(err, manifest.ErrInput), "empty file component accepted: %v", err)
		}
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_StrictSubstitutions(t *testing.T) {
	for _, tc := range []struct {
		name, expression, postBuild, disabled, want string
		strict, fail                                bool
	}{
		{name: "default undefined", expression: "${VALUE}", postBuild: "  postBuild: {}\n", want: "${VALUE}"},
		{name: "strict undefined", expression: "${VALUE}", postBuild: "  postBuild: {substituteStrategy: Always}\n", strict: true, fail: true},
		{name: "defined empty", expression: "${VALUE}", postBuild: "  postBuild:\n    substitute: {VALUE: ''}\n", strict: true},
		{name: "assignment default", expression: "${VALUE:=fallback}", postBuild: "  postBuild: {substituteStrategy: Always}\n", strict: true, want: "fallback"},
		{name: "colon default", expression: "${VALUE:-fallback}", postBuild: "  postBuild: {substituteStrategy: Always}\n", strict: true, want: "fallback"},
		{name: "empty with default", expression: "${VALUE:-fallback}", postBuild: "  postBuild:\n    substitute: {VALUE: ''}\n", strict: true, want: "fallback"},
		{name: "escape", expression: "$${VALUE}", postBuild: "  postBuild: {substituteStrategy: Always}\n", strict: true, want: "${VALUE}"},
		{name: "annotation disabled", expression: "${VALUE}", postBuild: "  postBuild: {substituteStrategy: Always}\n", disabled: "annotations", strict: true, want: "${VALUE}"},
		{name: "label disabled", expression: "${VALUE}", postBuild: "  postBuild: {substituteStrategy: Always}\n", disabled: "labels", strict: true, want: "${VALUE}"},
		{name: "no postBuild", expression: "${VALUE}", strict: true, want: "${VALUE}"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := substitutionFixture(t, tc.postBuild, tc.expression)
			if tc.disabled != "" {
				testutil.WriteFile(
					t,
					root,
					"app/cm.yaml",
					"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: rendered\n  namespace: apps\n  "+tc.disabled+": {kustomize.toolkit.fluxcd.io/substitute: disabled}\ndata:\n  value: '"+tc.expression+"'\n",
				)
			}
			args := []string{"build", "all", "--path", root, "--cache-dir", t.TempDir(), "--concurrency", "4"}
			if tc.strict {
				args = append(args, "--strict-substitutions")
			}
			migratedResult4 := runCLI(t, args...)
			out, diagnostic, code := migratedResult4.stdout, migratedResult4.stderr, migratedResult4.code
			assert.FatalIf(t, (code != 0) != tc.fail, "exit=%d: %s", code, diagnostic)
			if tc.fail {
				for _, want := range []string{"apps/consumer", "VALUE", "variable not set (strict mode)"} {
					assert.FatalIf(t, !strings.Contains(diagnostic, want), "diagnostic lost %q: %s", want, diagnostic)
				}
			} else if got := substitutionOutput(t, out); got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_SubstitutionHelpAndSharedCommands(t *testing.T) {
	root := substitutionFixture(t, substitutionReference("ConfigMap", false), "${VALUE}")
	input := substitutionInput(t,
		externalCM+"---\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: rendered, namespace: apps}\nda"+
			"ta: {value: private-fixture-value}\n---\napiVersion: kustomize.toolkit.fluxcd.io/v1\n"+
			"kind: Kustomization\nmetadata: {name: consumer, namespace: apps}\nspec: {path: ./abse"+
			"nt-supply}\n"+"---\napiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: u"+
			"nused-supply, namespace: supply-only}\nspec: {url: 'file:///absent-supply'}\n---\napi"+
			"Version: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: unused"+
			"-supply, namespace: supply-only}\nspec: {path: ./absent-supply}\n---\napiVersion: api"+
			"extensions.k8s.io/v1\nkind: CustomResourceDefinition\nmetadata: {name: supplied.examp"+
			"le.io}\n---\napiVersion: example.io/v1\nkind: Widget\nmetadata: {name: unused-supply,"+
			" namespace: supply-only}\nspec: {payload: private-fixture-value}\n")
	for _, verb := range []string{"build", "get", "test", "diff"} {
		t.Run(verb, func(t *testing.T) { testSuppliedSharedCommand(t, verb, root, input) })
	}
	t.Run("device stdin", testLoadInputsDevStdin)
	t.Run("stdin", testRunSupplyStdin)
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_SuppliedReaderValidation(t *testing.T) {
	for _, ref := range []string{"secretRef", "certSecretRef", "proxySecretRef"} {
		t.Run(ref, func(t *testing.T) {
			root := substitutionFixture(t, "", "literal")
			testutil.WriteFile(
				t,
				root,
				"flux.yaml",
				testutil.KustomizationYAML(
					"consumer",
					"apps",
					"  path: ./app\n  sourceRef: {kind: OCIRepository, name: remote}\n",
				),
			)
			testutil.WriteFile(
				t,
				root,
				"source.yaml",
				"apiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata: {name: remote, namespace: apps}\nspec:\n  url: oci://example.invalid/fixture\n  "+ref+": {name: external}\n",
			)
			testutil.WriteFile(t, root, "kustomization.yaml", "resources: [flux.yaml, source.yaml]\n")
			input := substitutionInput(t, externalSecret)
			migratedResult5 := runCLI(
				t,
				"build",
				"all",
				"--path",
				root,
				"--cache-dir",
				t.TempDir(),
				"--concurrency",
				"2",
				"--supply",
				input,
			)
			diagnostic, code := migratedResult5.stderr, migratedResult5.code

			assert.FatalIf(
				t,
				code != 1 || !strings.Contains(diagnostic, "external") || strings.Contains(diagnostic, "not found"),
				"supplied %s was not read by its parser: %s",
				ref,
				diagnostic,
			)
		})
	}
	for _, kind := range []string{"ConfigMap", "Secret"} {
		t.Run("Helm values "+kind, func(t *testing.T) {
			root := substitutionFixture(t, "", "literal")
			testutil.WriteFile(
				t,
				root,
				"hr.yaml",
				"apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: chart, namespace: apps}\nspec:\n  interval: 1m\n  chart:\n    spec:\n      chart: charts/app\n      sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n  valuesFrom:\n    - kind: "+kind+"\n      name: external\n",
			)
			testutil.WriteFile(t, root, "kustomization.yaml", "resources: [flux.yaml, hr.yaml]\n")
			testutil.WriteFile(t, root, "charts/app/Chart.yaml", "apiVersion: v2\nname: app\nversion: 0.1.0\n")
			testutil.WriteFile(
				t,
				root,
				"charts/app/templates/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: chart-output}\n",
			)
			input := externalCM
			if kind == "Secret" {
				input = externalSecret
			}
			migratedResult6 := runCLI(
				t,
				"build",
				"all",
				"--path",
				root,
				"--cache-dir",
				t.TempDir(),
				"--concurrency",
				"4",
				"--supply",
				substitutionInput(t, input),
			)
			diagnostic, code := migratedResult6.stderr, migratedResult6.code

			assert.FatalIf(
				t,
				code != 1 || !strings.Contains(diagnostic, "values.yaml") || !strings.Contains(diagnostic, "external"),
				"supplied %s did not retain requested-key validation: %s",
				kind,
				diagnostic,
			)
		})
	}
}

func TestRun_StrictSubstitutionEnvironmentPrecedence(t *testing.T) {
	for _, tc := range []struct {
		name, env      string
		override, fail bool
	}{
		{name: "enabled", env: "true", fail: true},
		{name: "disabled", env: "false"},
		{name: "explicit false", env: "true", override: true},
		{name: "overridden invalid env", env: "invalid", override: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("FLATE_STRICT_SUBSTITUTIONS", tc.env)
			root := substitutionFixture(t, "  postBuild: {substituteStrategy: Always}\n", "${UNDEFINED}")
			args := []string{"build", "all", "--path", root, "--cache-dir", t.TempDir(), "--concurrency", "2"}
			if tc.override {
				args = append(args, "--strict-substitutions=false")
			}
			migratedResult7 := runCLI(t, args...)
			out, diagnostic, code := migratedResult7.stdout, migratedResult7.stderr, migratedResult7.code
			assert.FatalIf(t, (code != 0) != tc.fail, "exit=%d: %s", code, diagnostic)
			assert.FatalIf(
				t,
				!tc.fail && substitutionOutput(t, out) != "",
				"%v",
				"undefined variable did not expand empty",
			)
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_SubstitutionRepositoryCollision(t *testing.T) {
	root := substitutionFixture(t, substitutionReference("ConfigMap", false), "${VALUE}")
	testutil.WriteFile(t, root, "kustomization.yaml", "resources: [flux.yaml, producer.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"producer.yaml",
		testutil.KustomizationYAML("producer", "apps", "  {path: ./producer}\n"),
	)
	testutil.WriteFile(t, root, "producer/kustomization.yaml", "resources: [input.yaml]\n")
	testutil.WriteFile(t, root, "producer/input.yaml", strings.Replace(externalCM, "supplied", "repository", 1))
	input := substitutionInput(t, strings.Replace(externalCM, "supplied", "private-fixture-value", 1))
	migratedResult8 := runCLI(
		t,
		"build",
		"all",
		"--path",
		root,
		"--cache-dir",
		t.TempDir(),
		"--concurrency",
		"4",
		"--supply",
		input,
		"--strict-substitutions",
		"--log-level",
		"warn",
	)
	out, diagnostic, code := migratedResult8.stdout, migratedResult8.stderr, migratedResult8.code

	assert.FatalIf(t, code != 0, "%v", diagnostic)
	assert.FatalIf(t, strings.Count(diagnostic, "repository data takes precedence over") != 1 ||
		!strings.Contains(diagnostic, input) ||
		!strings.Contains(diagnostic, "ConfigMap/apps/external: VALUE") ||
		strings.Contains(diagnostic, "private-fixture-value"), "collision diagnostic: %s", diagnostic)
	assert.FatalIf(t, strings.Contains(out, "private-fixture-value"), "%v", "supplied collision values emitted")
	docs, err := testutil.SplitDocs([]byte(out))
	assert.NoError(t, err)
	for _, doc := range docs {
		result3 := manifest.DocMetadata(doc)
		name := result3.Name
		assert.FatalIf(
			t,
			name == "rendered" && doc["data"].(map[string]any)["value"] != "repository",
			"repository values lost: %v",
			doc,
		)
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_SubstitutionSecretReadableAdvisory(t *testing.T) {
	root := substitutionFixture(t, substitutionReference("Secret", false), "${VALUE}")
	migratedResult9 := runCLI(
		t,
		"test",
		"all",
		"--path",
		root,
		"--cache-dir",
		t.TempDir(),
		"--concurrency",
		"2",
		"--supply",
		substitutionInput(t, externalSecret),
		"--strict-substitutions",
	)
	out, diagnostic, code := migratedResult9.stdout, migratedResult9.stderr, migratedResult9.code

	assert.FatalIf(
		t,
		code != 0 || strings.Contains(out+diagnostic, "could not be read offline"),
		"supplied Secret reported unreadable: %s %s",
		out,
		diagnostic,
	)
}
