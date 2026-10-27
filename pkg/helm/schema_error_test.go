package helm

import (
	"errors"
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

type schemaErrorCause struct {
	text  string
	cause error
}

func (e *schemaErrorCause) Error() string { return e.text }

func (e *schemaErrorCause) Unwrap() error { return e.cause }

func TestSchemaError_Canonical(t *testing.T) {
	t.Parallel()
	const preamble = "values don't meet the specifications of the schema(s) in the following chart(s):\n"
	const chart = preamble + "chart:\n"
	for _, tc := range []struct {
		name, input, want string
	}{
		{
			name:  "nested siblings",
			input: chart + "- at '/z': 'allOf' failed\n  - at '/z/b': wrong type\n  - at '/z/a': wrong type\n- at '/a': wrong type\n",
			want:  chart + "- at '/a': wrong type\n- at '/z': 'allOf' failed\n  - at '/z/a': wrong type\n  - at '/z/b': wrong type\n",
		},
		{
			name:  "other sibling permutation",
			input: chart + "- at '/a': wrong type\n- at '/z': 'allOf' failed\n  - at '/z/b': wrong type\n  - at '/z/a': wrong type\n",
			want:  chart + "- at '/a': wrong type\n- at '/z': 'allOf' failed\n  - at '/z/a': wrong type\n  - at '/z/b': wrong type\n",
		},
		{
			name:  "tied parents and duplicate subtrees",
			input: chart + "- at '/x': 'allOf' failed\n  - at '/z': wrong type\n- at '/x': 'allOf' failed\n  - at '/a': wrong type\n- at '/x': 'allOf' failed\n  - at '/a': wrong type\n",
			want:  chart + "- at '/x': 'allOf' failed\n  - at '/a': wrong type\n- at '/x': 'allOf' failed\n  - at '/a': wrong type\n- at '/x': 'allOf' failed\n  - at '/z': wrong type\n",
		},
		{
			name:  "chart boundaries",
			input: preamble + "z-chart:\n- at '/z': wrong type\n- at '/a': wrong type\na-chart:\n- at '/y': wrong type\n- at '/b': wrong type\n",
			want:  preamble + "z-chart:\n- at '/a': wrong type\n- at '/z': wrong type\na-chart:\n- at '/b': wrong type\n- at '/y': wrong type\n",
		},
		{
			name:  "chart heading with spaces",
			input: preamble + "a chart with spaces:\n- at '/z': wrong type\n- at '/a': wrong type\n",
			want:  preamble + "a chart with spaces:\n- at '/a': wrong type\n- at '/z': wrong type\n",
		},
		{
			name:  "escaped property tokens and duplicates",
			input: chart + `- at '': additional properties '雪', 'z', 'a\'post', 'path\\name', 'é', 'a,b', 'a\'post' not allowed` + "\n",
			want:  chart + `- at '': additional properties 'a,b', 'a\'post', 'a\'post', 'path\\name', 'z', 'é', '雪' not allowed` + "\n",
		},
		{
			name:  "other token permutation",
			input: chart + `- at '': additional properties 'path\\name', 'é', 'a\'post', 'a,b', 'z', '雪', 'a\'post' not allowed` + "\n",
			want:  chart + `- at '': additional properties 'a,b', 'a\'post', 'a\'post', 'path\\name', 'z', 'é', '雪' not allowed` + "\n",
		},
		{
			name:  "missing properties preserve encoding",
			input: chart + `- at '': missing properties 'z', '\u200b', 'a'` + "\n",
			want:  chart + `- at '': missing properties '\u200b', 'a', 'z'` + "\n",
		},
		{
			name:  "dependent properties retain trigger",
			input: chart + `- at '': properties 'z', 'a', 'a' required, if 'trigger, \'name' exists` + "\n",
			want:  chart + `- at '': properties 'a', 'a', 'z' required, if 'trigger, \'name' exists` + "\n",
		},
		{
			name:  "escaped deceptive location",
			input: chart + `- at '/additional properties \'z\', \'a\' not allowed': additional properties 'z', 'a' not allowed` + "\n",
			want:  chart + `- at '/additional properties \'z\', \'a\' not allowed': additional properties 'a', 'z' not allowed` + "\n",
		},
		{
			name:  "opaque separator",
			input: chart + "- at '/z': wrong type\n- at '/a': wrong type\nopaque output stays here  \n- at '/y': wrong type\n- at '/b': wrong type\n",
			want:  chart + "- at '/a': wrong type\n- at '/z': wrong type\nopaque output stays here  \n- at '/b': wrong type\n- at '/y': wrong type\n",
		},
		{
			name:  "continuations whitespace and absent final newline",
			input: chart + "- at '/z': wrong type  \n  continuation stays with z\n- at '/a': missing properties 'z', 'a'\t",
			want:  chart + "- at '/a': missing properties 'a', 'z'\t\n- at '/z': wrong type  \n  continuation stays with z",
		},
		{
			name:  "unrelated action error",
			input: "template: additional properties 'z', 'a' not allowed\nchart:\n- at '/z': failure\n- at '/a': failure\n",
			want:  "template: additional properties 'z', 'a' not allowed\nchart:\n- at '/z': failure\n- at '/a': failure\n",
		},
		{
			name:  "canonical identity",
			input: chart + "- at '/a': missing properties 'a', 'z'\n- at '/z': wrong type\n",
			want:  chart + "- at '/a': missing properties 'a', 'z'\n- at '/z': wrong type\n",
		},
		{
			name:  "schema compilation diagnostics stay opaque",
			input: chart + "jsonschema compilation failed\n- at '/z': invalid schema\n- at '/a': invalid schema\n",
			want:  chart + "jsonschema compilation failed\n- at '/z': invalid schema\n- at '/a': invalid schema\n",
		},
		{
			name:  "metaschema compile diagnostics",
			input: chart + "\"file:///values.schema.json\" is not valid against metaschema: jsonschema validation failed with 'https://json-schema.org/draft/2020-12/schema#'\n- at '/z': invalid schema\n- at '/a': invalid schema\n",
			want:  chart + "\"file:///values.schema.json\" is not valid against metaschema: jsonschema validation failed with 'https://json-schema.org/draft/2020-12/schema#'\n- at '/z': invalid schema\n- at '/a': invalid schema\n",
		},
		{
			name:  "schema compile text",
			input: chart + "invalid character 'z' looking for beginning of value\n",
			want:  chart + "invalid character 'z' looking for beginning of value\n",
		},
		{
			name:  "incomplete missing-property list",
			input: chart + "- at '': missing properties 'z', 'a\n",
			want:  chart + "- at '': missing properties 'z', 'a\n",
		},
		{
			name:  "missing closing quote",
			input: chart + "- at '': additional properties 'z', 'a not allowed\n",
			want:  chart + "- at '': additional properties 'z', 'a not allowed\n",
		},
		{
			name:  "malformed separator",
			input: chart + "- at '': missing properties 'z',  'a'\n",
			want:  chart + "- at '': missing properties 'z',  'a'\n",
		},
		{
			name:  "invalid token escape",
			input: chart + `- at '': missing properties 'z', '\q'` + "\n",
			want:  chart + `- at '': missing properties 'z', '\q'` + "\n",
		},
		{
			name:  "incomplete dependency trigger",
			input: chart + "- at '': properties 'z', 'a' required, if 'trigger exists\n",
			want:  chart + "- at '': properties 'z', 'a' required, if 'trigger exists\n",
		},
		{
			name:  "unverified list suffix",
			input: chart + "- at '': additional properties 'z', 'a' are invalid\n",
			want:  chart + "- at '': additional properties 'z', 'a' are invalid\n",
		},
		{
			name:  "unverified missing suffix",
			input: chart + "- at '': missing properties 'z', 'a' unexpectedly\n",
			want:  chart + "- at '': missing properties 'z', 'a' unexpectedly\n",
		},
		{
			name:  "unverified dependency suffix",
			input: chart + "- at '': properties 'z', 'a' required, if 'trigger' existed\n",
			want:  chart + "- at '': properties 'z', 'a' required, if 'trigger' existed\n",
		},
		{
			name:  "unverified dependency wording",
			input: chart + "- at '': properties 'z', 'a' unexpectedly required\n",
			want:  chart + "- at '': properties 'z', 'a' unexpectedly required\n",
		},
		{
			name:  "enum quoted commas",
			input: chart + "- at '': value must be one of 'z,a', 'b,c'\n",
			want:  chart + "- at '': value must be one of 'z,a', 'b,c'\n",
		},
		{
			name:  "pattern quoted commas",
			input: chart + `- at '/additional properties \'z\', \'a\' not allowed': 'z,a' does not match pattern 'additional properties \'z\', \'a\' not allowed'` + "\n",
			want:  chart + `- at '/additional properties \'z\', \'a\' not allowed': 'z,a' does not match pattern 'additional properties \'z\', \'a\' not allowed'` + "\n",
		},
		{
			name:  "malformed instance location",
			input: chart + "- at '/broken: missing properties 'z', 'a'\n",
			want:  chart + "- at '/broken: missing properties 'z', 'a'\n",
		},
		{
			name:  "non-diagnostic bullet",
			input: chart + "- missing properties 'z', 'a'\n",
			want:  chart + "- missing properties 'z', 'a'\n",
		},
		{
			name:  "unverified indentation",
			input: chart + "- at '/parent': failure\n   - at '/z': wrong type\n   - at '/a': wrong type\n",
			want:  chart + "- at '/parent': failure\n   - at '/z': wrong type\n   - at '/a': wrong type\n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sentinel := errors.New("schema cause")
			original := &schemaErrorCause{text: tc.input, cause: sentinel}
			got := schemaError(original)
			assert.Equal(t, got.Error(), tc.want)
			assert.Equal(t, errors.Is(got, sentinel), true)
			cause, ok := errors.AsType[*schemaErrorCause](got)
			assert.Equal(t, ok, true)
			assert.Equal(t, cause, original)
			if tc.input == tc.want {
				assert.Equal(t, got, error(original))
			} else {
				assert.Equal(t, errors.Unwrap(got), error(original))
			}
		})
	}
}
