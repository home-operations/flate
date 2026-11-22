package resourcesetinput

import (
	"testing"

	"github.com/home-operations/flate/internal/testutil/assert"
)

func TestDraft_UnknownOutput(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, source, want string }{
		{"if with alternatives", "kind: << if inputs.track >>Kustomization<< else >>ConfigMap<< end >>", "kind: <<value>><<value>>"},
		{"if with else output only", "kind: << if inputs.skip >><< else >>Kustomization<< end >>", "kind: <<value>>"},
		{"range", "kind: << range inputs.parts >>Kustomization<< end >>", "kind: <<value>>"},
		{"with", "kind: << with inputs.optional >>Kustomization<< end >>", "kind: <<value>>"},
		{"payload indentation", "data:\n  << if inputs.track >>\n    key: value\n  << end >>\n", "data:\n  \n    <<value>>\n  \n"},
		{"named template", `<< define "body" >>text<< end >><< template "body" . >>`, "\n<<template>>\n"},
		{"conditional template", `<< define "body" >>text<< end >><< if inputs.track >><< template "body" . >><< end >>`, "\n<<template>>\n"},
		{"conditional document", "data: << if inputs.track >>{}\n---\nkind: Kustomization\n<< end >>", "data: \n<<template>>\n"},
		{"branch starts with a document", "<< if inputs.track >>---\nkind: Kustomization\n<< end >>", "\n<<template>>\n"},
		{"variable declaration", "kind: ConfigMap\n<< $value := inputs.value >>data: {key: '<< $value >>'}", "kind: ConfigMap\ndata: {key: '<<value>>'}"},
		{"variable assignment", "<< $value := false >><< if inputs.track >><< $value = true >><< end >>kind: ConfigMap", "kind: ConfigMap"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tmpl, err := Parse(tc.source)
			assert.NoError(t, err)
			assert.Equal(t, string(tmpl.Draft()), tc.want)
		})
	}
}
