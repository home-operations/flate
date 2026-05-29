package format

import (
	"bytes"
	"strings"
	"testing"
)

func TestTable(t *testing.T) {
	var b bytes.Buffer
	cols := []Column{{"NAME", "name"}, {"PATH", "path"}}
	rows := []map[string]string{
		{"name": "apps", "path": "./apps"},
		{"name": "infra", "path": "./infrastructure"},
	}
	if err := Table(&b, cols, rows); err != nil {
		t.Fatalf("Table: %v", err)
	}
	got := b.String()
	if !strings.Contains(got, "NAME") || !strings.Contains(got, "PATH") {
		t.Errorf("missing headers: %s", got)
	}
	if !strings.Contains(got, "apps") || !strings.Contains(got, "./infrastructure") {
		t.Errorf("missing rows: %s", got)
	}
}

func TestYAMLMulti(t *testing.T) {
	var b bytes.Buffer
	if err := YAMLMulti(&b, []map[string]any{
		{"kind": "A", "metadata": map[string]any{"name": "1"}},
		{"kind": "B", "metadata": map[string]any{"name": "2"}},
	}); err != nil {
		t.Fatalf("YAMLMulti: %v", err)
	}
	out := b.String()
	if strings.Count(out, "---") != 2 {
		t.Errorf("expected 2 doc separators: %s", out)
	}
}

func TestJSON(t *testing.T) {
	var b bytes.Buffer
	if err := JSON(&b, map[string]string{"k": "v"}); err != nil {
		t.Fatalf("JSON: %v", err)
	}
	if !strings.Contains(b.String(), `"k": "v"`) {
		t.Errorf("json output: %s", b.String())
	}
}

func TestMarkdownTable(t *testing.T) {
	tests := []struct {
		name        string
		cols        []Column
		rows        []map[string]string
		wantContain []string
		wantAbsent  []string
	}{
		{
			name: "basic three columns",
			cols: []Column{{"NAME", "name"}, {"KIND", "kind"}, {"NS", "ns"}},
			rows: []map[string]string{
				{"name": "app1", "kind": "Kustomization", "ns": "flux-system"},
				{"name": "app2", "kind": "HelmRelease", "ns": "default"},
			},
			wantContain: []string{
				"| NAME | KIND | NS |",
				"| --- | --- | --- |",
				"| app1 | Kustomization | flux-system |",
				"| app2 | HelmRelease | default |",
			},
		},
		{
			name: "cell with pipe character",
			cols: []Column{{"NAME", "name"}, {"EXPR", "expr"}},
			rows: []map[string]string{
				{"name": "rule", "expr": "a | b"},
			},
			wantContain: []string{`| rule | a \| b |`},
			wantAbsent:  []string{"| rule | a | b |"},
		},
		{
			name: "empty rows renders header only",
			cols: []Column{{"NAME", "name"}, {"PATH", "path"}},
			rows: nil,
			wantContain: []string{
				"| NAME | PATH |",
				"| --- | --- |",
			},
			wantAbsent: []string{"| app"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var b bytes.Buffer
			if err := MarkdownTable(&b, tc.cols, tc.rows); err != nil {
				t.Fatalf("MarkdownTable: %v", err)
			}
			got := b.String()
			for _, want := range tc.wantContain {
				if !strings.Contains(got, want) {
					t.Errorf("missing %q in:\n%s", want, got)
				}
			}
			for _, absent := range tc.wantAbsent {
				if strings.Contains(got, absent) {
					t.Errorf("unexpected %q in:\n%s", absent, got)
				}
			}
		})
	}
}

func TestMarkdownDocs(t *testing.T) {
	tests := []struct {
		name        string
		docs        []map[string]any
		wantContain []string
	}{
		{
			name: "two namespaced docs",
			docs: []map[string]any{
				{
					"apiVersion": "v1",
					"kind":       "ConfigMap",
					"metadata":   map[string]any{"name": "cm1", "namespace": "default"},
				},
				{
					"apiVersion": "v1",
					"kind":       "Secret",
					"metadata":   map[string]any{"name": "s1", "namespace": "kube-system"},
				},
			},
			wantContain: []string{
				"### ConfigMap/default/cm1",
				"### Secret/kube-system/s1",
				"```yaml",
				"kind: ConfigMap",
				"kind: Secret",
				"```\n",
			},
		},
		{
			name: "cluster-scoped doc omits namespace segment",
			docs: []map[string]any{
				{
					"apiVersion": "rbac.authorization.k8s.io/v1",
					"kind":       "ClusterRole",
					"metadata":   map[string]any{"name": "view"},
				},
			},
			wantContain: []string{
				"### ClusterRole/view",
				"kind: ClusterRole",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var b bytes.Buffer
			if err := MarkdownDocs(&b, tc.docs); err != nil {
				t.Fatalf("MarkdownDocs: %v", err)
			}
			got := b.String()
			for _, want := range tc.wantContain {
				if !strings.Contains(got, want) {
					t.Errorf("missing %q in:\n%s", want, got)
				}
			}
		})
	}
	// Cluster-scoped also asserts the namespaced segment is absent.
	var b bytes.Buffer
	if err := MarkdownDocs(&b, []map[string]any{
		{"kind": "ClusterRole", "metadata": map[string]any{"name": "view"}},
	}); err != nil {
		t.Fatalf("MarkdownDocs: %v", err)
	}
	if strings.Contains(b.String(), "ClusterRole//view") {
		t.Errorf("cluster-scoped header leaked empty namespace: %s", b.String())
	}
}

func TestMarkdownTaskList(t *testing.T) {
	tests := []struct {
		name        string
		items       []TaskItem
		wantContain []string
		wantAbsent  []string
	}{
		{
			name: "mix of checked and unchecked",
			items: []TaskItem{
				{Label: "passing-resource", Checked: true},
				{Label: "pending-resource", Checked: false},
			},
			wantContain: []string{
				"- [x] passing-resource",
				"- [ ] pending-resource",
			},
		},
		{
			name: "summary-only renders inline em dash",
			items: []TaskItem{
				{Label: "skipped-test", Checked: true, Summary: "no fixtures"},
			},
			wantContain: []string{
				"- [x] skipped-test — no fixtures",
			},
			wantAbsent: []string{"<details>", "<summary>"},
		},
		{
			name: "detail wraps in collapsible block with code fence",
			items: []TaskItem{
				{
					Label:   "failing-resource",
					Checked: false,
					Summary: "diff mismatch",
					Detail:  "expected: foo\nactual: bar",
				},
			},
			wantContain: []string{
				"- [ ] failing-resource",
				"<details><summary>diff mismatch</summary>",
				"  ```",
				"expected: foo",
				"actual: bar",
				"</details>",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var b bytes.Buffer
			if err := MarkdownTaskList(&b, tc.items); err != nil {
				t.Fatalf("MarkdownTaskList: %v", err)
			}
			got := b.String()
			for _, want := range tc.wantContain {
				if !strings.Contains(got, want) {
					t.Errorf("missing %q in:\n%s", want, got)
				}
			}
			for _, absent := range tc.wantAbsent {
				if strings.Contains(got, absent) {
					t.Errorf("unexpected %q in:\n%s", absent, got)
				}
			}
		})
	}
}
