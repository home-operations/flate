package manifest

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func TestStripResourceAttributes(t *testing.T) {
	t.Parallel()
	r := map[string]any{
		"kind": "Deployment",
		"metadata": map[string]any{
			"annotations": map[string]any{"helm.sh/chart": "myapp-1.2.3", "keep": "yes"},
			"labels":      map[string]any{"app.kubernetes.io/version": "1.2.3"},
		},
		"spec": map[string]any{
			"template": map[string]any{
				"metadata": map[string]any{
					"annotations": map[string]any{"checksum/config": "abc123"},
				},
			},
		},
	}
	StripResourceAttributes(r, []string{
		"helm.sh/chart",
		"app.kubernetes.io/version",
		"checksum/config",
	})

	meta := r["metadata"].(map[string]any)
	ann := meta["annotations"].(map[string]any)
	{
		_, ok := ann["helm.sh/chart"]
		assert.ErrorIf(t, ok, "annotation not stripped")
	}
	{
		assert.ErrorIf(t, ann["keep"] != "yes", "kept annotation lost")
	}
	{
		_, ok := meta["labels"]
		assert.ErrorIf(t, ok, "empty labels map should have been removed")
	}
	tplMeta := r["spec"].(map[string]any)["template"].(map[string]any)["metadata"].(map[string]any)
	{
		_, ok := tplMeta["annotations"]
		assert.ErrorIf(t, ok, "template annotation not stripped (empty map should have been removed)")
	}
	for _, malformed := range []map[string]any{
		{"metadata": "bad", "spec": "bad"},
		{"metadata": map[string]any{"labels": "bad", "annotations": map[string]any(nil)}, "spec": map[string]any{"template": "bad", "jobTemplate": map[string]any{"metadata": "bad", "spec": "bad"}, "volumeClaimTemplates": "bad"}},
		{"spec": map[string]any{"volumeClaimTemplates": []any{nil, "bad", map[string]any{"metadata": "bad"}}}},
	} {
		before := DeepCopyMap(malformed)
		StripResourceAttributes(malformed, []string{"helm.sh/chart", "checksum/"})
		assert.Diff(t, malformed, before)
	}
}

// TestStripResourceAttributes_CronJob pins the CronJob walk: chart
// labels live both on the JobTemplateSpec metadata AND on the nested
// PodTemplateSpec metadata. Bitnami / app-template charts decorate
// both, so chart bumps would otherwise produce two diff entries per
// CronJob after the strip pass missed them.
func TestStripResourceAttributes_CronJob(t *testing.T) {
	t.Parallel()
	r := map[string]any{
		"kind": "CronJob",
		"metadata": map[string]any{
			"labels": map[string]any{"helm.sh/chart": "backup-1.0.0", "keep": "yes"},
		},
		"spec": map[string]any{
			"jobTemplate": map[string]any{
				"metadata": map[string]any{
					"labels": map[string]any{"helm.sh/chart": "backup-1.0.0"},
				},
				"spec": map[string]any{
					"template": map[string]any{
						"metadata": map[string]any{
							"annotations": map[string]any{"checksum/config": "abc123"},
						},
					},
				},
			},
		},
	}
	StripResourceAttributes(r, []string{"helm.sh/chart", "checksum/config"})

	{
		meta := r["metadata"].(map[string]any)
		assert.ErrorIf(t, meta["labels"].(map[string]any)["keep"] != "yes", "top-level kept label lost")
	}
	jobTmpl := r["spec"].(map[string]any)["jobTemplate"].(map[string]any)
	{
		_, ok := jobTmpl["metadata"].(map[string]any)["labels"]
		assert.ErrorIf(t, ok, "jobTemplate.metadata.labels should have been emptied + removed")
	}
	podTmplMeta := jobTmpl["spec"].(map[string]any)["template"].(map[string]any)["metadata"].(map[string]any)
	{
		_, ok := podTmplMeta["annotations"]
		assert.ErrorIf(t, ok, "jobTemplate.spec.template.metadata.annotations should have been emptied + removed")
	}
}

// TestStripResourceAttributes_StatefulSetPVCs pins the
// volumeClaimTemplates walk: chart labels on StatefulSet PVC
// templates (common pattern in Postgres / Loki / Prometheus charts)
// must be stripped, or every chart bump produces N diff entries
// (one per replica's PVC template).
func TestStripResourceAttributes_StatefulSetPVCs(t *testing.T) {
	t.Parallel()
	r := map[string]any{
		"kind": "StatefulSet",
		"spec": map[string]any{
			"template": map[string]any{
				"metadata": map[string]any{
					"labels": map[string]any{"helm.sh/chart": "pg-1.0.0"},
				},
			},
			"volumeClaimTemplates": []any{
				map[string]any{
					"metadata": map[string]any{
						"name":   "data",
						"labels": map[string]any{"helm.sh/chart": "pg-1.0.0", "app": "pg"},
					},
				},
				map[string]any{
					"metadata": map[string]any{
						"name":        "wal",
						"annotations": map[string]any{"checksum/config": "abc"},
					},
				},
			},
		},
	}
	StripResourceAttributes(r, []string{"helm.sh/chart", "checksum/config"})

	pvcs := r["spec"].(map[string]any)["volumeClaimTemplates"].([]any)
	data := pvcs[0].(map[string]any)["metadata"].(map[string]any)
	{
		_, ok := data["labels"].(map[string]any)["helm.sh/chart"]
		assert.ErrorIf(t, ok, "PVC[0].metadata.labels.helm.sh/chart should have been stripped")
	}
	{
		assert.ErrorIf(
			t,
			data["labels"].(map[string]any)["app"] != "pg",
			"PVC[0].metadata.labels.app (non-stripped) should remain",
		)
	}
	wal := pvcs[1].(map[string]any)["metadata"].(map[string]any)
	{
		_, ok := wal["annotations"]
		assert.ErrorIf(t, ok, "PVC[1].metadata.annotations should have been emptied + removed")
	}
}

// TestStripResourceAttributes_ChecksumPrefix pins the trailing-slash
// prefix match: a single "checksum/" entry drops every suffix charts
// emit (config, secret, secrets, …). The plural "checksum/secrets"
// (matrix-synapse) used to slip past the exact-match list and churn
// `flate diff`; non-checksum annotations must survive untouched.
func TestStripResourceAttributes_ChecksumPrefix(t *testing.T) {
	t.Parallel()
	r := map[string]any{
		"kind": "Deployment",
		"spec": map[string]any{
			"template": map[string]any{
				"metadata": map[string]any{
					"annotations": map[string]any{
						"checksum/config":            "a",
						"checksum/secret":            "b",
						"checksum/secrets":           "c",
						"reloader.stakater.com/auto": "true",
					},
				},
			},
		},
	}
	StripResourceAttributes(r, []string{"checksum/"})

	ann := r["spec"].(map[string]any)["template"].(map[string]any)["metadata"].(map[string]any)["annotations"].(map[string]any)
	for _, k := range []string{"checksum/config", "checksum/secret", "checksum/secrets"} {
		{
			_, ok := ann[k]
			assert.ErrorIf(t, ok, "prefix strip left %q behind", k)
		}
	}
	{
		assert.ErrorIf(
			t,
			ann["reloader.stakater.com/auto"] != "true",
			"non-checksum annotation should survive prefix strip; got %v",
			ann,
		)
	}
}
