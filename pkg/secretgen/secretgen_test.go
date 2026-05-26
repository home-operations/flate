package secretgen

import (
	"strings"
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
)

func TestFromRaw_ExternalSecretRendersTemplatePlaceholders(t *testing.T) {
	raw := &manifest.RawObject{
		Kind:      "ExternalSecret",
		Name:      "tailscale",
		Namespace: "tailscale",
		Spec: map[string]any{
			"target": map[string]any{
				"name": "tailscale-operator-values",
				"template": map[string]any{
					"data": map[string]any{
						"values.yaml": "oauth:\n  clientId: {{ .clientId }}\n  audience: api.tailscale.com/{{ .clientId }}\n",
					},
				},
			},
		},
	}

	secret, ok := FromRaw(raw)
	if !ok {
		t.Fatal("FromRaw did not recognize ExternalSecret")
	}
	if secret.Name != "tailscale-operator-values" || secret.Namespace != "tailscale" {
		t.Fatalf("wrong target Secret: %+v", secret.Named())
	}
	values, _ := secret.StringData["values.yaml"].(string)
	if !strings.Contains(values, Placeholder("clientId")) {
		t.Fatalf("template did not receive clientId placeholder:\n%s", values)
	}
	if strings.Contains(values, "{{") {
		t.Fatalf("template action leaked into fake Secret:\n%s", values)
	}
}

func TestFromRaw_ExternalSecretAddsDataSecretKeys(t *testing.T) {
	raw := &manifest.RawObject{
		Kind:      "ExternalSecret",
		Name:      "app",
		Namespace: "default",
		Spec: map[string]any{
			"data": []any{
				map[string]any{"secretKey": "password"},
			},
		},
	}

	secret, ok := FromRaw(raw)
	if !ok {
		t.Fatal("FromRaw did not recognize ExternalSecret")
	}
	if got := secret.StringData["password"]; got != Placeholder("password") {
		t.Fatalf("password placeholder = %q, want %q", got, Placeholder("password"))
	}
}

func TestFromRaw_SealedSecretUsesTemplateMetadataAndEncryptedKeys(t *testing.T) {
	raw := &manifest.RawObject{
		Kind:      "SealedSecret",
		Name:      "sealed",
		Namespace: "default",
		Spec: map[string]any{
			"template": map[string]any{
				"metadata": map[string]any{"name": "app-secret"},
			},
			"encryptedData": map[string]any{"config": "sealed-bytes"},
		},
	}

	secret, ok := FromRaw(raw)
	if !ok {
		t.Fatal("FromRaw did not recognize SealedSecret")
	}
	if secret.Name != "app-secret" {
		t.Fatalf("Secret name = %q, want app-secret", secret.Name)
	}
	if got := secret.StringData["config"]; got != Placeholder("config") {
		t.Fatalf("config placeholder = %q, want %q", got, Placeholder("config"))
	}
}

func TestEnsureKeyAugmentsPlaceholderSecret(t *testing.T) {
	secret := &manifest.Secret{
		Name:       "app-values",
		Namespace:  "default",
		StringData: map[string]any{"existing": Placeholder("existing")},
	}

	got := EnsureKey(secret, "values.yaml")
	if got == secret {
		t.Fatal("EnsureKey should clone before adding a key")
	}
	if _, ok := secret.StringData["values.yaml"]; ok {
		t.Fatal("EnsureKey mutated the input Secret")
	}
	if got.StringData["values.yaml"] != Placeholder("values.yaml") {
		t.Fatalf("values.yaml placeholder = %q", got.StringData["values.yaml"])
	}
}

func TestIsPlaceholderSecretAllowsEmptyGeneratedSecret(t *testing.T) {
	if !IsPlaceholderSecret(&manifest.Secret{Name: "generated", Namespace: "default"}) {
		t.Fatal("empty generated Secret should be augmentable")
	}
	if IsPlaceholderSecret(&manifest.Secret{
		Name:       "real",
		Namespace:  "default",
		StringData: map[string]any{"values.yaml": "replicas: 2\n"},
	}) {
		t.Fatal("Secret containing real data must not be treated as placeholder-only")
	}
}
