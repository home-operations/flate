// Package secretgen synthesizes placeholder Secrets for live-cluster
// secret generators that flate cannot reconcile offline.
package secretgen

import (
	"bytes"
	"fmt"
	"regexp"
	"text/template"

	"github.com/home-operations/flate/pkg/manifest"
)

const (
	kindExternalSecret = "ExternalSecret"
	kindSealedSecret   = "SealedSecret"
)

var (
	templateActionRE = regexp.MustCompile(`\{\{[^{}]*\}\}`)
	dotIdentRE       = regexp.MustCompile(`\.([A-Za-z_][A-Za-z0-9_]*)`)
	indexIdentRE     = regexp.MustCompile(`index\s+\.\s+"([^"]+)"`)
)

// FromRaw returns the placeholder Secret produced by a known
// in-cluster secret generator.
func FromRaw(raw *manifest.RawObject) (*manifest.Secret, bool) {
	if raw == nil {
		return nil, false
	}
	switch raw.Kind {
	case kindExternalSecret:
		return fromExternalSecret(raw)
	case kindSealedSecret:
		return fromSealedSecret(raw)
	default:
		return nil, false
	}
}

// Produces reports whether raw generates a Secret with name.
func Produces(raw *manifest.RawObject, name string) bool {
	secret, ok := FromRaw(raw)
	return ok && secret.Name == name
}

// Placeholder returns flate's standard placeholder value for key.
func Placeholder(key string) string {
	return fmt.Sprintf(manifest.ValuePlaceholderTemplate, key)
}

// EnsureKey returns a Secret clone whose stringData contains key.
func EnsureKey(secret *manifest.Secret, key string) *manifest.Secret {
	if secret == nil || key == "" || hasKey(secret, key) {
		return secret
	}
	out := cloneSecret(secret)
	if out.StringData == nil {
		out.StringData = map[string]any{}
	}
	out.StringData[key] = Placeholder(key)
	return out
}

// Merge returns a placeholder Secret containing keys from base and overlay.
func Merge(base, overlay *manifest.Secret) *manifest.Secret {
	if base == nil {
		return cloneSecret(overlay)
	}
	out := cloneSecret(base)
	if overlay == nil {
		return out
	}
	if len(overlay.Data) > 0 {
		if out.Data == nil {
			out.Data = map[string]any{}
		}
		for k, v := range overlay.Data {
			out.Data[k] = v
		}
	}
	if len(overlay.StringData) > 0 {
		if out.StringData == nil {
			out.StringData = map[string]any{}
		}
		for k, v := range overlay.StringData {
			out.StringData[k] = v
		}
	}
	return out
}

// IsPlaceholderSecret reports whether secret contains no real data.
func IsPlaceholderSecret(secret *manifest.Secret) bool {
	if secret == nil || len(secret.Data) > 0 {
		return false
	}
	for _, v := range secret.StringData {
		s, ok := v.(string)
		if !ok || !manifest.IsValuePlaceholder(s) {
			return false
		}
	}
	return true
}

func fromExternalSecret(raw *manifest.RawObject) (*manifest.Secret, bool) {
	name := raw.Name
	target, _ := raw.Spec["target"].(map[string]any)
	if targetName, _ := target["name"].(string); targetName != "" {
		name = targetName
	}
	if name == "" {
		return nil, false
	}

	stringData := map[string]any{}
	if tmpl, _ := target["template"].(map[string]any); tmpl != nil {
		if data, _ := tmpl["data"].(map[string]any); data != nil {
			for key, value := range data {
				stringData[key] = renderTemplate(fmt.Sprint(value))
			}
		}
	}
	if data, _ := raw.Spec["data"].([]any); data != nil {
		for _, item := range data {
			entry, _ := item.(map[string]any)
			key, _ := entry["secretKey"].(string)
			if key != "" {
				if _, exists := stringData[key]; !exists {
					stringData[key] = Placeholder(key)
				}
			}
		}
	}

	return &manifest.Secret{Name: name, Namespace: raw.Namespace, StringData: stringData}, true
}

func fromSealedSecret(raw *manifest.RawObject) (*manifest.Secret, bool) {
	name := raw.Name
	namespace := raw.Namespace
	if tmpl, _ := raw.Spec["template"].(map[string]any); tmpl != nil {
		if metadata, _ := tmpl["metadata"].(map[string]any); metadata != nil {
			if targetName, _ := metadata["name"].(string); targetName != "" {
				name = targetName
			}
			if targetNamespace, _ := metadata["namespace"].(string); targetNamespace != "" {
				namespace = targetNamespace
			}
		}
	}
	if name == "" {
		return nil, false
	}
	stringData := map[string]any{}
	if data, _ := raw.Spec["encryptedData"].(map[string]any); data != nil {
		for key := range data {
			stringData[key] = Placeholder(key)
		}
	}
	return &manifest.Secret{Name: name, Namespace: namespace, StringData: stringData}, true
}

func renderTemplate(input string) string {
	ids := templateIdentifiers(input)
	data := make(map[string]string, len(ids))
	for id := range ids {
		data[id] = Placeholder(id)
	}
	tmpl, err := template.New("secret").Option("missingkey=zero").Parse(input)
	if err == nil {
		var b bytes.Buffer
		if err := tmpl.Execute(&b, data); err == nil {
			return b.String()
		}
	}
	return templateActionRE.ReplaceAllStringFunc(input, func(action string) string {
		if id := firstTemplateIdentifier(action); id != "" {
			return Placeholder(id)
		}
		return Placeholder("template")
	})
}

func templateIdentifiers(input string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, match := range dotIdentRE.FindAllStringSubmatch(input, -1) {
		out[match[1]] = struct{}{}
	}
	for _, match := range indexIdentRE.FindAllStringSubmatch(input, -1) {
		out[match[1]] = struct{}{}
	}
	return out
}

func firstTemplateIdentifier(input string) string {
	if match := dotIdentRE.FindStringSubmatch(input); len(match) == 2 {
		return match[1]
	}
	if match := indexIdentRE.FindStringSubmatch(input); len(match) == 2 {
		return match[1]
	}
	return ""
}

func cloneSecret(secret *manifest.Secret) *manifest.Secret {
	if secret == nil {
		return nil
	}
	out := *secret
	out.Data = manifest.DeepCopyMap(secret.Data)
	out.StringData = manifest.DeepCopyMap(secret.StringData)
	return &out
}

func hasKey(secret *manifest.Secret, key string) bool {
	if _, ok := secret.StringData[key]; ok {
		return true
	}
	if _, ok := secret.Data[key]; ok {
		return true
	}
	return false
}
