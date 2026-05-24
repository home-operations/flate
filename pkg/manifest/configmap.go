package manifest

import (
	"encoding/base64"
	"fmt"
)

// ConfigMap is the core/v1 ConfigMap.
type ConfigMap struct {
	Name       string         `json:"name"                yaml:"name"`
	Namespace  string         `json:"namespace,omitempty" yaml:"namespace,omitempty"`
	Data       map[string]any `json:"-"                   yaml:"-"`
	BinaryData map[string]any `json:"-"                   yaml:"-"`
}

// Named identifies the ConfigMap.
func (c *ConfigMap) Named() NamedResource {
	return NamedResource{Kind: KindConfigMap, Namespace: c.Namespace, Name: c.Name}
}

// parseConfigMap decodes a core/v1 ConfigMap.
func parseConfigMap(doc map[string]any) (*ConfigMap, error) {
	if err := checkAPIVersion(doc, "v1"); err != nil {
		return nil, err
	}
	name, ns, err := requireMetadata("ConfigMap", doc)
	if err != nil {
		return nil, err
	}
	cm := &ConfigMap{Name: name, Namespace: ns}
	if v, ok := doc["data"].(map[string]any); ok {
		cm.Data = v
	}
	if v, ok := doc["binaryData"].(map[string]any); ok {
		cm.BinaryData = v
	}
	return cm, nil
}

// Secret is the core/v1 Secret. By default cleartext data is wiped to a
// placeholder during parsing.
type Secret struct {
	Name       string         `json:"name"                yaml:"name"`
	Namespace  string         `json:"namespace,omitempty" yaml:"namespace,omitempty"`
	Data       map[string]any `json:"-"                   yaml:"-"`
	StringData map[string]any `json:"-"                   yaml:"-"`
}

// Named identifies the Secret.
func (s *Secret) Named() NamedResource {
	return NamedResource{Kind: KindSecret, Namespace: s.Namespace, Name: s.Name}
}

// parseSecret decodes a Secret, wiping cleartext when wipeSecrets is true.
func parseSecret(doc map[string]any, wipeSecrets bool) (*Secret, error) {
	if err := checkAPIVersion(doc, "v1"); err != nil {
		return nil, err
	}
	name, ns, err := requireMetadata("Secret", doc)
	if err != nil {
		return nil, err
	}
	s := &Secret{Name: name, Namespace: ns}
	if data, ok := doc["data"].(map[string]any); ok {
		if wipeSecrets {
			for k := range data {
				data[k] = base64.StdEncoding.EncodeToString(
					fmt.Appendf(nil, ValuePlaceholderTemplate, k),
				)
			}
		}
		s.Data = data
	}
	if sd, ok := doc["stringData"].(map[string]any); ok {
		if wipeSecrets {
			for k := range sd {
				sd[k] = fmt.Sprintf(ValuePlaceholderTemplate, k)
			}
		}
		s.StringData = sd
	}
	return s, nil
}
