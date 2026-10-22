package manifest

import (
	"encoding/base64"
	"fmt"
	"iter"
	"maps"
	"slices"
)

// ConfigMap is the core/v1 ConfigMap.
type ConfigMap struct {
	Name      string         `json:"name" yaml:"name"`
	Namespace string         `json:"namespace,omitempty" yaml:"namespace,omitempty"`
	Data      map[string]any `json:"-" yaml:"-"`
}

// Named identifies the ConfigMap.
func (c *ConfigMap) Named() NamedResource {
	return NamedResource{Group: kindTable[kindConfigMap].Group, Kind: KindConfigMap, Namespace: c.Namespace, Name: c.Name}
}

// Suspended reports whether reconciliation is paused on this resource.
func (c *ConfigMap) Suspended() bool { return false }

// WithNamespace returns a shallow namespace copy; borrowed payloads MUST remain immutable.
func (c *ConfigMap) WithNamespace(namespace string) BaseManifest {
	out := *c
	out.Namespace = namespace
	return &out
}

// Value reads only the requested data key; binaryData is never a Flux variable source.
func (c *ConfigMap) Value(key string) (string, bool, error) {
	v, ok := c.Data[key]
	if !ok {
		return "", false, nil
	}
	s, err := scalarString(v)
	if err != nil {
		return "", false, fmt.Errorf("%w: data[%s]: %w", ErrInput, key, err)
	}
	return s, true, nil
}

// HasKeys reports whether the ConfigMap has substitution keys.
func (c *ConfigMap) HasKeys() bool { return len(c.Data) != 0 }

// Keys yields the data keys without imposing output order.
func (c *ConfigMap) Keys() iter.Seq[string] { return maps.Keys(c.Data) }

// Snapshot returns an owned document for sanitation through ParseDoc.
func (c *ConfigMap) Snapshot() map[string]any {
	return map[string]any{"apiVersion": "v1", "kind": KindConfigMap,
		"metadata": map[string]any{"name": c.Name, "namespace": c.Namespace},
		"data":     copyData(c.Data)}
}

func parseConfigMap(doc map[string]any) (*ConfigMap, error) {
	name, ns, missing := requireMetadata(doc)
	if missing != "" {
		return nil, inputf("%s missing %s", KindConfigMap, missing)
	}
	data, _ := doc["data"].(map[string]any)
	return &ConfigMap{Name: name, Namespace: ns, Data: data}, nil
}

// Secret is the core/v1 Secret.
type Secret struct {
	Name       string         `json:"name" yaml:"name"`
	Namespace  string         `json:"namespace,omitempty" yaml:"namespace,omitempty"`
	Data       map[string]any `json:"-" yaml:"-"`
	StringData map[string]any `json:"-" yaml:"-"`
}

// Named identifies the Secret.
func (s *Secret) Named() NamedResource {
	return NamedResource{Group: kindTable[kindSecret].Group, Kind: KindSecret, Namespace: s.Namespace, Name: s.Name}
}

// Suspended reports whether reconciliation is paused on this resource.
func (s *Secret) Suspended() bool { return false }

// WithNamespace returns a shallow namespace copy; borrowed payloads MUST remain immutable.
func (s *Secret) WithNamespace(namespace string) BaseManifest {
	out := *s
	out.Namespace = namespace
	return &out
}

// Value gives stringData membership precedence, including empty values.
// Shadowed and unrequested data keys are never decoded.
func (s *Secret) Value(key string) (string, bool, error) {
	if v, ok := s.StringData[key]; ok {
		value, err := scalarString(v)
		if err != nil {
			return "", false, fmt.Errorf("%w: stringData[%s]: %w", ErrInput, key, err)
		}
		return value, true, nil
	}
	v, ok := s.Data[key]
	if !ok {
		return "", false, nil
	}
	value, err := scalarString(v)
	if err != nil {
		return "", false, fmt.Errorf("%w: data[%s]: %w", ErrInput, key, err)
	}
	decoded, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		return "", false, fmt.Errorf("%w: decode data[%s]: %w", ErrInput, key, err)
	}
	return string(decoded), true, nil
}

// HasKeys reports whether the Secret has substitution keys.
func (s *Secret) HasKeys() bool { return len(s.Data) != 0 || len(s.StringData) != 0 }

// Keys yields each key once without allocating a union map.
func (s *Secret) Keys() iter.Seq[string] {
	return func(yield func(string) bool) {
		for key := range s.StringData {
			if !yield(key) {
				return
			}
		}
		for key := range s.Data {
			if _, shadowed := s.StringData[key]; !shadowed && !yield(key) {
				return
			}
		}
	}
}

// HasPlaceholder reports whether the payload contains a wiped value.
func (s *Secret) HasPlaceholder() bool {
	return ContainsValuePlaceholder(s.StringData) || ContainsValuePlaceholder(s.Data)
}

// Snapshot returns an owned document for sanitation through ParseDoc.
func (s *Secret) Snapshot() map[string]any {
	return map[string]any{"apiVersion": "v1", "kind": KindSecret,
		"metadata": map[string]any{"name": s.Name, "namespace": s.Namespace},
		"data":     copyData(s.Data), "stringData": copyData(s.StringData)}
}

func copyData(data map[string]any) map[string]any {
	owned := DeepCopyMap(data)
	for name, value := range owned {
		if bytes, ok := value.([]byte); ok {
			owned[name] = slices.Clone(bytes)
		}
	}
	return owned
}

func parseSecret(doc map[string]any) (*Secret, error) {
	name, ns, missing := requireMetadata(doc)
	if missing != "" {
		return nil, inputf("%s missing %s", KindSecret, missing)
	}
	data, _ := doc["data"].(map[string]any)
	stringData, _ := doc["stringData"].(map[string]any)
	return &Secret{Name: name, Namespace: ns, Data: data, StringData: stringData}, nil
}

func (c *ConfigMap) kind() KindInfo { return kindTable[kindConfigMap].KindInfo }

func (s *Secret) kind() KindInfo { return kindTable[kindSecret].KindInfo }
