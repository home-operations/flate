package manifest

import (
	"encoding/base64"
	"fmt"
	"iter"
	"maps"
)

// ConfigMap is the core/v1 ConfigMap.
type ConfigMap struct {
	Name      string         `json:"name" yaml:"name"`
	Namespace string         `json:"namespace,omitempty" yaml:"namespace,omitempty"`
	Data      map[string]any `json:"-" yaml:"-"`
}

// Named identifies the ConfigMap.
func (c *ConfigMap) Named() NamedResource {
	return NamedResource{Kind: KindConfigMap, Namespace: c.Namespace, Name: c.Name}
}

// Value reads only the requested data key; binaryData is never a Flux variable source.
func (c *ConfigMap) Value(key string) (string, bool, error) {
	v, ok := c.Data[key]
	if !ok {
		return "", false, nil
	}
	s, err := scalarString(v)
	if err != nil {
		return "", false, fmt.Errorf("%w: stringData[%s]: %w", ErrInput, key, err)
	}
	return s, true, nil
}

// Keys yields the data keys without imposing output order.
func (c *ConfigMap) Keys() iter.Seq[string] { return maps.Keys(c.Data) }

func parseConfigMap(doc map[string]any) (*ConfigMap, error) {
	name, ns, err := requireMetadata(KindConfigMap, doc)
	if err != nil {
		return nil, err
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
	return NamedResource{Kind: KindSecret, Namespace: s.Namespace, Name: s.Name}
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
		return "", false, fmt.Errorf("%w: binaryData[%s]: %w", ErrInput, key, err)
	}
	decoded, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		return "", false, fmt.Errorf("%w: decode binaryData[%s]: %w", ErrInput, key, err)
	}
	return string(decoded), true, nil
}

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

func parseSecret(doc map[string]any) (*Secret, error) {
	name, ns, err := requireMetadata(KindSecret, doc)
	if err != nil {
		return nil, err
	}
	data, _ := doc["data"].(map[string]any)
	stringData, _ := doc["stringData"].(map[string]any)
	return &Secret{Name: name, Namespace: ns, Data: data, StringData: stringData}, nil
}
