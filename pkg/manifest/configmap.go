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
	Name              string         `json:"name"                yaml:"name"`
	Namespace         string         `json:"namespace,omitempty" yaml:"namespace,omitempty"`
	Data              map[string]any `json:"-"                   yaml:"-"`
	opaqueData        any
	suppliedEncrypted map[string]Data
}

func newConfigMap(id NamedResource, data map[string]any) *ConfigMap {
	return &ConfigMap{Name: id.Name, Namespace: id.Namespace, Data: data}
}

// Named identifies the ConfigMap.
func (c *ConfigMap) Named() NamedResource {
	return NamedResource{
		Group:     kindTable[kindConfigMap].Group,
		Kind:      KindConfigMap,
		Namespace: c.Namespace,
		Name:      c.Name,
	}
}

// Suspended reports whether reconciliation is paused on this resource.
func (c *ConfigMap) Suspended() bool { return false }

// GetLabels returns nil because ConfigMap has no typed label storage.
func (*ConfigMap) GetLabels() map[string]string { return nil }

// WithNamespace returns a shallow namespace copy; borrowed payloads MUST remain immutable.
func (c *ConfigMap) WithNamespace(namespace string) BaseManifest {
	out := *c
	out.Namespace = namespace
	return &out
}

// ProjectionCompatible reports whether an eligible previous projection can be reused.
func (c *ConfigMap) ProjectionCompatible(BaseManifest) bool { return true }

// Value reads only the requested data key; binaryData is never a Flux variable source.
func (c *ConfigMap) Value(key string) (DataValue, error) {
	v, ok := c.Data[key]
	if !ok {
		if supplied := c.suppliedEncrypted[key]; supplied != nil {
			return supplied.Value(key)
		}
		return DataValue{}, nil
	}
	s, err := scalarString(v)
	if err != nil {
		return DataValue{}, NewError(ErrInput, fmt.Errorf("data[%s]: %w", key, err), "")
	}
	if encryptedValue(s, nil, false) {
		return DataValue{Found: true, Encrypted: true}, nil
	}
	return DataValue{Value: s, Found: true}, nil
}

// HasKeys reports whether the ConfigMap has substitution keys.
func (c *ConfigMap) HasKeys() bool { return len(c.Data) != 0 || len(c.suppliedEncrypted) != 0 }

// KeysKnown MUST remain false when encrypted field names are opaque.
func (c *ConfigMap) KeysKnown() bool { return c.opaqueData == nil }

// Keys yields the data keys without imposing output order.
func (c *ConfigMap) Keys() iter.Seq[string] {
	if len(c.suppliedEncrypted) == 0 {
		return maps.Keys(c.Data)
	}
	return func(yield func(string) bool) {
		for key := range c.Data {
			if !yield(key) {
				return
			}
		}
		for key := range c.suppliedEncrypted {
			if !yield(key) {
				return
			}
		}
	}
}

// WithSupplied fills absent logical keys without changing repository data.
func (c *ConfigMap) WithSupplied(supplied Data) SuppliedData {
	out := *c
	out.Data = nil
	unreadable := c.suppliedEncrypted
	overridden := fillSupplied(c, supplied, &unreadable, func(key, value string) {
		if out.Data == nil {
			out.Data = maps.Clone(c.Data)
			if out.Data == nil {
				out.Data = make(map[string]any)
			}
		}
		out.Data[key] = value
	})
	if out.Data == nil && unreadable == nil {
		return SuppliedData{Data: c, Overridden: overridden}
	}
	if out.Data == nil {
		out.Data = c.Data
	}
	if unreadable != nil {
		out.suppliedEncrypted = unreadable
	}
	return SuppliedData{Data: &out, Overridden: overridden}
}

// Snapshot returns an owned document for sanitation through ParseDoc.
func (c *ConfigMap) Snapshot() map[string]any {
	return map[string]any{
		apiVersionKey: "v1", kindKey: KindConfigMap,
		metadataKey: map[string]any{nameKey: c.Name, namespaceKey: c.Namespace},
		dataKey:     dataSnapshot(c.Data, c.opaqueData),
	}
}

func parseConfigMap(doc map[string]any) (*ConfigMap, error) {
	metadata := requireMetadata(doc)
	if metadata.missing != "" {
		return nil, inputf("%s missing %s", KindConfigMap, metadata.missing)
	}
	data, _ := doc[dataKey].(map[string]any)
	out := newConfigMap(NamedResource{Name: metadata.Name, Namespace: metadata.Namespace}, data)
	if opaqueField(doc[dataKey]) {
		out.opaqueData = doc[dataKey]
	}
	return out, nil
}

// Secret is the core/v1 Secret.
type Secret struct {
	Name              string         `json:"name"                yaml:"name"`
	Namespace         string         `json:"namespace,omitempty" yaml:"namespace,omitempty"`
	Data              map[string]any `json:"-"                   yaml:"-"`
	StringData        map[string]any `json:"-"                   yaml:"-"`
	opaqueData        any
	opaqueStringData  any
	suppliedEncrypted map[string]Data
}

func newSecret(id NamedResource, data, stringData map[string]any) *Secret {
	return &Secret{Name: id.Name, Namespace: id.Namespace, Data: data, StringData: stringData}
}

// Named identifies the Secret.
func (s *Secret) Named() NamedResource {
	return NamedResource{Group: kindTable[kindSecret].Group, Kind: KindSecret, Namespace: s.Namespace, Name: s.Name}
}

// Suspended reports whether reconciliation is paused on this resource.
func (s *Secret) Suspended() bool { return false }

// GetLabels returns nil because Secret has no typed label storage.
func (*Secret) GetLabels() map[string]string { return nil }

// WithNamespace returns a shallow namespace copy; borrowed payloads MUST remain immutable.
func (s *Secret) WithNamespace(namespace string) BaseManifest {
	out := *s
	out.Namespace = namespace
	return &out
}

// ProjectionCompatible reports whether an eligible previous projection can be reused.
func (s *Secret) ProjectionCompatible(BaseManifest) bool { return true }

// Value gives stringData membership precedence, including empty values.
// Shadowed and unrequested data keys are never decoded.
func (s *Secret) Value(key string) (DataValue, error) {
	if v, ok := s.StringData[key]; ok {
		value, err := scalarString(v)
		if err != nil {
			return DataValue{}, NewError(ErrInput, fmt.Errorf("stringData[%s]: %w", key, err), "")
		}
		if encryptedValue(value, nil, false) {
			return DataValue{Found: true, Encrypted: true}, nil
		}
		return DataValue{Value: value, Found: true}, nil
	}
	v, ok := s.Data[key]
	if !ok {
		if supplied := s.suppliedEncrypted[key]; supplied != nil {
			return supplied.Value(key)
		}
		return DataValue{}, nil
	}
	value, err := scalarString(v)
	if err != nil {
		return DataValue{}, NewError(ErrInput, fmt.Errorf("data[%s]: %w", key, err), "")
	}
	if encryptedValue(value, base64.StdEncoding, true) {
		return DataValue{Found: true, Encrypted: true}, nil
	}
	decoded, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		return DataValue{}, NewError(ErrInput, fmt.Errorf("decode data[%s]: %w", key, err), "")
	}
	return DataValue{Value: string(decoded), Found: true}, nil
}

// HasKeys reports whether the Secret has substitution keys.
func (s *Secret) HasKeys() bool {
	return len(s.Data) != 0 || len(s.StringData) != 0 || len(s.suppliedEncrypted) != 0
}

// KeysKnown MUST remain false when encrypted field names are opaque.
func (s *Secret) KeysKnown() bool { return s.opaqueData == nil && s.opaqueStringData == nil }

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
		for key := range s.suppliedEncrypted {
			if !yield(key) {
				return
			}
		}
	}
}

// WithSupplied fills absent logical keys while preserving lazy decoding.
func (s *Secret) WithSupplied(supplied Data) SuppliedData {
	out := *s
	out.Data = nil
	out.StringData = nil
	unreadable := s.suppliedEncrypted
	overridden := fillSupplied(s, supplied, &unreadable, func(key, value string) {
		if out.StringData == nil {
			out.StringData = maps.Clone(s.StringData)
			if out.StringData == nil {
				out.StringData = make(map[string]any)
			}
		}
		if _, exists := s.Data[key]; exists {
			if out.Data == nil {
				out.Data = maps.Clone(s.Data)
			}
			delete(out.Data, key)
		}
		out.StringData[key] = value
	})
	if out.StringData == nil && unreadable == nil {
		return SuppliedData{Data: s, Overridden: overridden}
	}
	if out.StringData == nil {
		out.StringData = s.StringData
	}
	if unreadable != nil {
		out.suppliedEncrypted = unreadable
	}
	if out.Data == nil {
		out.Data = s.Data
	}
	if len(out.Data) == 0 {
		out.Data = nil
	}
	return SuppliedData{Data: &out, Overridden: overridden}
}

// Snapshot returns an owned document for sanitation through ParseDoc.
func (s *Secret) Snapshot() map[string]any {
	return map[string]any{
		apiVersionKey: "v1", kindKey: KindSecret,
		metadataKey: map[string]any{nameKey: s.Name, namespaceKey: s.Namespace},
		dataKey:     dataSnapshot(s.Data, s.opaqueData), stringDataKey: dataSnapshot(s.StringData, s.opaqueStringData),
	}
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
	metadata := requireMetadata(doc)
	if metadata.missing != "" {
		return nil, inputf("%s missing %s", KindSecret, metadata.missing)
	}
	data, _ := doc[dataKey].(map[string]any)
	stringData, _ := doc[stringDataKey].(map[string]any)
	out := newSecret(NamedResource{Name: metadata.Name, Namespace: metadata.Namespace}, data, stringData)
	if opaqueField(doc[dataKey]) {
		out.opaqueData = doc[dataKey]
	}
	if opaqueField(doc[stringDataKey]) {
		out.opaqueStringData = doc[stringDataKey]
	}
	return out, nil
}

func (c *ConfigMap) kind() KindInfo { return kindTable[kindConfigMap].KindInfo }

func (s *Secret) kind() KindInfo { return kindTable[kindSecret].KindInfo }

func dataSnapshot(data map[string]any, opaque any) any {
	if opaque != nil {
		return opaque
	}
	return copyData(data)
}

func opaqueField(raw any) bool {
	value, ok := raw.(string)
	return ok && encryptedValue(value, nil, false)
}
