package output

import (
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"time"
	"unicode/utf8"

	"go.yaml.in/yaml/v2"
)

func marshalYAML(value any) ([]byte, error) {
	normalized, err := normalizeYAML(value, 0, nil)
	if err != nil {
		return nil, fmt.Errorf("error marshaling into JSON: %w", err)
	}
	return yaml.Marshal(normalized)
}

func normalizeYAML(value any, depth int, seen map[any]struct{}) (any, error) {
	// Like encoding/json, defer identity tracking until deeply nested containers.
	if depth > 1000 {
		var identity any
		switch v := value.(type) {
		case map[string]any, map[any]any:
			identity = reflect.ValueOf(v).UnsafePointer()
		case []any:
			identity = struct {
				pointer any
				length  int
			}{reflect.ValueOf(v).UnsafePointer(), len(v)}
		}
		if identity != nil {
			if _, exists := seen[identity]; exists {
				return nil, &json.UnsupportedValueError{Value: reflect.ValueOf(value), Str: "encountered a cycle"}
			}
			if seen == nil {
				seen = make(map[any]struct{})
			}
			seen[identity] = struct{}{}
			defer delete(seen, identity)
		}
	}
	switch v := value.(type) {
	case nil, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return v, nil
	case string:
		return normalizeYAMLString(v), nil
	case map[string]any:
		return normalizeYAMLMap(v, depth, seen)
	case map[string]string:
		return normalizeYAMLMap(v, depth, seen)
	case map[any]any:
		if v == nil {
			return nil, nil
		}
		normalized := make(map[string]any, len(v))
		for key, item := range v {
			var name string
			switch k := key.(type) {
			case string:
				name = normalizeYAMLString(k)
			case time.Time:
				token, err := json.Marshal(k)
				if err != nil {
					return nil, err
				}
				// A JSON timestamp is always a string.
				_ = json.Unmarshal(token, &name)
			case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
				token, _ := json.Marshal(k)
				name = string(token)
			default:
				return nil, &json.UnsupportedValueError{Value: reflect.ValueOf(key), Str: "object member name must be a string"}
			}
			normalizedItem, err := normalizeYAML(item, depth+1, seen)
			if err != nil {
				return nil, err
			}
			normalized[name] = normalizedItem
		}
		return normalized, nil
	case []any:
		return normalizeYAMLSlice(v, depth, seen)
	case []string:
		return normalizeYAMLSlice(v, depth, seen)
	case float32, float64, json.Number, time.Time, []byte:
	default:
		return nil, &json.UnsupportedTypeError{Type: reflect.TypeOf(value)}
	}
	token, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var normalized any
	err = yaml.Unmarshal(token, &normalized)
	return normalized, err
}

func normalizeYAMLMap[T any](value map[string]T, depth int, seen map[any]struct{}) (any, error) {
	if value == nil {
		return nil, nil
	}
	normalized := make(map[string]any, len(value))
	keys := maps.Keys(value)
	for key := range value {
		if !utf8.ValidString(key) {
			// JSON orders raw keys before replacing invalid UTF-8; the last collision wins.
			keys = slices.Values(slices.Sorted(keys))
			break
		}
	}
	for key := range keys {
		item, err := normalizeYAML(value[key], depth+1, seen)
		if err != nil {
			return nil, err
		}
		normalized[normalizeYAMLString(key)] = item
	}
	return normalized, nil
}

func normalizeYAMLSlice[T any](value []T, depth int, seen map[any]struct{}) (any, error) {
	if value == nil {
		return nil, nil
	}
	normalized := make([]any, len(value))
	for i, item := range value {
		var err error
		normalized[i], err = normalizeYAML(item, depth+1, seen)
		if err != nil {
			return nil, err
		}
	}
	return normalized, nil
}

func normalizeYAMLString(value string) string {
	if utf8.ValidString(value) {
		return value
	}
	// A JSON string and its own decoder cannot fail.
	token, _ := json.Marshal(value)
	var normalized string
	_ = json.Unmarshal(token, &normalized)
	return normalized
}
