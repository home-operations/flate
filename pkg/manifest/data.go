package manifest

import (
	"fmt"
	"iter"
	"slices"
	"strings"
)

// Data provides lazy ConfigMap/Secret variables and owned snapshot documents.
type Data interface {
	HasKeys() bool
	Value(string) (DataValue, error)
	Keys() iter.Seq[string]
	Snapshot() map[string]any
	WithSupplied(Data) SuppliedData
}

// DataValue keeps empty present values distinct from absent keys.
type DataValue struct {
	Value string
	Found bool
}

// SuppliedData retains immutable data and records keys that repository data overrides.
type SuppliedData struct {
	Data       Data
	Overridden []string
}

func fillSupplied(repository, supplied Data, write func(string, string)) []string {
	var overridden []string
	supplied.Keys()(func(key string) bool {
		value, err := repository.Value(key)
		if err != nil {
			return true
		}
		if value.Found {
			overridden = append(overridden, key)
			return true
		}
		value, _ = supplied.Value(key)
		write(key, value.Value)
		return true
	})
	slices.Sort(overridden)
	return overridden
}

func scalarString(v any) (string, error) {
	switch t := v.(type) {
	case string:
		return t, nil
	case []byte:
		return string(t), nil
	case nil:
		return "", nil
	case bool, int, int32, int64, uint, uint32, uint64, float32, float64:
		return fmt.Sprint(t), nil
	default:
		return "", fmt.Errorf("unsupported value type %T", v)
	}
}

// VarsMap strips newlines from typed substitutions as Flux does.
func VarsMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = strings.ReplaceAll(v, "\n", "")
	}
	return out
}
