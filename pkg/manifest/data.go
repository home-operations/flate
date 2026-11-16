package manifest

import (
	"fmt"
	"iter"
	"maps"
	"slices"
	"strings"
)

// Data provides lazy ConfigMap/Secret variables and owned snapshot documents.
type Data interface {
	HasKeys() bool
	KeysKnown() bool
	Value(string) (DataValue, error)
	// Keys MUST return a replayable sequence over immutable key membership.
	Keys() iter.Seq[string]
	Snapshot() map[string]any
	WithSupplied(Data) SuppliedData
}

// DataValue keeps empty present values distinct from absent keys.
type DataValue struct {
	Value     string
	Found     bool
	Encrypted bool
}

// SuppliedData retains immutable data and records keys that repository data overrides.
type SuppliedData struct {
	Data       Data
	Overridden []string
}

//nolint:gocritic // The output replaces caller storage while the previous map remains immutable.
func fillSupplied(
	repository, supplied Data,
	projection *map[string]Data,
	write func(string, string),
) []string {
	previous := *projection
	var overridden []string
	var unreadable map[string]Data
	supplied.Keys()(func(key string) bool {
		value, err := repository.Value(key)
		if err != nil {
			return true
		}
		if value.Found && !value.Encrypted {
			overridden = append(overridden, key)
			return true
		}
		repositoryFound := value.Found
		value, err = supplied.Value(key)
		if err != nil || !value.Found || value.Encrypted && repositoryFound {
			return true
		}
		if value.Encrypted {
			if unreadable == nil {
				unreadable = make(map[string]Data, len(previous)+1)
				maps.Copy(unreadable, previous)
			}
			unreadable[key] = supplied
			return true
		}
		if _, exists := previous[key]; exists {
			if unreadable == nil {
				unreadable = maps.Clone(previous)
			}
			delete(unreadable, key)
		}
		write(key, value.Value)
		return true
	})
	slices.Sort(overridden)
	*projection = unreadable
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
