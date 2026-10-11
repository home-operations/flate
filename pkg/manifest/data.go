package manifest

import (
	"fmt"
	"iter"
	"log/slog"
	"strings"
)

// Data is a lazy view of ConfigMap or Secret variables.
type Data interface {
	Value(string) (string, bool, error)
	Keys() iter.Seq[string]
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

// VarsMap coerces flat scalar substitutions and strips newlines as Flux does.
// Unsupported values are omitted.
func VarsMap(in map[string]any) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		s, err := scalarString(v)
		if err != nil {
			slog.Debug("manifest: dropping non-scalar substitute var", "key", k, "err", err)
			continue
		}
		out[k] = strings.ReplaceAll(s, "\n", "")
	}
	return out
}
