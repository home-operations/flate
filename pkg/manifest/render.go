package manifest

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"maps"
	"math"
	"slices"
	"time"
)

const (
	renderNull byte = iota
	renderFalse
	renderTrue
	renderInt
	renderInt64
	renderUint64
	renderFloat64
	renderString
	renderTime
	renderSequence
	renderStringMap
	renderAnyMap
)

// EncodeRender encodes constructed render documents without changing scalar types.
func EncodeRender(docs []map[string]any) ([]byte, error) {
	data := binary.AppendUvarint(nil, uint64(len(docs)))
	for _, doc := range docs {
		var err error
		data, err = appendRender(data, doc)
		if err != nil {
			return nil, fmt.Errorf("%w: encode render: %w", ErrFlux, err)
		}
	}
	return data, nil
}

func appendRender(data []byte, value any) ([]byte, error) {
	switch v := value.(type) {
	case nil:
		return append(data, renderNull), nil
	case bool:
		if v {
			return append(data, renderTrue), nil
		}
		return append(data, renderFalse), nil
	case int:
		return binary.AppendVarint(append(data, renderInt), int64(v)), nil
	case int64:
		return binary.AppendVarint(append(data, renderInt64), v), nil
	case uint64:
		return binary.AppendUvarint(append(data, renderUint64), v), nil
	case float64:
		return binary.LittleEndian.AppendUint64(append(data, renderFloat64), math.Float64bits(v)), nil
	case string:
		data = binary.AppendUvarint(append(data, renderString), uint64(len(v)))
		return append(data, v...), nil
	case time.Time:
		var buf [16]byte
		encoded, err := v.AppendBinary(buf[:0])
		if err != nil {
			return nil, err
		}
		data = binary.AppendUvarint(append(data, renderTime), uint64(len(encoded)))
		return append(data, encoded...), nil
	case []any:
		data = binary.AppendUvarint(append(data, renderSequence), uint64(len(v)))
		for _, item := range v {
			var err error
			data, err = appendRender(data, item)
			if err != nil {
				return nil, err
			}
		}
	case map[string]any:
		data = binary.AppendUvarint(append(data, renderStringMap), uint64(len(v)))
		for _, key := range slices.Sorted(maps.Keys(v)) {
			data = binary.AppendUvarint(data, uint64(len(key)))
			data = append(data, key...)
			var err error
			data, err = appendRender(data, v[key])
			if err != nil {
				return nil, err
			}
		}
	case map[any]any:
		// Complete entries also order repeated NaN keys, which cannot be looked up.
		entries := make([][]byte, 0, len(v))
		for key, item := range v {
			entry, err := appendRender(nil, key)
			if err != nil {
				return nil, err
			}
			entry, err = appendRender(entry, item)
			if err != nil {
				return nil, err
			}
			entries = append(entries, entry)
		}
		slices.SortFunc(entries, bytes.Compare)
		data = binary.AppendUvarint(append(data, renderAnyMap), uint64(len(v)))
		for _, entry := range entries {
			data = append(data, entry...)
		}
	default:
		return nil, fmt.Errorf("unsupported render value %T", value)
	}
	return data, nil
}

// DecodeRender returns independently owned constructed render documents.
func DecodeRender(data []byte) ([]map[string]any, error) {
	d := renderDecoder{data: data, budget: len(data)}
	docs := make([]map[string]any, d.reserve(1))
	for i := range docs {
		value := d.value(0)
		if d.err != nil {
			break
		}
		var ok bool
		docs[i], ok = value.(map[string]any)
		if !ok {
			d.err = fmt.Errorf("render document is not a string-keyed map")
			break
		}
	}
	if d.err != nil {
		return nil, fmt.Errorf("%w: decode render: %w", ErrFlux, d.err)
	}
	if len(d.data) != 0 {
		return nil, fmt.Errorf("%w: decode render: trailing data", ErrFlux)
	}
	return docs, nil
}

type renderDecoder struct {
	data   []byte
	budget int
	err    error
}

func (d *renderDecoder) unsigned() uint64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Uvarint(d.data)
	if n <= 0 {
		d.err = fmt.Errorf("invalid render varint")
		return 0
	}
	d.data = d.data[n:]
	return v
}

func (d *renderDecoder) count() int {
	n := d.unsigned()
	if d.err != nil {
		return 0
	}
	if n > uint64(len(d.data)) {
		d.err = fmt.Errorf("truncated render value")
		return 0
	}
	return int(n) //nolint:gosec // Bounded by the input slice length.
}

func (d *renderDecoder) reserve(units int) int {
	n := d.count()
	if d.err != nil {
		return 0
	}
	if n > d.budget/units {
		d.err = fmt.Errorf("render capacity exceeds input budget")
		return 0
	}
	d.budget -= n * units
	return n
}

func (d *renderDecoder) value(depth int) any {
	if d.err != nil {
		return nil
	}
	if len(d.data) == 0 || depth > 10000 {
		d.err = fmt.Errorf("truncated or excessively nested render value")
		return nil
	}
	tag := d.data[0]
	d.data = d.data[1:]
	switch tag {
	case renderNull:
		return nil
	case renderFalse:
		return false
	case renderTrue:
		return true
	case renderInt, renderInt64:
		v, n := binary.Varint(d.data)
		if n <= 0 {
			d.err = fmt.Errorf("invalid render integer")
			return nil
		}
		d.data = d.data[n:]
		if tag == renderInt {
			return int(v)
		}
		return v
	case renderUint64:
		return d.unsigned()
	case renderFloat64:
		if len(d.data) < 8 {
			d.err = fmt.Errorf("truncated render float")
			return nil
		}
		v := math.Float64frombits(binary.LittleEndian.Uint64(d.data))
		d.data = d.data[8:]
		return v
	case renderString, renderTime:
		n := d.count()
		if d.err != nil {
			return nil
		}
		data := d.data[:n]
		d.data = d.data[n:]
		if tag == renderString {
			return string(data)
		}
		var v time.Time
		if err := v.UnmarshalBinary(data); err != nil {
			d.err = err
			return nil
		}
		return v
	case renderSequence:
		n := d.reserve(1)
		if d.err != nil {
			return nil
		}
		v := make([]any, n)
		for i := range v {
			v[i] = d.value(depth + 1)
			if d.err != nil {
				return nil
			}
		}
		return v
	case renderStringMap:
		n := d.reserve(2)
		if d.err != nil {
			return nil
		}
		v := make(map[string]any, n)
		for range n {
			size := d.count()
			if d.err != nil {
				return nil
			}
			key := string(d.data[:size])
			d.data = d.data[size:]
			value := d.value(depth + 1)
			if d.err != nil {
				return nil
			}
			v[key] = value
		}
		return v
	case renderAnyMap:
		n := d.reserve(2)
		if d.err != nil {
			return nil
		}
		v := make(map[any]any, n)
		for range n {
			key := d.value(depth + 1)
			if d.err != nil {
				return nil
			}
			switch key.(type) {
			case []any, map[string]any, map[any]any:
				d.err = fmt.Errorf("invalid render map key")
				return nil
			}
			value := d.value(depth + 1)
			if d.err != nil {
				return nil
			}
			v[key] = value
		}
		return v
	default:
		d.err = fmt.Errorf("unknown render tag %d", tag)
		return nil
	}
}
