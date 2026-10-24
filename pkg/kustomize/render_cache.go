package kustomize

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"
)

// The read-set header must be validated before decoding the document payload.
func encodeFrame(snap readSetSnapshot, output []byte) ([]byte, error) {
	js, err := json.Marshal(snap)
	if err != nil {
		return nil, err
	}
	if len(js) > math.MaxUint32 {
		return nil, errors.New("read-set snapshot too large to frame")
	}
	buf := make([]byte, 4+len(js)+len(output))
	binary.BigEndian.PutUint32(buf[:4], uint32(len(js))) //nolint:gosec // bounded by the MaxUint32 check above
	copy(buf[4:], js)
	copy(buf[4+len(js):], output)
	return buf, nil
}

// decodeFrame rejects incomplete framing before exposing a completed snapshot.
func decodeFrame(plain []byte) (readSetSnapshot, []byte, error) {
	if len(plain) < 4 {
		return readSetSnapshot{}, nil, errors.New("entry too short")
	}
	n := binary.BigEndian.Uint32(plain[:4])
	if int(n) > len(plain)-4 {
		return readSetSnapshot{}, nil, errors.New("entry header length out of range")
	}
	var snap readSetSnapshot
	if err := json.Unmarshal(plain[4:4+n], &snap); err != nil {
		return readSetSnapshot{}, nil, err
	}
	output := plain[4+n:]
	return snap, output, nil
}
