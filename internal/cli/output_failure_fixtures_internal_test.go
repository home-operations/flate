package cli

import (
	"bytes"
)

type failingWriter struct {
	err error
}

func (w failingWriter) Write(_ []byte) (int, error) {
	return 0, w.err
}

type outputFailWriter struct {
	buf       bytes.Buffer
	remaining int
	err       error
}

func (w *outputFailWriter) Write(p []byte) (int, error) {
	n := min(len(p), w.remaining)
	_, _ = w.buf.Write(p[:n])
	w.remaining -= n
	if n < len(p) {
		return n, w.err
	}
	return n, nil
}

type countFailWriter struct {
	writes int
	err    error
}

func (w *countFailWriter) Write([]byte) (int, error) { w.writes++; return 0, w.err }

type recordingStream struct {
	bytes.Buffer

	all *bytes.Buffer
}

func (w *recordingStream) Write(p []byte) (int, error) {
	_, _ = w.all.Write(p)
	return w.Buffer.Write(p)
}

func (w *recordingStream) WriteString(s string) (int, error) { return w.Write([]byte(s)) }

type phaseFailWriter struct {
	bytes.Buffer

	token  string
	err    error
	failed bool
}

func (w *phaseFailWriter) Write(p []byte) (int, error) {
	if !w.failed && bytes.Contains(p, []byte(w.token)) {
		w.failed = true
		return 0, w.err
	}
	return w.Buffer.Write(p)
}

func (w *phaseFailWriter) WriteString(s string) (int, error) { return w.Write([]byte(s)) }
