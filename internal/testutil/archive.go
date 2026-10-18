package testutil

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"io"
	"maps"
	"slices"
	"testing"
)

// WriteTar writes regular files in name order with the supplied mode.
func WriteTar(tb testing.TB, dst io.Writer, files map[string]string, mode int64) {
	tb.Helper()
	tw := tar.NewWriter(dst)
	for _, name := range slices.Sorted(maps.Keys(files)) {
		body := files[name]
		if err := tw.WriteHeader(&tar.Header{Name: name, Typeflag: tar.TypeReg, Size: int64(len(body)), Mode: mode}); err != nil {
			tb.Fatal(err)
		}
		if _, err := io.WriteString(tw, body); err != nil {
			tb.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		tb.Fatal(err)
	}
}

// TarGz returns regular files compressed at level in name order.
func TarGz(tb testing.TB, files map[string]string, mode int64, level int) []byte {
	tb.Helper()
	var buf bytes.Buffer
	gw, err := gzip.NewWriterLevel(&buf, level)
	if err != nil {
		tb.Fatal(err)
	}
	WriteTar(tb, gw, files, mode)
	if err := gw.Close(); err != nil {
		tb.Fatal(err)
	}
	return buf.Bytes()
}
