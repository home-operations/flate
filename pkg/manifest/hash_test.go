package manifest

import "testing"

func TestSHA256Hex(t *testing.T) {
	// Known vector: sha256("") and sha256("abc").
	if got := SHA256Hex(nil); got != "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" {
		t.Errorf("SHA256Hex(nil) = %q", got)
	}
	if got := SHA256Hex([]byte("abc")); got != "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad" {
		t.Errorf("SHA256Hex(abc) = %q", got)
	}
	// Stable + deterministic across calls.
	a, b := SHA256Hex([]byte("flate")), SHA256Hex([]byte("flate"))
	if a != b {
		t.Errorf("not deterministic: %q != %q", a, b)
	}
}

func TestSHA256Hex_Allocations(t *testing.T) {
	var fingerprint string
	allocs := testing.AllocsPerRun(100, func() { fingerprint = SHA256Hex([]byte("abc")) })
	if allocs != 1 {
		t.Fatalf("SHA256Hex allocated %v times, want 1", allocs)
	}
	if fingerprint != "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad" {
		t.Fatalf("unexpected fingerprint %q", fingerprint)
	}
}
