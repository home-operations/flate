package source

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSlotMeta_RoundTrip(t *testing.T) {
	slot := t.TempDir()
	want := SlotMeta{Revision: "abc123", Digest: "sha256:dead", Verified: "fp42"}
	if err := WriteSlotMeta(slot, want); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, ok := ReadSlotMeta(slot)
	if !ok {
		t.Fatal("expected ok")
	}
	if got != want {
		t.Errorf("round-trip mismatch: got %+v want %+v", got, want)
	}
}

func TestSlotMeta_MissingIsNotOK(t *testing.T) {
	if _, ok := ReadSlotMeta(t.TempDir()); ok {
		t.Error("missing sidecar should read ok=false")
	}
}

// TestSlotMeta_MalformedIsNotOK pins that a non-JSON sidecar (a legacy slot or
// external tampering — atomic writes make a torn write impossible) reads as no
// marker, so the caller rebuilds rather than trusting garbage.
func TestSlotMeta_MalformedIsNotOK(t *testing.T) {
	slot := t.TempDir()
	if err := os.WriteFile(filepath.Join(slot, SlotMetaFile), []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, ok := ReadSlotMeta(slot); ok {
		t.Error("malformed sidecar should read ok=false")
	}
}

// TestSlotMeta_Fresh covers the freshness gate: a just-written sidecar is fresh
// within maxAge; a backdated one is stale; maxAge<=0 is always stale.
func TestSlotMeta_Fresh(t *testing.T) {
	slot := t.TempDir()
	if err := WriteSlotMeta(slot, SlotMeta{Revision: "r"}); err != nil {
		t.Fatal(err)
	}
	if _, ok := ReadSlotMetaFresh(slot, time.Hour); !ok {
		t.Error("just-written sidecar should be fresh")
	}
	if _, ok := ReadSlotMetaFresh(slot, 0); ok {
		t.Error("maxAge<=0 should always be stale")
	}
	past := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(filepath.Join(slot, SlotMetaFile), past, past); err != nil {
		t.Fatal(err)
	}
	if _, ok := ReadSlotMetaFresh(slot, time.Hour); ok {
		t.Error("backdated sidecar should be stale")
	}
}

// TestSlotMeta_NoTempLeftover pins the atomic-write contract: a successful write
// leaves only the sidecar, never a temp file.
func TestSlotMeta_NoTempLeftover(t *testing.T) {
	slot := t.TempDir()
	if err := WriteSlotMeta(slot, SlotMeta{Revision: "r"}); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(slot)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != SlotMetaFile {
		t.Errorf("expected only %s, got %v", SlotMetaFile, entries)
	}
}
