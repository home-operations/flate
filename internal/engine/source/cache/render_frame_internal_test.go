package cache

import (
	"errors"
	"io/fs"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRenderFrame_Rebind(t *testing.T) {
	t.Parallel()
	original := RenderFrame{Key: "render", Lookup: "runtime", Binding: "binding", Output: []byte("output")}
	stored, err := original.Encode("domain", "metadata")
	assert.NoError(t, err)
	original.Stored = stored
	current := original
	current.Rebind("binding")
	assert.Equal(t, current.Stored, stored)
	current.Rebind("new-binding")
	assert.Equal(t, current.Stored == nil, true)
	assert.Equal(t, original.Stored, stored)
	assert.Equal(t, original.Binding, "binding")
	assert.Equal(t, current.Binding, "new-binding")
	assert.Equal(t, current.Lookup, "runtime")
}

func TestRenderFrame_Encode(t *testing.T) {
	t.Parallel()
	frame := RenderFrame{Key: "render", Binding: "binding", Output: []byte("output")}
	stored, err := frame.Encode("domain", "metadata")
	assert.NoError(t, err)
	frame.Stored = stored
	again, err := frame.Encode("domain", make(chan int))
	assert.NoError(t, err)
	assert.Equal(t, again, stored)
	frame.Rebind("new-binding")
	again, err = frame.Encode("domain", "metadata")
	assert.NoError(t, err)
	assert.Equal(t, again == stored, false)
}

func TestOpenRenderFrame_Metadata(t *testing.T) {
	t.Parallel()
	for _, domain := range []string{"helm-output", "kustomize-render"} {
		t.Run(domain, func(t *testing.T) {
			t.Parallel()
			metadata := struct{ Value string }{Value: "engine"}
			frame := RenderFrame{Key: "render", Binding: "binding", Lookup: "runtime-only", Output: []byte("output")}
			stored, err := frame.Encode(domain, metadata)
			assert.NoError(t, err)
			generation := frameGeneration(frameBytes(t, stored, FramePublicationInput{}))
			var decoded struct{ Value string }
			opened, err := OpenRenderFrame(t.Context(), domain, generation, &decoded)
			assert.NoError(t, err)
			assert.Equal(t, decoded, metadata)
			assert.Equal(t, opened.Key, frame.Key)
			assert.Equal(t, opened.Binding, frame.Binding)
			assert.Equal(t, opened.Lookup, "")
			assert.Diff(t, opened.Output, frame.Output)
			assert.Equal(t, opened.Stored != nil, true)
			_, err = OpenRenderFrame(t.Context(), "foreign", generation, &decoded)
			assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
			var wrong int
			_, err = OpenRenderFrame(t.Context(), domain, generation, &wrong)
			assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
			_, err = OpenRenderFrame(t.Context(), domain, fstest.MapFS{}, &decoded)
			_, storage := errors.AsType[*Error](err)
			assert.Equal(t, storage, true)
			assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
		})
	}
}

func TestRenderDomain_Receipt(t *testing.T) {
	t.Parallel()
	equal := func(a, b int) bool { return a == b }
	definition := RenderDomain[int, int]{EqualPolicy: equal, EqualHeader: equal}
	domain := NewRenderDomain(definition)
	otherDomain := NewRenderDomain(definition)
	snapshot := func(policy int, output string, header int) RenderReceipt[int, int] {
		return RenderReceipt[int, int]{
			Key: "render", Output: []byte(output),
			Inputs: RenderBinding[int]{Key: "render", Policy: policy},
			Header: header,
		}
	}
	original := domain.Receipt(snapshot(1, "output", 2))
	otherKey := snapshot(1, "output", 2)
	otherKey.Key = "other"
	domainless := snapshot(1, "output", 2)
	for _, test := range []struct {
		name        string
		left, right *manifest.RenderReceipt
		equal       bool
	}{
		{name: "separate receipt", left: original, right: domain.Receipt(snapshot(1, "output", 2)), equal: true},
		{name: "different binding", left: original, right: domain.Receipt(snapshot(2, "output", 2))},
		{name: "different output", left: original, right: domain.Receipt(snapshot(1, "other", 2))},
		{name: "different header", left: original, right: domain.Receipt(snapshot(1, "output", 3))},
		{name: "different domain", left: original, right: otherDomain.Receipt(snapshot(1, "output", 2))},
		{name: "different frame key", left: original, right: domain.Receipt(otherKey)},
		{
			name:  "missing domain",
			left:  manifest.NewRenderReceipt(domainless, domain.equalReceipt),
			right: manifest.NewRenderReceipt(domainless, domain.equalReceipt),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, test.left.Equal(test.right), test.equal)
			assert.Equal(t, test.right.Equal(test.left), test.equal)
		})
	}
}

func TestRenderReceipt_RejectInvalidPublication(t *testing.T) {
	t.Parallel()
	domain := NewRenderDomain(RenderDomain[int, int]{
		Name:        "domain",
		ValidHeader: func(header int) bool { return header == 1 },
	})
	for _, test := range []struct {
		name    string
		receipt RenderReceipt[int, int]
	}{
		{name: "missing domain", receipt: RenderReceipt[int, int]{Key: "render", Header: 1}},
		{name: "missing frame key", receipt: RenderReceipt[int, int]{domain: domain, Header: 1}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := test.receipt.RenderPublication(t.Context(), nil)
			assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
		})
	}
}

func TestRenderDomain_RejectHistoryBinding(t *testing.T) {
	t.Parallel()
	domain := NewRenderDomain(RenderDomain[int, int]{
		Name:        "domain",
		EqualPolicy: func(a, b int) bool { return a == b },
		ValidHeader: func(header int) bool { return header == 1 },
	})
	previous := RenderBinding[int]{Key: "render", Policy: 1}
	inputs := RenderBinding[int]{Key: "render", Policy: 2}
	snapshot := inputs
	output, err := manifest.EncodeRender(nil)
	assert.NoError(t, err)
	frame := RenderReceipt[int, int]{
		Key: previous.Key, Binding: previous.Fingerprint(), Output: output,
		Inputs: previous,
		Header: 1,
	}
	stored, err := frame.Encode(domain.Name, frame.Header)
	assert.NoError(t, err)
	frame.Stored = stored

	var calls int
	header := func(int) (bool, error) {
		calls++
		return true, nil
	}
	history, err := domain.Validate(t.Context(), &frame, &inputs, RenderCheck[int]{History: true, Header: header})
	assert.NoError(t, err)
	assert.Equal(t, history.Valid, false)
	assert.Equal(t, calls, 0)
	assert.Equal(t, frame.Inputs, previous)
	assert.Equal(t, frame.Stored, stored)
	assert.Equal(t, inputs, snapshot)

	execution, err := domain.Validate(t.Context(), &frame, &inputs, RenderCheck[int]{Header: header})
	assert.NoError(t, err)
	assert.Equal(t, execution.Valid, true)
	assert.Equal(t, execution.Rebound, true)
	assert.Equal(t, calls, 1)
	assert.Equal(t, frame.Inputs, snapshot)
	assert.Equal(t, frame.Binding, snapshot.Fingerprint())
	assert.Equal(t, frame.Stored == nil, true)
	assert.Equal(t, inputs, snapshot)
}
