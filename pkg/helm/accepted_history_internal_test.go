package helm

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"iter"
	"maps"
	"runtime"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/testutil"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
)

func TestRenderReceipt_Domains(t *testing.T) {
	t.Parallel()
	owner := manifest.NamedResource{
		Group:     manifest.FluxKustomizeDomain,
		Kind:      manifest.KindKustomization,
		Namespace: "team",
		Name:      "emitter",
	}
	source := manifest.BootstrapSourceID
	child := owner
	child.Name = "child"
	payload, err := testutil.EncodeRender([]map[string]any{{"data": map[string]any{"key": "original"}}})
	assert.NoError(t, err)
	receipt, err := encodeFrame(
		renderHeader{Key: "raw-key", Binding: "source-and-policy", Analysis: &chartAnalysis{Version: "1.0.0"}},
		payload,
	)
	assert.NoError(t, err)
	frame, err := decodeFrame(receipt)
	assert.NoError(t, err)
	records := map[manifest.NamedResource]manifest.DiscoveryRecord{
		child: {
			Refs:         []manifest.NamedResource{source, source},
			ContentRoots: map[manifest.NamedResource][]string{source: {"apps/red", ".", "apps/red"}},
			Productions:  []manifest.Production{{Owner: owner, Source: source}, {Owner: owner, Source: source}},
		},
	}
	accepted, err := encodeAccepted(owner, &frame, maps.All(records))
	assert.NoError(t, err)
	again, err := encodeAccepted(owner, &frame, maps.All(records))
	assert.NoError(t, err)
	assert.Equal(t, bytes.Equal(accepted, again), true)
	frame, err = decodeFrame(append(bytes.Clone(receipt), accepted...))
	assert.NoError(t, err)
	history, complete := frame.history(owner)
	assert.Equal(t, complete, true)
	found := maps.Collect(history)
	assert.Diff(t, found[child].ContentRoots[source], []string{".", "apps/red"})
	assert.Equal(t, len(found[child].Refs), 1)
	assert.Equal(t, len(found[child].Productions), 1)
	found[child].Refs[0].Name = "mutated"
	found[child].Productions[0].Path = "mutated"
	found[child].ContentRoots[source][0] = "mutated"
	delete(found[child].ContentRoots, source)
	untouched := maps.Collect(history)
	assert.Equal(t, untouched[child].Refs[0], source)
	assert.Equal(t, untouched[child].Productions[0].Path, "")
	assert.Diff(t, untouched[child].ContentRoots[source], []string{".", "apps/red"})
	foreign := owner
	foreign.Namespace = "foreign"
	{
		_, historyComplete := frame.history(foreign)
		assert.FatalIf(t, historyComplete, "%s", "foreign owner borrowed accepted history")
	}
	for _, tc := range []struct {
		name   string
		mutate func(*acceptedRender)
	}{
		{"version", func(a *acceptedRender) { a.Version++ }},
		{"complete", func(a *acceptedRender) { a.Complete = false }},
		{"raw binding", func(a *acceptedRender) { a.Raw = "foreign" }},
		{"duplicate identity", func(a *acceptedRender) { a.Records = append(a.Records, a.Records[0]) }},
		{"duplicate root", func(a *acceptedRender) { a.Records[0].Roots = append(a.Records[0].Roots, a.Records[0].Roots[0]) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testRenderReceiptDomainsCase(
				t,
				&testRenderReceiptDomainsCaseInputs{
					owner:    &owner,
					payload:  &payload,
					receipt:  &receipt,
					accepted: &accepted,
					tc:       &tc,
				},
			)
		},
		)
	}
	for _, tc := range []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{"domain", func(b []byte) []byte { b[0] ^= 1; return b }},
		{"version", func(b []byte) []byte { b[7]++; return b }},
		{"header length", func(b []byte) []byte { binary.BigEndian.PutUint32(b[8:12], frameHeaderLimit+1); return b }},
		{"truncated payload", func(b []byte) []byte { return b[:len(b)-1] }},
		{"raw identity", func(b []byte) []byte { b[len(b)-1] ^= 1; return b }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testRenderReceiptDomainsCase2(t, &testRenderReceiptDomainsCase2Inputs{receipt: &receipt, tc: &tc})
		},
		)
	}
	empty, err := encodeAccepted(owner, &frame, emptyRenderInventory)
	assert.NoError(t, err)
	frame, err = decodeFrame(append(bytes.Clone(receipt), empty...))
	assert.NoError(t, err)
	history, complete = frame.history(owner)
	assert.Equal(t, complete && history != nil, true)
	for range history {
		t.Fatal("complete-empty history contained a child")
	}
	frame.accepted = nil
	{
		_, complete := frame.history(owner)
		assert.FatalIf(t, complete, "%s", "missing history became complete-empty")
	}
}

func encodeAccepted(
	owner manifest.NamedResource,
	frame *renderFrame,
	records iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord],
) ([]byte, error) {
	accepted, err := prepareAccepted(owner, frame, records)
	if err != nil {
		return nil, err
	}
	return json.Marshal(accepted)
}

func TestAcceptedRender_ExactValue(t *testing.T) {
	t.Parallel()
	id := manifest.NamedResource{Group: "group", Kind: "kind", Namespace: "namespace", Name: "name"}
	original := acceptedRender{Version: 1, Complete: true, Owner: id, Raw: "raw", Records: []acceptedRecord{{
		ID: id, Record: manifest.DiscoveryRecord{
			Authored: id,
			Path:     "path",
			File:     "file",
			Parent:   id,
			Refs:     []manifest.NamedResource{id},
			Productions: []manifest.Production{
				{Owner: id, Declaration: id, Authored: id, Source: id, Path: "path", File: "file"},
			},
			Builder: "builder",
			Inputs:  []string{"input"},
			Pinned:  true,
		}, Roots: []acceptedRoot{{Source: id, Paths: []string{"path"}}},
	}}}
	copyValue := func(t *testing.T, a acceptedRender) acceptedRender {
		t.Helper()
		data, err := json.Marshal(a)
		assert.NoError(t, err)
		var b acceptedRender
		assert.NoError(t, json.Unmarshal(data, &b))
		return b
	}
	assert.FatalIf(
		t,
		!original.equal(&original) || original.equal(nil) || (*acceptedRender)(nil).equal(&original),
		"%s",
		"accepted-value presence comparison differs",
	)
	assert.FatalIf(t, !(*acceptedRender)(nil).equal(nil), "%s", "nil accepted values differ")
	for _, tc := range []struct {
		name   string
		mutate func(*acceptedRender)
	}{
		{"version", func(a *acceptedRender) { a.Version++ }},
		{"complete", func(a *acceptedRender) { a.Complete = false }},
		{"raw", func(a *acceptedRender) { a.Raw = "different" }},
		{"application", func(a *acceptedRender) { a.Records[0].Record.Application = "different" }},
		{"static output", func(a *acceptedRender) { a.Records[0].Record.StaticOutput = true }},
		{"record count", func(a *acceptedRender) { a.Records = nil }},
		{"record length", func(a *acceptedRender) { a.Records = a.Records[:0] }},
		{"authored", func(a *acceptedRender) { a.Records[0].Record.Authored.Name = "different" }},
		{"path", func(a *acceptedRender) { a.Records[0].Record.Path = "different" }},
		{"file", func(a *acceptedRender) { a.Records[0].Record.File = "different" }},
		{"parent", func(a *acceptedRender) { a.Records[0].Record.Parent.Name = "different" }},
		{"builder", func(a *acceptedRender) { a.Records[0].Record.Builder = "different" }},
		{"inputs", func(a *acceptedRender) { a.Records[0].Record.Inputs[0] = "different" }},
		{"pinned", func(a *acceptedRender) { a.Records[0].Record.Pinned = false }},
		{"ref count", func(a *acceptedRender) { a.Records[0].Record.Refs = nil }},
		{"production count", func(a *acceptedRender) { a.Records[0].Record.Productions = nil }},
		{"production path", func(a *acceptedRender) { a.Records[0].Record.Productions[0].Path = "different" }},
		{"production file", func(a *acceptedRender) { a.Records[0].Record.Productions[0].File = "different" }},
		{"root count", func(a *acceptedRender) { a.Records[0].Roots = nil }},
		{"root length", func(a *acceptedRender) { a.Records[0].Roots = a.Records[0].Roots[:0] }},
		{"root path count", func(a *acceptedRender) { a.Records[0].Roots[0].Paths = nil }},
		{"root path", func(a *acceptedRender) { a.Records[0].Roots[0].Paths[0] = "different" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testAcceptedRenderExactValueCase(
				t,
				&testAcceptedRenderExactValueCaseInputs{original: &original, copyValue: &copyValue, tc: &tc},
			)
		},
		)
	}
	for _, location := range []struct {
		name     string
		selectID func(*acceptedRender) *manifest.NamedResource
	}{
		{"owner", func(a *acceptedRender) *manifest.NamedResource { return &a.Owner }},
		{"record", func(a *acceptedRender) *manifest.NamedResource { return &a.Records[0].ID }},
		{"authored", func(a *acceptedRender) *manifest.NamedResource { return &a.Records[0].Record.Authored }},
		{"parent", func(a *acceptedRender) *manifest.NamedResource { return &a.Records[0].Record.Parent }},
		{"reference", func(a *acceptedRender) *manifest.NamedResource { return &a.Records[0].Record.Refs[0] }},
		{"production owner", func(a *acceptedRender) *manifest.NamedResource { return &a.Records[0].Record.Productions[0].Owner }},
		{"production declaration", func(a *acceptedRender) *manifest.NamedResource {
			return &a.Records[0].Record.Productions[0].Declaration
		}},
		{"production authored", func(a *acceptedRender) *manifest.NamedResource { return &a.Records[0].Record.Productions[0].Authored }},
		{"production source", func(a *acceptedRender) *manifest.NamedResource { return &a.Records[0].Record.Productions[0].Source }},
		{"root source", func(a *acceptedRender) *manifest.NamedResource { return &a.Records[0].Roots[0].Source }},
	} {
		for _, component := range []struct {
			name   string
			mutate func(*manifest.NamedResource)
		}{
			{"group", func(n *manifest.NamedResource) { n.Group = "different" }},
			{"kind", func(n *manifest.NamedResource) { n.Kind = "different" }},
			{"namespace", func(n *manifest.NamedResource) { n.Namespace = "different" }},
			{"name", func(n *manifest.NamedResource) { n.Name = "different" }},
		} {
			t.Run(location.name+"/"+component.name, func(t *testing.T) {
				t.Parallel()
				testAcceptedRenderExactValueCase2(
					t,
					&testAcceptedRenderExactValueCase2Inputs{
						original:  &original,
						copyValue: &copyValue,
						location:  &location,
						component: &component,
					},
				)
			},
			)
		}
	}
	for _, tc := range []struct {
		name     string
		setNil   func(*acceptedRender)
		setEmpty func(*acceptedRender)
	}{
		{"records", func(a *acceptedRender) { a.Records = nil }, func(a *acceptedRender) { a.Records = []acceptedRecord{} }},
		{"inputs", func(a *acceptedRender) {
			a.Records[0].Record.Inputs = nil
		}, func(a *acceptedRender) {
			a.Records[0].Record.Inputs = []string{}
		}},
		{"references", func(a *acceptedRender) {
			a.Records[0].Record.Refs = nil
		}, func(a *acceptedRender) {
			a.Records[0].Record.Refs = []manifest.NamedResource{}
		}},
		{"productions", func(a *acceptedRender) {
			a.Records[0].Record.Productions = nil
		}, func(a *acceptedRender) {
			a.Records[0].Record.Productions = []manifest.Production{}
		}},
		{"roots", func(a *acceptedRender) { a.Records[0].Roots = nil }, func(a *acceptedRender) { a.Records[0].Roots = []acceptedRoot{} }},
		{"paths", func(a *acceptedRender) {
			a.Records[0].Roots[0].Paths = nil
		}, func(a *acceptedRender) {
			a.Records[0].Roots[0].Paths = []string{}
		}},
	} {
		t.Run("nil-empty/"+tc.name, func(t *testing.T) {
			t.Parallel()
			testAcceptedRenderExactValueCase3(
				t,
				&testAcceptedRenderExactValueCase3Inputs{original: &original, copyValue: &copyValue, tc: &tc},
			)
		},
		)
	}
}

func TestAcceptedRender_OneShotCanonicalInventory(t *testing.T) {
	t.Parallel()
	a := manifest.NamedResource{Kind: "kind", Name: "a"}
	b := a
	b.Name = "b"
	record := manifest.DiscoveryRecord{
		Refs:         []manifest.NamedResource{b, a, b},
		Productions:  []manifest.Production{{Owner: b}, {Owner: a}, {Owner: b}},
		ContentRoots: map[manifest.NamedResource][]string{b: {"z", "a", "z"}, a: nil},
	}
	calls := 0
	oneShot := func(yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {
		calls++
		assert.FatalIf(t, calls > 1, "%s", "accepted inventory was traversed more than once")
		if yield(b, record) {
			yield(a, record)
		}
	}
	frame := renderFrame{raw: "raw"}
	got, err := prepareAccepted(a, &frame, oneShot)
	assert.NoError(t, err)
	want, err := prepareAccepted(
		a,
		&frame,
		maps.All(map[manifest.NamedResource]manifest.DiscoveryRecord{a: record, b: record}),
	)
	assert.NoError(t, err)
	assert.FatalIf(t, !got.equal(&want) || calls != 1, "%s", "one-shot unordered inventory did not canonicalize")
	assert.Equal(t, got.Records[0].ID, a)
	assert.Diff(t, got.Records[0].Record.Refs, []manifest.NamedResource{a, b})
	assert.Diff(t, got.Records[0].Roots[1].Paths, []string{"a", "z"})
	record.Refs[0].Name = "mutated"
	record.Productions[0].Path = "mutated"
	record.ContentRoots[b][0] = "mutated"
	assert.FatalIf(t, !got.equal(&want), "%s", "caller mutation changed the prepared inventory")
	_, err = prepareAccepted(a, &frame, func(yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {
		if yield(a, record) {
			yield(a, record)
		}
	})
	assert.FatalIf(t, err == nil, "%s", "duplicate accepted identity was accepted")
}

func TestRenderReceipt_CompactBacking(t *testing.T) {
	t.Parallel()
	owner := manifest.NamedResource{Kind: "owner", Name: "renderer"}
	payload, err := testutil.EncodeRender([]map[string]any{{"data": map[string]any{"key": "original"}}})
	assert.NoError(t, err)
	raw, err := encodeFrame(renderHeader{Key: "key", Binding: "binding"}, payload)
	assert.NoError(t, err)
	frame, err := decodeFrame(raw)
	assert.NoError(t, err)
	accepted, err := encodeAccepted(owner, &frame, emptyRenderInventory)
	assert.NoError(t, err)
	wire := make([]byte, 0, len(raw)+len(accepted)+(1<<20))
	wire = append(wire, raw...)
	wire = append(wire, accepted...)
	frame, err = decodeFrame(wire)
	assert.NoError(t, err)
	assert.FatalIf(
		t,
		frame.encoded != nil || cap(frame.output) >= cap(wire),
		"%s",
		"decoded receipt retains serialized frame backing",
	)
	assert.FatalIf(
		t,
		frame.accepted == nil || frame.acceptedKey == "" || frame.raw == "",
		"%s",
		"compact receipt lost accepted evidence",
	)
	for i := range wire {
		wire[i] = 'x'
	}
	assert.Equal(t, bytes.Equal(frame.output, payload), true)
	previous, complete := frame.history(owner)
	assert.FatalIf(t, !complete || previous == nil, "%s", "input-buffer mutation changed compact history")
	for range previous {
		t.Fatal("complete-empty compact history gained a child")
	}
}

func TestRenderReceipt_FrameBounds(t *testing.T) {
	t.Parallel()
	owner := manifest.NamedResource{Group: "group", Kind: "owner", Namespace: "team", Name: "renderer"}
	payload, err := testutil.EncodeRender([]map[string]any{{"value": "original"}})
	assert.NoError(t, err)
	raw, err := encodeFrame(renderHeader{Key: "key", Binding: "binding"}, payload)
	assert.NoError(t, err)
	frame, err := decodeFrame(raw)
	assert.NoError(t, err)
	headerBytes := func(t *testing.T, header renderHeader) []byte {
		t.Helper()
		data, marshalErr2 := json.Marshal(header)
		assert.NoError(t, marshalErr2)
		wire := make([]byte, 12+len(data)+len(payload))
		copy(wire, frameDomain)
		binary.BigEndian.PutUint32(wire[8:12], uint32(len(data)))
		copy(wire[12:], data)
		copy(wire[12+len(data):], payload)
		return wire
	}
	for _, tc := range []struct {
		name string
		wire func(t *testing.T) []byte
	}{
		{"minimum", func(_ *testing.T) []byte { return raw[:7:7] }},
		{"header beyond data", func(_ *testing.T) []byte {
			wire := bytes.Clone(raw)
			binary.BigEndian.PutUint32(wire[8:12], uint32(len(wire)))
			return wire
		}},
		{"header limit", func(_ *testing.T) []byte {
			size := int(binary.BigEndian.Uint32(raw[8:12]))
			data := append(bytes.Clone(raw[12:12+size]), bytes.Repeat([]byte(" "), frameHeaderLimit+1-size)...)
			wire := make([]byte, 12+len(data)+len(payload))
			copy(wire, frameDomain)
			binary.BigEndian.PutUint32(wire[8:12], uint32(len(data)))
			copy(wire[12:], data)
			copy(wire[12+len(data):], payload)
			return wire
		}},
		{"negative payload size", func(t *testing.T) []byte {
			t.Helper()
			header := frame.header
			header.Size = -1
			return headerBytes(t, header)
		}},
		{"payload beyond data", func(t *testing.T) []byte {
			t.Helper()
			header := frame.header
			header.Size = len(raw) + 1
			return headerBytes(t, header)
		}},
		{"required output identity", func(t *testing.T) []byte {
			t.Helper()
			header := frame.header
			header.Output = ""
			return headerBytes(t, header)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testRenderReceiptFrameBoundsCase(t, &tc)
		},
		)
	}
	accepted, err := encodeAccepted(owner, &frame, emptyRenderInventory)
	assert.NoError(t, err)
	for _, tc := range []struct {
		name string
		tail func() []byte
	}{
		{"accepted limit", func() []byte {
			return append(bytes.Clone(accepted), bytes.Repeat([]byte(" "), frameHeaderLimit+1-len(accepted))...)
		}},
		{"accepted record type", func() []byte { return bytes.Replace(accepted, []byte(`"Records":null`), []byte(`"Records":{}`), 1) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testRenderReceiptFrameBoundsCase2(
				t,
				&testRenderReceiptFrameBoundsCase2Inputs{owner: &owner, payload: &payload, raw: &raw, tc: &tc},
			)
		},
		)
	}
	t.Run("encoded header limit", func(t *testing.T) {
		t.Parallel()
		testRenderReceiptFrameBoundsEncodedHeaderLimit(
			t,
			&payload,
		)
	},
	)
	for _, component := range []string{"group", "kind", "namespace", "name"} {
		t.Run("owner/"+component, func(t *testing.T) {
			t.Parallel()
			testRenderReceiptFrameBoundsCase3(
				t,
				&testRenderReceiptFrameBoundsCase3Inputs{
					owner:     &owner,
					raw:       &raw,
					accepted:  &accepted,
					component: &component,
				},
			)
		},
		)
	}
}

func TestRenderReceipt_AcceptedLifetime(t *testing.T) {
	t.Parallel()
	if !renderPersistenceEligible(t) {
		t.Skip("requires a clean VCS-stamped build for positive persistence")
	}
	renderer := NewClient(cachetest.New(t, t.TempDir(), cache.New))
	released := func() <-chan struct{} {
		payload, err := testutil.EncodeRender(
			[]map[string]any{{"value": string(bytes.Repeat([]byte{'x'}, frameHeaderLimit+1))}},
		)
		assert.NoError(t, err)
		done := make(chan struct{})
		runtime.AddCleanup(&payload[0], func(done chan struct{}) { close(done) }, done)
		receipt := manifest.NewRenderReceipt(
			renderFrame{
				header: renderHeader{Key: "large-accepted-lifetime", Analysis: &chartAnalysis{Version: "0.1.0"}},
				output: payload,
			},
			equalRenderFrame,
		)
		owner := manifest.NamedResource{Kind: "owner", Name: "renderer"}
		assert.NoError(
			t,
			renderer.RecordRender(
				t.Context(),
				RenderAcceptance{
					Owner:   owner,
					Receipt: receipt,
					Sources: []manifest.SourceEvidence{},
					Records: emptyRenderInventory,
				},
			),
		)
		assert.NoError(t, renderer.cache.Close())
		runtime.GC()
		docs, err := testutil.DecodeRender(receipt.Value().(renderFrame).output)
		assert.NoError(t, err)
		assert.Equal(t, len(docs[0]["value"].(string)), frameHeaderLimit+1)
		runtime.KeepAlive(receipt)
		return done
	}()
	runtime.GC()
	wait, stop := context.WithTimeout(t.Context(), 10*time.Second)
	defer stop()
	select {
	case <-released:
	case <-wait.Done():
		t.Fatal("closed renderer retained a released large receipt payload")
	}
	runtime.KeepAlive(renderer)
}

type testRenderReceiptDomainsCaseInputs struct {
	owner    *manifest.NamedResource
	payload  *[]byte
	receipt  *[]byte
	accepted *[]byte
	tc       *struct {
		name   string
		mutate func(*acceptedRender)
	}
}

func testRenderReceiptDomainsCase(t *testing.T, testCaseInput *testRenderReceiptDomainsCaseInputs) {
	t.Helper()

	var a acceptedRender
	assert.NoError(t, json.Unmarshal((*testCaseInput.accepted), &a))
	testCaseInput.tc.mutate(&a)
	encoded, marshalErr := json.Marshal(a)
	assert.NoError(t, marshalErr)
	candidate, marshalErr := decodeFrame(append(bytes.Clone((*testCaseInput.receipt)), encoded...))
	assert.NoError(t, marshalErr)
	{
		_, historyComplete2 := candidate.history((*testCaseInput.owner))
		assert.FatalIf(t, historyComplete2, "%s", "invalid accepted section supplied history")
	}
	assert.Equal(t, bytes.Equal(candidate.output, (*testCaseInput.payload)), true)
}

type testRenderReceiptDomainsCase2Inputs struct {
	receipt *[]byte
	tc      *struct {
		name   string
		mutate func([]byte) []byte
	}
}

func testRenderReceiptDomainsCase2(t *testing.T, testCaseInput *testRenderReceiptDomainsCase2Inputs) {
	t.Helper()

	{
		_, decodeFrameErr := decodeFrame(
			testCaseInput.tc.mutate(bytes.Clone((*testCaseInput.receipt))),
		)
		assert.FatalIf(t, decodeFrameErr == nil, "%s", "malformed raw frame was reusable")
	}
}

type testAcceptedRenderExactValueCaseInputs struct {
	original  *acceptedRender
	copyValue *func(t *testing.T, a acceptedRender) acceptedRender
	tc        *struct {
		name   string
		mutate func(*acceptedRender)
	}
}

func testAcceptedRenderExactValueCase(t *testing.T, testCaseInput *testAcceptedRenderExactValueCaseInputs) {
	t.Helper()

	changed := (*testCaseInput.copyValue)(t, (*testCaseInput.original))
	testCaseInput.tc.mutate(&changed)
	assert.FatalIf(
		t,
		testCaseInput.original.equal(&changed) || changed.equal(testCaseInput.original),
		"%s",
		"changed accepted value compared equal",
	)
}

type testAcceptedRenderExactValueCase2Inputs struct {
	original  *acceptedRender
	copyValue *func(t *testing.T, a acceptedRender) acceptedRender
	location  *struct {
		name     string
		selectID func(*acceptedRender) *manifest.NamedResource
	}
	component *struct {
		name   string
		mutate func(*manifest.NamedResource)
	}
}

func testAcceptedRenderExactValueCase2(t *testing.T, testCaseInput *testAcceptedRenderExactValueCase2Inputs) {
	t.Helper()

	changed := (*testCaseInput.copyValue)(t, (*testCaseInput.original))
	testCaseInput.component.mutate(testCaseInput.location.selectID(&changed))
	assert.FatalIf(
		t,
		testCaseInput.original.equal(&changed) || changed.equal(testCaseInput.original),
		"%s",
		"changed full identity compared equal",
	)
}

type testAcceptedRenderExactValueCase3Inputs struct {
	original  *acceptedRender
	copyValue *func(t *testing.T, a acceptedRender) acceptedRender
	tc        *struct {
		name     string
		setNil   func(*acceptedRender)
		setEmpty func(*acceptedRender)
	}
}

func testAcceptedRenderExactValueCase3(t *testing.T, testCaseInput *testAcceptedRenderExactValueCase3Inputs) {
	t.Helper()

	a, b := (*testCaseInput.copyValue)(
		t,
		(*testCaseInput.original),
	), (*testCaseInput.copyValue)(
		t,
		(*testCaseInput.original),
	)
	testCaseInput.tc.setNil(&a)
	testCaseInput.tc.setEmpty(&b)
	assert.FatalIf(t, a.equal(&b) || b.equal(&a), "%s", "nil and empty serialized slices compared equal")
}

func testRenderReceiptFrameBoundsCase(t *testing.T, testCaseInput *struct {
	name string
	wire func(t *testing.T) []byte
},
) {
	t.Helper()

	defer func() {
		{
			problem := recover()
			assert.FatalIf(
				t,
				problem != nil,
				"malformed raw frame panicked instead of returning an error: %v",
				problem,
			)
		}
	}()
	{
		_, decodeFrameErr2 := decodeFrame(testCaseInput.wire(t))
		assert.FatalIf(t, decodeFrameErr2 == nil, "%s", "malformed raw frame was reusable")
	}
}

type testRenderReceiptFrameBoundsCase2Inputs struct {
	owner   *manifest.NamedResource
	payload *[]byte
	raw     *[]byte
	tc      *struct {
		name string
		tail func() []byte
	}
}

func testRenderReceiptFrameBoundsCase2(t *testing.T, testCaseInput *testRenderReceiptFrameBoundsCase2Inputs) {
	t.Helper()

	decoded, err := decodeFrame(append(bytes.Clone((*testCaseInput.raw)), testCaseInput.tc.tail()...))
	assert.NoError(t, err)
	assert.Equal(t, bytes.Equal(decoded.output, (*testCaseInput.payload)), true)
	{
		_, complete := decoded.history((*testCaseInput.owner))
		assert.FatalIf(t, complete, "%s", "invalid accepted section supplied history")
	}
}

func testRenderReceiptFrameBoundsEncodedHeaderLimit(
	t *testing.T,
	testCaseInput *[]byte,
) {
	t.Helper()

	_, err := encodeFrame(
		renderHeader{Key: string(bytes.Repeat([]byte{'x'}, frameHeaderLimit+1))},
		(*testCaseInput),
	)
	assert.FatalIf(t, err == nil, "%s", "oversized raw header was encoded")
}

type testRenderReceiptFrameBoundsCase3Inputs struct {
	owner     *manifest.NamedResource
	raw       *[]byte
	accepted  *[]byte
	component *string
}

func testRenderReceiptFrameBoundsCase3(t *testing.T, testCaseInput *testRenderReceiptFrameBoundsCase3Inputs) {
	t.Helper()

	decoded, err := decodeFrame(append(bytes.Clone((*testCaseInput.raw)), (*testCaseInput.accepted)...))
	assert.NoError(t, err)
	foreign := (*testCaseInput.owner)
	switch *testCaseInput.component {
	case "group":
		foreign.Group = "foreign"
	case "kind":
		foreign.Kind = "foreign"
	case "namespace":
		foreign.Namespace = "foreign"
	case "name":
		foreign.Name = "foreign"
	}
	{
		_, complete := decoded.history(foreign)
		assert.FatalIf(t, complete, "%s", "foreign effective owner borrowed history")
	}
}
