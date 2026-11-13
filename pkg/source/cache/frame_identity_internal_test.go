package cache

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"maps"
	"sync"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestFrame_MetadataOffsets(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, envelope string }{
		{"spacing", ` { "Before" : [1,2], "Metadata" : {"key":"value"}, "Domain":"domain", "Size":0, "Payload":%q, "After":true } `},
		{"duplicate fields", `{"Metadata":{"key":"first"},"After":{"Metadata":null},"mEtAdAtA":{"key":"value"},"Domain":"domain","Size":0,"Payload":%q}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			data := []byte(fmt.Sprintf(tc.envelope, manifest.Fingerprint([]byte{})))
			wire := append(make([]byte, framePrefixSize), data...)
			copy(wire, framePrefix)
			binary.BigEndian.PutUint32(wire[8:framePrefixSize], uint32(len(data)))
			var metadata map[string]string
			opened, err := OpenFrame(t.Context(), "domain", frameGeneration(wire), &metadata)
			assert.NoError(t, err)
			assert.Diff(t, metadata, map[string]string{"key": "value"})
			assert.Equal(t, string(opened.envelope.Metadata.RawMessage), `{"key":"value"}`)
			assert.Equal(t, bytes.Equal(frameBytes(t, opened, FramePublicationInput{}), wire), true)
		})
	}
}

func TestFrameIdentity_JSONEquivalence(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		data string
	}{
		{"object", `{"Domain":"domain","Metadata":{"key":"value"},"Size":0,"Payload":"payload"}`},
		{"spacing", ` { "Domain" : "domain", "Metadata" : [ true, null, 1 ], "Size" : 0, "Payload" : "payload" } `},
		{"escapes", `{"Metadata":{"<>&":"\u003c<&>\u2028\u2029\/\u0041"}}`},
		{"numbers", `{"Metadata":[-0,1.00,1e+09,123456789012345678901234567890]}`},
		{"duplicate fields", `{"Metadata":{"key":1,"key":2},"Metadata":{"another":3}}`},
		{"unknown fields", `{"Before":1,"Metadata":{},"After":[2,3]}`},
		{"invalid utf8", "{\"Metadata\":\"\xff\"}"},
	} {
		for _, padding := range []int{0, frameIdentityBuffer + 1} {
			t.Run(fmt.Sprintf("%s/padding=%d", tc.name, padding), func(t *testing.T) {
				t.Parallel()
				data := append([]byte(tc.data), bytes.Repeat([]byte(" "), padding)...)
				reference, err := json.Marshal(struct {
					Version  int             `json:"Version"`
					Envelope json.RawMessage `json:"Envelope"`
				}{Version: frameVersion, Envelope: data})
				assert.NoError(t, err)
				assert.Equal(t, frameIdentity(data), manifest.SHA256Hex(reference))
			})
		}
	}
}

func TestFrameIdentity_RejectsMalformed(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, data string }{
		{"truncated", `{"Metadata":[1,2`},
		{"trailing value", `{"Metadata":[]} false`},
		{"invalid scalar", `{"Metadata":tru}`},
	} {
		for _, padding := range []int{0, frameIdentityBuffer + 1} {
			t.Run(fmt.Sprintf("%s/padding=%d", tc.name, padding), func(t *testing.T) {
				t.Parallel()
				data := append([]byte(tc.data), bytes.Repeat([]byte(" "), padding)...)
				assert.Equal(t, frameIdentity(data), "")
			})
		}
	}
}

func TestFrame_ConcurrentIdentityPublication(t *testing.T) {
	t.Parallel()
	owner := manifest.NamedResource{Kind: "owner", Name: "renderer"}
	frame, err := NewFrame(inputFrameDomain, "metadata", []byte("output"))
	assert.NoError(t, err)
	input := FramePublicationInput{
		Owner: owner, Content: "final", Complete: true,
		Records: maps.All(map[manifest.NamedResource]manifest.DiscoveryRecord{}),
	}
	expected, err := frame.PreparePublication(t.Context(), input)
	assert.NoError(t, err)
	for _, accepted := range []bool{false, true} {
		t.Run(fmt.Sprintf("accepted=%t", accepted), func(t *testing.T) {
			t.Parallel()
			publicationInput := FramePublicationInput{}
			if accepted {
				publicationInput = input
			}
			wire := frameBytes(t, frame, publicationInput)
			want := frameBytes(t, frame, input)
			var metadata string
			opened, openErr := OpenFrame(t.Context(), inputFrameDomain, frameGeneration(wire), &metadata)
			assert.NoError(t, openErr)
			start := make(chan struct{})
			var ready, joined sync.WaitGroup
			ready.Add(2)
			for range 2 {
				joined.Go(func() {
					ready.Done()
					<-start
					_, identity := InputFrameIdentity("source", "url", opened)
					assert.Equal(t, identity, frame.raw)
					_, complete := opened.History(owner, "final")
					assert.Equal(t, complete, accepted)
					prepared, prepareErr := opened.PreparePublication(t.Context(), input)
					assert.NoError(t, prepareErr)
					assert.Equal(t, prepared.Identity(), expected.Identity())
					var output bytes.Buffer
					_, writeErr := prepared.WriteTo(&output)
					assert.NoError(t, writeErr)
					assert.Diff(t, output.Bytes(), want)
					data, readErr := opened.Payload(t.Context())
					assert.NoError(t, readErr)
					assert.Equal(t, string(data), "output")
					_, complete = opened.History(owner, "final")
					assert.Equal(t, complete, accepted)
				})
			}
			ready.Wait()
			close(start)
			joined.Wait()
		})
	}
}
