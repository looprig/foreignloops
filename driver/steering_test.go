package driver

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/looprig/core/content"
)

type steeringTestAgent struct{}

func (steeringTestAgent) Spawn(context.Context, Turn) (Stream, error) { return nil, nil }

type steeringTestSteerer struct{}

func (steeringTestSteerer) Steer(context.Context, SteerRequest) (SteerResult, error) {
	return SteerResult{Outcome: SteerOutcomeInjected}, nil
}

var (
	_ Agent   = steeringTestAgent{}
	_ Steerer = steeringTestSteerer{}
)

func TestSteererIsOptionalForExistingAgents(t *testing.T) {
	var agent Agent = steeringTestAgent{}
	if _, ok := agent.(Steerer); ok {
		t.Fatal("legacy Agent unexpectedly satisfies optional Steerer")
	}

	var capable Agent = steeringTestCapableAgent{}
	if _, ok := capable.(Steerer); !ok {
		t.Fatal("steering-capable Agent does not expose optional Steerer")
	}
}

type steeringTestCapableAgent struct{}

func (steeringTestCapableAgent) Spawn(context.Context, Turn) (Stream, error) { return nil, nil }

func (steeringTestCapableAgent) Steer(context.Context, SteerRequest) (SteerResult, error) {
	return SteerResult{Outcome: SteerOutcomeUnsupported}, nil
}

func TestSteerRequestOwnsPromptBlocks(t *testing.T) {
	providerState := []byte(`{"opaque":"state"}`)
	toolInput := []byte(`{"path":"before"}`)
	imageData := []byte("image-before")
	documentData := []byte("document-before")
	audioData := []byte("audio-before")
	blocks := []content.Block{
		&content.TextBlock{Text: "text-before"},
		&content.ImageBlock{Source: content.ImageSource{Data: imageData}},
		&content.AudioBlock{Data: audioData},
		&content.DocumentBlock{Data: documentData},
		&content.ThinkingBlock{ProviderState: providerState},
		&content.ToolUseBlock{Input: toolInput},
		&content.ToolResultBlock{Content: []content.Block{
			&content.TextBlock{Text: "nested-before"},
		}},
	}

	request, err := NewSteerRequest(blocks)
	if err != nil {
		t.Fatalf("NewSteerRequest() error = %v", err)
	}

	// Mutating both the caller-owned source and a returned snapshot must not
	// change the request retained by the driver boundary.
	blocks[0].(*content.TextBlock).Text = "text-after"
	imageData[0] = 'X'
	audioData[0] = 'X'
	documentData[0] = 'X'
	providerState[0] = 'X'
	toolInput[0] = 'X'

	snapshot := request.Prompt()
	snapshot[0].(*content.TextBlock).Text = "snapshot-after"
	snapshot[1].(*content.ImageBlock).Source.Data[0] = 'Y'
	snapshot[6].(*content.ToolResultBlock).Content[0].(*content.TextBlock).Text = "nested-after"

	want := []content.Block{
		&content.TextBlock{Text: "text-before"},
		&content.ImageBlock{Source: content.ImageSource{Data: []byte("image-before")}},
		&content.AudioBlock{Data: []byte("audio-before")},
		&content.DocumentBlock{Data: []byte("document-before")},
		&content.ThinkingBlock{ProviderState: []byte(`{"opaque":"state"}`)},
		&content.ToolUseBlock{Input: []byte(`{"path":"before"}`)},
		&content.ToolResultBlock{Content: []content.Block{
			&content.TextBlock{Text: "nested-before"},
		}},
	}
	if got := request.Prompt(); !reflect.DeepEqual(got, want) {
		t.Fatalf("SteerRequest.Prompt() = %#v, want %#v", got, want)
	}
}

// TestSteerRequestCarriesEveryBlockVariantWhole is steering's copy of the
// guarantee backend/snapshot.go already carries: a steering prompt is a real
// message payload, so every sealed content.Block variant must survive the clone
// with every exported field intact.
//
// Two concrete losses this pins. ToolUseBlock's ProviderState /
// ProviderStateFormat are what let a provider accept the next turn of a tool
// loop; a struct literal that names only ID/Name/Input drops them silently, and
// the steering prompt then replays a tool_use the issuing dialect rejects.
// RefusalBlock had no arm at all, so a prompt containing one was rejected
// outright as an "unknown type" — a declined turn that cannot be steered.
//
// The comparison is reflect.DeepEqual against a separately built expectation
// with every field populated, not a field-by-field assertion, so a field added
// to core fails here rather than going unnoticed.
func TestSteerRequestCarriesEveryBlockVariantWhole(t *testing.T) {
	blocks := []content.Block{
		&content.TextBlock{Text: "text"},
		&content.ImageBlock{MediaType: content.MediaTypeImagePNG, Source: content.ImageSource{URL: "https://example.test/i.png", Data: []byte("image")}},
		&content.AudioBlock{MediaType: content.MediaTypeAudioWAV, Data: []byte("audio")},
		&content.DocumentBlock{MediaType: content.MediaTypeDocumentPDF, Name: "doc.pdf", Data: []byte("document"), Text: "document text"},
		&content.ThinkingBlock{Thinking: "thought", Signature: "sig", ProviderState: []byte(`{"opaque":"thinking"}`), ProviderStateFormat: "anthropic"},
		&content.ToolUseBlock{ID: "tool-1", Name: "lookup", Input: []byte(`{"path":"p"}`), ProviderState: []byte(`{"opaque":"tooluse"}`), ProviderStateFormat: "anthropic"},
		&content.RefusalBlock{Text: "I will not do that."},
		&content.ToolResultBlock{ToolUseID: "tool-1", IsError: true, Content: []content.Block{
			&content.RefusalBlock{Text: "nested refusal"},
			&content.ToolUseBlock{ID: "tool-2", Name: "nested", Input: []byte(`{"n":1}`), ProviderState: []byte(`{"opaque":"nested"}`), ProviderStateFormat: "anthropic"},
		}},
	}

	request, err := NewSteerRequest(blocks)
	if err != nil {
		t.Fatalf("NewSteerRequest() error = %v", err)
	}
	got := request.Prompt()
	if len(got) != len(blocks) {
		t.Fatalf("SteerRequest.Prompt() returned %d blocks, want %d", len(got), len(blocks))
	}
	// Compared per index: %#v on a []content.Block prints only pointers, and a
	// dropped field has to name itself to be worth pinning.
	for i := range blocks {
		if !reflect.DeepEqual(got[i], blocks[i]) {
			t.Errorf("prompt[%d] = %s, want %s", i, blockString(got[i]), blockString(blocks[i]))
		}
	}
	if t.Failed() {
		t.FailNow()
	}

	// Every byte-backed field must be a copy, not a view. Mutating the source
	// after construction proves it for the retained value; mutating one
	// snapshot proves it for the next.
	blocks[5].(*content.ToolUseBlock).ProviderState[2] = 'X'
	blocks[7].(*content.ToolResultBlock).Content[1].(*content.ToolUseBlock).ProviderState[2] = 'X'
	snapshot := request.Prompt()
	if got := string(snapshot[5].(*content.ToolUseBlock).ProviderState); got != `{"opaque":"tooluse"}` {
		t.Fatalf("retained tool_use provider state = %s; the clone aliases the caller's bytes", got)
	}
	if got := string(snapshot[7].(*content.ToolResultBlock).Content[1].(*content.ToolUseBlock).ProviderState); got != `{"opaque":"nested"}` {
		t.Fatalf("retained nested tool_use provider state = %s; the clone aliases the caller's bytes", got)
	}
	snapshot[5].(*content.ToolUseBlock).ProviderState[2] = 'Y'
	if got := string(request.Prompt()[5].(*content.ToolUseBlock).ProviderState); got != `{"opaque":"tooluse"}` {
		t.Fatalf("second snapshot provider state = %s; snapshots share backing arrays", got)
	}
}

// blockString renders a block's VALUE rather than its pointer, so a diff names
// the field that was dropped.
func blockString(block content.Block) string {
	return fmt.Sprintf("%T%+v", block, reflect.ValueOf(block).Elem().Interface())
}

func TestSteerRequestRejectsMissingPrompt(t *testing.T) {
	for _, blocks := range [][]content.Block{
		nil,
		{},
		{nil},
	} {
		if _, err := NewSteerRequest(blocks); err == nil {
			t.Fatalf("NewSteerRequest(%#v) error = nil, want missing/invalid prompt error", blocks)
		}
	}
}

func TestSteerOutcomesAreClosedAndExact(t *testing.T) {
	tests := []struct {
		outcome SteerOutcome
		wire    string
	}{
		{SteerOutcomeInjected, "injected"},
		{SteerOutcomeFallbackRequired, "fallback_required"},
		{SteerOutcomeUnsupported, "unsupported"},
		{SteerOutcomeAdmissionUnknown, "admission_unknown"},
		{SteerOutcomeDeliveryUnknown, "delivery_unknown"},
		{SteerOutcomeDeliveredUntrackable, "delivered_untrackable"},
	}
	for _, tt := range tests {
		if string(tt.outcome) != tt.wire {
			t.Errorf("SteerOutcome %q = %q, want %q", tt.wire, tt.outcome, tt.wire)
		}
		if !tt.outcome.Valid() {
			t.Errorf("SteerOutcome(%q).Valid() = false, want true", tt.wire)
		}
	}
	if SteerOutcome("future").Valid() {
		t.Fatal("unknown SteerOutcome is valid; want closed enum")
	}
	if err := (SteerResult{Outcome: SteerOutcome("future")}).Validate(); err == nil {
		t.Fatal("SteerResult.Validate() error = nil for unknown outcome")
	}
	for _, outcome := range []SteerOutcome{SteerOutcomeInjected, SteerOutcomeAdmissionUnknown, SteerOutcomeDeliveryUnknown, SteerOutcomeDeliveredUntrackable} {
		if outcome.RetrySafe() {
			t.Errorf("%q is retry-safe; uncertain/lifecycle-breach outcome must not be retried", outcome)
		}
	}
	for _, outcome := range []SteerOutcome{SteerOutcomeUnsupported, SteerOutcomeFallbackRequired} {
		if !outcome.RetrySafe() {
			t.Errorf("%q is not retry-safe; proven non-delivery must permit fallback", outcome)
		}
	}
}

func TestSteerResultAndObservationsCarryAdmissionAndOrder(t *testing.T) {
	result := SteerResult{
		Outcome:          SteerOutcomeInjected,
		Reason:           "active turn accepted",
		WriteAdmitted:    true,
		ReceiveSequence:  11,
		ResponseSequence: 11,
	}
	if err := result.Validate(); err != nil {
		t.Fatalf("SteerResult.Validate() error = %v", err)
	}
	steer := SteerObservation{SteerResult: result}
	if steer.ReceiveSequence != 11 || steer.ResponseSequence != 11 || !steer.WriteAdmitted {
		t.Fatalf("SteerObservation = %#v, want steering facts", steer)
	}

	prompt := PromptObservation{
		WriteAdmitted:    true,
		ReceiveSequence:  7,
		ResponseSequence: 7,
	}
	update := UpdateObservation{
		Event:           Event{Kind: KindTextDelta, Text: "before completion"},
		ReceiveSequence: 5,
	}
	if prompt.Sequence() != 7 || update.Sequence() != 5 || steer.Sequence() != 11 {
		t.Fatalf("observation sequences = prompt %d, update %d, steer %d; want 7, 5, 11", prompt.Sequence(), update.Sequence(), steer.Sequence())
	}

	var observations []Observation = []Observation{update, prompt, steer}
	if len(observations) != 3 {
		t.Fatalf("observations length = %d, want 3", len(observations))
	}
}

func TestSteerResultValidationEnforcesDeliveryFacts(t *testing.T) {
	tests := []struct {
		name string
		got  SteerResult
		want bool
	}{
		// unsupported is a proof that no write occurred and therefore carries
		// no inbound response fact.
		{name: "unsupported pre-write", got: SteerResult{Outcome: SteerOutcomeUnsupported}, want: true},
		{name: "unsupported admitted", got: SteerResult{Outcome: SteerOutcomeUnsupported, WriteAdmitted: true}, want: false},
		{name: "unsupported response", got: SteerResult{Outcome: SteerOutcomeUnsupported, ResponseSequence: 1}, want: false},
		{name: "unsupported receive", got: SteerResult{Outcome: SteerOutcomeUnsupported, ReceiveSequence: 1}, want: false},
		{name: "unsupported sequence mismatch", got: SteerResult{Outcome: SteerOutcomeUnsupported, ReceiveSequence: 1, ResponseSequence: 2}, want: false},

		{name: "injected admitted response", got: SteerResult{Outcome: SteerOutcomeInjected, WriteAdmitted: true, ReceiveSequence: 1, ResponseSequence: 1}, want: true},
		{name: "injected not admitted", got: SteerResult{Outcome: SteerOutcomeInjected, ReceiveSequence: 1, ResponseSequence: 1}, want: false},
		{name: "injected no response", got: SteerResult{Outcome: SteerOutcomeInjected, WriteAdmitted: true}, want: false},
		{name: "injected sequence mismatch", got: SteerResult{Outcome: SteerOutcomeInjected, WriteAdmitted: true, ReceiveSequence: 1, ResponseSequence: 2}, want: false},

		{name: "fallback pre-write", got: SteerResult{Outcome: SteerOutcomeFallbackRequired}, want: true},
		{name: "fallback admitted before response", got: SteerResult{Outcome: SteerOutcomeFallbackRequired, WriteAdmitted: true}, want: false},
		{name: "fallback post-write", got: SteerResult{Outcome: SteerOutcomeFallbackRequired, WriteAdmitted: true, ReceiveSequence: 2, ResponseSequence: 2}, want: true},
		{name: "fallback response without admission", got: SteerResult{Outcome: SteerOutcomeFallbackRequired, ResponseSequence: 2}, want: false},
		{name: "fallback sequence mismatch", got: SteerResult{Outcome: SteerOutcomeFallbackRequired, WriteAdmitted: true, ReceiveSequence: 1, ResponseSequence: 2}, want: false},

		{name: "unknown admitted before response", got: SteerResult{Outcome: SteerOutcomeDeliveryUnknown, WriteAdmitted: true}, want: true},
		{name: "unknown admitted response", got: SteerResult{Outcome: SteerOutcomeDeliveryUnknown, WriteAdmitted: true, ReceiveSequence: 3, ResponseSequence: 3}, want: true},
		{name: "unknown not admitted", got: SteerResult{Outcome: SteerOutcomeDeliveryUnknown}, want: false},
		{name: "unknown response without admission", got: SteerResult{Outcome: SteerOutcomeDeliveryUnknown, ReceiveSequence: 3, ResponseSequence: 3}, want: false},
		{name: "unknown sequence mismatch", got: SteerResult{Outcome: SteerOutcomeDeliveryUnknown, WriteAdmitted: true, ReceiveSequence: 1, ResponseSequence: 2}, want: false},
		{name: "admission unknown without admission", got: SteerResult{Outcome: SteerOutcomeAdmissionUnknown}, want: true},
		{name: "admission unknown cannot claim admission", got: SteerResult{Outcome: SteerOutcomeAdmissionUnknown, WriteAdmitted: true}, want: false},
		{name: "delivery unknown cannot omit admission", got: SteerResult{Outcome: SteerOutcomeDeliveryUnknown}, want: false},

		{name: "untrackable admitted response", got: SteerResult{Outcome: SteerOutcomeDeliveredUntrackable, WriteAdmitted: true, ReceiveSequence: 4, ResponseSequence: 4}, want: true},
		{name: "untrackable not admitted", got: SteerResult{Outcome: SteerOutcomeDeliveredUntrackable, ReceiveSequence: 4, ResponseSequence: 4}, want: false},
		{name: "untrackable no response", got: SteerResult{Outcome: SteerOutcomeDeliveredUntrackable, WriteAdmitted: true}, want: false},
		{name: "untrackable sequence mismatch", got: SteerResult{Outcome: SteerOutcomeDeliveredUntrackable, WriteAdmitted: true, ReceiveSequence: 1, ResponseSequence: 2}, want: false},

		{name: "all outcomes receive alias omitted", got: SteerResult{Outcome: SteerOutcomeFallbackRequired, WriteAdmitted: true, ResponseSequence: 5}, want: false},
		{name: "all outcomes response alias omitted", got: SteerResult{Outcome: SteerOutcomeFallbackRequired, WriteAdmitted: true, ReceiveSequence: 5}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.got.Validate()
			if (err == nil) != tt.want {
				t.Fatalf("SteerResult.Validate() error = %v, want valid = %t for %#v", err, tt.want, tt.got)
			}
		})
	}
}

func TestObservationKindValuesAreClosed(t *testing.T) {
	values := []ObservationKind{ObservationPrompt, ObservationUpdate, ObservationSteer}
	for want, got := range values {
		if int(got) != want {
			t.Errorf("ObservationKind value %d = %d, want %d", want, got, want)
		}
		if !got.Valid() {
			t.Errorf("ObservationKind(%d).Valid() = false, want true", got)
		}
	}
	if ObservationKind(99).Valid() {
		t.Fatal("unknown ObservationKind is valid; want closed enum")
	}
}
