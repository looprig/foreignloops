package codex

import (
	"errors"
	"strings"
	"testing"

	"github.com/looprig/core/content"
	"github.com/looprig/foreignloops/driver"
)

func TestDecodeLineHappyJSONL(t *testing.T) {
	t.Parallel()
	input := `{"type":"thread.started","thread_id":"0199a213-81c0-7800-8aa1-bbab2a035a53"}
{"type":"turn.started"}
{"type":"item.completed","item":{"id":"item_1","type":"agent_message","text":"done"}}
{"type":"turn.completed","usage":{"input_tokens":1,"output_tokens":2}}`

	var got []driver.Event
	for _, line := range strings.Split(input, "\n") {
		evs, err := decodeLine([]byte(line))
		if err != nil {
			t.Fatalf("decodeLine(%s) error = %v", line, err)
		}
		got = append(got, evs...)
	}

	if len(got) != 3 {
		t.Fatalf("events = %d, want 3: %#v", len(got), got)
	}
	if got[0].Kind != driver.KindInit {
		t.Fatalf("event[0].Kind = %v, want KindInit", got[0].Kind)
	}
	if got[0].SessionID != "0199a213-81c0-7800-8aa1-bbab2a035a53" {
		t.Fatalf("event[0].SessionID = %q", got[0].SessionID)
	}
	assertStepText(t, got[1], "done")
	if got[2].Kind != driver.KindTerminalOK {
		t.Fatalf("event[2].Kind = %v, want KindTerminalOK", got[2].Kind)
	}
}

func TestDecodeLineTerminalErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		line    string
		wantErr string
	}{
		{
			name:    "turn failed uses error detail",
			line:    `{"type":"turn.failed","error":{"message":"model failed","detail":"context limit"}}`,
			wantErr: "model failed: context limit",
		},
		{
			name:    "top-level error uses message",
			line:    `{"type":"error","error":{"message":"permission denied"}}`,
			wantErr: "permission denied",
		},
		{
			name:    "string error is accepted",
			line:    `{"type":"error","error":"plain failure"}`,
			wantErr: "plain failure",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := decodeLine([]byte(tt.line))
			if err != nil {
				t.Fatalf("decodeLine() error = %v", err)
			}
			if len(got) != 1 {
				t.Fatalf("events = %d, want 1: %#v", len(got), got)
			}
			if got[0].Kind != driver.KindTerminalError {
				t.Fatalf("Kind = %v, want KindTerminalError", got[0].Kind)
			}
			if got[0].ErrText != tt.wantErr {
				t.Fatalf("ErrText = %q, want %q", got[0].ErrText, tt.wantErr)
			}
		})
	}
}

func TestDecodeLineIgnoresUnknowns(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		line string
	}{
		{
			name: "unknown event type",
			line: `{"type":"future.event","payload":true}`,
		},
		{
			name: "unknown item type",
			line: `{"type":"item.completed","item":{"id":"item_2","type":"tool_call","text":"ignored"}}`,
		},
		{
			name: "agent message without text",
			line: `{"type":"item.completed","item":{"id":"item_3","type":"agent_message"}}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := decodeLine([]byte(tt.line))
			if err != nil {
				t.Fatalf("decodeLine() error = %v", err)
			}
			if len(got) != 0 {
				t.Fatalf("events = %#v, want none", got)
			}
		})
	}
}

func TestDecodeLineMalformedJSONReturnsDecodeError(t *testing.T) {
	t.Parallel()
	_, err := decodeLine([]byte(`{"type":"thread.started"`))
	if err == nil {
		t.Fatal("decodeLine() error = nil, want *driver.DecodeError")
	}
	var de *driver.DecodeError
	if !errors.As(err, &de) {
		t.Fatalf("decodeLine() error = %T %[1]v, want *driver.DecodeError", err)
	}
}

func TestDecodeLineRejectsInvalidThreadID(t *testing.T) {
	t.Parallel()
	got, err := decodeLine([]byte(`{"type":"thread.started","thread_id":"--dangerously-bypass-approvals-and-sandbox"}`))
	if len(got) != 0 {
		t.Fatalf("decodeLine() events = %#v, want none", got)
	}
	var decodeErr *driver.DecodeError
	if !errors.As(err, &decodeErr) {
		t.Fatalf("decodeLine() error = %T %v, want *driver.DecodeError", err, err)
	}
}

func assertStepText(t *testing.T, ev driver.Event, want string) {
	t.Helper()
	if ev.Kind != driver.KindStepComplete {
		t.Fatalf("Kind = %v, want KindStepComplete", ev.Kind)
	}
	if ev.Message == nil {
		t.Fatal("Message = nil")
	}
	if ev.Message.Role != content.RoleAssistant {
		t.Fatalf("Message.Role = %q, want assistant", ev.Message.Role)
	}
	if len(ev.Message.Blocks) != 1 {
		t.Fatalf("Message.Blocks = %d, want 1", len(ev.Message.Blocks))
	}
	tb, ok := ev.Message.Blocks[0].(*content.TextBlock)
	if !ok {
		t.Fatalf("Message.Blocks[0] = %T, want *content.TextBlock", ev.Message.Blocks[0])
	}
	if tb.Text != want {
		t.Fatalf("TextBlock.Text = %q, want %q", tb.Text, want)
	}
}
