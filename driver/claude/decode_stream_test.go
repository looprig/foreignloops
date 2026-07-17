package claude

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/looprig/core/content"
	"github.com/looprig/foreignloop/driver"
)

func drainStream(t *testing.T, fixture string) ([]driver.Event, error) {
	t.Helper()
	f, err := os.Open(filepath.Join("testdata", "stream", fixture))
	if err != nil {
		t.Fatalf("open fixture %s: %v", fixture, err)
	}
	t.Cleanup(func() { _ = f.Close() })
	ch, errFn := decodeStream(f)
	var got []driver.Event
	for ev := range ch {
		got = append(got, ev)
	}
	return got, errFn()
}

func eventKinds(evs []driver.Event) []driver.Kind {
	if len(evs) == 0 {
		return nil
	}
	out := make([]driver.Kind, len(evs))
	for i, ev := range evs {
		out[i] = ev.Kind
	}
	return out
}

func TestDecodeStreamFixtures(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		fixture   string
		wantKinds []driver.Kind
		wantErr   bool
	}{
		{
			name:    "happy multi-event stream",
			fixture: "happy.jsonl",
			wantKinds: []driver.Kind{
				driver.KindInit,
				driver.KindTextDelta,
				driver.KindTextDelta,
				driver.KindThinkingDelta,
				driver.KindToolUse,
				driver.KindStepComplete,
				driver.KindToolResult,
				driver.KindTerminalOK,
			},
		},
		{name: "empty reader closes cleanly", fixture: "empty.jsonl"},
		{
			name:      "unknown types ignored",
			fixture:   "unknown.jsonl",
			wantKinds: []driver.Kind{driver.KindInit, driver.KindTerminalOK},
		},
		{
			name:      "garbage line surfaces DecodeError but stream completes",
			fixture:   "garbage.jsonl",
			wantKinds: []driver.Kind{driver.KindInit, driver.KindTerminalError},
			wantErr:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := drainStream(t, tt.fixture)
			if (err != nil) != tt.wantErr {
				t.Fatalf("decodeStream error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				var decodeErr *driver.DecodeError
				if !errors.As(err, &decodeErr) {
					t.Fatalf("error = %T %v, want *driver.DecodeError", err, err)
				}
			}
			if gotKinds := eventKinds(got); !reflect.DeepEqual(gotKinds, tt.wantKinds) {
				t.Fatalf("event kinds = %v, want %v", gotKinds, tt.wantKinds)
			}
		})
	}
}

func TestDecodeStreamHappyFieldDetail(t *testing.T) {
	t.Parallel()
	got, err := drainStream(t, "happy.jsonl")
	if err != nil {
		t.Fatalf("decodeStream error = %v", err)
	}
	if got[0].SessionID != "sess-123" {
		t.Errorf("init SessionID = %q, want sess-123", got[0].SessionID)
	}
	if got[1].Text != "Hel" || got[2].Text != "lo" {
		t.Errorf("text deltas = %q,%q, want Hel,lo", got[1].Text, got[2].Text)
	}
	if got[3].Text != "hmm" {
		t.Errorf("thinking delta = %q, want hmm", got[3].Text)
	}
	if got[4].ToolUseID != "toolu_1" || got[4].ToolName != "Bash" {
		t.Errorf("tool use = %q/%q, want toolu_1/Bash", got[4].ToolUseID, got[4].ToolName)
	}
	step := got[5]
	if step.Message == nil {
		t.Fatal("step complete Message = nil")
	}
	if len(step.Message.Blocks) != 2 {
		t.Fatalf("step blocks = %d, want 2", len(step.Message.Blocks))
	}
	if block, ok := step.Message.Blocks[0].(*content.TextBlock); !ok || block.Text != "Hello" {
		t.Errorf("step block[0] = %#v, want TextBlock{Hello}", step.Message.Blocks[0])
	}
	if block, ok := step.Message.Blocks[1].(*content.ToolUseBlock); !ok || block.ID != "toolu_1" || block.Name != "Bash" || string(block.Input) != `{"command":"ls"}` {
		t.Errorf("step block[1] = %#v, want complete toolu_1/Bash block", step.Message.Blocks[1])
	}
	result := got[6]
	if result.ToolUseID != "toolu_1" || result.IsError || result.ResultPreview != "file1\nfile2" {
		t.Errorf("tool result = %+v, want toolu_1/false/file1\\nfile2", result)
	}
	terminal := got[7]
	if terminal.Message == nil || len(terminal.Message.Blocks) != 1 {
		t.Fatalf("terminal message = %#v, want one text block", terminal.Message)
	}
	if block, ok := terminal.Message.Blocks[0].(*content.TextBlock); !ok || block.Text != "Done" {
		t.Errorf("terminal block = %#v, want TextBlock{Done}", terminal.Message.Blocks[0])
	}
}

func TestDecodeStreamLineUnknownAndMalformed(t *testing.T) {
	t.Parallel()
	got, err := decodeStreamLine([]byte(`{"type":"future","data":1}`))
	if err != nil || got != nil {
		t.Fatalf("unknown line = (%#v, %v), want (nil, nil)", got, err)
	}
	got, err = decodeStreamLine([]byte(`{"type":`))
	if got != nil {
		t.Fatalf("malformed line events = %#v, want nil", got)
	}
	var decodeErr *driver.DecodeError
	if !errors.As(err, &decodeErr) {
		t.Fatalf("malformed line error = %T %v, want *driver.DecodeError", err, err)
	}
}
