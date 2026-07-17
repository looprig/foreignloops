package claude

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/looprig/core/content"
	"github.com/looprig/foreignloop/driver"
)

type transcriptProjectionGolden struct {
	Groups           []content.AgenticMessages `json:"groups"`
	StepDoneMessages []content.AgenticMessages `json:"step_done_messages"`
	Snapshot         content.AgenticMessages   `json:"snapshot"`
}

func userMessage(text string) *content.UserMessage {
	return &content.UserMessage{Message: content.Message{
		Role:   content.RoleUser,
		Blocks: []content.Block{&content.TextBlock{Text: text}},
	}}
}

func assistantMessage(blocks ...content.Block) *content.AIMessage {
	return &content.AIMessage{Message: content.Message{
		Role:   content.RoleAssistant,
		Blocks: blocks,
	}}
}

func transcriptCases() map[string][]content.AgenticMessages {
	return map[string][]content.AgenticMessages{
		"happy": {
			{userMessage("hi there")},
			{
				assistantMessage(
					&content.ThinkingBlock{Thinking: "let me think", Signature: "sig"},
					&content.TextBlock{Text: "Working"},
					&content.ToolUseBlock{ID: "toolu_9", Name: "Read", Input: json.RawMessage(`{"path":"/x"}`)},
				),
				&content.ToolResultMessage{
					Message: content.Message{
						Role:   content.RoleTool,
						Blocks: []content.Block{&content.TextBlock{Text: "contents"}},
					},
					ToolUseID: "toolu_9",
				},
			},
			{assistantMessage(&content.TextBlock{Text: "Done"})},
		},
		"empty": nil,
		"truncated": {
			{userMessage("hi")},
			{assistantMessage(&content.TextBlock{Text: "recovered"})},
		},
	}
}

func flattenTranscriptGroups(groups []content.AgenticMessages) content.AgenticMessages {
	var out content.AgenticMessages
	for _, group := range groups {
		out = append(out, group...)
	}
	return out
}

func canonicalJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatalf("marshal canonical JSON: %v", err)
	}
	return append(data, '\n')
}

func readGolden(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "transcript", name+".golden.json"))
	if err != nil {
		t.Fatalf("read golden %q: %v", name, err)
	}
	return data
}

func TestDecodeTranscriptGoldenProjection(t *testing.T) {
	t.Parallel()
	for name, wantSteps := range transcriptCases() {
		name, wantSteps := name, wantSteps
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join("testdata", "transcript", name+".jsonl")
			gotSteps, err := decodeTranscript(path)
			if err != nil {
				t.Fatalf("decodeTranscript() error = %v", err)
			}
			if !reflect.DeepEqual(gotSteps, wantSteps) {
				t.Fatalf("decodeTranscript() = %#v, want %#v", gotSteps, wantSteps)
			}

			history, err := historyFromPath(path)
			if err != nil {
				t.Fatalf("historyFromPath() error = %v", err)
			}
			wantHistory := driver.History{Available: true, Steps: wantSteps}
			if !reflect.DeepEqual(history, wantHistory) {
				t.Errorf("historyFromPath() = %#v, want %#v", history, wantHistory)
			}

			gotJSON := canonicalJSON(t, transcriptProjectionGolden{
				Groups:           gotSteps,
				StepDoneMessages: history.Steps,
				Snapshot:         flattenTranscriptGroups(history.Steps),
			})
			if wantJSON := readGolden(t, name); !bytes.Equal(gotJSON, wantJSON) {
				t.Fatalf("canonical projection differs from shared %s.golden.json\ngot:\n%s\nwant:\n%s", name, gotJSON, wantJSON)
			}
		})
	}
}

func TestDecodeTranscriptMissingFileIsHistoryError(t *testing.T) {
	t.Parallel()
	path := filepath.Join("testdata", "transcript", "missing.jsonl")
	got, err := decodeTranscript(path)
	if got != nil {
		t.Fatalf("decodeTranscript() steps = %#v, want nil", got)
	}
	var historyErr *driver.HistoryError
	if !errors.As(err, &historyErr) {
		t.Fatalf("decodeTranscript() error = %T %v, want *driver.HistoryError", err, err)
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("errors.Is(error, os.ErrNotExist) = false: %v", err)
	}

	history, err := historyFromPath(path)
	if !reflect.DeepEqual(history, driver.History{}) {
		t.Errorf("historyFromPath() = %#v, want zero history", history)
	}
	if !errors.As(err, &historyErr) {
		t.Fatalf("historyFromPath() error = %T %v, want *driver.HistoryError", err, err)
	}
	if _, ok := reflect.TypeOf(driver.HistoryError{}).FieldByName("Path"); ok {
		t.Fatal("driver.HistoryError exposes a provider path field")
	}
}

func TestDecodeTranscriptLineSidechainAndMalformed(t *testing.T) {
	t.Parallel()
	got, boundary, err := decodeTranscriptLine([]byte(`{"type":"assistant","isSidechain":true,"message":{"content":[{"type":"text","text":"skip"}]}}`))
	if err != nil || boundary || got != nil {
		t.Fatalf("sidechain line = (%#v, %v, %v), want (nil, false, nil)", got, boundary, err)
	}
	got, boundary, err = decodeTranscriptLine([]byte(`{"type":"assistant","message":`))
	if got != nil || boundary {
		t.Fatalf("malformed line = (%#v, %v), want (nil, false)", got, boundary)
	}
	var decodeErr *driver.DecodeError
	if !errors.As(err, &decodeErr) {
		t.Fatalf("malformed line error = %T %v, want *driver.DecodeError", err, err)
	}
}
