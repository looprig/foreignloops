package claude

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"strings"

	"github.com/looprig/core/content"
	"github.com/looprig/foreignloops/driver"
)

const maxLineBytes = 1 << 20

const maxResultPreviewRunes = 512

const (
	typeSystem      = "system"
	typeStreamEvent = "stream_event"
	typeAssistant   = "assistant"
	typeUser        = "user"
	typeResult      = "result"

	subtypeInit        = "init"
	subtypeSuccess     = "success"
	subtypeErrorPrefix = "error"

	blockText       = "text"
	blockThinking   = "thinking"
	blockToolUse    = "tool_use"
	blockToolResult = "tool_result"

	eventContentBlockDelta = "content_block_delta"
	deltaText              = "text_delta"
	deltaThinking          = "thinking_delta"
)

type streamLine struct {
	Type      string          `json:"type"`
	Subtype   string          `json:"subtype"`
	SessionID string          `json:"session_id"`
	Message   json.RawMessage `json:"message"`
	Event     json.RawMessage `json:"event"`
	Result    string          `json:"result"`
}

type streamMessage struct {
	Role    string        `json:"role"`
	Content []streamBlock `json:"content"`
}

type streamBlock struct {
	Type      string          `json:"type"`
	Text      string          `json:"text"`
	Thinking  string          `json:"thinking"`
	Signature string          `json:"signature"`
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Input     json.RawMessage `json:"input"`
	ToolUseID string          `json:"tool_use_id"`
	Content   json.RawMessage `json:"content"`
	IsError   bool            `json:"is_error"`
}

type streamEvent struct {
	Type  string `json:"type"`
	Delta struct {
		Type     string `json:"type"`
		Text     string `json:"text"`
		Thinking string `json:"thinking"`
	} `json:"delta"`
}

func decodeStream(r io.Reader) (<-chan driver.Event, func() error) {
	ch := make(chan driver.Event)
	var firstErr error
	go func() {
		defer close(ch)
		scanner := bufio.NewScanner(r)
		scanner.Buffer(make([]byte, 0, 64*1024), maxLineBytes)
		for scanner.Scan() {
			events, err := decodeStreamLine(scanner.Bytes())
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			for _, event := range events {
				ch <- event
			}
		}
		if err := scanner.Err(); err != nil && firstErr == nil {
			firstErr = &driver.DecodeError{Cause: err}
		}
	}()
	return ch, func() error { return firstErr }
}

func decodeStreamLine(line []byte) ([]driver.Event, error) {
	line = bytes.TrimSpace(line)
	if len(line) == 0 {
		return nil, nil
	}
	var decoded streamLine
	if err := json.Unmarshal(line, &decoded); err != nil {
		return nil, &driver.DecodeError{Cause: err}
	}
	switch decoded.Type {
	case typeSystem:
		return decodeSystem(decoded), nil
	case typeStreamEvent:
		return decodeStreamEvent(decoded)
	case typeAssistant:
		return decodeAssistant(decoded)
	case typeUser:
		return decodeUser(decoded)
	case typeResult:
		return decodeResult(decoded), nil
	default:
		return nil, nil
	}
}

func decodeSystem(line streamLine) []driver.Event {
	if line.Subtype != subtypeInit {
		return nil
	}
	return []driver.Event{{Kind: driver.KindInit, SessionID: line.SessionID}}
}

func decodeStreamEvent(line streamLine) ([]driver.Event, error) {
	if len(line.Event) == 0 {
		return nil, nil
	}
	var event streamEvent
	if err := json.Unmarshal(line.Event, &event); err != nil {
		return nil, &driver.DecodeError{Cause: err}
	}
	if event.Type != eventContentBlockDelta {
		return nil, nil
	}
	switch event.Delta.Type {
	case deltaText:
		return []driver.Event{{Kind: driver.KindTextDelta, Text: event.Delta.Text}}, nil
	case deltaThinking:
		return []driver.Event{{Kind: driver.KindThinkingDelta, Text: event.Delta.Thinking}}, nil
	default:
		return nil, nil
	}
}

func decodeAssistant(line streamLine) ([]driver.Event, error) {
	message, err := decodeMessage(line.Message)
	if err != nil {
		return nil, err
	}
	events := toolUseEvents(message.Content)
	ai := &content.AIMessage{Message: content.Message{
		Role:   content.RoleAssistant,
		Blocks: assistantBlocks(message.Content),
	}}
	return append(events, driver.Event{Kind: driver.KindStepComplete, Message: ai}), nil
}

func assistantBlocks(blocks []streamBlock) []content.Block {
	var out []content.Block
	for _, block := range blocks {
		switch block.Type {
		case blockText:
			out = append(out, &content.TextBlock{Text: block.Text})
		case blockThinking:
			out = append(out, &content.ThinkingBlock{Thinking: block.Thinking, Signature: block.Signature})
		case blockToolUse:
			out = append(out, &content.ToolUseBlock{ID: block.ID, Name: block.Name, Input: block.Input})
		}
	}
	return out
}

func toolUseEvents(blocks []streamBlock) []driver.Event {
	var out []driver.Event
	for _, block := range blocks {
		if block.Type == blockToolUse {
			out = append(out, driver.Event{
				Kind:      driver.KindToolUse,
				ToolUseID: block.ID,
				ToolName:  block.Name,
			})
		}
	}
	return out
}

func decodeUser(line streamLine) ([]driver.Event, error) {
	message, err := decodeMessage(line.Message)
	if err != nil {
		return nil, err
	}
	var out []driver.Event
	for _, block := range message.Content {
		if block.Type != blockToolResult {
			continue
		}
		out = append(out, driver.Event{
			Kind:          driver.KindToolResult,
			ToolUseID:     block.ToolUseID,
			IsError:       block.IsError,
			ResultPreview: renderToolResultPreview(block.Content),
		})
	}
	return out, nil
}

func decodeResult(line streamLine) []driver.Event {
	switch {
	case line.Subtype == subtypeSuccess:
		return []driver.Event{{Kind: driver.KindTerminalOK, Message: resultMessage(line.Result)}}
	case strings.HasPrefix(line.Subtype, subtypeErrorPrefix):
		text := line.Result
		if text == "" {
			text = line.Subtype
		}
		return []driver.Event{{Kind: driver.KindTerminalError, ErrText: text}}
	default:
		return nil
	}
}

func decodeMessage(raw json.RawMessage) (streamMessage, error) {
	var message streamMessage
	if len(raw) == 0 {
		return message, nil
	}
	if err := json.Unmarshal(raw, &message); err != nil {
		return message, &driver.DecodeError{Cause: err}
	}
	return message, nil
}

func resultMessage(text string) *content.AIMessage {
	if text == "" {
		return nil
	}
	return &content.AIMessage{Message: content.Message{
		Role:   content.RoleAssistant,
		Blocks: []content.Block{&content.TextBlock{Text: text}},
	}}
}

func renderToolResultPreview(raw json.RawMessage) string {
	return capPreview(renderToolResultText(raw))
}

func renderToolResultText(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return text
	}
	var parts []struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(raw, &parts); err == nil {
		var builder strings.Builder
		for _, part := range parts {
			builder.WriteString(part.Text)
		}
		return builder.String()
	}
	return string(raw)
}

func capPreview(text string) string {
	runes := []rune(text)
	if len(runes) <= maxResultPreviewRunes {
		return text
	}
	return string(runes[:maxResultPreviewRunes])
}
