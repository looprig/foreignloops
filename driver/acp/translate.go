package acp

import (
	"encoding/json"
	"log/slog"
	"strings"

	"github.com/looprig/acp/protocol"
	"github.com/looprig/core/content"
	"github.com/looprig/foreignloops/driver"
)

const maxACPPreviewRunes = 512

type translationState struct {
	blocks []content.Block
	tools  map[string]string
}

func (s *translationState) message() *content.AIMessage {
	if s == nil || len(s.blocks) == 0 {
		return nil
	}
	return &content.AIMessage{Message: content.Message{
		Role:   content.RoleAssistant,
		Blocks: append([]content.Block(nil), s.blocks...),
	}}
}

func translateUpdate(update protocol.SessionUpdate, state *translationState) []driver.Event {
	if state == nil {
		state = &translationState{}
	}
	switch {
	case update.AgentMessageChunk != nil:
		return translateMessageChunk(update.AgentMessageChunk, state, false)
	case update.AgentThoughtChunk != nil:
		return translateMessageChunk(update.AgentThoughtChunk, state, true)
	case update.ToolCall != nil:
		return translateToolCall(update.ToolCall, state)
	case update.ToolCallUpdate != nil:
		return translateToolCallUpdate(update.ToolCallUpdate, state)
	case update.UserMessageChunk != nil:
		slog.Debug("acp: ignored user message update")
	case update.Plan != nil:
		slog.Debug("acp: ignored plan update")
	case update.AvailableCommandsUpdate != nil:
		slog.Debug("acp: ignored available commands update")
	case update.CurrentModeUpdate != nil:
		slog.Debug("acp: ignored current mode update")
	case update.ConfigOptionUpdate != nil:
		slog.Debug("acp: ignored config option update")
	case update.SessionInfoUpdate != nil:
		slog.Debug("acp: ignored session info update")
	case update.UsageUpdate != nil:
		slog.Debug("acp: ignored usage update")
	default:
		// A zero value can represent a future update variant to an adapter
		// built against an older schema. Skipping it keeps the stream live.
		slog.Debug("acp: ignored unknown session update")
	}
	return nil
}

func translateMessageChunk(chunk *protocol.ContentChunk, state *translationState, thought bool) []driver.Event {
	if chunk == nil || chunk.Content.Text == nil {
		slog.Debug("acp: ignored non-text content chunk")
		return nil
	}
	text := chunk.Content.Text.Text
	if text == "" {
		return nil
	}
	if thought {
		state.blocks = append(state.blocks, &content.ThinkingBlock{Thinking: text})
		return []driver.Event{{Kind: driver.KindThinkingDelta, Text: text}}
	}
	state.blocks = append(state.blocks, &content.TextBlock{Text: text})
	return []driver.Event{{Kind: driver.KindTextDelta, Text: text}}
}

func translateToolCall(call *protocol.ToolCall, state *translationState) []driver.Event {
	if call == nil {
		return nil
	}
	id := string(call.ToolCallID)
	name := boundedToolName(call.Title, call.Kind)
	if state.tools == nil {
		state.tools = make(map[string]string)
	}
	state.tools[id] = name
	state.blocks = append(state.blocks, &content.ToolUseBlock{
		ID:    id,
		Name:  name,
		Input: append(json.RawMessage(nil), call.RawInput...),
	})
	return []driver.Event{{
		Kind:      driver.KindToolUse,
		ToolUseID: id,
		ToolName:  name,
	}}
}

func translateToolCallUpdate(update *protocol.ToolCallUpdate, state *translationState) []driver.Event {
	if update == nil {
		return nil
	}
	id := string(update.ToolCallID)
	if update.Title != nil && *update.Title != "" {
		if state.tools == nil {
			state.tools = make(map[string]string)
		}
		state.tools[id] = bounded(*update.Title)
	}
	return []driver.Event{{
		Kind:          driver.KindToolResult,
		ToolUseID:     id,
		IsError:       update.Status != nil && *update.Status == protocol.ToolCallStatusFailed,
		ResultPreview: bounded(renderToolResult(update.Content, update.RawOutput)),
	}}
}

func boundedToolName(title string, kind *protocol.ToolKind) string {
	if title == "" && kind != nil {
		title = string(*kind)
	}
	if title == "" {
		title = "tool"
	}
	return bounded(title)
}

func renderToolResult(parts []protocol.ToolCallContent, raw json.RawMessage) string {
	var out strings.Builder
	for _, part := range parts {
		switch {
		case part.Content != nil:
			appendContentText(&out, part.Content.Content)
		case part.Diff != nil:
			out.WriteString(part.Diff.NewText)
		case part.Terminal != nil:
			out.WriteString("terminal output available")
		}
	}
	if out.Len() == 0 && len(raw) > 0 {
		var text string
		if json.Unmarshal(raw, &text) == nil {
			return text
		}
		return string(raw)
	}
	return out.String()
}

func appendContentText(out *strings.Builder, block protocol.ContentBlock) {
	if block.Text != nil {
		out.WriteString(block.Text.Text)
		return
	}
	if block.Resource != nil && block.Resource.Resource.TextResourceContents != nil {
		out.WriteString(block.Resource.Resource.TextResourceContents.Text)
	}
}

func bounded(text string) string {
	runes := []rune(text)
	if len(runes) <= maxACPPreviewRunes {
		return text
	}
	return string(runes[:maxACPPreviewRunes])
}
