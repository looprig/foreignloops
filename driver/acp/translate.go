package acp

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/url"
	"regexp"
	"strings"

	"github.com/looprig/acp/protocol"
	"github.com/looprig/core/content"
	"github.com/looprig/foreignloops/driver"
)

const maxACPPreviewRunes = 512

const (
	redactedToolValue    = "[REDACTED]"
	unsafeToolOutput     = "[UNAVAILABLE]"
	redactedURL          = "[REDACTED_URL]"
	maxToolJSONDepth     = 64
	maxToolJSONValueSize = 1 << 20
)

var (
	toolURLPattern    = regexp.MustCompile(`(?i)\bhttps?://[^\s<>"']+`)
	toolAuthPattern   = regexp.MustCompile(`(?i)(\b(?:authorization|proxy-authorization)\b(?:\s*["']?\s*[:=]|\s+)\s*)([^\r\n,;&}\]]+)`)
	toolSecretPattern = regexp.MustCompile(`(?i)(\b(?:api[\s_-]*key|access[\s_-]*token|refresh[\s_-]*token|secret[\s_-]*key|token|password|secret)\b\s*["']?\s*[:=]\s*)(?:"[^"]*"|'[^']*'|[^\s,;&}\]]+)`)
)

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
		ID:   id,
		Name: name,
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
	if out.Len() > 0 {
		return bounded(sanitizeToolText(out.String()))
	}
	return bounded(sanitizeToolOutput(raw))
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

func sanitizeToolOutput(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	if len(raw) > maxToolJSONValueSize {
		return unsafeToolOutput
	}
	trimmed := bytes.TrimSpace(raw)
	if value, ok := decodeStrictJSON(trimmed); ok {
		return renderSanitizedJSON(value)
	}
	if looksLikeJSON(trimmed) {
		return unsafeToolOutput
	}
	return sanitizePlainToolText(string(raw))
}

func sanitizeToolText(text string) string {
	trimmed := bytes.TrimSpace([]byte(text))
	if value, ok := decodeStrictJSON(trimmed); ok {
		return renderSanitizedJSON(value)
	}
	if looksLikeJSON(trimmed) {
		return unsafeToolOutput
	}
	return sanitizePlainToolText(text)
}

func renderSanitizedJSON(value any) string {
	safe := sanitizeJSONValue(value, 0)
	if text, ok := safe.(string); ok {
		return text
	}
	encoded, err := json.Marshal(safe)
	if err != nil {
		return unsafeToolOutput
	}
	return string(encoded)
}

func decodeStrictJSON(raw []byte) (any, bool) {
	if len(raw) == 0 {
		return nil, false
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	value, err := decodeJSONValue(decoder, 0)
	if err != nil {
		return nil, false
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, false
	}
	return value, true
}

func decodeJSONValue(decoder *json.Decoder, depth int) (any, error) {
	if depth > maxToolJSONDepth {
		return nil, io.ErrUnexpectedEOF
	}
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	switch value := token.(type) {
	case json.Delim:
		switch value {
		case '{':
			object := make(map[string]any)
			seen := make(map[string]struct{})
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return nil, err
				}
				key, ok := keyToken.(string)
				if !ok {
					return nil, errors.New("JSON object key is not a string")
				}
				if _, ok := seen[key]; ok {
					return nil, errors.New("duplicate JSON object key")
				}
				seen[key] = struct{}{}
				item, err := decodeJSONValue(decoder, depth+1)
				if err != nil {
					return nil, err
				}
				object[key] = item
			}
			if end, err := decoder.Token(); err != nil || end != json.Delim('}') {
				return nil, errors.New("unterminated JSON object")
			}
			return object, nil
		case '[':
			var array []any
			for decoder.More() {
				item, err := decodeJSONValue(decoder, depth+1)
				if err != nil {
					return nil, err
				}
				array = append(array, item)
			}
			if end, err := decoder.Token(); err != nil || end != json.Delim(']') {
				return nil, errors.New("unterminated JSON array")
			}
			return array, nil
		default:
			return nil, errors.New("unexpected JSON delimiter")
		}
	default:
		return value, nil
	}
}

func sanitizeJSONValue(value any, depth int) any {
	if depth > maxToolJSONDepth {
		return unsafeToolOutput
	}
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			if isSecretField(key) {
				out[key] = redactedToolValue
				continue
			}
			out[key] = sanitizeJSONValue(item, depth+1)
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for i, item := range typed {
			out[i] = sanitizeJSONValue(item, depth+1)
		}
		return out
	case string:
		return sanitizePlainToolText(typed)
	default:
		return typed
	}
}

func looksLikeJSON(raw []byte) bool {
	if len(raw) == 0 {
		return false
	}
	switch raw[0] {
	case '{', '[', '"':
		return true
	default:
		return false
	}
}

func sanitizePlainToolText(text string) string {
	text = toolURLPattern.ReplaceAllStringFunc(text, sanitizeURLToken)
	text = toolAuthPattern.ReplaceAllString(text, "$1"+redactedToolValue)
	return toolSecretPattern.ReplaceAllString(text, "$1"+redactedToolValue)
}

func sanitizeURLToken(token string) string {
	trimmed := strings.TrimRight(token, ".,;:!?)]}")
	suffix := token[len(trimmed):]
	parsed, err := url.Parse(trimmed)
	if err != nil || parsed.Host == "" {
		return redactedURL + suffix
	}
	parsed.User = nil
	if parsed.RawQuery != "" {
		query, err := url.ParseQuery(parsed.RawQuery)
		if err != nil {
			parsed.RawQuery = ""
		} else {
			for key := range query {
				if isSecretField(key) {
					query[key] = []string{redactedToolValue}
				}
			}
			parsed.RawQuery = query.Encode()
		}
	}
	if parsed.Fragment != "" {
		parsed.Fragment = sanitizePlainToolText(parsed.Fragment)
	}
	return parsed.String() + suffix
}

func isSecretField(key string) bool {
	var normalized strings.Builder
	for _, r := range strings.ToLower(key) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			normalized.WriteRune(r)
		}
	}
	field := normalized.String()
	for _, marker := range []string{"token", "password", "secret", "apikey", "authorization"} {
		if strings.Contains(field, marker) {
			return true
		}
	}
	return false
}
