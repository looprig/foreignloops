package claude

import (
	"bufio"
	"bytes"
	"encoding/json"
	"log"
	"os"
	"path/filepath"

	"github.com/looprig/core/content"
	"github.com/looprig/foreignloops/driver"
)

type transcriptRecord struct {
	Type        string          `json:"type"`
	IsSidechain bool            `json:"isSidechain"`
	Message     json.RawMessage `json:"message"`
}

type transcriptMessage struct {
	Content json.RawMessage `json:"content"`
}

// decodeTranscript reads a complete Claude transcript and groups its messages
// at assistant-record boundaries.
func decodeTranscript(path string) ([]content.AgenticMessages, error) {
	clean := filepath.Clean(path)
	file, err := os.Open(clean) // #nosec G304 -- the caller supplies Claude's deterministic transcript path.
	if err != nil {
		return nil, &driver.HistoryError{Cause: err}
	}
	defer func() { _ = file.Close() }()
	return foldTranscript(file), nil
}

func foldTranscript(file *os.File) []content.AgenticMessages {
	var out []content.AgenticMessages
	var current content.AgenticMessages
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), maxLineBytes)
	for scanner.Scan() {
		messages, boundary, err := decodeTranscriptLine(scanner.Bytes())
		if err != nil {
			log.Printf("foreignloop: transcript line skipped: %v", err)
			continue
		}
		if boundary && len(current) > 0 {
			out = append(out, current)
			current = nil
		}
		current = append(current, messages...)
	}
	if err := scanner.Err(); err != nil {
		log.Printf("foreignloop: transcript scan stopped: %v", err)
	}
	if len(current) > 0 {
		out = append(out, current)
	}
	return out
}

func decodeTranscriptLine(line []byte) ([]content.Conversation, bool, error) {
	line = bytes.TrimSpace(line)
	if len(line) == 0 {
		return nil, false, nil
	}
	var record transcriptRecord
	if err := json.Unmarshal(line, &record); err != nil {
		return nil, false, &driver.DecodeError{Cause: err}
	}
	if record.IsSidechain {
		return nil, false, nil
	}
	switch record.Type {
	case typeAssistant:
		messages, err := decodeTranscriptAssistant(record.Message)
		return messages, true, err
	case typeUser:
		messages, err := decodeTranscriptUser(record.Message)
		return messages, false, err
	default:
		return nil, false, nil
	}
}

func decodeTranscriptAssistant(raw json.RawMessage) ([]content.Conversation, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var message transcriptMessage
	if err := json.Unmarshal(raw, &message); err != nil {
		return nil, &driver.DecodeError{Cause: err}
	}
	var blocks []streamBlock
	if len(message.Content) > 0 {
		if err := json.Unmarshal(message.Content, &blocks); err != nil {
			return nil, &driver.DecodeError{Cause: err}
		}
	}
	ai := &content.AIMessage{Message: content.Message{
		Role:   content.RoleAssistant,
		Blocks: assistantBlocks(blocks),
	}}
	return []content.Conversation{ai}, nil
}

func decodeTranscriptUser(raw json.RawMessage) ([]content.Conversation, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var message transcriptMessage
	if err := json.Unmarshal(raw, &message); err != nil {
		return nil, &driver.DecodeError{Cause: err}
	}
	var text string
	if err := json.Unmarshal(message.Content, &text); err == nil {
		user := &content.UserMessage{Message: content.Message{
			Role:   content.RoleUser,
			Blocks: []content.Block{&content.TextBlock{Text: text}},
		}}
		return []content.Conversation{user}, nil
	}
	var blocks []streamBlock
	if err := json.Unmarshal(message.Content, &blocks); err != nil {
		return nil, &driver.DecodeError{Cause: err}
	}
	return toolResultMessages(blocks), nil
}

func toolResultMessages(blocks []streamBlock) []content.Conversation {
	var out []content.Conversation
	for _, block := range blocks {
		if block.Type != blockToolResult {
			continue
		}
		out = append(out, &content.ToolResultMessage{
			Message: content.Message{
				Role:   content.RoleTool,
				Blocks: []content.Block{&content.TextBlock{Text: renderToolResultText(block.Content)}},
			},
			ToolUseID: block.ToolUseID,
			IsError:   block.IsError,
		})
	}
	return out
}
