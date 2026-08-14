package backend

import (
	"context"
	"fmt"

	"github.com/looprig/core/content"
	"github.com/looprig/harness/pkg/event"
)

// snapshotReq and snapshotResult are the actor query handshake. A clone failure
// travels back on the same reply so the actor never dies of one unrecognized
// content value.
type snapshotReq struct {
	reply chan snapshotResult
}

type snapshotResult struct {
	msgs      content.AgenticMessages
	turnIndex event.TurnIndex
	err       error
}

// snapshotReply builds the actor's answer to one snapshot query. A copy failure
// travels as err rather than as a short transcript, so the requester fails
// closed and the actor goroutine survives.
func (l *Loop) snapshotReply() snapshotResult {
	msgs, err := cloneMessages(l.msgs)
	if err != nil {
		return snapshotResult{err: err}
	}
	return snapshotResult{msgs: msgs, turnIndex: l.turnIndex}
}

func cloneMessages(messages content.AgenticMessages) (content.AgenticMessages, error) {
	if messages == nil {
		return nil, nil
	}
	cloned := make(content.AgenticMessages, len(messages))
	for index, message := range messages {
		copied, err := cloneConversation(message)
		if err != nil {
			return nil, err
		}
		cloned[index] = copied
	}
	return cloned, nil
}

func cloneConversation(message content.Conversation) (content.Conversation, error) {
	if message == nil {
		return nil, nil
	}
	switch typed := message.(type) {
	case *content.UserMessage:
		if typed == nil {
			return (*content.UserMessage)(nil), nil
		}
		inner, err := cloneMessage(typed.Message)
		if err != nil {
			return nil, err
		}
		return &content.UserMessage{Message: inner}, nil
	case *content.AIMessage:
		if typed == nil {
			return (*content.AIMessage)(nil), nil
		}
		inner, err := cloneMessage(typed.Message)
		if err != nil {
			return nil, err
		}
		cloned := &content.AIMessage{Message: inner}
		if typed.Usage != nil {
			usage := *typed.Usage
			cloned.Usage = &usage
		}
		return cloned, nil
	case *content.SystemMessage:
		if typed == nil {
			return (*content.SystemMessage)(nil), nil
		}
		inner, err := cloneMessage(typed.Message)
		if err != nil {
			return nil, err
		}
		return &content.SystemMessage{Message: inner}, nil
	case *content.ToolResultMessage:
		if typed == nil {
			return (*content.ToolResultMessage)(nil), nil
		}
		inner, err := cloneMessage(typed.Message)
		if err != nil {
			return nil, err
		}
		return &content.ToolResultMessage{
			Message:   inner,
			ToolUseID: typed.ToolUseID,
			IsError:   typed.IsError,
		}, nil
	default:
		return nil, fmt.Errorf("%w: conversation %T", errUnsupportedContent, message)
	}
}

func cloneMessage(message content.Message) (content.Message, error) {
	blocks, err := cloneBlocks(message.Blocks)
	if err != nil {
		return content.Message{}, err
	}
	return content.Message{Role: message.Role, Blocks: blocks}, nil
}

func cloneBlocks(blocks []content.Block) ([]content.Block, error) {
	if blocks == nil {
		return nil, nil
	}
	cloned := make([]content.Block, len(blocks))
	for index, block := range blocks {
		copied, err := cloneBlock(block)
		if err != nil {
			return nil, err
		}
		cloned[index] = copied
	}
	return cloned, nil
}

// cloneBlock deep-copies one block by delegating to content.CloneBlock, which
// is maintained beside the sealed union it copies. The switch used to live
// here, one module away from the union, so an upstream release could add a
// variant while this file kept compiling and dropped it; the arm below could
// only report that drift after it had already happened.
//
// The error contract is unchanged. content.CloneBlock returns a nil interface
// only for a nil block — a typed-nil payload comes back as the same typed nil
// inside a non-nil interface — so nil is still the one condition to report,
// and it is still reported rather than panicking: this runs on the backend
// actor goroutine, where a panic takes the whole host process down for a single
// unrecognized block.
func cloneBlock(block content.Block) (content.Block, error) {
	if block == nil {
		return nil, nil
	}
	cloned := content.CloneBlock(block)
	if cloned == nil {
		return nil, fmt.Errorf("%w: block %T", errUnsupportedContent, block)
	}
	return cloned, nil
}

// Snapshot requests a consistent defensive view from the backend actor.
func (l *Loop) Snapshot(ctx context.Context) (content.AgenticMessages, event.TurnIndex, error) {
	reply := make(chan snapshotResult, 1)
	select {
	case l.snapshots <- snapshotReq{reply: reply}:
	case <-l.Done:
		return nil, 0, &SnapshotError{Reason: SnapshotLoopExited}
	case <-ctx.Done():
		return nil, 0, &SnapshotError{Reason: SnapshotContextDone, Cause: ctx.Err()}
	}
	select {
	case result := <-reply:
		if result.err != nil {
			return nil, 0, &SnapshotError{Reason: SnapshotUnsupportedContent, Cause: result.err}
		}
		return result.msgs, result.turnIndex, nil
	case <-l.Done:
		return nil, 0, &SnapshotError{Reason: SnapshotLoopExited}
	case <-ctx.Done():
		return nil, 0, &SnapshotError{Reason: SnapshotContextDone, Cause: ctx.Err()}
	}
}
