package backend

import (
	"context"

	"github.com/looprig/core/content"
	"github.com/looprig/harness/pkg/event"
)

// snapshotReq and snapshotResult are the actor query handshake.
type snapshotReq struct {
	reply chan snapshotResult
}

type snapshotResult struct {
	msgs      content.AgenticMessages
	turnIndex event.TurnIndex
}

func cloneMessages(messages content.AgenticMessages) content.AgenticMessages {
	if len(messages) == 0 {
		return nil
	}
	cloned := make(content.AgenticMessages, len(messages))
	copy(cloned, messages)
	return cloned
}

// Snapshot requests a consistent view from the backend actor. Task 13 starts
// that actor; until then the public builders continue to fail closed.
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
		return result.msgs, result.turnIndex, nil
	case <-l.Done:
		return nil, 0, &SnapshotError{Reason: SnapshotLoopExited}
	case <-ctx.Done():
		return nil, 0, &SnapshotError{Reason: SnapshotContextDone, Cause: ctx.Err()}
	}
}
