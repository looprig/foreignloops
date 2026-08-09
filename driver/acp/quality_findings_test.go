package acp

import (
	"context"
	"testing"

	"github.com/looprig/acp/client"
	"github.com/looprig/acp/protocol"
	"github.com/looprig/core/content"
	"github.com/looprig/foreignloops/driver"
)

func TestACPOrderedObservationsCarryFinalAssembledAssistantOnce(t *testing.T) {
	sess := &steeringSession{scriptedSession: newScriptedSession("ordered-final")}
	release := make(chan struct{})
	sess.promptHook = func(int, []protocol.ContentBlock) (*client.PromptResult, error) {
		<-release
		return &client.PromptResult{
			StopReason:       protocol.StopReasonEndTurn,
			WriteAdmitted:    true,
			ReceiveSequence:  2,
			ResponseSequence: 2,
		}, nil
	}
	d := newTurnTestDriver(sess.scriptedSession)
	d.session, d.steeringOn = sess, true
	stream, err := d.Spawn(context.Background(), driver.Turn{})
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}
	<-sess.promptStarts
	sess.updates <- client.Update{
		ReceiveSequence: 1,
		SessionUpdate: protocol.SessionUpdate{AgentMessageChunk: &protocol.ContentChunk{
			Content: protocol.ContentBlock{Text: &protocol.TextContent{Text: "assembled answer"}},
		}},
	}
	close(release)

	var observations []driver.Observation
	for observation := range stream.(driver.OrderedStream).Observations() {
		observations = append(observations, observation)
	}

	var finalCount int
	var final *content.AIMessage
	for _, observation := range observations {
		prompt, ok := observation.(driver.PromptObservation)
		if !ok || prompt.Message == nil {
			continue
		}
		finalCount++
		final = prompt.Message
	}
	if finalCount != 1 {
		t.Fatalf("ordered final observations = %d, want exactly one; observations = %#v", finalCount, observations)
	}
	if final == nil || len(final.Blocks) != 1 {
		t.Fatalf("ordered final assistant = %#v, want one assembled block", final)
	}
	text, ok := final.Blocks[0].(*content.TextBlock)
	if !ok || text.Text != "assembled answer" {
		t.Fatalf("ordered final block = %#v, want assembled answer", final.Blocks[0])
	}
}

func TestACPLegacyEventsCarryFinalAssembledAssistantOnce(t *testing.T) {
	sess := &steeringSession{scriptedSession: newScriptedSession("legacy-final")}
	release := make(chan struct{})
	sess.promptHook = func(int, []protocol.ContentBlock) (*client.PromptResult, error) {
		<-release
		return &client.PromptResult{StopReason: protocol.StopReasonEndTurn}, nil
	}
	d := newTurnTestDriver(sess.scriptedSession)
	d.session = sess
	stream, err := d.Spawn(context.Background(), driver.Turn{})
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}
	<-sess.promptStarts
	sess.updates <- client.Update{SessionUpdate: protocol.SessionUpdate{AgentMessageChunk: &protocol.ContentChunk{
		Content: protocol.ContentBlock{Text: &protocol.TextContent{Text: "assembled answer"}},
	}}}
	close(release)

	var events []driver.Event
	for event := range stream.Events() {
		events = append(events, event)
	}
	var finalCount int
	for _, event := range events {
		if event.Kind == driver.KindStepComplete {
			finalCount++
		}
	}
	if finalCount != 1 {
		t.Fatalf("legacy final events = %d, want exactly one; events = %#v", finalCount, events)
	}
}
