package acp

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/looprig/acp/client"
	"github.com/looprig/acp/launch"
	"github.com/looprig/acp/protocol"
	"github.com/looprig/core/content"
	"github.com/looprig/foreignloops/driver"
)

func TestSteeringCapabilityRequiresAdvertisedSafeIdleFallback(t *testing.T) {
	tests := []struct {
		name     string
		harness  Harness
		nameInfo string
		version  string
		meta     string
		want     bool
	}{
		{
			name:     "missing metadata",
			harness:  HarnessClaudeCode,
			nameInfo: "claude-agent-acp",
			version:  "0.65.0",
			want:     false,
		},
		{
			name:     "malformed metadata",
			harness:  HarnessClaudeCode,
			nameInfo: "claude-agent-acp",
			version:  "0.65.0",
			meta:     `{"steering":{"supported":"true"}}`,
			want:     false,
		},
		{
			name:     "future safe advertisement",
			harness:  HarnessClaudeCode,
			nameInfo: "unknown-claude",
			version:  "9.0.0",
			meta:     `{"steering":{"supported":true,"idleBehaviors":["promptRequired"]}}`,
			want:     true,
		},
		{
			name:     "exact Claude exception",
			harness:  HarnessClaudeCode,
			nameInfo: "@agentclientprotocol/claude-agent-acp",
			version:  "0.65.0",
			meta:     `{"steering":{"supported":true}}`,
			want:     true,
		},
		{
			name:     "unknown Claude version",
			harness:  HarnessClaudeCode,
			nameInfo: "@agentclientprotocol/claude-agent-acp",
			version:  "0.65.1",
			meta:     `{"steering":{"supported":true}}`,
			want:     false,
		},
		{
			name:     "current Codex",
			harness:  HarnessCodex,
			nameInfo: "@agentclientprotocol/codex-acp",
			version:  "1.1.9",
			meta:     `{"steering":{"supported":true,"idleBehaviors":["promptRequired"]}}`,
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var meta json.RawMessage
			if tt.meta != "" {
				meta = json.RawMessage(tt.meta)
			}
			got := steeringCapability(tt.harness, client.InitializeMetadata{
				AgentInfo: &protocol.Implementation{Name: tt.nameInfo, Version: tt.version},
				Meta:      meta,
			})
			if got != tt.want {
				t.Fatalf("steeringCapability() = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestACPConfigMcpServersAreCopiedThroughNewAndLoad(t *testing.T) {
	servers := []protocol.McpServer{{
		Stdio: &protocol.McpServerStdio{
			Args:    []string{"--endpoint", "original"},
			Command: "/opt/collab-mcp",
			Env:     []protocol.EnvVariable{{Name: "TOKEN", Value: "secret"}},
			Name:    "collab",
			Meta:    json.RawMessage(`{"scope":"loop"}`),
		},
	}}
	want := []protocol.McpServer{{
		Stdio: &protocol.McpServerStdio{
			Args:    []string{"--endpoint", "original"},
			Command: "/opt/collab-mcp",
			Env:     []protocol.EnvVariable{{Name: "TOKEN", Value: "secret"}},
			Name:    "collab",
			Meta:    json.RawMessage(`{"scope":"loop"}`),
		},
	}}

	newConn := &fakeClient{newSession: newFakeSession("new-mcp")}
	newOwned := &fakeDialedClient{acpClient: newConn}
	installDial(t, func(context.Context, launch.Config) (dialedClient, error) {
		return newOwned, nil
	})
	cfg := validConfig(HarnessCodex)
	cfg.AgentSessionID = ""
	cfg.McpServers = servers
	d, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer d.Close()
	servers[0].Stdio.Args[1] = "mutated"
	servers[0].Stdio.Env[0].Value = "mutated"
	servers[0].Stdio.Meta[2] = 'X'
	if got := newConn.newParams[0].McpServers; !reflect.DeepEqual(got, want) {
		t.Fatalf("session/new McpServers = %#v, want defensive snapshot %#v", got, want)
	}

	loadConn := &fakeClient{loadSession: newFakeSession("restore-mcp")}
	loadOwned := &fakeDialedClient{acpClient: loadConn}
	installDial(t, func(context.Context, launch.Config) (dialedClient, error) {
		return loadOwned, nil
	})
	cfg.AgentSessionID = "restore-mcp"
	servers[0].Stdio.Args[1] = "original"
	servers[0].Stdio.Env[0].Value = "secret"
	servers[0].Stdio.Meta[2] = 's'
	cfg.McpServers = servers
	loaded, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("New(load) error = %v", err)
	}
	defer loaded.Close()
	servers[0].Stdio.Args[1] = "mutated-again"
	if got := loadConn.loadParams[0].McpServers; !reflect.DeepEqual(got, want) {
		t.Fatalf("session/load McpServers = %#v, want defensive snapshot %#v", got, want)
	}
}

func TestACPSteeringClassifiesEveryTypedOutcomeAndError(t *testing.T) {
	tests := []struct {
		name      string
		result    client.SteerResult
		err       error
		want      driver.SteerOutcome
		wantError bool
	}{
		{
			name:   "injected",
			result: client.SteerResult{Outcome: client.SteerOutcomeInjected, WriteAdmitted: true, ReceiveSequence: 4, ResponseSequence: 4},
			want:   driver.SteerOutcomeInjected,
		},
		{
			name:   "prompt required",
			result: client.SteerResult{Outcome: client.SteerOutcomePromptRequired, WriteAdmitted: true, ReceiveSequence: 5, ResponseSequence: 5},
			want:   driver.SteerOutcomeFallbackRequired,
		},
		{
			name:   "failed guarantees no delivery",
			result: client.SteerResult{Outcome: client.SteerOutcomeFailed, WriteAdmitted: true, ReceiveSequence: 6, ResponseSequence: 6},
			want:   driver.SteerOutcomeFallbackRequired,
		},
		{
			name:   "started new turn",
			result: client.SteerResult{Outcome: client.SteerOutcomeStartedNewTurn, WriteAdmitted: true, ReceiveSequence: 7, ResponseSequence: 7},
			want:   driver.SteerOutcomeDeliveredUntrackable,
		},
		{
			name:      "unknown response",
			result:    client.SteerResult{Outcome: "future", WriteAdmitted: true, ReceiveSequence: 8, ResponseSequence: 8},
			want:      driver.SteerOutcomeDeliveryUnknown,
			wantError: true,
		},
		{
			name:      "pre writer error",
			result:    client.SteerResult{WriteAdmitted: false},
			err:       errors.New("writer rejected before admission"),
			want:      driver.SteerOutcomeFallbackRequired,
			wantError: true,
		},
		{
			name:      "invalid params after writer",
			result:    client.SteerResult{WriteAdmitted: true, ReceiveSequence: 9, ResponseSequence: 9},
			err:       &client.SteeringError{Code: protocol.ErrorCodeInvalidParams, Message: "rejected"},
			want:      driver.SteerOutcomeFallbackRequired,
			wantError: true,
		},
		{
			name:      "generic error after writer",
			result:    client.SteerResult{WriteAdmitted: true},
			err:       errors.New("transport ended after write"),
			want:      driver.SteerOutcomeDeliveryUnknown,
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := normalizeSteering(tt.result, tt.err)
			if got.Outcome != tt.want {
				t.Fatalf("normalizeSteering() outcome = %q, want %q", got.Outcome, tt.want)
			}
			if (err != nil) != tt.wantError {
				t.Fatalf("normalizeSteering() error = %v, want error = %t", err, tt.wantError)
			}
		})
	}
}

type steeringSession struct {
	*scriptedSession
	steerResult client.SteerResult
	steerErr    error
	steerParams []client.SteerParams
	barriers    []uint64
}

func (s *steeringSession) Steer(_ context.Context, params client.SteerParams) (client.SteerResult, error) {
	s.steerParams = append(s.steerParams, params)
	return s.steerResult, s.steerErr
}

func (s *steeringSession) WaitForUpdatesThrough(_ context.Context, sequence uint64) error {
	s.barriers = append(s.barriers, sequence)
	return nil
}

func TestACPSteerSendsExactTypedRequestAndOrderedObservation(t *testing.T) {
	sess := &steeringSession{
		scriptedSession: newScriptedSession("steer-session"),
		steerResult: client.SteerResult{
			Outcome:          client.SteerOutcomeInjected,
			WriteAdmitted:    true,
			ReceiveSequence:  2,
			ResponseSequence: 2,
		},
	}
	release := make(chan struct{})
	sess.promptHook = func(int, []protocol.ContentBlock) (*client.PromptResult, error) {
		<-release
		return &client.PromptResult{StopReason: protocol.StopReasonEndTurn, ReceiveSequence: 3, ResponseSequence: 3, WriteAdmitted: true}, nil
	}
	d := newTurnTestDriver(sess.scriptedSession)
	d.session = sess
	d.steeringOn = true
	stream, err := d.Spawn(context.Background(), driver.Turn{})
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}
	<-sess.promptStarts

	request, err := driver.NewSteerRequest([]content.Block{&content.TextBlock{Text: "steer now"}})
	if err != nil {
		t.Fatalf("NewSteerRequest() error = %v", err)
	}
	got, err := d.Steer(context.Background(), request)
	if err != nil {
		t.Fatalf("Steer() error = %v", err)
	}
	if got.Outcome != driver.SteerOutcomeInjected {
		t.Fatalf("Steer() outcome = %q, want injected", got.Outcome)
	}
	if len(sess.steerParams) != 1 {
		t.Fatalf("Steer() calls = %d, want 1", len(sess.steerParams))
	}
	params := sess.steerParams[0]
	if params.SessionID != sess.ID() || string(params.Meta) != `{"steering":{"idleBehavior":"promptRequired"}}` {
		t.Fatalf("Steer() params = %#v, want exact session and metadata", params)
	}
	if len(params.Prompt) != 1 || params.Prompt[0].Text == nil || params.Prompt[0].Text.Text != "steer now" {
		t.Fatalf("Steer() prompt = %#v, want one text block", params.Prompt)
	}

	ordered, ok := stream.(driver.OrderedStream)
	if !ok {
		t.Fatal("stream does not implement OrderedStream")
	}
	select {
	case observation := <-ordered.Observations():
		steer, ok := observation.(driver.SteerObservation)
		if !ok || steer.Outcome != driver.SteerOutcomeInjected || steer.Sequence() != 2 {
			t.Fatalf("first ordered observation = %#v, want injected steer at sequence 2", observation)
		}
	default:
		t.Fatal("Steer() returned before ordered steer observation was admitted")
	}
	close(release)
	_ = collectTurnEvents(t, stream)
}

func TestACPOrderedObservationsUseReceiveSequenceAndBarrier(t *testing.T) {
	sess := &steeringSession{
		scriptedSession: newScriptedSession("ordered-session"),
		steerResult: client.SteerResult{
			Outcome:          client.SteerOutcomeInjected,
			WriteAdmitted:    true,
			ReceiveSequence:  2,
			ResponseSequence: 2,
		},
	}
	release := make(chan struct{})
	sess.promptHook = func(int, []protocol.ContentBlock) (*client.PromptResult, error) {
		<-release
		return &client.PromptResult{
			StopReason:       protocol.StopReasonEndTurn,
			WriteAdmitted:    true,
			ReceiveSequence:  3,
			ResponseSequence: 3,
		}, nil
	}
	d := newTurnTestDriver(sess.scriptedSession)
	d.session = sess
	d.steeringOn = true
	stream, err := d.Spawn(context.Background(), driver.Turn{})
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}
	<-sess.promptStarts
	sess.updates <- client.Update{
		ReceiveSequence: 1,
		SessionUpdate: protocol.SessionUpdate{AgentMessageChunk: &protocol.ContentChunk{Content: protocol.ContentBlock{
			Text: &protocol.TextContent{Text: "before steer"},
		}}},
	}
	request, err := driver.NewSteerRequest([]content.Block{&content.TextBlock{Text: "steer"}})
	if err != nil {
		t.Fatalf("NewSteerRequest() error = %v", err)
	}
	if _, err := d.Steer(context.Background(), request); err != nil {
		t.Fatalf("Steer() error = %v", err)
	}
	close(release)
	_ = collectTurnEvents(t, stream)

	ordered := stream.(driver.OrderedStream).Observations()
	var got []driver.Observation
	for observation := range ordered {
		got = append(got, observation)
	}
	if len(got) != 3 {
		t.Fatalf("ordered observations = %#v, want update, steer, prompt", got)
	}
	for i, want := range []uint64{1, 2, 3} {
		if got[i].Sequence() != want {
			t.Fatalf("observation[%d] sequence = %d, want %d (%#v)", i, got[i].Sequence(), want, got[i])
		}
	}
	if len(sess.barriers) == 0 || sess.barriers[0] != 2 {
		t.Fatalf("WaitForUpdatesThrough calls = %v, want first barrier through steer sequence 2", sess.barriers)
	}
}

func TestACPSteeringDisablesAfterKnownMethodRejection(t *testing.T) {
	sess := &steeringSession{
		scriptedSession: newScriptedSession("disable-session"),
		steerResult: client.SteerResult{
			WriteAdmitted:    true,
			ReceiveSequence:  2,
			ResponseSequence: 2,
		},
		steerErr: &client.SteeringError{Code: protocol.ErrorCodeMethodNotFound, Message: "not found"},
	}
	release := make(chan struct{})
	sess.promptHook = func(int, []protocol.ContentBlock) (*client.PromptResult, error) {
		<-release
		return &client.PromptResult{StopReason: protocol.StopReasonEndTurn}, nil
	}
	d := newTurnTestDriver(sess.scriptedSession)
	d.session = sess
	d.steeringOn = true
	stream, err := d.Spawn(context.Background(), driver.Turn{})
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}
	<-sess.promptStarts
	request, err := driver.NewSteerRequest([]content.Block{&content.TextBlock{Text: "once"}})
	if err != nil {
		t.Fatalf("NewSteerRequest() error = %v", err)
	}
	result, err := d.Steer(context.Background(), request)
	if err == nil || result.Outcome != driver.SteerOutcomeFallbackRequired {
		t.Fatalf("first Steer() = %#v, %v, want fallback with typed error", result, err)
	}
	sess.steerErr = nil
	sess.steerResult = client.SteerResult{Outcome: client.SteerOutcomeInjected, WriteAdmitted: true, ReceiveSequence: 3, ResponseSequence: 3}
	result, err = d.Steer(context.Background(), request)
	if err != nil || result.Outcome != driver.SteerOutcomeUnsupported {
		t.Fatalf("second Steer() = %#v, %v, want unsupported without probing", result, err)
	}
	if len(sess.steerParams) != 1 {
		t.Fatalf("ACP Steer calls = %d, want one before steering was disabled", len(sess.steerParams))
	}
	close(release)
	_ = collectTurnEvents(t, stream)
}
