//go:build (darwin || linux) && !android

package acpbackend_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/looprig/acp/protocol"
	"github.com/looprig/core/content"
	"github.com/looprig/core/uuid"
	"github.com/looprig/foreignloops/backend"
	"github.com/looprig/foreignloops/driver"
	foreignacp "github.com/looprig/foreignloops/driver/acp"
	"github.com/looprig/harness/pkg/command"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/foreign"
	"github.com/looprig/harness/pkg/loop"
	"github.com/looprig/harness/pkg/tool"
	"github.com/looprig/inference"
	"github.com/looprig/inference/model"
	inferencestream "github.com/looprig/inference/stream"
)

const helperEnv = "LOOPRIG_FOREIGNLOOPS_ACP_EXAMPLE_HELPER"

func TestMain(m *testing.M) {
	if os.Getenv(helperEnv) == "1" {
		os.Exit(serveScriptedACP())
	}
	os.Exit(m.Run())
}

func TestExampleACPBackendLifecycle(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("locate test executable: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	workspace := t.TempDir()
	ids := sequentialIDs()
	definition := boundLoop(t)
	cfg := foreignacp.Config{
		Harness:       foreignacp.HarnessCodex,
		Executable:    executable,
		Env:           []string{helperEnv + "=1"},
		Credential:    loop.CredentialNativeAuth,
		Posture:       driver.PostureReadOnly,
		WorkspaceRoot: workspace,
	}

	publisher := &recordingPublisher{}
	built, agentSessionID, err := foreignacp.BuildWith(cfg)(
		ctx,
		mustID("00000000-0000-4000-8000-000000000101"),
		mustID("00000000-0000-4000-8000-000000000102"),
		loop.Provenance{},
		publisher,
		definition,
		ids,
		event.NewFactory(ids, func() time.Time { return time.Unix(10, 0).UTC() }),
	)
	if err != nil {
		t.Fatalf("build ACP backend: %v", err)
	}
	if agentSessionID != "scripted-acp-session" {
		t.Fatalf("agent session id = %q", agentSessionID)
	}
	live := built.(*backend.Loop)
	live.CommandSink() <- command.UserInput{
		Header: command.Header{CommandID: mustID("00000000-0000-4000-8000-000000000103")},
		Blocks: []content.Block{&content.TextBlock{Text: "run through ACP"}},
	}
	waitForTurn(t, ctx, live, 1)
	messages, turnIndex, err := live.Snapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot ACP backend: %v", err)
	}
	if got := messageText(messages[0]); got != "reply over ACP" {
		t.Fatalf("assistant text = %q", got)
	}
	shutdown(t, ctx, live)

	// Harness journals the routing SID and the ACP session ID separately.
	// Restore feeds AgentSessionID to session/load, then seeds the backend's
	// committed messages and turn counter before any new input is accepted.
	restored, err := foreignacp.BuildRestoredWith(cfg)(
		ctx,
		mustID("00000000-0000-4000-8000-000000000104"),
		mustID("00000000-0000-4000-8000-000000000105"),
		loop.Provenance{},
		&recordingPublisher{},
		definition,
		ids,
		event.NewFactory(ids, func() time.Time { return time.Unix(11, 0).UTC() }),
		foreign.RestoredForeign{
			ForeignSID:     agentSessionID,
			AgentSessionID: agentSessionID,
			TurnIndex:      turnIndex,
			Msgs:           messages,
		},
	)
	if err != nil {
		t.Fatalf("restore ACP backend: %v", err)
	}
	restoredLoop := restored.(*backend.Loop)
	restoredLoop.CommandSink() <- command.UserInput{
		Header: command.Header{CommandID: mustID("00000000-0000-4000-8000-000000000106")},
		Blocks: []content.Block{&content.TextBlock{Text: "continue over loaded session"}},
	}
	waitForTurn(t, ctx, restoredLoop, 2)
	shutdown(t, ctx, restoredLoop)

	bad := cfg
	bad.Executable = "relative-adapter"
	_, err = foreignacp.New(ctx, bad)
	var configErr *foreignacp.ConfigError
	if !errors.As(err, &configErr) || configErr.Field != "Executable" {
		t.Fatalf("invalid ACP config error = %T %v", err, err)
	}
}

func TestScriptedACPRegistersInitializeBeforeTransportStarts(t *testing.T) {
	server, client := net.Pipe()
	t.Cleanup(func() {
		_ = server.Close()
		_ = client.Close()
	})
	if err := client.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set client deadline: %v", err)
	}

	params, err := json.Marshal(protocol.InitializeRequest{ProtocolVersion: protocol.CurrentProtocolVersion})
	if err != nil {
		t.Fatalf("marshal initialize params: %v", err)
	}
	request, err := json.Marshal(&protocol.Request{
		ID:     protocol.NewNumberID(1),
		Method: string(protocol.MethodInitialize),
		Params: params,
	})
	if err != nil {
		t.Fatalf("marshal initialize request: %v", err)
	}
	request = append(request, '\n')

	writeStarted := make(chan struct{})
	writeResult := make(chan error, 1)
	go func() {
		close(writeStarted)
		_, err := client.Write(request)
		writeResult <- err
	}()
	<-writeStarted

	conn := newScriptedACPConn(server, server)
	t.Cleanup(func() { _ = conn.Close() })

	line, err := bufio.NewReader(client).ReadBytes('\n')
	if err != nil {
		t.Fatalf("read initialize response: %v", err)
	}
	envelope, err := protocol.ParseEnvelope(line[:len(line)-1])
	if err != nil {
		t.Fatalf("parse initialize response: %v", err)
	}
	if envelope.Response == nil {
		t.Fatalf("initialize reply = %#v, want response", envelope)
	}
	if envelope.Response.Error != nil {
		t.Fatalf("initialize response error = %v", envelope.Response.Error)
	}
	var response protocol.InitializeResponse
	if err := json.Unmarshal(envelope.Response.Result, &response); err != nil {
		t.Fatalf("decode initialize response: %v", err)
	}
	if response.ProtocolVersion != protocol.CurrentProtocolVersion {
		t.Fatalf("protocol version = %d, want %d", response.ProtocolVersion, protocol.CurrentProtocolVersion)
	}
	if err := <-writeResult; err != nil {
		t.Fatalf("write initialize request: %v", err)
	}
}

func serveScriptedACP() int {
	conn := newScriptedACPConn(os.Stdin, os.Stdout)
	<-conn.Done()
	return 0
}

type registrationGateReader struct {
	source     io.Reader
	registered <-chan struct{}
}

func (r registrationGateReader) Read(p []byte) (int, error) {
	<-r.registered
	return r.source.Read(p)
}

func newScriptedACPConn(source io.Reader, destination io.Writer) *protocol.Conn {
	registered := make(chan struct{})
	conn := protocol.NewConn(registrationGateReader{source: source, registered: registered}, destination, protocol.ConnOptions{})
	clientConn := protocol.NewClientConn(conn)
	conn.Handle(string(protocol.MethodInitialize), func(_ context.Context, _ string, raw json.RawMessage) (any, error) {
		var request protocol.InitializeRequest
		if err := json.Unmarshal(raw, &request); err != nil {
			return nil, err
		}
		capabilities := protocol.DefaultAgentCapabilities()
		capabilities.LoadSession = true
		return protocol.InitializeResponse{
			ProtocolVersion:   request.ProtocolVersion,
			AgentCapabilities: &capabilities,
		}, nil
	})
	conn.Handle(string(protocol.MethodSessionNew), func(_ context.Context, _ string, raw json.RawMessage) (any, error) {
		var request protocol.NewSessionRequest
		if err := json.Unmarshal(raw, &request); err != nil {
			return nil, err
		}
		return protocol.NewSessionResponse{SessionID: "scripted-acp-session"}, nil
	})
	conn.Handle(string(protocol.MethodSessionLoad), func(_ context.Context, _ string, raw json.RawMessage) (any, error) {
		var request protocol.LoadSessionRequest
		if err := json.Unmarshal(raw, &request); err != nil {
			return nil, err
		}
		if request.SessionID != "scripted-acp-session" {
			return nil, errors.New("unexpected scripted session id")
		}
		return protocol.LoadSessionResponse{}, nil
	})
	conn.Handle(string(protocol.MethodSessionPrompt), func(ctx context.Context, _ string, raw json.RawMessage) (any, error) {
		var request protocol.PromptRequest
		if err := json.Unmarshal(raw, &request); err != nil {
			return nil, err
		}
		if err := clientConn.SessionUpdate(ctx, protocol.SessionNotification{
			SessionID: request.SessionID,
			Update: protocol.SessionUpdate{AgentMessageChunk: &protocol.ContentChunk{
				Content: protocol.ContentBlock{Text: &protocol.TextContent{Text: "reply over ACP"}},
			}},
		}); err != nil {
			return nil, err
		}
		return protocol.PromptResponse{StopReason: protocol.StopReasonEndTurn}, nil
	})
	conn.HandleNotify(string(protocol.MethodSessionCancel), func(context.Context, string, json.RawMessage) {})
	close(registered)
	return conn
}

type recordingPublisher struct{}

func (*recordingPublisher) PublishEvent(context.Context, event.Event) error { return nil }
func (p *recordingPublisher) PublishEventChecked(ctx context.Context, item event.Event) error {
	return p.PublishEvent(ctx, item)
}

func waitForTurn(t *testing.T, ctx context.Context, state *backend.Loop, want event.TurnIndex) {
	t.Helper()
	for {
		_, got, err := state.Snapshot(ctx)
		if err != nil {
			t.Fatalf("snapshot ACP loop: %v", err)
		}
		if got == want {
			return
		}
		select {
		case <-time.After(time.Millisecond):
		case <-ctx.Done():
			t.Fatalf("wait for ACP turn: %v", ctx.Err())
		}
	}
}

func shutdown(t *testing.T, ctx context.Context, state *backend.Loop) {
	t.Helper()
	ack := make(chan error, 1)
	state.CommandSink() <- command.Shutdown{Ack: ack}
	select {
	case err := <-ack:
		if err != nil {
			t.Fatalf("shutdown ACP backend: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("shutdown ACP backend: %v", ctx.Err())
	}
}

func messageText(message content.Conversation) string {
	return message.(*content.AIMessage).Blocks[0].(*content.TextBlock).Text
}

func sequentialIDs() func() (uuid.UUID, error) {
	var mu sync.Mutex
	var n byte = 20
	return func() (uuid.UUID, error) {
		mu.Lock()
		defer mu.Unlock()
		n++
		var id uuid.UUID
		id[6], id[8], id[15] = 0x40, 0x80, n
		return id, nil
	}
}

func mustID(value string) uuid.UUID { return uuid.MustParse(value) }

type unusedInferenceClient struct{}

func (unusedInferenceClient) Invoke(context.Context, inference.Request) (*inference.Response, error) {
	return nil, errors.New("unused by ACP foreign backend")
}

func (unusedInferenceClient) Stream(context.Context, inference.Request) (*inferencestream.StreamReader[content.Chunk], error) {
	return nil, errors.New("unused by ACP foreign backend")
}

func boundLoop(t *testing.T) loop.BoundDefinition {
	t.Helper()
	definition, err := loop.Define(
		loop.WithName("docs-acp-agent"),
		loop.WithInference(unusedInferenceClient{}, model.Model{
			Provider:  "lmstudio",
			APIFormat: model.APIFormatOpenAI,
			BaseURL:   "http://127.0.0.1:1234",
			Name:      "unused",
		}),
		loop.WithSystem("Be concise."),
	)
	if err != nil {
		t.Fatalf("define loop: %v", err)
	}
	bound, err := definition.Bind(context.Background(), tool.Bindings{
		SessionID: mustID("00000000-0000-4000-8000-000000000107"),
		LoopID:    mustID("00000000-0000-4000-8000-000000000108"),
	})
	if err != nil {
		t.Fatalf("bind loop: %v", err)
	}
	return bound
}
