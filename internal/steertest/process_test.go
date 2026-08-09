package steertest

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/looprig/acp/client"
	"github.com/looprig/acp/protocol"
	"github.com/looprig/acp/transport/stdio"
)

func TestFixtureProcessCapturesInitializeNewLoadAndMCP(t *testing.T) {
	meta := json.RawMessage(`{"steering":{"supported":true,"idleBehaviors":["promptRequired"]}}`)
	script := Script{
		AgentName:    "fixture-agent",
		AgentVersion: "7.3.1",
		Metadata:     meta,
		SessionID:    "fixture-session",
	}
	agent := New(t, script)
	info, err := os.Stat(agent.ControlEndpoint())
	if err != nil {
		t.Fatalf("stat control socket: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("control socket permissions = %v, want 0600", info.Mode().Perm())
	}

	stdin, frames, waitProcess := startFixtureProcess(t, agent)
	t.Cleanup(func() {
		_ = stdin.Close()
		_ = waitProcess()
		agent.Close()
	})

	if got := agent.WaitForKind(context.Background(), EventReady); got.Err != nil {
		t.Fatalf("fixture ready: %v", got.Err)
	}

	writeRequest(t, stdin, 1, "initialize", map[string]any{})
	initialize := readResponse(t, frames)
	var initResult struct {
		AgentInfo struct {
			Name    string `json:"name"`
			Version string `json:"version"`
		} `json:"agentInfo"`
		Meta json.RawMessage `json:"_meta"`
	}
	if err := json.Unmarshal(initialize.Result, &initResult); err != nil {
		t.Fatalf("decode initialize: %v", err)
	}
	if initResult.AgentInfo.Name != script.AgentName || initResult.AgentInfo.Version != script.AgentVersion {
		t.Fatalf("initialize agentInfo = %#v, want %q/%q", initResult.AgentInfo, script.AgentName, script.AgentVersion)
	}
	if string(initResult.Meta) != string(meta) {
		t.Fatalf("initialize metadata = %s, want %s", initResult.Meta, meta)
	}

	mcp := map[string]any{
		"name":    "collab",
		"command": "/absolute/collab-mcp",
		"args":    []string{"--stdio"},
		"env":     []map[string]string{{"name": "COLLAB_TOKEN", "value": "secret-value"}},
	}
	writeRequest(t, stdin, 2, "session/new", map[string]any{
		"cwd":        "/workspace",
		"mcpServers": []any{mcp},
	})
	newResult := readResponse(t, frames)
	var sessionResult struct {
		SessionID string `json:"sessionId"`
	}
	if err := json.Unmarshal(newResult.Result, &sessionResult); err != nil {
		t.Fatalf("decode session/new: %v", err)
	}
	if sessionResult.SessionID != script.SessionID {
		t.Fatalf("session/new id = %q, want %q", sessionResult.SessionID, script.SessionID)
	}

	writeRequest(t, stdin, 3, "session/load", map[string]any{
		"sessionId":  "restored-session",
		"cwd":        "/workspace",
		"mcpServers": []any{mcp},
	})
	loadResult := readResponse(t, frames)
	if loadResult.Error != nil {
		t.Fatalf("session/load error: %#v", loadResult.Error)
	}

	got := agent.WaitForKind(context.Background(), EventMCPDescriptor)
	if got.Err != nil {
		t.Fatalf("mcp capture: %v", got.Err)
	}
	if err := agent.WaitForMCPDescriptors(context.Background(), 2); err != nil {
		t.Fatalf("mcp capture count: %v", err)
	}
	servers := agent.MCPDescriptors()
	if len(servers) != 2 || servers[0].Name != "collab" || servers[0].Command != "/absolute/collab-mcp" {
		t.Fatalf("captured MCP descriptors mismatch (redacted transcript): %s", agent.Transcript())
	}
	if got := servers[0].EnvValue("COLLAB_TOKEN"); got != "secret-value" {
		t.Fatalf("captured MCP token mismatch")
	}
	if got := agent.Transcript().String(); containsSecret(got, "secret-value") {
		t.Fatalf("transcript leaked MCP environment value: %s", got)
	}
}

func TestFixtureScriptGatesPromptSteerAndTerminalOrdering(t *testing.T) {
	script := DefaultScript()
	script.Prompts = []PromptScript{{Actions: []Action{
		{Kind: ActionUpdate, Name: "update", Text: "before terminal", Gate: "release-update"},
		{Kind: ActionTerminal, Name: "terminal", Gate: "release-terminal"},
	}}}
	script.Steers = []SteerScript{{Actions: []Action{
		{Kind: ActionSteerReply, Name: "ack", Outcome: OutcomeInjected, Gate: "release-ack"},
	}}}
	agent := New(t, script)
	stdin, frames, waitProcess := startFixtureProcess(t, agent)
	t.Cleanup(func() {
		_ = stdin.Close()
		_ = waitProcess()
		agent.Close()
	})
	if got := agent.WaitForKind(context.Background(), EventReady); got.Err != nil {
		t.Fatalf("fixture ready: %v", got.Err)
	}

	writeRequest(t, stdin, 1, "initialize", map[string]any{})
	_ = readResponse(t, frames)
	writeRequest(t, stdin, 2, "session/new", map[string]any{"cwd": "/workspace", "mcpServers": []any{}})
	_ = readResponse(t, frames)
	writeRequest(t, stdin, 3, "session/prompt", map[string]any{
		"sessionId": script.SessionID,
		"prompt":    []map[string]any{{"type": "text", "text": "hello"}},
	})
	if got := agent.WaitForKind(context.Background(), EventGate); got.Err != nil || got.Event.Gate != "release-update" {
		t.Fatalf("first prompt gate = %#v, want release-update", got)
	}

	writeRequest(t, stdin, 4, "_session/steering", map[string]any{
		"sessionId": script.SessionID,
		"prompt":    []map[string]any{{"type": "text", "text": "steer"}},
	})
	if got := agent.WaitForKind(context.Background(), EventSteer); got.Err != nil || got.Event.Method != "_session/steering" {
		t.Fatalf("steer request event = %#v, want steering method", got)
	}
	if err := agent.Release("release-update"); err != nil {
		t.Fatalf("release update: %v", err)
	}
	if got := agent.WaitForKind(context.Background(), EventUpdate); got.Err != nil || got.Event.Text != "before terminal" {
		t.Fatalf("update event = %#v, want gated update", got)
	}
	if method := readMethod(t, frames); method != "session/update" {
		t.Fatalf("first post-release frame method = %q, want session/update", method)
	}

	if err := agent.Release("release-ack"); err != nil {
		t.Fatalf("release ack: %v", err)
	}
	steerResponse := readResponse(t, frames)
	var steer struct {
		Outcome string `json:"outcome"`
	}
	if err := json.Unmarshal(steerResponse.Result, &steer); err != nil || steer.Outcome != string(OutcomeInjected) {
		t.Fatalf("steer response = %s, want injected", steerResponse.Result)
	}
	if got, err := agent.WaitForNth(context.Background(), EventSteer, 1); err != nil || got.Outcome != OutcomeInjected {
		t.Fatalf("post-write steer event = %#v, %v; want injected response fact", got, err)
	}
	if err := agent.Release("release-terminal"); err != nil {
		t.Fatalf("release terminal: %v", err)
	}
	promptResponse := readResponse(t, frames)
	var prompt struct {
		StopReason string `json:"stopReason"`
	}
	if err := json.Unmarshal(promptResponse.Result, &prompt); err != nil || prompt.StopReason != "end_turn" {
		t.Fatalf("prompt response = %s, want end_turn", promptResponse.Result)
	}
	if got := agent.WaitForKind(context.Background(), EventTerminal); got.Err != nil {
		t.Fatalf("terminal event: %v", got.Err)
	}

	transcript := agent.Transcript().Records
	if !eventOrder(transcript, EventUpdate, EventSteer, EventTerminal) {
		t.Fatalf("transcript order = %s, want update before steer response and terminal", agent.Transcript())
	}
}

func TestFixtureSteerFactIsPublishedAfterRPCResponseWrite(t *testing.T) {
	controlPeer, controlChild := net.Pipe()
	defer controlPeer.Close()
	defer controlChild.Close()
	script := DefaultScript()
	script.Steers = []SteerScript{{Actions: []Action{{Kind: ActionSteerReply, Outcome: OutcomeInjected}}}}
	state := newChildState(script, controlChild)
	writer := &blockingFixtureWriter{started: make(chan struct{}), release: make(chan struct{})}
	state.stdout = writer

	events := make(chan Event, 4)
	go func() {
		reader := bufio.NewReader(controlPeer)
		for {
			line, err := reader.ReadBytes('\n')
			if err != nil {
				return
			}
			var event Event
			if json.Unmarshal(line, &event) == nil {
				events <- event
			}
		}
	}()

	done := make(chan struct{})
	go func() {
		state.handleSteer(rpcRequest{
			JSONRPC: "2.0",
			ID:      json.RawMessage(`1`),
			Method:  "_session/steering",
			Params:  json.RawMessage(`{"sessionId":"fixture-session"}`),
		})
		close(done)
	}()
	if first := <-events; first.Kind != EventSteer || first.Outcome != "" {
		t.Fatalf("initial steer fact = %#v, want request fact without outcome", first)
	}
	select {
	case <-writer.started:
	case <-time.After(time.Second):
		t.Fatal("steer response write did not start")
	}
	select {
	case got := <-events:
		t.Fatalf("steer response fact published before RPC write completed: %#v", got)
	default:
	}
	close(writer.release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("steer handler did not finish after RPC write release")
	}
	select {
	case got := <-events:
		if got.Kind != EventSteer || got.Outcome != OutcomeInjected {
			t.Fatalf("post-write steer fact = %#v, want injected response", got)
		}
	case <-time.After(time.Second):
		t.Fatal("post-write steer fact did not arrive")
	}
}

func TestFixtureTerminalFactIsPublishedAfterRPCResponseWrite(t *testing.T) {
	controlPeer, controlChild := net.Pipe()
	defer controlPeer.Close()
	defer controlChild.Close()
	script := DefaultScript()
	script.Prompts = []PromptScript{{Actions: []Action{{Kind: ActionTerminal, StopReason: "end_turn"}}}}
	state := newChildState(script, controlChild)
	writer := &blockingFixtureWriter{started: make(chan struct{}), release: make(chan struct{})}
	state.stdout = writer

	events := make(chan Event, 4)
	go func() {
		reader := bufio.NewReader(controlPeer)
		for {
			line, err := reader.ReadBytes('\n')
			if err != nil {
				return
			}
			var event Event
			if json.Unmarshal(line, &event) == nil {
				events <- event
			}
		}
	}()

	done := make(chan struct{})
	go func() {
		state.handlePrompt(rpcRequest{
			JSONRPC: "2.0",
			ID:      json.RawMessage(`1`),
			Method:  "session/prompt",
			Params:  json.RawMessage(`{"sessionId":"fixture-session"}`),
		})
		close(done)
	}()
	if first := <-events; first.Kind != EventPrompt {
		t.Fatalf("prompt request fact = %#v, want prompt", first)
	}
	select {
	case <-writer.started:
	case <-time.After(time.Second):
		t.Fatal("prompt response write did not start")
	}
	select {
	case got := <-events:
		t.Fatalf("terminal fact published before RPC write completed: %#v", got)
	default:
	}
	close(writer.release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("prompt handler did not finish after RPC write release")
	}
	select {
	case got := <-events:
		if got.Kind != EventTerminal {
			t.Fatalf("post-write terminal fact = %#v, want terminal", got)
		}
	case <-time.After(time.Second):
		t.Fatal("post-write terminal fact did not arrive")
	}
}

type blockingFixtureWriter struct {
	started chan struct{}
	release chan struct{}
}

func (w *blockingFixtureWriter) Write(p []byte) (int, error) {
	close(w.started)
	<-w.release
	return len(p), nil
}

func TestFixtureTransportLossAndStartedNewTurnAreScriptable(t *testing.T) {
	script := DefaultScript()
	script.Steers = []SteerScript{{Actions: []Action{{Kind: ActionSteerReply, Outcome: OutcomeStartedNewTurn, Gate: "release-breach"}}}}
	agent := New(t, script)
	stdin, frames, waitProcess := startFixtureProcess(t, agent)
	t.Cleanup(func() {
		_ = stdin.Close()
		_ = waitProcess()
		agent.Close()
	})
	if got := agent.WaitForKind(context.Background(), EventReady); got.Err != nil {
		t.Fatalf("fixture ready: %v", got.Err)
	}
	writeRequest(t, stdin, 1, "initialize", map[string]any{})
	_ = readResponse(t, frames)
	writeRequest(t, stdin, 2, "session/new", map[string]any{"cwd": "/workspace", "mcpServers": []any{}})
	_ = readResponse(t, frames)
	writeRequest(t, stdin, 3, "_session/steering", map[string]any{"sessionId": script.SessionID})
	if got := agent.WaitForKind(context.Background(), EventGate); got.Err != nil || got.Event.Gate != "release-breach" {
		t.Fatalf("breach gate = %#v, want release-breach", got)
	}
	if err := agent.Release("release-breach"); err != nil {
		t.Fatalf("release breach: %v", err)
	}
	if got := agent.WaitForKind(context.Background(), EventSteer); got.Err != nil {
		t.Fatalf("breach event: %v", got.Err)
	}
	var result struct {
		Outcome string `json:"outcome"`
	}
	response := readResponse(t, frames)
	if err := json.Unmarshal(response.Result, &result); err != nil || result.Outcome != string(OutcomeStartedNewTurn) {
		t.Fatalf("breach response = %s, want startedNewTurn", response.Result)
	}

}

func TestFixtureTransportLossClosesACPTransport(t *testing.T) {
	script := DefaultScript()
	script.Steers = []SteerScript{{Actions: []Action{{Kind: ActionTransportLoss, Gate: "release-loss"}}}}
	agent := New(t, script)
	stdin, frames, waitProcess := startFixtureProcess(t, agent)
	t.Cleanup(func() {
		_ = stdin.Close()
		_ = waitProcess()
		agent.Close()
	})
	if got := agent.WaitForKind(context.Background(), EventReady); got.Err != nil {
		t.Fatalf("fixture ready: %v", got.Err)
	}
	writeRequest(t, stdin, 1, "initialize", map[string]any{})
	_ = readResponse(t, frames)
	writeRequest(t, stdin, 2, "session/new", map[string]any{"cwd": "/workspace", "mcpServers": []any{}})
	_ = readResponse(t, frames)
	writeRequest(t, stdin, 3, "_session/steering", map[string]any{"sessionId": script.SessionID})
	if got := agent.WaitForKind(context.Background(), EventGate); got.Err != nil || got.Event.Gate != "release-loss" {
		t.Fatalf("loss gate = %#v, want release-loss", got)
	}
	if err := agent.Release("release-loss"); err != nil {
		t.Fatalf("release loss: %v", err)
	}
	if got := agent.WaitForKind(context.Background(), EventTransportLoss); got.Err != nil {
		t.Fatalf("transport-loss event: %v", got.Err)
	}
	readErr := make(chan error, 1)
	go func() {
		_, err := frames.ReadBytes('\n')
		readErr <- err
	}()
	if err := <-readErr; err == nil {
		t.Fatal("transport loss left ACP stdout open")
	}
}

func TestFixtureSpeaksTypedACPClientAndDelaysSteeringAck(t *testing.T) {
	script := DefaultScript()
	script.Prompts = []PromptScript{{Actions: []Action{
		{Kind: ActionUpdate, Text: "progress", Gate: "release-progress"},
		{Kind: ActionTerminal, Gate: "release-terminal"},
	}}}
	script.Steers = []SteerScript{{Actions: []Action{
		{Kind: ActionSteerReply, Outcome: OutcomeInjected, Gate: "release-steer"},
	}}}
	agent := New(t, script)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	owned, err := client.Dial(ctx, stdio.Command{Path: agent.Executable(), Env: agent.Env()}, client.Options{})
	if err != nil {
		t.Fatalf("dial typed client: %v", err)
	}
	t.Cleanup(func() { _ = owned.Close(context.Background()); agent.Close() })
	sess, err := owned.NewSession(ctx, client.NewSessionParams{Cwd: "/workspace", McpServers: []protocol.McpServer{{Stdio: &protocol.McpServerStdio{
		Name: "collab", Command: "/absolute/collab-mcp", Args: []string{"--stdio"}, Env: []protocol.EnvVariable{{Name: "TOKEN", Value: "secret"}},
	}}}})
	if err != nil {
		t.Fatalf("new typed session: %v", err)
	}
	if err := agent.WaitForMCPDescriptors(ctx, 1); err != nil {
		t.Fatalf("typed MCP capture: %v", err)
	}
	promptDone := make(chan *client.PromptResult, 1)
	promptErr := make(chan error, 1)
	go func() {
		result, err := sess.Prompt(ctx, []protocol.ContentBlock{{Text: &protocol.TextContent{Text: "hello"}}})
		promptDone <- result
		promptErr <- err
	}()
	if _, err := agent.WaitFor(ctx, EventGate); err != nil {
		t.Fatalf("prompt progress gate: %v", err)
	}
	steer := sess.StartSteer(ctx, client.SteerParams{Prompt: []protocol.ContentBlock{{Text: &protocol.TextContent{Text: "steer"}}}})
	select {
	case admitted := <-steer.Admission():
		if !admitted {
			t.Fatal("steering writer admission = false, want true")
		}
	case <-ctx.Done():
		t.Fatalf("steering admission: %v", ctx.Err())
	}
	if err := agent.Release("release-progress"); err != nil {
		t.Fatalf("release progress: %v", err)
	}
	select {
	case update := <-sess.Updates():
		if update.SessionUpdate.AgentMessageChunk == nil || update.SessionUpdate.AgentMessageChunk.Content.Text == nil || update.SessionUpdate.AgentMessageChunk.Content.Text.Text != "progress" {
			t.Fatalf("typed update = %#v, want progress", update)
		}
	case <-ctx.Done():
		t.Fatalf("typed update: %v", ctx.Err())
	}
	if err := agent.Release("release-steer"); err != nil {
		t.Fatalf("release steer: %v", err)
	}
	select {
	case completion := <-steer.Result():
		if completion.Err != nil || completion.Result.Outcome != client.SteerOutcomeInjected {
			t.Fatalf("typed steer completion = %#v, want injected", completion)
		}
	case <-ctx.Done():
		t.Fatalf("typed steer completion: %v", ctx.Err())
	}
	if err := agent.Release("release-terminal"); err != nil {
		t.Fatalf("release terminal: %v", err)
	}
	select {
	case err := <-promptErr:
		if err != nil {
			t.Fatalf("typed prompt: %v", err)
		}
		if result := <-promptDone; result == nil || result.StopReason != protocol.StopReasonEndTurn {
			t.Fatalf("typed prompt result = %#v, want end_turn", result)
		}
	case <-ctx.Done():
		t.Fatalf("typed prompt completion: %v", ctx.Err())
	}
}

func readMethod(t *testing.T, r *bufio.Reader) string {
	t.Helper()
	line, err := r.ReadBytes('\n')
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	var frame struct {
		Method string `json:"method"`
	}
	if err := json.Unmarshal(line, &frame); err != nil {
		t.Fatalf("decode frame: %v", err)
	}
	return frame.Method
}

func eventOrder(events []Event, first, second, third EventKind) bool {
	firstAt, secondAt, thirdAt := -1, -1, -1
	for i, event := range events {
		if firstAt < 0 && event.Kind == first {
			firstAt = i
		}
		if secondAt < 0 && event.Kind == second && event.Outcome != "" {
			secondAt = i
		}
		if thirdAt < 0 && event.Kind == third {
			thirdAt = i
		}
	}
	return firstAt >= 0 && secondAt >= 0 && thirdAt >= 0 && firstAt < secondAt && secondAt < thirdAt
}

func writeRequest(t *testing.T, w io.Writer, id int, method string, params any) {
	t.Helper()
	wire := map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"method":  method,
		"params":  params,
	}
	data, err := json.Marshal(wire)
	if err != nil {
		t.Fatalf("marshal %s: %v", method, err)
	}
	data = append(data, '\n')
	if _, err := w.Write(data); err != nil {
		t.Fatalf("write %s: %v", method, err)
	}
}

type testResponse struct {
	Result json.RawMessage `json:"result"`
	Error  json.RawMessage `json:"error"`
}

func readResponse(t *testing.T, r *bufio.Reader) testResponse {
	t.Helper()
	line, err := r.ReadBytes('\n')
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	var response testResponse
	if err := json.Unmarshal(line, &response); err != nil {
		t.Fatalf("decode response %q: %v", line, err)
	}
	return response
}

func containsSecret(value, secret string) bool {
	return secret != "" && strings.Contains(value, secret)
}

func startFixtureProcess(t *testing.T, agent *Agent) (io.WriteCloser, *bufio.Reader, func() error) {
	t.Helper()
	cmd := exec.Command(agent.Executable())
	cmd.Env = agent.Env()
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("stdin pipe: %v", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start fixture: %v", err)
	}
	return stdin, bufio.NewReader(stdout), func() error {
		if cmd.ProcessState == nil && cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		return cmd.Wait()
	}
}
