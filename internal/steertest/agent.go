package steertest

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
)

const (
	controlEndpointEnv = "STEERTEST_CONTROL_ENDPOINT"
	scriptEnv          = "STEERTEST_SCRIPT"
	maxControlFrame    = 256 << 10
	maxEventText       = 4096
	maxEventRecords    = 4096
	redactedValue      = "<redacted>"
)

// EventKind identifies a child-process observation delivered over the private
// fixture control socket.
type EventKind string

const (
	EventReady           EventKind = "ready"
	EventInitialize      EventKind = "initialize"
	EventNewSession      EventKind = "session_new"
	EventLoadSession     EventKind = "session_load"
	EventPrompt          EventKind = "prompt"
	EventSteer           EventKind = "steer"
	EventUpdate          EventKind = "update"
	EventTerminal        EventKind = "terminal"
	EventMCPDescriptor   EventKind = "mcp_descriptor"
	EventGate            EventKind = "gate"
	EventTransportLoss   EventKind = "transport_loss"
	EventSessionInfo     EventKind = "session_info"
	EventSessionCanceled EventKind = "session_cancel"
	EventClosed          EventKind = "closed"
)

// Event is one bounded, process-side fixture observation. Request IDs are
// retained only for test correlation and are never put into model-facing ACP
// arguments by the fixture.
type Event struct {
	Kind      EventKind       `json:"kind"`
	Name      string          `json:"name,omitempty"`
	Gate      string          `json:"gate,omitempty"`
	Method    string          `json:"method,omitempty"`
	SessionID string          `json:"sessionId,omitempty"`
	RequestID json.RawMessage `json:"requestId,omitempty"`
	Text      string          `json:"text,omitempty"`
	Outcome   SteeringOutcome `json:"outcome,omitempty"`
	Reason    string          `json:"reason,omitempty"`
	MCP       []MCPDescriptor `json:"mcp,omitempty"`
	ErrorCode int             `json:"errorCode,omitempty"`
}

// MCPDescriptor is the captured session/new or session/load MCP server
// descriptor. Values are retained for assertions through EnvValue; String
// and Transcript always redact them.
type MCPDescriptor struct {
	Name    string           `json:"name,omitempty"`
	Command string           `json:"command,omitempty"`
	Args    []string         `json:"args,omitempty"`
	Env     []MCPEnvironment `json:"env,omitempty"`
}

// String formats a descriptor with all captured environment values redacted.
// Tests can safely include it in failure messages; use EnvValue for an
// explicit assertion on a value.
func (d MCPDescriptor) String() string {
	data, err := json.Marshal(d.redacted())
	if err != nil {
		return `{"invalid":"mcp descriptor"}`
	}
	return string(data)
}

// MCPEnvironment is one captured MCP environment variable.
type MCPEnvironment struct {
	Name  string `json:"name,omitempty"`
	Value string `json:"value,omitempty"`
}

// EnvValue returns the captured value for name, or an empty string if absent.
func (d MCPDescriptor) EnvValue(name string) string {
	for _, variable := range d.Env {
		if variable.Name == name {
			return variable.Value
		}
	}
	return ""
}

func (d MCPDescriptor) clone() MCPDescriptor {
	d.Args = append([]string(nil), d.Args...)
	d.Env = append([]MCPEnvironment(nil), d.Env...)
	return d
}

func (d MCPDescriptor) redacted() MCPDescriptor {
	out := d.clone()
	for i := range out.Env {
		if out.Env[i].Value != "" {
			out.Env[i].Value = redactedValue
		}
	}
	return out
}

// WaitResult is the result of WaitForKind. Err is context cancellation,
// fixture shutdown, or a control transport failure.
type WaitResult struct {
	Event Event
	Err   error
}

// Transcript is a bounded snapshot of process observations. Its String form
// is deliberately redacted and suitable for test failures.
type Transcript struct {
	Records   []Event
	Truncated bool
	secrets   []string
}

// String formats a transcript with environment values redacted and all fields
// bounded. It never returns raw child wire payloads.
func (t Transcript) String() string {
	var b strings.Builder
	for i, event := range t.Records {
		if i > 0 {
			b.WriteByte('\n')
		}
		copyEvent := redactEvent(event, t.secrets)
		encoded, err := json.Marshal(copyEvent)
		if err != nil {
			b.WriteString(`{"kind":"invalid"}`)
			continue
		}
		b.Write(encoded)
	}
	if t.Truncated {
		if b.Len() > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(`{"truncated":true}`)
	}
	return b.String()
}

func redactEvent(in Event, secrets []string) Event {
	out := in
	out.RequestID = nil
	out.MCP = make([]MCPDescriptor, len(in.MCP))
	for i, descriptor := range in.MCP {
		out.MCP[i] = descriptor.redacted()
	}
	out.Text = redactStringBounded(in.Text, secrets)
	out.Reason = redactStringBounded(in.Reason, secrets)
	return out
}

func redactStringBounded(value string, secrets []string) string {
	if len(value) > maxEventText {
		value = value[:maxEventText]
	}
	for _, secret := range secrets {
		if secret != "" {
			value = strings.ReplaceAll(value, secret, redactedValue)
		}
	}
	return value
}

// Agent owns the parent side of one fake ACP process control socket. It does
// not start an ACP child itself; use Executable and Env in the caller's ACP
// configuration, then use WaitForKind/Release to drive the process.
type Agent struct {
	path string
	env  []string

	listener net.Listener
	socket   string
	sockDir  string
	connMu   sync.Mutex
	conn     net.Conn
	writeMu  sync.Mutex

	mu        sync.Mutex
	changed   chan struct{}
	events    []Event
	stream    chan Event
	closed    bool
	truncated bool
	maxRecord int
	secrets   []string
	mcp       []MCPDescriptor

	closeOnce sync.Once
}

// New builds the tiny helper executable, allocates a private Unix control
// socket, and returns a reusable fixture handle. The returned handle is safe
// for concurrent WaitForKind, Release, Transcript, and MCPDescriptors calls.
func New(tb testing.TB, script Script) *Agent {
	tb.Helper()
	normalized, err := normalizeScript(script)
	if err != nil {
		tb.Fatalf("steertest script: %v", err)
	}
	encoded, err := scriptBytes(normalized)
	if err != nil {
		tb.Fatalf("steertest script: %v", err)
	}
	encodedEnv := base64.RawStdEncoding.EncodeToString(encoded)
	if len(encodedEnv) > maxControlFrame*2 {
		tb.Fatalf("steertest script: encoded script exceeds environment bound")
	}

	root := moduleRoot()
	path := filepath.Join(tb.TempDir(), "steertest-agent")
	build := exec.Command("go", "build", "-trimpath", "-o", path, "./internal/steertest/cmd")
	build.Dir = root
	if output, err := build.CombinedOutput(); err != nil {
		tb.Fatalf("build steertest agent: %v", boundedDiagnostic(string(output)))
	}

	sockDir, socketPath, listener, err := listenShortUnixSocket()
	if err != nil {
		tb.Fatalf("listen steertest control socket: %v", err)
	}

	env := []string{
		controlEndpointEnv + "=" + socketPath,
		scriptEnv + "=" + encodedEnv,
	}
	secrets := make([]string, 0, len(normalized.Extra))
	for _, key := range stableKeys(normalized.Extra) {
		env = append(env, key+"="+normalized.Extra[key])
		if normalized.Extra[key] != "" {
			secrets = append(secrets, normalized.Extra[key])
		}
	}

	a := &Agent{
		path:      path,
		env:       env,
		listener:  listener,
		socket:    socketPath,
		sockDir:   sockDir,
		changed:   make(chan struct{}),
		stream:    make(chan Event, normalized.MaxRecords),
		maxRecord: normalized.MaxRecords,
		secrets:   secrets,
	}
	go a.accept()
	tb.Cleanup(a.Close)
	return a
}

// Executable returns the absolute helper path suitable for acp.Config.
func (a *Agent) Executable() string {
	if a == nil {
		return ""
	}
	return a.path
}

// Command is an alias for Executable.
func (a *Agent) Command() string { return a.Executable() }

// Path is an alias for Executable.
func (a *Agent) Path() string { return a.Executable() }

// ControlEndpoint returns the private Unix endpoint used by the fixture
// process. It is intended for security/cleanup assertions, not for model
// arguments.
func (a *Agent) ControlEndpoint() string {
	if a == nil {
		return ""
	}
	return a.socket
}

// Env returns the complete child environment. It contains only fixture
// control values and the explicitly configured Script.Extra values.
func (a *Agent) Env() []string {
	if a == nil {
		return nil
	}
	return append([]string(nil), a.env...)
}

// Environment is an alias for Env.
func (a *Agent) Environment() []string { return a.Env() }

// Events returns a bounded stream of observations. Consumers that need to
// wait for a specific kind without consuming the stream should use
// WaitForKind instead.
func (a *Agent) Events() <-chan Event {
	if a == nil {
		return nil
	}
	return a.stream
}

// WaitForKind waits until the first recorded event of kind appears. The
// method does not consume Events, so multiple assertions can independently
// inspect the same observation history.
func (a *Agent) WaitForKind(ctx context.Context, kind EventKind) WaitResult {
	event, err := a.waitFor(ctx, kind)
	return WaitResult{Event: event, Err: err}
}

// WaitFor is the idiomatic tuple-returning form of WaitForKind.
func (a *Agent) WaitFor(ctx context.Context, kind EventKind) (Event, error) {
	return a.waitFor(ctx, kind)
}

// WaitForNth waits for the zero-based occurrence of kind. Unlike
// WaitForKind, it is suitable for scripts that issue multiple prompts or
// steering calls.
func (a *Agent) WaitForNth(ctx context.Context, kind EventKind, occurrence int) (Event, error) {
	if occurrence < 0 {
		return Event{}, errors.New("steertest: event occurrence must not be negative")
	}
	if a == nil {
		return Event{}, errors.New("steertest: nil agent")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		a.mu.Lock()
		seen := 0
		for _, event := range a.events {
			if event.Kind != kind {
				continue
			}
			if seen == occurrence {
				a.mu.Unlock()
				return event, nil
			}
			seen++
		}
		if a.closed {
			a.mu.Unlock()
			return Event{}, errors.New("steertest: agent closed")
		}
		changed := a.changed
		a.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return Event{}, ctx.Err()
		}
	}
}

// WaitForCount waits until at least count events of kind have been recorded.
func (a *Agent) WaitForCount(ctx context.Context, kind EventKind, count int) error {
	if count < 0 {
		return errors.New("steertest: event count must not be negative")
	}
	if count == 0 {
		return nil
	}
	_, err := a.WaitForNth(ctx, kind, count-1)
	return err
}

func (a *Agent) waitFor(ctx context.Context, kind EventKind) (Event, error) {
	if a == nil {
		return Event{}, errors.New("steertest: nil agent")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		a.mu.Lock()
		for _, event := range a.events {
			if event.Kind == kind {
				a.mu.Unlock()
				return event, nil
			}
		}
		if a.closed {
			a.mu.Unlock()
			return Event{}, errors.New("steertest: agent closed")
		}
		changed := a.changed
		a.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return Event{}, ctx.Err()
		}
	}
}

// Release opens a named script gate. Releasing before the child reaches the
// gate is safe: the release is remembered by the helper process.
func (a *Agent) Release(gate string) error {
	if gate == "" {
		return errors.New("steertest: gate is required")
	}
	return a.sendControl(controlCommand{Kind: "release", Gate: gate})
}

// Continue is an alias for Release.
func (a *Agent) Continue(gate string) error { return a.Release(gate) }

// ReleaseGate is an alias for Release.
func (a *Agent) ReleaseGate(gate string) error { return a.Release(gate) }

// Transcript returns a defensive bounded snapshot. String() is safe to use
// directly in test failure output.
func (a *Agent) Transcript() Transcript {
	if a == nil {
		return Transcript{}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return Transcript{
		Records:   cloneEvents(a.events),
		Truncated: a.truncated,
		secrets:   append([]string(nil), a.secrets...),
	}
}

// MCPDescriptors returns defensive copies of all captured MCP descriptors.
func (a *Agent) MCPDescriptors() []MCPDescriptor {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]MCPDescriptor, len(a.mcp))
	for i, descriptor := range a.mcp {
		out[i] = descriptor.clone()
	}
	return out
}

// WaitForMCPDescriptors waits until at least count descriptors have been
// captured. It is useful when session/new and session/load are issued back to
// back and their control events are delivered asynchronously.
func (a *Agent) WaitForMCPDescriptors(ctx context.Context, count int) error {
	if a == nil {
		return errors.New("steertest: nil agent")
	}
	if count < 0 {
		return errors.New("steertest: descriptor count must not be negative")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		a.mu.Lock()
		if len(a.mcp) >= count {
			a.mu.Unlock()
			return nil
		}
		if a.closed {
			a.mu.Unlock()
			return errors.New("steertest: agent closed")
		}
		changed := a.changed
		a.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Close stops control observation and releases the private Unix socket. The
// ACP owner remains responsible for terminating a child process launched with
// Env; this method never sends a target-process interrupt.
func (a *Agent) Close() {
	if a == nil {
		return
	}
	a.closeOnce.Do(func() {
		a.mu.Lock()
		a.closed = true
		close(a.changed)
		close(a.stream)
		a.mu.Unlock()
		a.connMu.Lock()
		if a.conn != nil {
			_ = a.conn.Close()
			a.conn = nil
		}
		a.connMu.Unlock()
		if a.listener != nil {
			_ = a.listener.Close()
		}
		if a.socket != "" {
			_ = os.Remove(a.socket)
		}
		if a.sockDir != "" {
			_ = os.Remove(a.sockDir)
		}
	})
}

func (a *Agent) accept() {
	conn, err := a.listener.Accept()
	if err != nil {
		a.record(Event{Kind: EventClosed, Reason: "control accept failed"})
		return
	}
	a.connMu.Lock()
	a.conn = conn
	a.connMu.Unlock()
	go a.readEvents(conn)
}

func (a *Agent) readEvents(conn net.Conn) {
	reader := bufio.NewReaderSize(conn, 32<<10)
	for {
		line, err := readBoundedLine(reader, maxControlFrame)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				a.record(Event{Kind: EventTransportLoss, Reason: "control transport lost"})
			}
			return
		}
		var event Event
		if err := json.Unmarshal(line, &event); err != nil {
			a.record(Event{Kind: EventTransportLoss, Reason: "malformed control event"})
			return
		}
		a.record(event)
	}
}

func (a *Agent) record(event Event) {
	event.Text = redactStringBounded(event.Text, nil)
	event.Reason = redactStringBounded(event.Reason, nil)
	if len(event.MCP) > 64 {
		event.MCP = event.MCP[:64]
	}
	for i := range event.MCP {
		event.MCP[i] = event.MCP[i].clone()
	}
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return
	}
	if len(a.events) >= a.maxRecord {
		a.truncated = true
	} else if len(a.events) < maxEventRecords {
		a.events = append(a.events, event)
	}
	if event.Kind == EventMCPDescriptor {
		for _, descriptor := range event.MCP {
			if len(a.mcp) < maxEventRecords {
				a.mcp = append(a.mcp, descriptor.clone())
			}
		}
	}
	changed := a.changed
	a.changed = make(chan struct{})
	close(changed)
	select {
	case a.stream <- event:
	default:
		// The history remains authoritative and bounded even when a test does
		// not consume the optional live stream.
	}
	a.mu.Unlock()
}

func (a *Agent) sendControl(command controlCommand) error {
	if a == nil {
		return errors.New("steertest: nil agent")
	}
	a.connMu.Lock()
	conn := a.conn
	a.connMu.Unlock()
	if conn == nil {
		return errors.New("steertest: process control connection is not ready")
	}
	line, err := json.Marshal(command)
	if err != nil {
		return err
	}
	line = append(line, '\n')
	a.writeMu.Lock()
	defer a.writeMu.Unlock()
	_, err = conn.Write(line)
	return err
}

func cloneEvents(in []Event) []Event {
	if in == nil {
		return nil
	}
	out := make([]Event, len(in))
	for i, event := range in {
		out[i] = event
		out[i].RequestID = append(json.RawMessage(nil), event.RequestID...)
		out[i].MCP = make([]MCPDescriptor, len(event.MCP))
		for j, descriptor := range event.MCP {
			out[i].MCP[j] = descriptor.clone()
		}
	}
	return out
}

func moduleRoot() string {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return "."
	}
	return filepath.Clean(filepath.Join(filepath.Dir(filename), "../.."))
}

func listenShortUnixSocket() (string, string, net.Listener, error) {
	var lastErr error
	for _, root := range []string{"/private/tmp", "/tmp"} {
		dir, err := os.MkdirTemp(root, "st-")
		if err != nil {
			lastErr = err
			continue
		}
		path := filepath.Join(dir, "c.sock")
		listener, err := net.Listen("unix", path)
		if err == nil {
			if err := os.Chmod(path, 0o600); err != nil {
				_ = listener.Close()
				_ = os.Remove(path)
				_ = os.Remove(dir)
				lastErr = err
				continue
			}
			return dir, path, listener, nil
		}
		lastErr = err
		_ = os.Remove(dir)
	}
	if lastErr == nil {
		lastErr = errors.New("no temporary directory available")
	}
	return "", "", nil, lastErr
}

func boundedDiagnostic(value string) string {
	if len(value) > maxEventText {
		return value[:maxEventText]
	}
	return value
}

type controlCommand struct {
	Kind string `json:"kind"`
	Gate string `json:"gate,omitempty"`
}

func readBoundedLine(reader *bufio.Reader, limit int) ([]byte, error) {
	line, err := reader.ReadBytes('\n')
	if len(line) > limit {
		return nil, errors.New("steertest: control frame too large")
	}
	if err != nil {
		if len(line) == 0 {
			return nil, err
		}
		return nil, io.ErrUnexpectedEOF
	}
	return line[:len(line)-1], nil
}

// RunProcess is the entry point used by internal/steertest/cmd. It speaks a
// bounded newline-delimited JSON-RPC subset over stdin/stdout and keeps all
// deterministic test controls on the private Unix socket.
func RunProcess() error {
	script, err := scriptFromEnvironment()
	if err != nil {
		return err
	}
	endpoint := os.Getenv(controlEndpointEnv)
	if endpoint == "" {
		return errors.New("steertest: control endpoint is required")
	}
	control, err := net.Dial("unix", endpoint)
	if err != nil {
		return err
	}
	state := newChildState(script, control)
	state.emit(Event{Kind: EventReady})
	go state.readCommands()

	reader := bufio.NewReaderSize(os.Stdin, 32<<10)
	for {
		line, err := readBoundedLine(reader, maxControlFrame)
		if err != nil {
			state.closeControl()
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		var request rpcRequest
		if err := json.Unmarshal(line, &request); err != nil || request.Method == "" {
			_ = state.sendRPCError(nil, -32600, "invalid request")
			continue
		}
		switch request.Method {
		case "initialize", "session/new", "session/load", "session/set_config_option", "session/set_mode":
			state.handleSetup(request)
		case "session/prompt":
			go state.handlePrompt(request)
		case "_session/steering":
			go state.handleSteer(request)
		case "session/cancel":
			state.handleCancel(request)
		default:
			_ = state.sendRPCError(request.ID, -32601, "method not found")
		}
		if state.stopped() {
			return nil
		}
	}
}

func scriptFromEnvironment() (Script, error) {
	encoded := os.Getenv(scriptEnv)
	if encoded == "" {
		return Script{}, errors.New("steertest: script is required")
	}
	data, err := base64.RawStdEncoding.DecodeString(encoded)
	if err != nil || len(data) > maxScriptBytes {
		return Script{}, errors.New("steertest: invalid script encoding")
	}
	var script Script
	if err := json.Unmarshal(data, &script); err != nil {
		return Script{}, errors.New("steertest: invalid script JSON")
	}
	return normalizeScript(script)
}

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type childState struct {
	script  Script
	control net.Conn
	stdout  io.Writer

	controlMu sync.Mutex
	stdoutMu  sync.Mutex

	gateMu   sync.Mutex
	gates    map[string]*childGate
	stoppedC chan struct{}
	stopOnce sync.Once

	indexMu    sync.Mutex
	promptNext int
	steerNext  int

	cancelMu   sync.Mutex
	cancelCh   chan struct{}
	cancelOnce sync.Once
}

type childGate struct {
	ch       chan struct{}
	released bool
}

func newChildState(script Script, control net.Conn) *childState {
	return &childState{
		script:   script,
		control:  control,
		stdout:   os.Stdout,
		gates:    make(map[string]*childGate),
		stoppedC: make(chan struct{}),
	}
}

func (s *childState) stopped() bool {
	select {
	case <-s.stoppedC:
		return true
	default:
		return false
	}
}

func (s *childState) readCommands() {
	reader := bufio.NewReaderSize(s.control, 16<<10)
	for {
		line, err := readBoundedLine(reader, maxControlFrame)
		if err != nil {
			s.stop()
			return
		}
		var command controlCommand
		if json.Unmarshal(line, &command) != nil {
			continue
		}
		switch command.Kind {
		case "release":
			s.release(command.Gate)
		case "shutdown":
			s.stop()
			return
		}
	}
}

func (s *childState) gate(name string) *childGate {
	s.gateMu.Lock()
	defer s.gateMu.Unlock()
	gate := s.gates[name]
	if gate == nil {
		gate = &childGate{ch: make(chan struct{})}
		s.gates[name] = gate
	}
	return gate
}

func (s *childState) release(name string) {
	if name == "" {
		return
	}
	s.gateMu.Lock()
	gate := s.gates[name]
	if gate == nil {
		gate = &childGate{ch: make(chan struct{})}
		s.gates[name] = gate
	}
	if !gate.released {
		gate.released = true
		close(gate.ch)
	}
	s.gateMu.Unlock()
}

func (s *childState) waitAction(action Action, method string) bool {
	if action.Gate == "" {
		return !s.stopped()
	}
	s.emit(Event{Kind: EventGate, Gate: action.Gate, Name: action.Name, Method: method})
	gate := s.gate(action.Gate)
	select {
	case <-gate.ch:
		return true
	case <-s.stoppedC:
		return false
	}
}

func (s *childState) emit(event Event) {
	if len(event.Text) > maxEventText {
		event.Text = event.Text[:maxEventText]
	}
	if len(event.Reason) > maxEventText {
		event.Reason = event.Reason[:maxEventText]
	}
	data, err := json.Marshal(event)
	if err != nil || len(data) > maxControlFrame-1 {
		return
	}
	data = append(data, '\n')
	s.controlMu.Lock()
	defer s.controlMu.Unlock()
	_, _ = s.control.Write(data)
}

func (s *childState) sendRPCResponse(id json.RawMessage, result any) error {
	return s.sendRPC(rpcResponse{JSONRPC: "2.0", ID: append(json.RawMessage(nil), id...), Result: result})
}

func (s *childState) sendRPCError(id json.RawMessage, code int, message string) error {
	if len(message) > maxEventText {
		message = message[:maxEventText]
	}
	return s.sendRPC(rpcResponse{JSONRPC: "2.0", ID: append(json.RawMessage(nil), id...), Error: &rpcError{Code: code, Message: message}})
}

func (s *childState) sendRPC(response rpcResponse) error {
	data, err := json.Marshal(response)
	if err != nil || len(data) > maxControlFrame-1 {
		return errors.New("steertest: response too large")
	}
	data = append(data, '\n')
	s.stdoutMu.Lock()
	defer s.stdoutMu.Unlock()
	writer := s.stdout
	if writer == nil {
		writer = os.Stdout
	}
	_, err = writer.Write(data)
	return err
}

func (s *childState) handleSetup(request rpcRequest) {
	switch request.Method {
	case "initialize":
		s.emit(Event{Kind: EventInitialize, Method: request.Method, RequestID: request.ID})
		result := map[string]any{
			"protocolVersion": 1,
			"agentInfo": map[string]any{
				"name":    s.script.AgentName,
				"version": s.script.AgentVersion,
				"title":   s.script.AgentTitle,
			},
			"agentCapabilities": map[string]any{
				"loadSession":        true,
				"promptCapabilities": map[string]bool{"audio": false, "embeddedContext": false, "image": false},
				"mcpCapabilities":    map[string]bool{"http": false, "sse": false},
			},
			"_meta": json.RawMessage(append([]byte(nil), s.script.Metadata...)),
		}
		_ = s.sendRPCResponse(request.ID, result)
	case "session/new":
		var params struct {
			Cwd        string            `json:"cwd"`
			MCPServers []json.RawMessage `json:"mcpServers"`
		}
		_ = json.Unmarshal(request.Params, &params)
		sessionID := s.script.SessionID
		s.emit(Event{Kind: EventNewSession, Method: request.Method, RequestID: request.ID, SessionID: sessionID})
		s.captureMCP(params.MCPServers)
		_ = s.sendRPCResponse(request.ID, s.newSessionResult(sessionID))
	case "session/load":
		var params struct {
			SessionID  string            `json:"sessionId"`
			MCPServers []json.RawMessage `json:"mcpServers"`
		}
		_ = json.Unmarshal(request.Params, &params)
		if params.SessionID == "" {
			params.SessionID = s.script.SessionID
		}
		s.emit(Event{Kind: EventLoadSession, Method: request.Method, RequestID: request.ID, SessionID: params.SessionID})
		s.captureMCP(params.MCPServers)
		_ = s.sendRPCResponse(request.ID, map[string]any{})
	case "session/set_config_option":
		_ = s.sendRPCResponse(request.ID, s.configOptionsResult())
	case "session/set_mode":
		_ = s.sendRPCResponse(request.ID, map[string]any{})
	}
}

func (s *childState) newSessionResult(sessionID string) map[string]any {
	return map[string]any{
		"sessionId":     sessionID,
		"configOptions": s.configOptions(),
		"modes": map[string]any{
			"availableModes": []map[string]any{{"id": "default", "name": "default"}, {"id": "acceptEdits", "name": "acceptEdits"}},
			"currentModeId":  "default",
		},
	}
}

func (s *childState) configOptionsResult() map[string]any {
	return map[string]any{"configOptions": s.configOptions()}
}

func (s *childState) configOptions() []map[string]any {
	values := make([]map[string]string, 0, len(s.script.ModelValues))
	for _, value := range s.script.ModelValues {
		values = append(values, map[string]string{"name": value, "value": value})
	}
	options := make([]map[string]any, 0, 2)
	options = append(options, map[string]any{
		"type": "select", "category": "model", "id": "model", "name": "Model",
		"currentValue": s.script.ModelValues[0], "options": values,
	})
	if len(s.script.EffortValues) > 0 {
		effort := make([]map[string]string, 0, len(s.script.EffortValues))
		for _, value := range s.script.EffortValues {
			effort = append(effort, map[string]string{"name": value, "value": value})
		}
		options = append(options, map[string]any{
			"type": "select", "category": "thought_level", "id": "thought_level", "name": "Effort",
			"currentValue": s.script.EffortValues[0], "options": effort,
		})
	}
	return options
}

func (s *childState) captureMCP(rawServers []json.RawMessage) {
	if len(rawServers) == 0 {
		return
	}
	descriptors := make([]MCPDescriptor, 0, len(rawServers))
	for _, raw := range rawServers {
		var wire struct {
			Name    string           `json:"name"`
			Command string           `json:"command"`
			Args    []string         `json:"args"`
			Env     []MCPEnvironment `json:"env"`
		}
		if json.Unmarshal(raw, &wire) != nil {
			continue
		}
		if len(wire.Name) > 128 || len(wire.Command) > maxEventText {
			continue
		}
		if len(wire.Args) > 128 {
			wire.Args = wire.Args[:128]
		}
		for i := range wire.Args {
			if len(wire.Args[i]) > maxEventText {
				wire.Args[i] = wire.Args[i][:maxEventText]
			}
		}
		if len(wire.Env) > 128 {
			wire.Env = wire.Env[:128]
		}
		for i := range wire.Env {
			if len(wire.Env[i].Name) > 128 {
				wire.Env[i].Name = wire.Env[i].Name[:128]
			}
			if len(wire.Env[i].Value) > maxEventText {
				wire.Env[i].Value = wire.Env[i].Value[:maxEventText]
			}
		}
		descriptors = append(descriptors, MCPDescriptor{
			Name: wire.Name, Command: wire.Command, Args: append([]string(nil), wire.Args...), Env: append([]MCPEnvironment(nil), wire.Env...),
		})
	}
	if len(descriptors) > 0 {
		s.emit(Event{Kind: EventMCPDescriptor, Method: "session/mcp", MCP: descriptors})
	}
}

func (s *childState) handlePrompt(request rpcRequest) {
	var params struct {
		SessionID string `json:"sessionId"`
	}
	_ = json.Unmarshal(request.Params, &params)
	s.emit(Event{Kind: EventPrompt, Method: request.Method, RequestID: request.ID, SessionID: params.SessionID})

	s.indexMu.Lock()
	index := s.promptNext
	s.promptNext++
	s.indexMu.Unlock()
	var script PromptScript
	if index < len(s.script.Prompts) {
		script = s.script.Prompts[index]
	}
	actions := script.actions()
	if len(actions) == 0 {
		actions = []Action{{Kind: ActionTerminal}}
	}
	for _, action := range actions {
		if !s.waitAction(action, request.Method) {
			return
		}
		switch action.Kind {
		case ActionUpdate:
			s.emit(Event{Kind: EventUpdate, Method: "session/update", Name: action.Name, SessionID: params.SessionID, Text: action.Text})
			_ = s.sendNotification("session/update", map[string]any{
				"sessionId": params.SessionID,
				"update": map[string]any{
					"sessionUpdate": "agent_message_chunk",
					"content":       map[string]any{"type": "text", "text": action.Text},
				},
			})
		case ActionSetSessionInfo:
			s.emit(Event{Kind: EventSessionInfo, Method: "session/update", Name: action.Name, SessionID: params.SessionID, Text: action.Text})
		case ActionTerminal:
			var err error
			if action.ErrorCode != 0 {
				err = s.sendRPCError(request.ID, action.ErrorCode, action.ErrorMessage)
			} else {
				stop := action.StopReason
				if stop == "" {
					stop = "end_turn"
				}
				err = s.sendRPCResponse(request.ID, map[string]any{"stopReason": stop})
			}
			if err == nil {
				s.emit(Event{Kind: EventTerminal, Method: request.Method, Name: action.Name, SessionID: params.SessionID, Text: action.Text})
			}
			return
		case ActionTransportLoss:
			s.transportLoss()
			return
		case ActionWait:
			// A gate-only action is useful for aligning a response with another
			// request while keeping the wire quiet.
		}
	}
	// A script containing only updates still terminates deterministically.
	_ = s.sendRPCResponse(request.ID, map[string]any{"stopReason": "end_turn"})
}

func (s *childState) handleSteer(request rpcRequest) {
	var params struct {
		SessionID string `json:"sessionId"`
	}
	_ = json.Unmarshal(request.Params, &params)
	s.emit(Event{Kind: EventSteer, Method: request.Method, RequestID: request.ID, SessionID: params.SessionID})

	s.indexMu.Lock()
	index := s.steerNext
	s.steerNext++
	s.indexMu.Unlock()
	var script SteerScript
	if index < len(s.script.Steers) {
		script = s.script.Steers[index]
	}
	actions := script.actions()
	if len(actions) == 0 {
		actions = []Action{{Kind: ActionSteerReply, Outcome: OutcomeInjected}}
	}
	replied := false
	for _, action := range actions {
		if !s.waitAction(action, request.Method) {
			return
		}
		switch action.Kind {
		case ActionSteerReply:
			replied = true
			outcome := action.Outcome
			if outcome == "" {
				outcome = OutcomeInjected
			}
			var err error
			if action.ErrorCode != 0 {
				err = s.sendRPCError(request.ID, action.ErrorCode, action.ErrorMessage)
			} else {
				err = s.sendRPCResponse(request.ID, map[string]any{"outcome": outcome, "reason": action.Reason})
			}
			if err == nil {
				s.emit(Event{Kind: EventSteer, Method: request.Method, Name: action.Name, SessionID: params.SessionID, Outcome: outcome, Reason: action.Reason})
			}
		case ActionTransportLoss:
			s.transportLoss()
			return
		case ActionWait:
		}
		if replied {
			return
		}
	}
	if !replied {
		_ = s.sendRPCResponse(request.ID, map[string]any{"outcome": OutcomeInjected})
	}
}

func (s *childState) handleCancel(request rpcRequest) {
	var params struct {
		SessionID string `json:"sessionId"`
	}
	_ = json.Unmarshal(request.Params, &params)
	s.emit(Event{Kind: EventSessionCanceled, Method: request.Method, SessionID: params.SessionID})
	s.cancelMu.Lock()
	if s.cancelCh != nil {
		s.cancelOnce.Do(func() { close(s.cancelCh) })
	}
	s.cancelMu.Unlock()
}

func (s *childState) sendNotification(method string, params any) error {
	data, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "method": method, "params": params})
	if err != nil || len(data) > maxControlFrame-1 {
		return errors.New("steertest: notification too large")
	}
	data = append(data, '\n')
	s.stdoutMu.Lock()
	defer s.stdoutMu.Unlock()
	writer := s.stdout
	if writer == nil {
		writer = os.Stdout
	}
	_, err = writer.Write(data)
	return err
}

func (s *childState) transportLoss() {
	s.emit(Event{Kind: EventTransportLoss, Method: "transport"})
	s.stop()
	_ = s.control.Close()
	_ = os.Stdout.Close()
	_ = os.Stdin.Close()
}

func (s *childState) closeControl() {
	s.stop()
	_ = s.control.Close()
}

func (s *childState) stop() {
	s.stopOnce.Do(func() { close(s.stoppedC) })
}
