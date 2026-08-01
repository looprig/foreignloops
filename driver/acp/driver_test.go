package acp

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/looprig/acp/client"
	"github.com/looprig/acp/launch"
	"github.com/looprig/acp/protocol"
	"github.com/looprig/foreignloops/driver"
	"github.com/looprig/harness/pkg/loop"
)

type fakeSession struct {
	id protocol.SessionID

	operations []string
	configErr  error
	modeErr    error
}

func (s *fakeSession) ID() protocol.SessionID { return s.id }

func (s *fakeSession) ConfigOptions() []protocol.SessionConfigOption { return nil }

func (s *fakeSession) Modes() *protocol.SessionModeState {
	return &protocol.SessionModeState{
		AvailableModes: []protocol.SessionMode{
			{ID: "default"},
			{ID: "acceptEdits"},
		},
	}
}

func (s *fakeSession) SetConfigOption(_ context.Context, configID protocol.SessionConfigID, valueID protocol.SessionConfigValueID) error {
	s.operations = append(s.operations, "set_config:"+string(configID)+"="+string(valueID))
	return s.configErr
}

func (s *fakeSession) SetMode(_ context.Context, modeID protocol.SessionModeID) error {
	s.operations = append(s.operations, "set_mode:"+string(modeID))
	return s.modeErr
}

type fakeClient struct {
	newSession  session
	loadSession session
	loadErr     error

	newCalls   int
	loadCalls  int
	newParams  []client.NewSessionParams
	loadParams []client.LoadSessionParams
}

func (c *fakeClient) NewSession(_ context.Context, p client.NewSessionParams) (session, error) {
	c.newCalls++
	c.newParams = append(c.newParams, p)
	return c.newSession, nil
}

func (c *fakeClient) LoadSession(_ context.Context, p client.LoadSessionParams) (session, error) {
	c.loadCalls++
	c.loadParams = append(c.loadParams, p)
	if c.loadErr != nil {
		return nil, c.loadErr
	}
	return c.loadSession, nil
}

type newOnlyClient struct {
	session session
	calls   int
}

func (c *newOnlyClient) NewSession(_ context.Context, _ client.NewSessionParams) (session, error) {
	c.calls++
	return c.session, nil
}

type fakeDialedClient struct {
	acpClient  acpClient
	closeErr   error
	closeCalls int
	closeHook  func()
}

func (c *fakeDialedClient) client() acpClient { return c.acpClient }

func (c *fakeDialedClient) close(context.Context) error {
	c.closeCalls++
	if c.closeHook != nil {
		c.closeHook()
	}
	return c.closeErr
}

type driverContextSession struct {
	*fakeSession

	updates        chan client.Update
	promptContexts chan context.Context
	promptDone     chan error

	blockOnCancel bool
	cancelled     chan struct{}
	allowReturn   chan struct{}
	cancelOnce    sync.Once
}

func newDriverContextSession(id string) *driverContextSession {
	return &driverContextSession{
		fakeSession:    newFakeSession(id),
		updates:        make(chan client.Update),
		promptContexts: make(chan context.Context, 2),
		promptDone:     make(chan error, 2),
	}
}

func (s *driverContextSession) Updates() <-chan client.Update { return s.updates }

func (s *driverContextSession) Prompt(ctx context.Context, _ []protocol.ContentBlock) (*client.PromptResult, error) {
	s.promptContexts <- ctx
	if s.blockOnCancel {
		select {
		case <-ctx.Done():
			s.cancelOnce.Do(func() { close(s.cancelled) })
			<-s.allowReturn
		case <-s.allowReturn:
		}
	}
	if err := ctx.Err(); err != nil {
		s.promptDone <- err
		return nil, err
	}
	s.promptDone <- nil
	return &client.PromptResult{StopReason: protocol.StopReasonEndTurn}, nil
}

func (s *driverContextSession) Cancel(ctx context.Context) error { return ctx.Err() }

type fakeClaudeConnector struct {
	models launch.ClaudeModels
	fail   error
}

func (c *fakeClaudeConnector) SelectDefaultModel(ctx context.Context, sess session) error {
	if c.fail != nil {
		return c.fail
	}
	return sess.SetConfigOption(ctx, "model", protocol.SessionConfigValueID(c.models.Default))
}

func (c *fakeClaudeConnector) SelectSmallModel(ctx context.Context, sess session) error {
	if c.fail != nil {
		return c.fail
	}
	return sess.SetConfigOption(ctx, "model", protocol.SessionConfigValueID(c.models.Small))
}

func (c *fakeClaudeConnector) ApplyPermissionMode(ctx context.Context, sess session, modeID protocol.SessionModeID) error {
	if c.fail != nil {
		return c.fail
	}
	return sess.SetMode(ctx, modeID)
}

func installDial(t *testing.T, fn dialFunc) {
	t.Helper()
	previous := dial
	dial = fn
	t.Cleanup(func() { dial = previous })
}

func installClaudeConnectorFactory(t *testing.T, fn claudeConnectorFactory) {
	t.Helper()
	previous := newClaudeConnector
	newClaudeConnector = fn
	t.Cleanup(func() { newClaudeConnector = previous })
}

func newFakeSession(id string) *fakeSession {
	return &fakeSession{id: protocol.SessionID(id)}
}

func TestNewValidatesBeforeDial(t *testing.T) {
	cfg := validConfig(HarnessCodex)
	cfg.ModelAlias = ""

	var dialCalls int
	installDial(t, func(context.Context, launch.Config) (dialedClient, error) {
		dialCalls++
		return nil, errors.New("dial should not run")
	})

	if _, err := New(context.Background(), cfg); err == nil {
		t.Fatal("New() error = nil, want validation error")
	}
	if dialCalls != 0 {
		t.Fatalf("dial called %d times, want 0 for invalid config", dialCalls)
	}
}

func TestNewClaudeCreatesOneSessionAppliesModelsAndPosture(t *testing.T) {
	cfg := validConfig(HarnessClaudeCode)
	cfg.AgentSessionID = ""
	sess := newFakeSession("assigned-claude-session")
	conn := &fakeClient{newSession: sess}
	owned := &fakeDialedClient{acpClient: conn}
	var launchCfg launch.Config
	models := launch.ClaudeModels{Default: cfg.ModelAlias, Small: cfg.SmallModelAlias}

	installClaudeConnectorFactory(t, func(got launch.ClaudeModels) claudeConnector {
		if got != models {
			t.Fatalf("Claude models = %+v, want %+v", got, models)
		}
		return &fakeClaudeConnector{models: got}
	})
	installDial(t, func(_ context.Context, got launch.Config) (dialedClient, error) {
		launchCfg = got
		return owned, nil
	})

	d, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if conn.newCalls != 1 || conn.loadCalls != 0 {
		t.Fatalf("session calls = new:%d load:%d, want new:1 load:0", conn.newCalls, conn.loadCalls)
	}
	if got, want := sess.operations, []string{
		"set_config:model=" + cfg.ModelAlias,
		"set_config:model=" + cfg.SmallModelAlias,
		"set_mode:acceptEdits",
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("session operations = %v, want %v", got, want)
	}
	if got := d.AgentSessionID(); got != string(sess.id) {
		t.Fatalf("AgentSessionID() = %q, want %q", got, sess.id)
	}

	if launchCfg.SharedProxy == nil {
		t.Fatal("launch config SharedProxy = nil, want borrowed binding")
	}
	if got := *launchCfg.SharedProxy; got != cfg.Binding {
		t.Fatalf("launch SharedProxy = %+v, want %+v", got, cfg.Binding)
	}
	harness, ok := launchCfg.Harness.(*launch.ClaudeConnector)
	if !ok {
		t.Fatalf("launch Harness = %T, want *launch.ClaudeConnector", launchCfg.Harness)
	}
	if harness.Models != models {
		t.Fatalf("launch Claude models = %+v, want %+v", harness.Models, models)
	}
	if got := launchCfg.Command.Path; got != cfg.Executable {
		t.Fatalf("launch command path = %q, want %q", got, cfg.Executable)
	}
	if got := launchCfg.Command.Dir; got != cfg.WorkspaceRoot {
		t.Fatalf("launch command dir = %q, want %q", got, cfg.WorkspaceRoot)
	}

	if err := d.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if owned.closeCalls != 1 {
		t.Fatalf("owned client close calls = %d, want 1", owned.closeCalls)
	}
}

func TestNewCodexCreatesOneSessionWithModelAndPosture(t *testing.T) {
	cfg := validConfig(HarnessCodex)
	cfg.AgentSessionID = ""
	cfg.Posture = driver.PostureReadOnly
	conn := &fakeClient{newSession: newFakeSession("assigned-codex-session")}
	owned := &fakeDialedClient{acpClient: conn}
	var launchCfg launch.Config

	installDial(t, func(_ context.Context, got launch.Config) (dialedClient, error) {
		launchCfg = got
		return owned, nil
	})

	d, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if conn.newCalls != 1 || conn.loadCalls != 0 {
		t.Fatalf("session calls = new:%d load:%d, want new:1 load:0", conn.newCalls, conn.loadCalls)
	}
	harness, ok := launchCfg.Harness.(*launch.CodexConnector)
	if !ok {
		t.Fatalf("launch Harness = %T, want *launch.CodexConnector", launchCfg.Harness)
	}
	if harness.Model != cfg.ModelAlias {
		t.Fatalf("Codex model = %q, want %q", harness.Model, cfg.ModelAlias)
	}
	wantPosture := launch.CodexPosture{
		ApprovalPolicy:       "never",
		SandboxMode:          "read-only",
		SandboxNetworkAccess: true,
	}
	if harness.Posture != wantPosture {
		t.Fatalf("Codex posture = %+v, want %+v", harness.Posture, wantPosture)
	}
	if launchCfg.SharedProxy == nil {
		t.Fatal("launch config SharedProxy = nil, want gateway binding")
	}
	if got := *launchCfg.SharedProxy; got != cfg.Binding {
		t.Fatalf("launch SharedProxy = %+v, want %+v", got, cfg.Binding)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestNewCodexWorkspaceWritePreservesSandboxPosture(t *testing.T) {
	cfg := validConfig(HarnessCodex)
	cfg.AgentSessionID = ""
	cfg.Posture = driver.PostureWorkspaceWrite
	conn := &fakeClient{newSession: newFakeSession("workspace-write-session")}
	owned := &fakeDialedClient{acpClient: conn}
	var launchCfg launch.Config
	installDial(t, func(_ context.Context, got launch.Config) (dialedClient, error) {
		launchCfg = got
		return owned, nil
	})

	d, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer func() { _ = d.Close() }()
	harness, ok := launchCfg.Harness.(*launch.CodexConnector)
	if !ok {
		t.Fatalf("launch Harness = %T, want *launch.CodexConnector", launchCfg.Harness)
	}
	want := launch.CodexPosture{
		ApprovalPolicy:       "never",
		SandboxMode:          "workspace-write",
		SandboxNetworkAccess: true,
	}
	if harness.Posture != want {
		t.Fatalf("Codex posture = %+v, want %+v", harness.Posture, want)
	}
}

func TestNewNativeAuthOmitsProxyAndGatewayOverrides(t *testing.T) {
	for _, harnessName := range []Harness{HarnessClaudeCode, HarnessCodex} {
		t.Run(string(harnessName), func(t *testing.T) {
			cfg := validConfig(harnessName)
			cfg.Credential = loop.CredentialNativeAuth
			cfg.Binding = launch.ProxyBinding{}
			cfg.AgentSessionID = ""
			originalEnv := append([]string(nil), cfg.Env...)
			sess := newFakeSession("native-auth-session")
			conn := &fakeClient{newSession: sess}
			owned := &fakeDialedClient{acpClient: conn}
			var launchCfg launch.Config
			if harnessName == HarnessClaudeCode {
				installClaudeConnectorFactory(t, func(models launch.ClaudeModels) claudeConnector {
					return &fakeClaudeConnector{models: models}
				})
			}

			installDial(t, func(_ context.Context, got launch.Config) (dialedClient, error) {
				launchCfg = got
				return owned, nil
			})

			d, err := New(context.Background(), cfg)
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			defer func() { _ = d.Close() }()

			if launchCfg.SharedProxy != nil {
				t.Fatal("native launch config SharedProxy != nil, want nil")
			}
			if !launchCfg.NoProxy {
				t.Fatal("native launch config NoProxy = false, want true")
			}
			if !reflect.DeepEqual(launchCfg.Command.Env, originalEnv) {
				t.Fatalf("native launch env = %#v, want caller env %#v without gateway additions", launchCfg.Command.Env, originalEnv)
			}

			for _, entry := range launchCfg.Command.Env {
				if strings.HasPrefix(entry, "ANTHROPIC_BASE_URL=") ||
					strings.HasPrefix(entry, "ANTHROPIC_AUTH_TOKEN=") ||
					strings.HasPrefix(entry, "LOOPRIG_PROXY_TOKEN=") {
					t.Errorf("native configured env contains gateway entry %q", entry)
				}
			}
			if harnessName == HarnessCodex {
				harness, ok := launchCfg.Harness.(*launch.CodexConnector)
				if !ok {
					t.Fatalf("native launch Harness = %T, want *launch.CodexConnector", launchCfg.Harness)
				}
				if harness.Posture.SandboxNetworkAccess {
					t.Fatal("native Codex posture SandboxNetworkAccess = true, want false")
				}
			}
		})
	}
}

func TestNewCallerContextCancellationDoesNotPoisonLaterTurn(t *testing.T) {
	cfg := validConfig(HarnessCodex)
	cfg.AgentSessionID = ""
	setupCtx, cancelSetup := context.WithCancel(context.Background())
	sess := newDriverContextSession("context-owned-session")
	conn := &fakeClient{newSession: sess}
	owned := &fakeDialedClient{acpClient: conn}
	installDial(t, func(context.Context, launch.Config) (dialedClient, error) {
		return owned, nil
	})

	d, err := New(setupCtx, cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer func() { _ = d.Close() }()
	cancelSetup()

	stream, err := d.Spawn(context.Background(), driver.Turn{})
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}
	promptCtx := <-sess.promptContexts
	if err := promptCtx.Err(); err != nil {
		t.Fatalf("later-turn Prompt context error = %v, want nil after setup context cancellation", err)
	}
	events := collectTurnEvents(t, stream)
	if len(events) == 0 || events[len(events)-1].Kind != driver.KindTerminalOK {
		t.Fatalf("later-turn events = %#v, want successful terminal event", events)
	}
}

func TestCloseCancelsActivePromptBeforeClosingOwnedClient(t *testing.T) {
	cfg := validConfig(HarnessCodex)
	cfg.AgentSessionID = ""
	sess := newDriverContextSession("close-session")
	sess.blockOnCancel = true
	sess.cancelled = make(chan struct{})
	sess.allowReturn = make(chan struct{})
	conn := &fakeClient{newSession: sess}
	ownedClosed := make(chan struct{})
	owned := &fakeDialedClient{
		acpClient: conn,
		closeHook: func() { close(ownedClosed) },
	}
	installDial(t, func(context.Context, launch.Config) (dialedClient, error) {
		return owned, nil
	})

	d, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	stream, err := d.Spawn(context.Background(), driver.Turn{})
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}
	<-sess.promptContexts

	closeDone := make(chan error, 1)
	go func() { closeDone <- d.Close() }()

	select {
	case <-sess.cancelled:
	case <-time.After(3 * time.Second):
		close(sess.allowReturn)
		<-sess.promptDone
		<-closeDone
		t.Fatal("Close() did not cancel the active Prompt context")
	}
	select {
	case <-ownedClosed:
		close(sess.allowReturn)
		<-sess.promptDone
		<-closeDone
		t.Fatal("Close() closed the owned client before the active Prompt returned")
	case <-time.After(50 * time.Millisecond):
	}

	close(sess.allowReturn)
	if err := <-closeDone; err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if owned.closeCalls != 1 {
		t.Fatalf("owned client close calls = %d, want 1", owned.closeCalls)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if err := <-sess.promptDone; err != context.Canceled {
		t.Fatalf("Prompt() error = %v, want context.Canceled", err)
	}
	waitTurnDone(t, stream)
}

type restoredClaudeSession struct {
	*fakeSession
	setConfigCalls int
	setModeCalls   int
}

func (s *restoredClaudeSession) ConfigOptions() []protocol.SessionConfigOption { return nil }

func (s *restoredClaudeSession) Modes() *protocol.SessionModeState { return nil }

func (s *restoredClaudeSession) SetConfigOption(context.Context, protocol.SessionConfigID, protocol.SessionConfigValueID) error {
	s.setConfigCalls++
	return nil
}

func (s *restoredClaudeSession) SetMode(context.Context, protocol.SessionModeID) error {
	s.setModeCalls++
	return nil
}

func TestNewRestoredClaudePreservesSessionWithoutConfigCapabilities(t *testing.T) {
	cfg := validConfig(HarnessClaudeCode)
	cfg.AgentSessionID = "restored-claude"
	sess := &restoredClaudeSession{fakeSession: newFakeSession(cfg.AgentSessionID)}
	conn := &fakeClient{loadSession: sess}
	owned := &fakeDialedClient{acpClient: conn}
	installDial(t, func(context.Context, launch.Config) (dialedClient, error) {
		return owned, nil
	})

	d, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("New() error = %v, want restored session to load without mutable capabilities", err)
	}
	defer func() { _ = d.Close() }()
	if conn.loadCalls != 1 || conn.newCalls != 0 {
		t.Fatalf("session calls = new:%d load:%d, want new:0 load:1", conn.newCalls, conn.loadCalls)
	}
	if sess.setConfigCalls != 0 || sess.setModeCalls != 0 {
		t.Fatalf("restored session mutations = config:%d mode:%d, want zero", sess.setConfigCalls, sess.setModeCalls)
	}
}

func TestNewResumesWithLoadInsteadOfNew(t *testing.T) {
	cfg := validConfig(HarnessCodex)
	cfg.AgentSessionID = "resume-me"
	sess := newFakeSession(cfg.AgentSessionID)
	conn := &fakeClient{loadSession: sess}
	owned := &fakeDialedClient{acpClient: conn}

	installDial(t, func(context.Context, launch.Config) (dialedClient, error) {
		return owned, nil
	})

	d, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if conn.loadCalls != 1 || conn.newCalls != 0 {
		t.Fatalf("session calls = new:%d load:%d, want new:0 load:1", conn.newCalls, conn.loadCalls)
	}
	if got := conn.loadParams[0].SessionID; got != protocol.SessionID(cfg.AgentSessionID) {
		t.Fatalf("LoadSession SessionID = %q, want %q", got, cfg.AgentSessionID)
	}
	if got := d.AgentSessionID(); got != cfg.AgentSessionID {
		t.Fatalf("AgentSessionID() = %q, want %q", got, cfg.AgentSessionID)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestNewFailsClosedWhenLoadCapabilityIsMissing(t *testing.T) {
	cfg := validConfig(HarnessCodex)
	cfg.AgentSessionID = "resume-me"
	conn := &newOnlyClient{session: newFakeSession("unexpected-new-session")}
	owned := &fakeDialedClient{acpClient: conn}

	installDial(t, func(context.Context, launch.Config) (dialedClient, error) {
		return owned, nil
	})

	if _, err := New(context.Background(), cfg); err == nil {
		t.Fatal("New() error = nil, want missing load capability error")
	}
	if conn.calls != 0 {
		t.Fatalf("NewSession calls = %d, want 0 when resuming without load capability", conn.calls)
	}
	if owned.closeCalls != 1 {
		t.Fatalf("owned client close calls = %d, want 1", owned.closeCalls)
	}
}

func TestNewWrapsClaudeModelAliasErrorAndClosesOwnedClient(t *testing.T) {
	cfg := validConfig(HarnessClaudeCode)
	cfg.AgentSessionID = ""
	sess := newFakeSession("assigned-claude-session")
	conn := &fakeClient{newSession: sess}
	owned := &fakeDialedClient{acpClient: conn}
	aliasErr := &launch.ModelAliasError{Alias: cfg.ModelAlias}

	installClaudeConnectorFactory(t, func(launch.ClaudeModels) claudeConnector {
		return &fakeClaudeConnector{fail: aliasErr}
	})
	installDial(t, func(context.Context, launch.Config) (dialedClient, error) {
		return owned, nil
	})

	_, err := New(context.Background(), cfg)
	if err == nil {
		t.Fatal("New() error = nil, want model alias error")
	}
	var gotAliasErr *launch.ModelAliasError
	if !errors.As(err, &gotAliasErr) {
		t.Fatalf("New() error = %v (%T), want wrapped *launch.ModelAliasError", err, err)
	}
	if gotAliasErr != aliasErr {
		t.Fatalf("wrapped alias error = %p, want original %p", gotAliasErr, aliasErr)
	}
	if owned.closeCalls != 1 {
		t.Fatalf("owned client close calls = %d, want 1 after construction failure", owned.closeCalls)
	}
}
