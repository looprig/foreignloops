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
	"github.com/looprig/acp/transport/stdio"
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
	models     launch.ClaudeModels
	effort     string
	fail       error
	failEffort error
}

type fakeCodexConnector struct {
	model      string
	effort     string
	calls      []string
	failModel  error
	failEffort error
}

func (c *fakeCodexConnector) SelectModel(ctx context.Context, sess session) error {
	c.calls = append(c.calls, "model="+c.model)
	if c.failModel != nil {
		return c.failModel
	}
	return sess.SetConfigOption(ctx, "model", protocol.SessionConfigValueID(c.model))
}

func (c *fakeCodexConnector) SelectEffort(ctx context.Context, sess session) error {
	c.calls = append(c.calls, "effort="+c.effort)
	if c.failEffort != nil {
		return c.failEffort
	}
	return sess.SetConfigOption(ctx, "thought_level", protocol.SessionConfigValueID(c.effort))
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

func (c *fakeClaudeConnector) SelectEffort(ctx context.Context, sess session) error {
	if c.failEffort != nil {
		return c.failEffort
	}
	if c.effort == "" {
		return nil
	}
	return sess.SetConfigOption(ctx, "thought_level", protocol.SessionConfigValueID(c.effort))
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

func installNativeDial(t *testing.T, fn nativeDialFunc) {
	t.Helper()
	previous := nativeDial
	nativeDial = fn
	t.Cleanup(func() { nativeDial = previous })
}

func installClaudeConnectorFactory(t *testing.T, fn claudeConnectorFactory) {
	t.Helper()
	previous := newClaudeConnector
	newClaudeConnector = fn
	t.Cleanup(func() { newClaudeConnector = previous })
}

func installCodexConnectorFactory(t *testing.T, fn codexConnectorFactory) {
	t.Helper()
	previous := newCodexConnector
	newCodexConnector = fn
	t.Cleanup(func() { newCodexConnector = previous })
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

type constructionContextKey struct{}

func TestNewUsesDriverOwnedContextForDial(t *testing.T) {
	cfg := validConfig(HarnessCodex)
	cfg.AgentSessionID = ""
	callerBase := context.WithValue(context.Background(), constructionContextKey{}, "preserved-value")
	callerCtx, cancelCaller := context.WithTimeout(callerBase, time.Hour)
	defer cancelCaller()

	conn := &fakeClient{newSession: newFakeSession("construction-context-session")}
	owned := &fakeDialedClient{acpClient: conn}
	var dialCtx context.Context
	installDial(t, func(ctx context.Context, _ launch.Config) (dialedClient, error) {
		dialCtx = ctx
		return owned, nil
	})

	d, err := New(callerCtx, cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if dialCtx == nil {
		t.Fatal("dial context = nil, want driver-owned context")
	}
	if dialCtx == callerCtx {
		t.Fatal("dial received caller context directly")
	}
	if got := dialCtx.Value(constructionContextKey{}); got != "preserved-value" {
		t.Fatalf("dial context value = %v, want preserved caller value", got)
	}
	if _, ok := dialCtx.Deadline(); ok {
		t.Fatal("dial context unexpectedly retained caller deadline")
	}
	cancelCaller()
	if err := dialCtx.Err(); err != nil {
		t.Fatalf("dial context error after caller cancellation = %v, want nil", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := dialCtx.Err(); err != context.Canceled {
		t.Fatalf("dial context error after driver Close = %v, want context.Canceled", err)
	}
}

func TestNewCancelsDriverOwnedContextWhenDialFails(t *testing.T) {
	cfg := validConfig(HarnessCodex)
	cfg.AgentSessionID = ""
	var dialCtx context.Context
	installDial(t, func(ctx context.Context, _ launch.Config) (dialedClient, error) {
		dialCtx = ctx
		return nil, errors.New("dial failed")
	})

	if _, err := New(context.Background(), cfg); err == nil {
		t.Fatal("New() error = nil, want dial error")
	}
	if dialCtx == nil {
		t.Fatal("dial context = nil, want captured driver-owned context")
	}
	if err := dialCtx.Err(); err != context.Canceled {
		t.Fatalf("dial context error after construction failure = %v, want context.Canceled", err)
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

	installClaudeConnectorFactory(t, func(got launch.ClaudeModels, _ string) claudeConnector {
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

func TestNewNativeCodexReceivesModelAndEffortSeparately(t *testing.T) {
	cfg := validConfig(HarnessCodex)
	cfg.Credential = loop.CredentialNativeAuth
	cfg.Binding = launch.ProxyBinding{}
	cfg.AgentSessionID = ""
	cfg.ModelAlias = "adapter/model[effort]"
	cfg.Effort = "high"
	originalEnv := append([]string(nil), cfg.Env...)
	conn := &fakeClient{newSession: newFakeSession("native-codex-selection")}
	owned := &fakeDialedClient{acpClient: conn}
	selector := &fakeCodexConnector{model: cfg.ModelAlias, effort: cfg.Effort}
	installCodexConnectorFactory(t, func(model, effort string) codexConnector {
		if model != cfg.ModelAlias || effort != cfg.Effort {
			t.Fatalf("Codex selector config = (%q, %q), want (%q, %q)", model, effort, cfg.ModelAlias, cfg.Effort)
		}
		return selector
	})
	var nativeCfg launch.NativeConfig
	installNativeDial(t, func(_ context.Context, got launch.NativeConfig) (dialedClient, error) {
		nativeCfg = got
		return owned, nil
	})

	d, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer func() { _ = d.Close() }()

	native := nativeCfg.Harness
	harness, ok := native.(*launch.CodexConnector)
	if !ok {
		t.Fatalf("native launch Harness = %T, want *launch.CodexConnector", native)
	}
	if harness.Model != cfg.ModelAlias {
		t.Fatalf("Codex model = %q, want unparsed %q", harness.Model, cfg.ModelAlias)
	}
	if harness.Effort != cfg.Effort {
		t.Fatalf("Codex effort = %q, want %q", harness.Effort, cfg.Effort)
	}
	if !reflect.DeepEqual(nativeCfg.Command.Env, originalEnv) {
		t.Fatalf("native launch env = %#v, want original env %#v", nativeCfg.Command.Env, originalEnv)
	}
	if !reflect.DeepEqual(selector.calls, []string{"model=" + cfg.ModelAlias, "effort=" + cfg.Effort}) {
		t.Fatalf("native Codex selection calls = %v, want model then effort", selector.calls)
	}
	configured, err := harness.ConfigureNative(stdio.Command{Path: cfg.Executable})
	if err != nil {
		t.Fatalf("ConfigureNative() error = %v", err)
	}
	for _, arg := range configured.Args {
		if strings.HasPrefix(arg, "model=") || strings.Contains(arg, "effort") {
			t.Fatalf("native Codex selector leaked into argv: %#v", configured.Args)
		}
	}
	for _, value := range nativeCfg.Command.Env {
		if value == "EFFORT="+cfg.Effort || value == "MODEL_EFFORT="+cfg.Effort {
			t.Fatalf("native launch env contains effort selector %q: %#v", value, nativeCfg.Command.Env)
		}
	}
}

func TestNewNativeCodexModelOnlyUsesLegacyWithModel(t *testing.T) {
	cfg := validConfig(HarnessCodex)
	cfg.Credential = loop.CredentialNativeAuth
	cfg.Binding = launch.ProxyBinding{}
	cfg.AgentSessionID = ""
	cfg.ModelAlias = "legacy-native-model"
	cfg.Effort = ""
	conn := &fakeClient{newSession: newFakeSession("native-codex-model-only")}
	owned := &fakeDialedClient{acpClient: conn}
	selector := &fakeCodexConnector{model: cfg.ModelAlias}
	installCodexConnectorFactory(t, func(model, effort string) codexConnector {
		if model != cfg.ModelAlias || effort != "" {
			t.Fatalf("Codex selector config = (%q, %q), want (%q, empty)", model, effort, cfg.ModelAlias)
		}
		return selector
	})
	var nativeCfg launch.NativeConfig
	installNativeDial(t, func(_ context.Context, got launch.NativeConfig) (dialedClient, error) {
		nativeCfg = got
		return owned, nil
	})

	d, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("New() error = %v, want model-only native launch", err)
	}
	defer func() { _ = d.Close() }()

	harness, ok := nativeCfg.Harness.(*launch.CodexConnector)
	if !ok {
		t.Fatalf("native launch Harness = %T, want *launch.CodexConnector", nativeCfg.Harness)
	}
	configured, err := harness.ConfigureNative(stdio.Command{Path: cfg.Executable})
	if err != nil {
		t.Fatalf("ConfigureNative() error = %v, want legacy model-only connector", err)
	}
	if got := configured.Args; len(got) == 0 {
		t.Fatalf("ConfigureNative() args = %#v, want model override", got)
	}
	if harness.Model != cfg.ModelAlias || harness.Effort != "" {
		t.Fatalf("Codex connector = %+v, want model %q and empty effort", harness, cfg.ModelAlias)
	}
	if !reflect.DeepEqual(selector.calls, []string{"model=" + cfg.ModelAlias}) {
		t.Fatalf("native Codex model-only selection calls = %v, want model only", selector.calls)
	}
}

func TestNewNativeCodexSessionSelection(t *testing.T) {
	cfg := validConfig(HarnessCodex)
	cfg.Credential = loop.CredentialNativeAuth
	cfg.Binding = launch.ProxyBinding{}
	cfg.AgentSessionID = ""
	cfg.ModelAlias = "gpt-5.6-luna"
	cfg.Effort = "max"
	sess := newFakeSession("native-codex-fresh-selection")
	conn := &fakeClient{newSession: sess}
	owned := &fakeDialedClient{acpClient: conn}
	selector := &fakeCodexConnector{model: cfg.ModelAlias, effort: cfg.Effort}
	installCodexConnectorFactory(t, func(model, effort string) codexConnector {
		if model != cfg.ModelAlias || effort != cfg.Effort {
			t.Fatalf("Codex selector config = (%q, %q), want (%q, %q)", model, effort, cfg.ModelAlias, cfg.Effort)
		}
		return selector
	})
	installNativeDial(t, func(_ context.Context, _ launch.NativeConfig) (dialedClient, error) {
		return owned, nil
	})

	d, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer func() { _ = d.Close() }()

	wantCalls := []string{"model=" + cfg.ModelAlias, "effort=" + cfg.Effort}
	if !reflect.DeepEqual(selector.calls, wantCalls) {
		t.Fatalf("fresh Codex selection calls = %v, want ordered %v", selector.calls, wantCalls)
	}
}

func TestRestoreNativeCodexSessionSelection(t *testing.T) {
	cfg := validConfig(HarnessCodex)
	cfg.Credential = loop.CredentialNativeAuth
	cfg.Binding = launch.ProxyBinding{}
	cfg.AgentSessionID = "restored-codex-selection"
	cfg.ModelAlias = "gpt-5.6-luna"
	cfg.Effort = "max"
	sess := newFakeSession(cfg.AgentSessionID)
	conn := &fakeClient{loadSession: sess}
	owned := &fakeDialedClient{acpClient: conn}
	selector := &fakeCodexConnector{model: cfg.ModelAlias, effort: cfg.Effort}
	installCodexConnectorFactory(t, func(model, effort string) codexConnector {
		if model != cfg.ModelAlias || effort != cfg.Effort {
			t.Fatalf("Codex selector config = (%q, %q), want (%q, %q)", model, effort, cfg.ModelAlias, cfg.Effort)
		}
		return selector
	})
	installNativeDial(t, func(_ context.Context, _ launch.NativeConfig) (dialedClient, error) {
		return owned, nil
	})

	d, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer func() { _ = d.Close() }()
	if conn.newCalls != 0 || conn.loadCalls != 1 {
		t.Fatalf("session calls = new:%d load:%d, want new:0 load:1", conn.newCalls, conn.loadCalls)
	}

	wantCalls := []string{"model=" + cfg.ModelAlias, "effort=" + cfg.Effort}
	if !reflect.DeepEqual(selector.calls, wantCalls) {
		t.Fatalf("restored Codex selection calls = %v, want ordered %v", selector.calls, wantCalls)
	}
}

func TestNewNativeCodexSelectionUsesAdapterFacingModelID(t *testing.T) {
	const friendlyAlias = "luna"
	const adapterModelID = "gpt-5.6-luna"
	cfg := validConfig(HarnessCodex)
	cfg.Credential = loop.CredentialNativeAuth
	cfg.Binding = launch.ProxyBinding{}
	cfg.AgentSessionID = ""
	cfg.ModelAlias = adapterModelID // Carbon has already resolved friendlyAlias.
	cfg.Effort = "none"
	sess := newFakeSession("native-codex-adapter-model-id")
	conn := &fakeClient{newSession: sess}
	owned := &fakeDialedClient{acpClient: conn}
	selector := &fakeCodexConnector{model: adapterModelID}
	installCodexConnectorFactory(t, func(model, effort string) codexConnector {
		if model != adapterModelID {
			t.Fatalf("Codex selector model = %q, want adapter-facing ID %q (friendly alias %q)", model, adapterModelID, friendlyAlias)
		}
		if effort != "" {
			t.Fatalf("Codex selector effort = %q, want empty for none", effort)
		}
		return selector
	})
	installNativeDial(t, func(_ context.Context, _ launch.NativeConfig) (dialedClient, error) {
		return owned, nil
	})

	d, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer func() { _ = d.Close() }()

	wantCalls := []string{"model=" + adapterModelID}
	if !reflect.DeepEqual(selector.calls, wantCalls) {
		t.Fatalf("Codex model-only selection calls = %v, want %v", selector.calls, wantCalls)
	}
}

func TestCodexManagedEmptySelectorsDoNotCallSelector(t *testing.T) {
	cfg := validConfig(HarnessCodex)
	cfg.Credential = loop.CredentialNativeAuth
	cfg.Binding = launch.ProxyBinding{}
	cfg.AgentSessionID = ""
	cfg.ModelAlias = ""
	cfg.Effort = ""
	sess := newFakeSession("native-codex-managed")
	conn := &fakeClient{newSession: sess}
	owned := &fakeDialedClient{acpClient: conn}
	selector := &fakeCodexConnector{}
	installCodexConnectorFactory(t, func(model, effort string) codexConnector {
		if model != "" || effort != "" {
			t.Fatalf("managed Codex selector config = (%q, %q), want empty pair", model, effort)
		}
		return selector
	})
	installNativeDial(t, func(_ context.Context, _ launch.NativeConfig) (dialedClient, error) {
		return owned, nil
	})

	d, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer func() { _ = d.Close() }()
	if len(selector.calls) != 0 {
		t.Fatalf("managed Codex selection calls = %v, want none", selector.calls)
	}
	if len(sess.operations) != 0 {
		t.Fatalf("managed Codex session operations = %v, want none", sess.operations)
	}
}

func TestNewNativeCodexModelOnlyNoneCallsOnlySelectModel(t *testing.T) {
	cfg := validConfig(HarnessCodex)
	cfg.Credential = loop.CredentialNativeAuth
	cfg.Binding = launch.ProxyBinding{}
	cfg.AgentSessionID = ""
	cfg.ModelAlias = "gpt-5.6-luna"
	cfg.Effort = "none"
	sess := newFakeSession("native-codex-model-only-none")
	conn := &fakeClient{newSession: sess}
	owned := &fakeDialedClient{acpClient: conn}
	selector := &fakeCodexConnector{model: cfg.ModelAlias}
	installCodexConnectorFactory(t, func(model, effort string) codexConnector {
		if model != cfg.ModelAlias || effort != "" {
			t.Fatalf("Codex selector config = (%q, %q), want (%q, empty)", model, effort, cfg.ModelAlias)
		}
		return selector
	})
	installNativeDial(t, func(_ context.Context, _ launch.NativeConfig) (dialedClient, error) {
		return owned, nil
	})

	d, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer func() { _ = d.Close() }()

	wantCalls := []string{"model=" + cfg.ModelAlias}
	if !reflect.DeepEqual(selector.calls, wantCalls) {
		t.Fatalf("Codex model-only/none selection calls = %v, want %v", selector.calls, wantCalls)
	}
}

func TestCodexModelSelectionFailureClosesOwnedClientOnce(t *testing.T) {
	cfg := validConfig(HarnessCodex)
	cfg.Credential = loop.CredentialNativeAuth
	cfg.Binding = launch.ProxyBinding{}
	cfg.AgentSessionID = ""
	cfg.ModelAlias = "gpt-5.6-luna"
	cfg.Effort = "max"
	sess := newDriverContextSession("native-codex-model-selection-failure")
	conn := &fakeClient{newSession: sess}
	owned := &fakeDialedClient{acpClient: conn}
	selectionErr := &launch.ModelAliasError{Alias: cfg.ModelAlias}
	selector := &fakeCodexConnector{model: cfg.ModelAlias, effort: cfg.Effort, failModel: selectionErr}
	installCodexConnectorFactory(t, func(model, effort string) codexConnector { return selector })
	installNativeDial(t, func(_ context.Context, _ launch.NativeConfig) (dialedClient, error) {
		return owned, nil
	})

	_, err := New(context.Background(), cfg)
	if err == nil {
		t.Fatal("New() error = nil, want model selection error")
	}
	var got *launch.ModelAliasError
	if !errors.As(err, &got) || got != selectionErr {
		t.Fatalf("New() error = %v, want wrapped model selection error %p", err, selectionErr)
	}
	if owned.closeCalls != 1 {
		t.Fatalf("owned client close calls = %d, want exactly one", owned.closeCalls)
	}
	if !reflect.DeepEqual(selector.calls, []string{"model=" + cfg.ModelAlias}) {
		t.Fatalf("selection calls before model failure = %v, want only model", selector.calls)
	}
	select {
	case <-sess.promptContexts:
		t.Fatal("Prompt began before model selection failure returned")
	default:
	}
}

func TestCodexEffortSelectionFailureClosesOwnedClientOnceAndPreservesCancellation(t *testing.T) {
	cfg := validConfig(HarnessCodex)
	cfg.Credential = loop.CredentialNativeAuth
	cfg.Binding = launch.ProxyBinding{}
	cfg.AgentSessionID = ""
	cfg.ModelAlias = "gpt-5.6-luna"
	cfg.Effort = "max"
	sess := newDriverContextSession("native-codex-effort-selection-failure")
	conn := &fakeClient{newSession: sess}
	owned := &fakeDialedClient{acpClient: conn}
	effortErr := &launch.EffortAliasError{Effort: cfg.Effort}
	selectionErr := errors.Join(effortErr, context.DeadlineExceeded)
	selector := &fakeCodexConnector{model: cfg.ModelAlias, effort: cfg.Effort, failEffort: selectionErr}
	installCodexConnectorFactory(t, func(model, effort string) codexConnector { return selector })
	installNativeDial(t, func(_ context.Context, _ launch.NativeConfig) (dialedClient, error) {
		return owned, nil
	})

	_, err := New(context.Background(), cfg)
	if err == nil {
		t.Fatal("New() error = nil, want effort selection error")
	}
	var got *launch.EffortAliasError
	if !errors.As(err, &got) || got != effortErr {
		t.Fatalf("New() error = %v, want wrapped effort selection error %p", err, effortErr)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("New() error = %v, want errors.Is(context.DeadlineExceeded)", err)
	}
	if owned.closeCalls != 1 {
		t.Fatalf("owned client close calls = %d, want exactly one", owned.closeCalls)
	}
	wantCalls := []string{"model=" + cfg.ModelAlias, "effort=" + cfg.Effort}
	if !reflect.DeepEqual(selector.calls, wantCalls) {
		t.Fatalf("selection calls before effort failure = %v, want %v", selector.calls, wantCalls)
	}
	select {
	case <-sess.promptContexts:
		t.Fatal("Prompt began before effort selection failure returned")
	default:
	}
}

func TestNewNativeClaudeAppliesModelThenEffortSelection(t *testing.T) {
	cfg := validConfig(HarnessClaudeCode)
	cfg.Credential = loop.CredentialNativeAuth
	cfg.Binding = launch.ProxyBinding{}
	cfg.AgentSessionID = ""
	cfg.Effort = "high"
	sess := newFakeSession("native-claude-selection")
	conn := &fakeClient{newSession: sess}
	owned := &fakeDialedClient{acpClient: conn}
	var nativeCfg launch.NativeConfig

	installClaudeConnectorFactory(t, func(models launch.ClaudeModels, effort string) claudeConnector {
		if effort != cfg.Effort {
			t.Fatalf("Claude effort = %q, want %q", effort, cfg.Effort)
		}
		return &fakeClaudeConnector{models: models, effort: effort}
	})
	installNativeDial(t, func(_ context.Context, got launch.NativeConfig) (dialedClient, error) {
		nativeCfg = got
		return owned, nil
	})

	d, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer func() { _ = d.Close() }()

	harness, ok := nativeCfg.Harness.(*launch.ClaudeConnector)
	if !ok {
		t.Fatalf("native launch Harness = %T, want *launch.ClaudeConnector", nativeCfg.Harness)
	}
	if harness.Effort != cfg.Effort {
		t.Fatalf("Claude connector effort = %q, want %q", harness.Effort, cfg.Effort)
	}
	wantOperations := []string{
		"set_config:model=" + cfg.ModelAlias,
		"set_config:model=" + cfg.SmallModelAlias,
		"set_config:thought_level=" + cfg.Effort,
		"set_mode:acceptEdits",
	}
	if !reflect.DeepEqual(sess.operations, wantOperations) {
		t.Fatalf("native Claude session operations = %v, want model-then-effort order %v", sess.operations, wantOperations)
	}
}

func TestNewNativeClaudeModelOnlySelectsModelsWithoutEffort(t *testing.T) {
	cfg := validConfig(HarnessClaudeCode)
	cfg.Credential = loop.CredentialNativeAuth
	cfg.Binding = launch.ProxyBinding{}
	cfg.AgentSessionID = ""
	cfg.Effort = "none"
	sess := newFakeSession("native-claude-model-only")
	conn := &fakeClient{newSession: sess}
	owned := &fakeDialedClient{acpClient: conn}
	var nativeCfg launch.NativeConfig
	installClaudeConnectorFactory(t, func(models launch.ClaudeModels, effort string) claudeConnector {
		if effort != "" {
			t.Fatalf("Claude effort = %q, want empty for structured none model-only selection", effort)
		}
		return &fakeClaudeConnector{models: models, effort: effort}
	})
	installNativeDial(t, func(_ context.Context, got launch.NativeConfig) (dialedClient, error) {
		nativeCfg = got
		return owned, nil
	})

	d, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("New() error = %v, want model-only native launch", err)
	}
	defer func() { _ = d.Close() }()

	harness, ok := nativeCfg.Harness.(*launch.ClaudeConnector)
	if !ok {
		t.Fatalf("native launch Harness = %T, want *launch.ClaudeConnector", nativeCfg.Harness)
	}
	if harness.Effort != "" {
		t.Fatalf("Claude connector effort = %q, want empty", harness.Effort)
	}
	wantOperations := []string{
		"set_config:model=" + cfg.ModelAlias,
		"set_config:model=" + cfg.SmallModelAlias,
		"set_mode:acceptEdits",
	}
	if !reflect.DeepEqual(sess.operations, wantOperations) {
		t.Fatalf("native Claude model-only operations = %v, want %v", sess.operations, wantOperations)
	}
}

func TestNewNativeClaudeModelOnlySelectionFailureClosesOwnedClient(t *testing.T) {
	cfg := validConfig(HarnessClaudeCode)
	cfg.Credential = loop.CredentialNativeAuth
	cfg.Binding = launch.ProxyBinding{}
	cfg.AgentSessionID = ""
	cfg.Effort = ""
	sess := newFakeSession("native-claude-model-only-failure")
	conn := &fakeClient{newSession: sess}
	owned := &fakeDialedClient{acpClient: conn}
	selectionErr := &launch.ModelAliasError{Alias: cfg.ModelAlias}
	installClaudeConnectorFactory(t, func(models launch.ClaudeModels, effort string) claudeConnector {
		return &fakeClaudeConnector{models: models, effort: effort, fail: selectionErr}
	})
	installNativeDial(t, func(context.Context, launch.NativeConfig) (dialedClient, error) {
		return owned, nil
	})

	_, err := New(context.Background(), cfg)
	if err == nil {
		t.Fatal("New() error = nil, want model-only selection error")
	}
	var got *launch.ModelAliasError
	if !errors.As(err, &got) || got != selectionErr {
		t.Fatalf("New() error = %v, want wrapped model selection error %p", err, selectionErr)
	}
	if owned.closeCalls != 1 {
		t.Fatalf("owned client close calls = %d, want exactly one after model-only selection failure", owned.closeCalls)
	}
}

func TestNewClaudeEffortSelectionErrorClosesOwnedClientOnce(t *testing.T) {
	cfg := validConfig(HarnessClaudeCode)
	cfg.Credential = loop.CredentialNativeAuth
	cfg.Binding = launch.ProxyBinding{}
	cfg.AgentSessionID = ""
	cfg.Effort = "unsupported"
	sess := newFakeSession("native-claude-selection-error")
	conn := &fakeClient{newSession: sess}
	owned := &fakeDialedClient{acpClient: conn}
	effortErr := &launch.EffortAliasError{Effort: cfg.Effort}

	installClaudeConnectorFactory(t, func(models launch.ClaudeModels, effort string) claudeConnector {
		return &fakeClaudeConnector{models: models, effort: effort, failEffort: effortErr}
	})
	installNativeDial(t, func(context.Context, launch.NativeConfig) (dialedClient, error) {
		return owned, nil
	})

	_, err := New(context.Background(), cfg)
	if err == nil {
		t.Fatal("New() error = nil, want typed effort selection error")
	}
	var gotEffortErr *launch.EffortAliasError
	if !errors.As(err, &gotEffortErr) {
		t.Fatalf("New() error = %v (%T), want wrapped *launch.EffortAliasError", err, err)
	}
	if gotEffortErr != effortErr {
		t.Fatalf("wrapped effort error = %p, want original %p", gotEffortErr, effortErr)
	}
	if owned.closeCalls != 1 {
		t.Fatalf("owned client close calls = %d, want exactly one after selection failure", owned.closeCalls)
	}
	wantOperations := []string{
		"set_config:model=" + cfg.ModelAlias,
		"set_config:model=" + cfg.SmallModelAlias,
	}
	if !reflect.DeepEqual(sess.operations, wantOperations) {
		t.Fatalf("session operations before effort failure = %v, want %v", sess.operations, wantOperations)
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
			cfg.Effort = "medium"
			originalEnv := append([]string(nil), cfg.Env...)
			sess := newFakeSession("native-auth-session")
			conn := &fakeClient{newSession: sess}
			owned := &fakeDialedClient{acpClient: conn}
			var nativeCfg launch.NativeConfig
			if harnessName == HarnessClaudeCode {
				installClaudeConnectorFactory(t, func(models launch.ClaudeModels, effort string) claudeConnector {
					return &fakeClaudeConnector{models: models, effort: effort}
				})
			} else {
				installCodexConnectorFactory(t, func(model, effort string) codexConnector {
					return &fakeCodexConnector{model: model, effort: effort}
				})
			}

			installNativeDial(t, func(_ context.Context, got launch.NativeConfig) (dialedClient, error) {
				nativeCfg = got
				return owned, nil
			})

			d, err := New(context.Background(), cfg)
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			defer func() { _ = d.Close() }()

			if !reflect.DeepEqual(nativeCfg.Command.Env, originalEnv) {
				t.Fatalf("native launch env = %#v, want caller env %#v before public native connector configuration", nativeCfg.Command.Env, originalEnv)
			}

			switch harnessName {
			case HarnessClaudeCode:
				native := nativeCfg.Harness
				harness, ok := native.(*launch.ClaudeConnector)
				if !ok {
					t.Fatalf("native launch Harness = %T, want *launch.ClaudeConnector", native)
				}
				wantModels := launch.ClaudeModels{Default: cfg.ModelAlias, Small: cfg.SmallModelAlias}
				if harness.Models != wantModels {
					t.Fatalf("native Claude models = %+v, want %+v", harness.Models, wantModels)
				}
				if harness.Effort != cfg.Effort {
					t.Fatalf("native Claude effort = %q, want %q", harness.Effort, cfg.Effort)
				}
			case HarnessCodex:
				native := nativeCfg.Harness
				harness, ok := native.(*launch.CodexConnector)
				if !ok {
					t.Fatalf("native launch Harness = %T, want *launch.CodexConnector", native)
				}
				if harness.Model != cfg.ModelAlias {
					t.Fatalf("native Codex model = %q, want %q", harness.Model, cfg.ModelAlias)
				}
				if harness.Effort != cfg.Effort {
					t.Fatalf("native Codex effort = %q, want %q", harness.Effort, cfg.Effort)
				}
				wantPosture := codexPosture(cfg.Posture, false)
				if harness.Posture != wantPosture {
					t.Fatalf("native Codex posture = %+v, want %+v", harness.Posture, wantPosture)
				}
			}
			if harnessName == HarnessClaudeCode {
				wantOperations := []string{
					"set_config:model=" + cfg.ModelAlias,
					"set_config:model=" + cfg.SmallModelAlias,
					"set_config:thought_level=" + cfg.Effort,
					"set_mode:acceptEdits",
				}
				if !reflect.DeepEqual(sess.operations, wantOperations) {
					t.Fatalf("native Claude session operations = %v, want %v", sess.operations, wantOperations)
				}
			}
		})
	}
}

func TestNewNativeHarnessManagedDoesNotSelectOrInjectModel(t *testing.T) {
	for _, harnessName := range []Harness{HarnessClaudeCode, HarnessCodex} {
		t.Run(string(harnessName), func(t *testing.T) {
			cfg := validConfig(harnessName)
			cfg.Credential = loop.CredentialNativeAuth
			cfg.Binding = launch.ProxyBinding{}
			cfg.ModelAlias = ""
			if harnessName == HarnessClaudeCode {
				cfg.SmallModelAlias = ""
			}
			cfg.AgentSessionID = ""

			sess := newFakeSession("native-managed-session")
			conn := &fakeClient{newSession: sess}
			owned := &fakeDialedClient{acpClient: conn}
			var nativeCfg launch.NativeConfig
			if harnessName == HarnessClaudeCode {
				installClaudeConnectorFactory(t, func(models launch.ClaudeModels, _ string) claudeConnector {
					if models != (launch.ClaudeModels{}) {
						t.Fatalf("managed Claude models = %+v, want empty", models)
					}
					return &fakeClaudeConnector{models: models}
				})
			}
			installNativeDial(t, func(_ context.Context, got launch.NativeConfig) (dialedClient, error) {
				nativeCfg = got
				return owned, nil
			})

			d, err := New(context.Background(), cfg)
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			defer func() { _ = d.Close() }()

			var wantOperations []string
			if harnessName == HarnessClaudeCode {
				wantOperations = []string{"set_mode:acceptEdits"}
			}
			if got := sess.operations; !reflect.DeepEqual(got, wantOperations) {
				t.Fatalf("managed session operations = %v, want %v", got, wantOperations)
			}
			switch harnessName {
			case HarnessClaudeCode:
				native := nativeCfg.Harness
				harness, ok := native.(*launch.ClaudeConnector)
				if !ok {
					t.Fatalf("managed native Harness = %T, want *launch.ClaudeConnector", native)
				}
				if harness.Models != (launch.ClaudeModels{}) {
					t.Fatalf("managed native Claude models = %+v, want empty", harness.Models)
				}
			case HarnessCodex:
				native := nativeCfg.Harness
				harness, ok := native.(*launch.CodexConnector)
				if !ok {
					t.Fatalf("managed native Harness = %T, want *launch.CodexConnector", native)
				}
				if harness.Model != "" {
					t.Fatalf("managed native Codex model = %q, want empty", harness.Model)
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

	installClaudeConnectorFactory(t, func(launch.ClaudeModels, string) claudeConnector {
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
