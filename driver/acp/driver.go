package acp

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/looprig/acp/client"
	"github.com/looprig/acp/launch"
	"github.com/looprig/acp/protocol"
	"github.com/looprig/acp/transport/stdio"
	"github.com/looprig/foreignloops/driver"
)

// session is the portion of an ACP session needed while the driver is being
// constructed. The concrete client.Session satisfies it; keeping the seam
// here lets construction tests use an in-process session without spawning an
// adapter process.
type session interface {
	ID() protocol.SessionID
	ConfigOptions() []protocol.SessionConfigOption
	Modes() *protocol.SessionModeState
	SetConfigOption(context.Context, protocol.SessionConfigID, protocol.SessionConfigValueID) error
	SetMode(context.Context, protocol.SessionModeID) error
}

// acpClient is the minimum client capability needed to create a driver
// session. LoadSession is deliberately optional: a caller cannot resume
// against a client that does not provide that capability.
type acpClient interface {
	NewSession(context.Context, client.NewSessionParams) (session, error)
}

type sessionLoader interface {
	LoadSession(context.Context, client.LoadSessionParams) (session, error)
}

// dialedClient owns the long-lived ACP process and connection returned by a
// dial. The production adapter below wraps launch.ManagedClient; tests replace
// dial with an in-process fake.
type dialedClient interface {
	client() acpClient
	close(context.Context) error
}

type dialFunc func(context.Context, launch.Config) (dialedClient, error)

var dial dialFunc = dialLaunch

// claudeConnector is the session-level portion of launch.ClaudeConnector.
// launch's public methods accept *client.Session, so production uses the
// adapter below while construction tests can use a narrow in-process fake.
type claudeConnector interface {
	SelectDefaultModel(context.Context, session) error
	SelectSmallModel(context.Context, session) error
	ApplyPermissionMode(context.Context, session, protocol.SessionModeID) error
}

type claudeConnectorFactory func(launch.ClaudeModels) claudeConnector

var newClaudeConnector claudeConnectorFactory = func(models launch.ClaudeModels) claudeConnector {
	return &realClaudeConnector{connector: launch.ClaudeCode(models)}
}

// Driver owns one launch.Dial result and one ACP session for its entire
// lifetime. Turns will use this session in later driver work; construction
// never creates a per-turn client or process.
type Driver struct {
	owned   dialedClient
	session session

	agentSessionID string

	closeOnce sync.Once
	closeErr  error
}

// New validates cfg, launches one ACP client/process, creates or loads one
// session, and applies the harness-specific model and posture before
// returning. Any failure after dialing closes the owned launch result before
// returning.
func New(ctx context.Context, cfg Config) (*Driver, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	harness, claude, err := connectorFor(cfg)
	if err != nil {
		return nil, err
	}

	owned, err := dial(ctx, launch.Config{
		SharedProxy: &cfg.Binding,
		Harness:     harness,
		Command: stdio.Command{
			Path: cfg.Executable,
			Env:  append([]string(nil), cfg.Env...),
			Dir:  cfg.WorkspaceRoot,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("acp: dial: %w", err)
	}
	if owned == nil {
		return nil, errors.New("acp: dial returned a nil client")
	}

	acpClient := owned.client()
	if acpClient == nil {
		return nil, closeAfterConstructionFailure(owned, errors.New("acp: dial returned a nil ACP client"))
	}

	sess, err := createSession(ctx, acpClient, cfg)
	if err != nil {
		return nil, closeAfterConstructionFailure(owned, err)
	}
	if sess == nil {
		return nil, closeAfterConstructionFailure(owned, errors.New("acp: session creation returned a nil session"))
	}

	d := &Driver{
		owned:          owned,
		session:        sess,
		agentSessionID: string(sess.ID()),
	}

	if claude != nil {
		if err := claude.SelectDefaultModel(ctx, sess); err != nil {
			return nil, closeAfterConstructionFailure(d, fmt.Errorf("acp: select default model: %w", err))
		}
		if err := claude.SelectSmallModel(ctx, sess); err != nil {
			return nil, closeAfterConstructionFailure(d, fmt.Errorf("acp: select small model: %w", err))
		}
		if err := claude.ApplyPermissionMode(ctx, sess, claudePermissionMode(cfg.Posture)); err != nil {
			return nil, closeAfterConstructionFailure(d, fmt.Errorf("acp: apply permission mode: %w", err))
		}
	}

	return d, nil
}

func connectorFor(cfg Config) (launch.HarnessAdapter, claudeConnector, error) {
	switch cfg.Harness {
	case HarnessClaudeCode:
		models := launch.ClaudeModels{
			Default: cfg.ModelAlias,
			Small:   cfg.SmallModelAlias,
		}
		return launch.ClaudeCode(models), newClaudeConnector(models), nil
	case HarnessCodex:
		codex := launch.Codex("").WithModel(cfg.ModelAlias)
		codex.Posture = codexPosture(cfg.Posture)
		return codex, nil, nil
	default:
		return nil, nil, &ConfigError{Field: "Harness", Reason: "must be a supported harness"}
	}
}

func createSession(ctx context.Context, c acpClient, cfg Config) (session, error) {
	if cfg.AgentSessionID == "" {
		sess, err := c.NewSession(ctx, client.NewSessionParams{Cwd: cfg.WorkspaceRoot})
		if err != nil {
			return nil, fmt.Errorf("acp: session/new: %w", err)
		}
		return sess, nil
	}

	loader, ok := c.(sessionLoader)
	if !ok {
		return nil, errors.New("acp: session/load capability unavailable")
	}
	sess, err := loader.LoadSession(ctx, client.LoadSessionParams{
		SessionID: protocol.SessionID(cfg.AgentSessionID),
		Cwd:       cfg.WorkspaceRoot,
	})
	if err != nil {
		return nil, fmt.Errorf("acp: session/load: %w", err)
	}
	return sess, nil
}

func claudePermissionMode(posture driver.Posture) protocol.SessionModeID {
	switch posture {
	case driver.PostureWorkspaceWrite:
		return "acceptEdits"
	case driver.PostureReadOnly:
		return "default"
	default:
		return "default"
	}
}

func codexPosture(posture driver.Posture) launch.CodexPosture {
	switch posture {
	case driver.PostureReadOnly:
		return launch.CodexPosture{
			ApprovalPolicy: "never",
			SandboxMode:    "read-only",
		}
	case driver.PostureWorkspaceWrite:
		return launch.CodexPosture{
			ApprovalPolicy: "never",
			SandboxMode:    "workspace-write",
		}
	default:
		// Config.validate rejects this path. Keep the fallback restrictive if
		// this helper is ever called independently in the future.
		return launch.CodexPosture{
			ApprovalPolicy: "never",
			SandboxMode:    "read-only",
		}
	}
}

// AgentSessionID returns the ACP session id assigned by session/new or used
// for session/load. It remains stable for the driver's lifetime.
func (d *Driver) AgentSessionID() string {
	if d == nil {
		return ""
	}
	return d.agentSessionID
}

// Close releases the owned ACP connection/process exactly once. The shared
// proxy binding is only data passed to launch.Dial and is never touched here.
func (d *Driver) Close() error {
	return d.close(context.Background())
}

func (d *Driver) close(ctx context.Context) error {
	if d == nil {
		return nil
	}
	d.closeOnce.Do(func() {
		if d.owned != nil {
			d.closeErr = d.owned.close(ctx)
		}
	})
	return d.closeErr
}

func closeAfterConstructionFailure(closer interface{ close(context.Context) error }, cause error) error {
	if closeErr := closer.close(context.Background()); closeErr != nil {
		return errors.Join(cause, fmt.Errorf("acp: close after construction failure: %w", closeErr))
	}
	return cause
}

type launchedClient struct {
	managed *launch.ManagedClient
	acp     acpClient
}

func (c *launchedClient) client() acpClient { return c.acp }

func (c *launchedClient) close(ctx context.Context) error { return c.managed.Close(ctx) }

func dialLaunch(ctx context.Context, cfg launch.Config) (dialedClient, error) {
	managed, err := launch.Dial(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &launchedClient{
		managed: managed,
		acp:     &realClient{client: managed.Client()},
	}, nil
}

type realClient struct {
	client *client.Client
}

func (c *realClient) NewSession(ctx context.Context, p client.NewSessionParams) (session, error) {
	return c.client.NewSession(ctx, p)
}

func (c *realClient) LoadSession(ctx context.Context, p client.LoadSessionParams) (session, error) {
	return c.client.LoadSession(ctx, p)
}

type realClaudeConnector struct {
	connector *launch.ClaudeConnector
}

func (c *realClaudeConnector) SelectDefaultModel(ctx context.Context, sess session) error {
	concrete, err := concreteSession(sess)
	if err != nil {
		return err
	}
	return c.connector.SelectDefaultModel(ctx, concrete)
}

func (c *realClaudeConnector) SelectSmallModel(ctx context.Context, sess session) error {
	concrete, err := concreteSession(sess)
	if err != nil {
		return err
	}
	return c.connector.SelectSmallModel(ctx, concrete)
}

func (c *realClaudeConnector) ApplyPermissionMode(ctx context.Context, sess session, modeID protocol.SessionModeID) error {
	concrete, err := concreteSession(sess)
	if err != nil {
		return err
	}
	return c.connector.ApplyPermissionMode(ctx, concrete, modeID)
}

func concreteSession(sess session) (*client.Session, error) {
	concrete, ok := sess.(*client.Session)
	if !ok {
		return nil, errors.New("acp: internal session is not an acp/client session")
	}
	return concrete, nil
}

var _ dialFunc = dialLaunch
var _ claudeConnectorFactory = func(launch.ClaudeModels) claudeConnector {
	return &realClaudeConnector{}
}
