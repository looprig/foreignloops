package backend

import (
	"context"
	"errors"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/looprig/core/uuid"
	"github.com/looprig/foreignloop/driver"
	"github.com/looprig/harness/pkg/command"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/foreign"
	"github.com/looprig/harness/pkg/loop"
	"github.com/looprig/harness/pkg/tool"
	model "github.com/looprig/inference/model"
)

func parityBoundDefinition(t *testing.T, system, instructions string) loop.BoundDefinition {
	t.Helper()
	options := []loop.Option{
		loop.WithName("agent"),
		loop.WithInference(boundTestClient{}, model.Model{
			Provider:  "lmstudio",
			APIFormat: model.APIFormatOpenAI,
			BaseURL:   "http://localhost:1234",
			Name:      "m",
		}),
		loop.WithSystem(system),
	}
	if instructions != "" {
		options = append(options,
			loop.WithModes(loop.Mode{Name: "mode", Instructions: instructions}),
			loop.WithInitialMode("mode"),
		)
	}
	definition, err := loop.Define(options...)
	if err != nil {
		t.Fatalf("Define: %v", err)
	}
	bound, err := definition.Bind(context.Background(), tool.Bindings{SessionID: mustID(t), LoopID: mustID(t)})
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	return bound
}

func TestNewRuntimeWiringAndInstructionsOnlyParity(t *testing.T) {
	t.Parallel()
	baseConfig := func(t *testing.T) Config {
		t.Helper()
		return Config{Agent: &fakeAgent{}, Cwd: t.TempDir(), SIDMode: SIDPrebound}
	}
	tests := []struct {
		name      string
		loopCfg   loop.BoundDefinition
		publisher foreign.EventPublisher
		idGen     func() (uuid.UUID, error)
		factory   *event.Factory
		wantField string
	}{
		{name: "missing effective prompt", publisher: &fakePublisher{}, idGen: seqIDGen(), factory: workingFac(), wantField: "System"},
		{name: "nil publisher", loopCfg: validBoundDefinition(), idGen: seqIDGen(), factory: workingFac(), wantField: "pub"},
		{name: "nil id generator", loopCfg: validBoundDefinition(), publisher: &fakePublisher{}, factory: workingFac(), wantField: "idGen"},
		{name: "nil event factory", loopCfg: validBoundDefinition(), publisher: &fakePublisher{}, idGen: seqIDGen(), wantField: "fac"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			state, sid, err := New(context.Background(), mustID(t), mustID(t), loop.Provenance{}, tt.publisher, tt.loopCfg, baseConfig(t), tt.idGen, tt.factory)
			if state != nil || sid != "" {
				t.Fatalf("invalid New returned state=%T sid=%q", state, sid)
			}
			var configErr *ConfigError
			if !errors.As(err, &configErr) || configErr.Field != tt.wantField {
				t.Fatalf("New error = %T %v, want ConfigError field %q", err, err, tt.wantField)
			}
		})
	}

	bound := parityBoundDefinition(t, "", "mode instructions")
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	state, _, err := New(ctx, mustID(t), mustID(t), loop.Provenance{}, &fakePublisher{}, bound, baseConfig(t), seqIDGen(), workingFac())
	if err != nil {
		t.Fatalf("New instructions-only definition: %v", err)
	}
	shutdown(t, state)
}

func TestManagedAcceptanceMintFailurePreservesExactErrorAndStartsNoWork(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("foreign acceptance event id mint failed")
	agent := &fakeAgent{}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	pub := &fakePublisher{}
	state, _, err := New(
		ctx,
		mustID(t),
		mustID(t),
		loop.Provenance{},
		pub,
		validBoundDefinition(),
		Config{Agent: agent, Cwd: t.TempDir(), SIDMode: SIDLateBound},
		seqIDGen(),
		event.NewFactory(func() (uuid.UUID, error) { return uuid.UUID{}, sentinel }, time.Now),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	commandID := mustID(t)
	accepted := make(chan error, 1)
	state.Commands <- command.UserInput{
		Header:       command.Header{CommandID: commandID},
		NoFold:       true,
		TargetLoopID: state.loopID,
		Accepted:     accepted,
	}
	if got := <-accepted; got != sentinel {
		t.Fatalf("acceptance error = %T %v, want exact sentinel", got, got)
	}
	if agent.calls() != 0 {
		t.Fatalf("agent spawn calls = %d, want 0", agent.calls())
	}
	for _, published := range pub.snapshot() {
		if published.EventHeader().Cause.CommandID == commandID {
			t.Fatalf("failed acceptance published work: %T", published)
		}
	}
	shutdown(t, state)
}

func TestManagedAcceptanceAppendFailurePreservesExactErrorAndStartsNoWork(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("foreign acceptance durable append failed")
	agent := &fakeAgent{}
	pub := &fakePublisher{checkedErr: sentinel}
	state, _ := newTestLoop(t, Config{Agent: agent, SIDMode: SIDLateBound}, pub)
	commandID := mustID(t)
	accepted := make(chan error, 1)
	state.Commands <- command.UserInput{
		Header:       command.Header{CommandID: commandID},
		NoFold:       true,
		TargetLoopID: state.loopID,
		Accepted:     accepted,
	}
	if got := <-accepted; got != sentinel {
		t.Fatalf("acceptance error = %T %v, want exact sentinel", got, got)
	}
	if agent.calls() != 0 {
		t.Fatalf("agent spawn calls = %d, want 0", agent.calls())
	}
	for _, published := range pub.snapshot() {
		if published.EventHeader().Cause.CommandID == commandID {
			t.Fatalf("failed acceptance published work: %T", published)
		}
	}
	shutdown(t, state)
}

func TestBusyAndStaleDurableLocksAtActorBoundary(t *testing.T) {
	t.Run("busy holder fails before spawn", func(t *testing.T) {
		t.Parallel()
		cwd := t.TempDir()
		agent := &fakeAgent{events: []driver.Event{{Kind: driver.KindTerminalOK}}}
		pub := &fakePublisher{}
		state, sid := newTestLoop(t, Config{Agent: agent, Cwd: cwd, SIDMode: SIDPrebound}, pub)
		held, err := acquireForeignLock(sid, cwd)
		if err != nil {
			t.Fatalf("hold durable lock: %v", err)
		}
		t.Cleanup(func() { cleanupForeignLock(t, held) })
		submit(t, state, "go")
		waitFor(t, pub, func(input event.Event) bool { _, ok := input.(event.TurnFailed); return ok })
		failed := pub.snapshot()[1].(event.TurnFailed)
		var busy *ForeignSessionBusyError
		if !errors.As(failed.Err, &busy) || busy.PID != os.Getpid() {
			t.Fatalf("TurnFailed.Err = %T %v, want live-holder ForeignSessionBusyError", failed.Err, failed.Err)
		}
		if agent.calls() != 0 {
			t.Fatalf("agent spawn calls = %d, want 0", agent.calls())
		}
		messages, turnIndex, err := state.Snapshot(context.Background())
		if err != nil || len(messages) != 0 || turnIndex != 0 {
			t.Fatalf("busy snapshot = %v/%d/%v, want empty/0/nil", messages, turnIndex, err)
		}
		shutdown(t, state)
	})

	t.Run("unlocked metadata is reclaimed and kernel lock released", func(t *testing.T) {
		t.Parallel()
		cwd := t.TempDir()
		agent := &fakeAgent{events: []driver.Event{{Kind: driver.KindTerminalOK}}}
		state, sid := newTestLoop(t, Config{Agent: agent, Cwd: cwd, SIDMode: SIDPrebound}, &fakePublisher{})
		preWriteLock(t, sid, cwd, strconv.Itoa(deadPID(t)))
		submit(t, state, "go")
		waitTurnIndex(t, state, 1)
		if agent.calls() != 1 {
			t.Fatalf("agent spawn calls = %d, want 1", agent.calls())
		}
		contender, err := acquireForeignLock(sid, cwd)
		if err != nil {
			t.Fatalf("durable kernel lock was not released: %v", err)
		}
		cleanupForeignLock(t, contender)
		shutdown(t, state)
	})
}

func TestEffectiveSystemPromptParity(t *testing.T) {
	t.Parallel()
	agent := &fakeAgent{events: []driver.Event{{Kind: driver.KindTerminalOK}}}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	state, _, err := New(
		ctx,
		mustID(t),
		mustID(t),
		loop.Provenance{},
		&fakePublisher{},
		parityBoundDefinition(t, "base", "mode instructions"),
		Config{Agent: agent, Cwd: t.TempDir(), SIDMode: SIDPrebound},
		seqIDGen(),
		workingFac(),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	submit(t, state, "hello")
	waitTurnIndex(t, state, 1)
	if got := agent.lastForeignTurn().SystemPrompt; got != "base\n\nmode instructions" {
		t.Fatalf("SystemPrompt = %q", got)
	}
	shutdown(t, state)
}

func TestCloseErrorsFailSuccessfulTerminalAndRetainType(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		err  error
		as   func(error) bool
	}{
		{name: "exit", err: &driver.ExitError{Code: 7}, as: func(err error) bool { var target *driver.ExitError; return errors.As(err, &target) && target.Code == 7 }},
		{name: "decode", err: &driver.DecodeError{Cause: errors.New("bad jsonl")}, as: func(err error) bool { var target *driver.DecodeError; return errors.As(err, &target) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			agent := &fakeAgent{events: []driver.Event{{Kind: driver.KindTerminalOK}}, closeErr: tt.err}
			pub := &fakePublisher{}
			state, _ := newTestLoop(t, Config{Agent: agent, SIDMode: SIDPrebound}, pub)
			submit(t, state, "go")
			waitFor(t, pub, func(input event.Event) bool { _, ok := input.(event.TurnFailed); return ok })
			failed := pub.snapshot()[1].(event.TurnFailed)
			if !tt.as(failed.Err) {
				t.Fatalf("TurnFailed.Err = %T %v, want %s", failed.Err, failed.Err, tt.name)
			}
			if got := agent.lastStream().lifecycle(); len(got) != 2 || got[0] != "close" || got[1] != "history" {
				t.Fatalf("stream lifecycle = %v, want one close then history", got)
			}
			_, turnIndex, err := state.Snapshot(context.Background())
			if err != nil || turnIndex != 0 {
				t.Fatalf("Snapshot turnIndex/error = %d/%v, want 0/nil", turnIndex, err)
			}
			shutdown(t, state)
		})
	}
}

func TestLateBoundPreInitFailureRetriesStartNew(t *testing.T) {
	tests := []struct {
		name      string
		events    []driver.Event
		closeErr  error
		block     bool
		interrupt bool
	}{
		{name: "EOF"},
		{name: "close failure", events: []driver.Event{{Kind: driver.KindTerminalOK}}, closeErr: &driver.ExitError{Code: 9}},
		{name: "interruption", block: true, interrupt: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			agent := &fakeAgent{events: tt.events, closeErr: tt.closeErr, block: tt.block}
			pub := &fakePublisher{}
			state, _ := newTestLoop(t, Config{Agent: agent, SIDMode: SIDLateBound}, pub)
			submit(t, state, "first")
			if tt.interrupt {
				waitFor(t, pub, func(input event.Event) bool { _, ok := input.(event.TurnStarted); return ok })
				sendInterrupt(t, state)
			} else {
				waitFor(t, pub, func(input event.Event) bool { _, ok := input.(event.TurnFailed); return ok })
				waitLoopIdle(t, state)
			}
			agent.mu.Lock()
			agent.events = []driver.Event{{Kind: driver.KindInit, SessionID: "second-session"}, {Kind: driver.KindTerminalOK}}
			agent.closeErr = nil
			agent.block = false
			agent.mu.Unlock()
			submit(t, state, "second")
			waitTurnIndex(t, state, 1)
			second := agent.lastForeignTurn()
			if !second.StartNew || second.ForeignSID != "" {
				t.Fatalf("second turn = {StartNew:%v ForeignSID:%q}, want true/empty", second.StartNew, second.ForeignSID)
			}
			shutdown(t, state)
		})
	}
}

func TestLateBoundTerminalBeforeInitRetainsErrorsAndRetriesStartNew(t *testing.T) {
	tests := []struct {
		name       string
		terminal   driver.Event
		wantResult bool
	}{
		{name: "terminal OK", terminal: driver.Event{Kind: driver.KindTerminalOK}},
		{name: "terminal error", terminal: driver.Event{Kind: driver.KindTerminalError, ErrText: "error_max_turns"}, wantResult: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			agent := &fakeAgent{events: []driver.Event{tt.terminal, {Kind: driver.KindInit, SessionID: "too-late"}}}
			pub := &fakePublisher{}
			state, _ := newTestLoop(t, Config{Agent: agent, SIDMode: SIDLateBound}, pub)
			submit(t, state, "first")
			waitFor(t, pub, func(input event.Event) bool { _, ok := input.(event.TurnFailed); return ok })
			waitLoopIdle(t, state)
			failed := pub.snapshot()[1].(event.TurnFailed)
			var protocolErr *ForeignProtocolError
			if !errors.As(failed.Err, &protocolErr) {
				t.Fatalf("TurnFailed.Err = %T %v, want ForeignProtocolError", failed.Err, failed.Err)
			}
			var resultErr *ForeignResultError
			if got := errors.As(failed.Err, &resultErr); got != tt.wantResult {
				t.Fatalf("ForeignResultError present = %v, want %v (%v)", got, tt.wantResult, failed.Err)
			}
			agent.mu.Lock()
			agent.events = []driver.Event{{Kind: driver.KindInit, SessionID: "second-session"}, {Kind: driver.KindTerminalOK}}
			agent.mu.Unlock()
			submit(t, state, "second")
			waitTurnIndex(t, state, 1)
			second := agent.lastForeignTurn()
			if !second.StartNew || second.ForeignSID != "" {
				t.Fatalf("second turn = {StartNew:%v ForeignSID:%q}, want true/empty", second.StartNew, second.ForeignSID)
			}
			shutdown(t, state)
		})
	}
}

func TestInterruptLeavesSnapshotUncommittedAfterUnsupportedCommand(t *testing.T) {
	t.Parallel()
	agent := &fakeAgent{block: true, events: []driver.Event{{Kind: driver.KindInit}}}
	pub := &fakePublisher{}
	state, _ := newTestLoop(t, Config{Agent: agent, SIDMode: SIDPrebound}, pub)
	submit(t, state, "long running task")
	waitFor(t, pub, func(input event.Event) bool { _, ok := input.(event.TurnStarted); return ok })

	select {
	case state.Commands <- command.ApproveToolCall{
		Header:    command.Header{CommandID: mustID(t)},
		GateRoute: command.GateRoute{ToolExecutionID: mustID(t)},
	}:
	case <-time.After(2 * time.Second):
		t.Fatal("unsupported command was not consumed")
	}
	sendInterrupt(t, state)
	waitFor(t, pub, func(input event.Event) bool { _, ok := input.(event.TurnInterrupted); return ok })
	if got, want := eventKinds(pub.snapshot()), []string{"event.TurnStarted", "event.TurnInterrupted"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("events = %v, want %v", got, want)
	}
	messages, turnIndex, err := state.Snapshot(context.Background())
	if err != nil || len(messages) != 0 || turnIndex != 0 {
		t.Fatalf("interrupted snapshot = %v/%d/%v, want empty/0/nil", messages, turnIndex, err)
	}
	shutdown(t, state)
}

func TestInterruptFlushesAcceptedQueueWithoutSpawningIt(t *testing.T) {
	t.Parallel()
	agent := &queueAgent{spawned: make(chan queueSpawn, 3)}
	pub := &fakePublisher{}
	state, _ := newTestLoop(t, Config{Agent: agent, SIDMode: SIDPrebound}, pub)
	submit(t, state, "active")
	_ = nextSpawn(t, agent)
	first, err := sendManaged(t, state, "first queued")
	if err != nil {
		t.Fatalf("queue first: %v", err)
	}
	second, err := sendManaged(t, state, "second queued")
	if err != nil {
		t.Fatalf("queue second: %v", err)
	}
	sendInterrupt(t, state)
	waitCancelled(t, pub, event.CancelTurnInterrupted, first, second)
	select {
	case unexpected := <-agent.spawned:
		t.Fatalf("interrupt spawned queued input: %+v", unexpected.turn)
	default:
	}
	shutdown(t, state)
}
