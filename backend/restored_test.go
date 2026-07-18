package backend

import (
	"context"
	"errors"
	"testing"

	"github.com/looprig/core/content"
	"github.com/looprig/core/uuid"
	"github.com/looprig/foreignloop/driver"
	"github.com/looprig/harness/pkg/foreign"
	"github.com/looprig/harness/pkg/loop"
)

func restoredValidConfig(t *testing.T) Config {
	t.Helper()
	return Config{
		Agent:   &fakeAgent{},
		Cwd:     t.TempDir(),
		Posture: driver.PostureDefault,
		SIDMode: SIDPrebound,
	}
}

func validRestoredSeed() foreign.RestoredForeign {
	return foreign.RestoredForeign{
		ForeignSID: "sid-restored",
		TurnIndex:  3,
		Msgs:       content.AgenticMessages{aiMessage("first"), aiMessage("second")},
	}
}

func TestBuildRestoredWithReturnsHarnessBuilder(t *testing.T) {
	t.Parallel()
	var _ foreign.RestoredBuilder = BuildRestoredWith(restoredValidConfig(t))
}

func TestBuildRestoredWithFailsClosedOnInvalidConfig(t *testing.T) {
	t.Parallel()
	cfg := restoredValidConfig(t)
	cfg.Cwd = ""
	got, err := BuildRestoredWith(cfg)(
		context.Background(),
		uuid.UUID{},
		uuid.UUID{},
		loop.Provenance{},
		nil,
		nil,
		nil,
		nil,
		foreign.RestoredForeign{},
	)
	if got != nil {
		t.Fatalf("BuildRestoredWith error returned non-nil Backend %T", got)
	}
	var configErr *ConfigError
	if !errors.As(err, &configErr) || configErr.Field != "Config.Cwd" {
		t.Fatalf("error = %T %v, want ConfigError for Config.Cwd", err, err)
	}
}

func TestBuildRestoredWithRejectsMissingForeignSessionID(t *testing.T) {
	t.Parallel()
	got, err := BuildRestoredWith(restoredValidConfig(t))(
		context.Background(),
		mustID(t),
		mustID(t),
		loop.Provenance{},
		&fakePublisher{},
		validBoundDefinition(),
		seqIDGen(),
		workingFac(),
		foreign.RestoredForeign{},
	)
	if got != nil {
		t.Fatalf("BuildRestoredWith error returned non-nil Backend %T", got)
	}
	var configErr *ConfigError
	if !errors.As(err, &configErr) || configErr.Field != "RestoredForeign.ForeignSID" {
		t.Fatalf("error = %T %v, want ConfigError for missing foreign sid", err, err)
	}
}

func TestRestoreConstructionValidation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		mutateCfg func(*Config)
		loopCfg   loop.BoundDefinition
		pub       foreign.EventPublisher
		idGen     func() (uuid.UUID, error)
		facNil    bool
		seed      foreign.RestoredForeign
		wantField string
	}{
		{name: "nil agent", mutateCfg: func(cfg *Config) { cfg.Agent = nil }, loopCfg: validBoundDefinition(), pub: &fakePublisher{}, idGen: seqIDGen(), seed: validRestoredSeed(), wantField: "Config.Agent"},
		{name: "empty workspace", mutateCfg: func(cfg *Config) { cfg.Cwd = "" }, loopCfg: validBoundDefinition(), pub: &fakePublisher{}, idGen: seqIDGen(), seed: validRestoredSeed(), wantField: "Config.Cwd"},
		{name: "empty system prompt", loopCfg: nil, pub: &fakePublisher{}, idGen: seqIDGen(), seed: validRestoredSeed(), wantField: "System"},
		{name: "nil publisher", loopCfg: validBoundDefinition(), idGen: seqIDGen(), seed: validRestoredSeed(), wantField: "pub"},
		{name: "nil id generator", loopCfg: validBoundDefinition(), pub: &fakePublisher{}, seed: validRestoredSeed(), wantField: "idGen"},
		{name: "nil factory", loopCfg: validBoundDefinition(), pub: &fakePublisher{}, idGen: seqIDGen(), facNil: true, seed: validRestoredSeed(), wantField: "fac"},
		{name: "empty foreign sid", loopCfg: validBoundDefinition(), pub: &fakePublisher{}, idGen: seqIDGen(), seed: foreign.RestoredForeign{TurnIndex: 1}, wantField: "RestoredForeign.ForeignSID"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := restoredValidConfig(t)
			if tt.mutateCfg != nil {
				tt.mutateCfg(&cfg)
			}
			fac := workingFac()
			if tt.facNil {
				fac = nil
			}
			got, err := newRestoredState(context.Background(), mustID(t), mustID(t), loop.Provenance{}, tt.pub, tt.loopCfg, cfg, tt.idGen, fac, tt.seed)
			if got != nil {
				t.Fatalf("newRestoredState error returned non-nil state %T", got)
			}
			var configErr *ConfigError
			if !errors.As(err, &configErr) {
				t.Fatalf("error = %T %v, want ConfigError", err, err)
			}
			if configErr.Field != tt.wantField {
				t.Fatalf("ConfigError.Field = %q, want %q", configErr.Field, tt.wantField)
			}
		})
	}
}

func TestRestoreConstructionSeedsActorState(t *testing.T) {
	t.Parallel()
	seed := validRestoredSeed()
	sessionID := mustID(t)
	loopID := mustID(t)
	parent := loop.Provenance{LoopID: mustID(t), TurnID: mustID(t), StepID: mustID(t)}
	cfg := restoredValidConfig(t)
	bound := validBoundDefinition()
	pub := &fakePublisher{}
	idGen := seqIDGen()
	fac := workingFac()

	state, err := newRestoredState(context.Background(), sessionID, loopID, parent, pub, bound, cfg, idGen, fac, seed)
	if err != nil {
		t.Fatalf("newRestoredState: %v", err)
	}
	if state.sessionID != sessionID || state.loopID != loopID || state.parent != parent {
		t.Fatalf("identity = %v/%v/%v, want %v/%v/%v", state.sessionID, state.loopID, state.parent, sessionID, loopID, parent)
	}
	if state.sid != seed.ForeignSID || !state.sidBound || !state.hasSpawned {
		t.Fatalf("restore binding = sid %q sidBound %v hasSpawned %v", state.sid, state.sidBound, state.hasSpawned)
	}
	if state.turnIndex != seed.TurnIndex || len(state.msgs) != len(seed.Msgs) {
		t.Fatalf("restore state = turn %d messages %d, want %d/%d", state.turnIndex, len(state.msgs), seed.TurnIndex, len(seed.Msgs))
	}
	if state.Commands == nil || state.Done == nil || state.snapshots == nil {
		t.Fatal("restore construction left actor channels nil")
	}
	if state.cfg != bound || state.backendCfg != cfg || state.pub != pub || state.idGen == nil || state.fac != fac {
		t.Fatal("restore dependencies were not retained exactly")
	}
}

func TestRestoreConstructionClonesSnapshotSeed(t *testing.T) {
	t.Parallel()
	seed := validRestoredSeed()
	state, err := newRestoredState(context.Background(), mustID(t), mustID(t), loop.Provenance{}, &fakePublisher{}, validBoundDefinition(), restoredValidConfig(t), seqIDGen(), workingFac(), seed)
	if err != nil {
		t.Fatalf("newRestoredState: %v", err)
	}

	original := state.msgs[0]
	seed.Msgs[0] = aiMessage("mutated seed")
	if state.msgs[0] != original {
		t.Fatal("restored state aliases seed message slice")
	}

	clone := cloneMessages(state.msgs)
	clone[0] = aiMessage("mutated clone")
	if state.msgs[0] != original {
		t.Fatal("cloneMessages result aliases actor-owned slice")
	}
	if cloneMessages(nil) != nil {
		t.Fatal("cloneMessages(nil) must preserve nil")
	}
}

func TestUnstartedRestoredStateSnapshotFailsOnCallerContext(t *testing.T) {
	t.Parallel()
	state, err := newRestoredState(context.Background(), mustID(t), mustID(t), loop.Provenance{}, &fakePublisher{}, validBoundDefinition(), restoredValidConfig(t), seqIDGen(), workingFac(), validRestoredSeed())
	if err != nil {
		t.Fatalf("newRestoredState: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	msgs, turnIndex, err := state.Snapshot(ctx)
	if msgs != nil || turnIndex != 0 {
		t.Fatalf("Snapshot context error returned partial state: %v/%d", msgs, turnIndex)
	}
	var snapshotErr *SnapshotError
	if !errors.As(err, &snapshotErr) || snapshotErr.Reason != SnapshotContextDone || !errors.Is(err, context.Canceled) {
		t.Fatalf("Snapshot error = %T %v, want context-done SnapshotError", err, err)
	}
}

func TestBuildRestoredWithStartsRestoredActor(t *testing.T) {
	t.Parallel()
	build := BuildRestoredWith(restoredValidConfig(t))
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	got, err := build(ctx, mustID(t), mustID(t), loop.Provenance{}, &fakePublisher{}, validBoundDefinition(), seqIDGen(), workingFac(), validRestoredSeed())
	if err != nil || got == nil {
		t.Fatalf("BuildRestoredWith = %T %v, want backend", got, err)
	}
	state := got.(*Loop)
	msgs, turnIndex, err := state.Snapshot(context.Background())
	if err != nil || turnIndex != validRestoredSeed().TurnIndex || len(msgs) != len(validRestoredSeed().Msgs) {
		t.Fatalf("restored Snapshot = %v/%d/%v", msgs, turnIndex, err)
	}
	shutdown(t, state)
}

func TestRestoredActorResumesSIDAndAppendsAfterSeed(t *testing.T) {
	t.Parallel()
	agent := &fakeAgent{
		history: driver.History{Available: true, Steps: []content.AgenticMessages{{aiMessage("restored reply")}}},
		events:  []driver.Event{{Kind: driver.KindTerminalOK, Message: aiMessage("restored reply")}},
	}
	cfg := restoredValidConfig(t)
	cfg.Agent = agent
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	built, err := BuildRestoredWith(cfg)(ctx, mustID(t), mustID(t), loop.Provenance{}, &fakePublisher{}, validBoundDefinition(), seqIDGen(), workingFac(), validRestoredSeed())
	if err != nil {
		t.Fatalf("BuildRestoredWith: %v", err)
	}
	state := built.(*Loop)
	submit(t, state, "resume")
	waitTurnIndex(t, state, validRestoredSeed().TurnIndex+1)
	turn := agent.lastForeignTurn()
	if turn.StartNew || turn.ForeignSID != validRestoredSeed().ForeignSID {
		t.Fatalf("restored turn = {StartNew:%v ForeignSID:%q}", turn.StartNew, turn.ForeignSID)
	}
	msgs, _, err := state.Snapshot(context.Background())
	if err != nil || len(msgs) != len(validRestoredSeed().Msgs)+1 || firstText(t, msgs[len(msgs)-1]) != "restored reply" {
		t.Fatalf("restored snapshot = %v err %v", msgs, err)
	}
	shutdown(t, state)
}
