package acp

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/looprig/core/content"
	"github.com/looprig/foreignloops/driver"
	"github.com/looprig/foreignloops/internal/steertest"
	"github.com/looprig/harness/pkg/loop"
)

func TestSteeringIntegrationACPInjectedObservationOrder(t *testing.T) {
	script := steertest.DefaultScript()
	script.Prompts = []steertest.PromptScript{{Actions: []steertest.Action{
		{Kind: steertest.ActionTerminal, Gate: "prompt-terminal"},
	}}}
	script.Steers = []steertest.SteerScript{{Actions: []steertest.Action{
		{Kind: steertest.ActionSteerReply, Outcome: steertest.OutcomeInjected, Gate: "steer-ack"},
	}}}

	overallCtx, overallCancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(overallCancel)
	fixture := steertest.New(t, script)
	d, err := New(overallCtx, Config{
		Harness:       HarnessClaudeCode,
		Executable:    fixture.Executable(),
		Env:           fixture.Env(),
		Credential:    loop.CredentialNativeAuth,
		Posture:       driver.PostureReadOnly,
		WorkspaceRoot: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	if _, err := fixture.WaitForNth(overallCtx, steertest.EventNewSession, 0); err != nil {
		t.Fatalf("ACP fixture session readiness: %v; transcript=%s", err, fixture.Transcript())
	}
	ctx, cancel := context.WithTimeout(overallCtx, 5*time.Second)
	defer cancel()

	stream, err := d.Spawn(ctx, driver.Turn{
		StartNew: true,
		Input:    []content.Block{&content.TextBlock{Text: "active"}},
		Cwd:      t.TempDir(),
		Posture:  driver.PostureDefault,
	})
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}
	t.Cleanup(func() { _ = stream.Close() })
	ordered, ok := stream.(driver.OrderedStream)
	if !ok {
		t.Fatal("Spawn() stream does not implement OrderedStream")
	}

	if prompt := fixture.WaitForKind(ctx, steertest.EventPrompt); prompt.Err != nil {
		t.Fatalf("prompt event: %v", prompt.Err)
	}
	request, err := driver.NewSteerRequest([]content.Block{&content.TextBlock{Text: "steer"}})
	if err != nil {
		t.Fatalf("NewSteerRequest() error = %v", err)
	}
	steerResult := make(chan struct {
		result driver.SteerResult
		err    error
	}, 1)
	go func() {
		result, err := d.Steer(ctx, request)
		steerResult <- struct {
			result driver.SteerResult
			err    error
		}{result: result, err: err}
	}()
	if gate, err := fixture.WaitForNth(ctx, steertest.EventGate, 1); err != nil || gate.Gate != "steer-ack" {
		t.Fatalf("steer gate = %#v, %v, want steer-ack", gate, err)
	}
	if err := fixture.Release("steer-ack"); err != nil {
		t.Fatalf("release steer acknowledgement: %v", err)
	}
	if steer, err := fixture.WaitForNth(ctx, steertest.EventSteer, 1); err != nil || steer.Outcome != steertest.OutcomeInjected {
		t.Fatalf("steer response event = %#v, %v, want injected", steer, err)
	}
	select {
	case completed := <-steerResult:
		if completed.err != nil || completed.result.Outcome != driver.SteerOutcomeInjected {
			t.Fatalf("Steer() = %#v, %v, want injected", completed.result, completed.err)
		}
	case <-ctx.Done():
		t.Fatalf("Steer() did not complete: %v", ctx.Err())
	}
	if err := fixture.Release("prompt-terminal"); err != nil {
		t.Fatalf("release prompt terminal: %v", err)
	}
	if terminal := fixture.WaitForKind(ctx, steertest.EventTerminal); terminal.Err != nil {
		t.Fatalf("terminal event: %v", terminal.Err)
	}

	var observations []driver.Observation
	for {
		select {
		case observation, ok := <-ordered.Observations():
			if !ok {
				wantKinds := []driver.ObservationKind{driver.ObservationSteer, driver.ObservationPrompt}
				gotKinds := make([]driver.ObservationKind, len(observations))
				for i, item := range observations {
					gotKinds[i] = item.Kind()
				}
				if !reflect.DeepEqual(gotKinds, wantKinds) {
					t.Fatalf("ordered observation kinds = %v, want %v; observations=%#v; transcript=%s", gotKinds, wantKinds, observations, fixture.Transcript())
				}
				for i := 1; i < len(observations); i++ {
					if observations[i].Sequence() <= observations[i-1].Sequence() {
						t.Fatalf("ordered observation sequences = [%d %d], want strictly increasing; transcript=%s", observations[0].Sequence(), observations[1].Sequence(), fixture.Transcript())
					}
				}
				return
			}
			observations = append(observations, observation)
		case <-ctx.Done():
			t.Fatalf("ordered observations did not close: %v", ctx.Err())
		}
	}
}

func newSteeringIntegrationACPDriver(t *testing.T, ctx context.Context, script steertest.Script, harness Harness) (*Driver, *steertest.Agent, driver.Stream) {
	t.Helper()
	fixture := steertest.New(t, script)
	d, err := New(ctx, Config{
		Harness:       harness,
		Executable:    fixture.Executable(),
		Env:           fixture.Env(),
		Credential:    loop.CredentialNativeAuth,
		Posture:       driver.PostureReadOnly,
		WorkspaceRoot: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	stream, err := d.Spawn(ctx, driver.Turn{
		StartNew: true,
		Input:    []content.Block{&content.TextBlock{Text: "active"}},
		Cwd:      t.TempDir(),
		Posture:  driver.PostureDefault,
	})
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}
	t.Cleanup(func() { _ = stream.Close() })
	return d, fixture, stream
}

func newSteeringIntegrationRequest(t *testing.T, text string) driver.SteerRequest {
	t.Helper()
	request, err := driver.NewSteerRequest([]content.Block{&content.TextBlock{Text: text}})
	if err != nil {
		t.Fatalf("NewSteerRequest() error = %v", err)
	}
	return request
}

func waitSteeringIntegrationObservations(t *testing.T, ctx context.Context, ordered driver.OrderedStream) []driver.Observation {
	t.Helper()
	var observations []driver.Observation
	for {
		select {
		case observation, ok := <-ordered.Observations():
			if !ok {
				return observations
			}
			observations = append(observations, observation)
		case <-ctx.Done():
			t.Fatalf("ordered observations did not close: %v", ctx.Err())
		}
	}
}

func steeringIntegrationCountEvents(fixture *steertest.Agent, kind steertest.EventKind) int {
	count := 0
	for _, input := range fixture.Transcript().Records {
		if input.Kind == kind {
			count++
		}
	}
	return count
}

func TestSteeringIntegrationACPOutcomeNormalization(t *testing.T) {
	tests := []struct {
		name      string
		wire      steertest.SteeringOutcome
		want      driver.SteerOutcome
		wantError bool
	}{
		{name: "promptRequired", wire: steertest.OutcomePromptRequired, want: driver.SteerOutcomeFallbackRequired},
		{name: "startedNewTurn", wire: steertest.OutcomeStartedNewTurn, want: driver.SteerOutcomeDeliveredUntrackable},
		{name: "failed", wire: steertest.OutcomeFailed, want: driver.SteerOutcomeFallbackRequired},
		{name: "unknown", wire: steertest.SteeringOutcome("future"), want: driver.SteerOutcomeDeliveryUnknown, wantError: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			overallCtx, overallCancel := context.WithTimeout(context.Background(), 30*time.Second)
			t.Cleanup(overallCancel)
			script := steertest.DefaultScript()
			script.Prompts = []steertest.PromptScript{{Actions: []steertest.Action{
				{Kind: steertest.ActionTerminal, Gate: "prompt-terminal"},
			}}}
			script.Steers = []steertest.SteerScript{{Actions: []steertest.Action{
				{Kind: steertest.ActionSteerReply, Outcome: tt.wire, Gate: "steer-ack"},
			}}}
			d, fixture, stream := newSteeringIntegrationACPDriver(t, overallCtx, script, HarnessClaudeCode)
			// The ACP fixture builds a helper binary and completes the ACP initialize
			// handshake in the constructor above. Charging that setup to the
			// assertion budget is what made these integration tests fail on loaded
			// runners, so setup runs under one bounded overall context and the tight
			// per-phase budget starts only once the fixture and driver are live.
			ctx, cancel := context.WithTimeout(overallCtx, 5*time.Second)
			defer cancel()
			ordered, ok := stream.(driver.OrderedStream)
			if !ok {
				t.Fatal("Claude Spawn() stream does not implement OrderedStream")
			}
			if prompt := fixture.WaitForKind(ctx, steertest.EventPrompt); prompt.Err != nil {
				t.Fatalf("prompt event: %v", prompt.Err)
			}
			resultCh := make(chan struct {
				result driver.SteerResult
				err    error
			}, 1)
			request := newSteeringIntegrationRequest(t, "steer")
			go func() {
				result, err := d.Steer(ctx, request)
				resultCh <- struct {
					result driver.SteerResult
					err    error
				}{result: result, err: err}
			}()
			if gate, err := fixture.WaitForNth(ctx, steertest.EventGate, 1); err != nil || gate.Gate != "steer-ack" {
				t.Fatalf("steer gate = %#v, %v", gate, err)
			}
			if err := fixture.Release("steer-ack"); err != nil {
				t.Fatalf("release steer acknowledgement: %v", err)
			}
			if steer, err := fixture.WaitForNth(ctx, steertest.EventSteer, 1); err != nil || steer.Outcome != tt.wire {
				t.Fatalf("steer response = %#v, %v, want %q", steer, err, tt.wire)
			}
			completed := <-resultCh
			if completed.result.Outcome != tt.want {
				t.Fatalf("Steer() outcome = %q, want %q (err %v)", completed.result.Outcome, tt.want, completed.err)
			}
			if (completed.err != nil) != tt.wantError {
				t.Fatalf("Steer() error = %v, want error %v", completed.err, tt.wantError)
			}
			if err := fixture.Release("prompt-terminal"); err != nil {
				t.Fatalf("release prompt terminal: %v", err)
			}
			if terminal := fixture.WaitForKind(ctx, steertest.EventTerminal); terminal.Err != nil {
				t.Fatalf("terminal event: %v", terminal.Err)
			}
			observations := waitSteeringIntegrationObservations(t, ctx, ordered)
			if len(observations) != 2 {
				t.Fatalf("ordered observations = %#v, want one steer and one prompt", observations)
			}
			if got := steeringIntegrationCountEvents(fixture, steertest.EventSteer); got != 2 {
				t.Fatalf("fixture steer records = %d, want exactly 2 request/response records", got)
			}
		})
	}
}

func TestSteeringIntegrationACPCodexCurrentProfileHasNoExtensionFallback(t *testing.T) {
	overallCtx, overallCancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(overallCancel)
	script := steertest.CodexScript()
	script.Prompts = []steertest.PromptScript{{Actions: []steertest.Action{
		{Kind: steertest.ActionTerminal, Gate: "prompt-terminal"},
	}}}
	d, fixture, stream := newSteeringIntegrationACPDriver(t, overallCtx, script, HarnessCodex)
	// The ACP fixture builds a helper binary and completes the ACP initialize
	// handshake in the constructor above. Charging that setup to the
	// assertion budget is what made these integration tests fail on loaded
	// runners, so setup runs under one bounded overall context and the tight
	// per-phase budget starts only once the fixture and driver are live.
	ctx, cancel := context.WithTimeout(overallCtx, 5*time.Second)
	defer cancel()
	if _, ok := stream.(driver.OrderedStream); ok {
		t.Fatal("current Codex Spawn() stream implements OrderedStream; want legacy Events projection")
	}
	if prompt := fixture.WaitForKind(ctx, steertest.EventPrompt); prompt.Err != nil {
		t.Fatalf("prompt event: %v", prompt.Err)
	}
	result, err := d.Steer(ctx, newSteeringIntegrationRequest(t, "steer"))
	if err != nil || result.Outcome != driver.SteerOutcomeUnsupported {
		t.Fatalf("Codex Steer() = %#v, %v, want unsupported without extension call", result, err)
	}
	if got := steeringIntegrationCountEvents(fixture, steertest.EventSteer); got != 0 {
		t.Fatalf("fixture steer records = %d, want 0", got)
	}
	if err := fixture.Release("prompt-terminal"); err != nil {
		t.Fatalf("release prompt terminal: %v", err)
	}
	if terminal := fixture.WaitForKind(ctx, steertest.EventTerminal); terminal.Err != nil {
		t.Fatalf("terminal event: %v", terminal.Err)
	}
	var events []driver.Event
	for input := range stream.Events() {
		events = append(events, input)
	}
	var gotTerminal bool
	for _, input := range events {
		if input.Kind == driver.KindTerminalOK {
			gotTerminal = true
		}
	}
	if !gotTerminal {
		t.Fatalf("Codex legacy events = %#v, want terminal event", events)
	}
}

func TestSteeringIntegrationACPLateAcknowledgementIsIgnored(t *testing.T) {
	overallCtx, overallCancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(overallCancel)
	script := steertest.DefaultScript()
	script.Prompts = []steertest.PromptScript{{Actions: []steertest.Action{
		{Kind: steertest.ActionTerminal, Gate: "prompt-terminal"},
	}}}
	script.Steers = []steertest.SteerScript{{Actions: []steertest.Action{
		{Kind: steertest.ActionSteerReply, Outcome: steertest.OutcomeInjected, Gate: "late-ack"},
	}}}
	d, fixture, stream := newSteeringIntegrationACPDriver(t, overallCtx, script, HarnessClaudeCode)
	// The ACP fixture builds a helper binary and completes the ACP initialize
	// handshake in the constructor above. Charging that setup to the
	// assertion budget is what made these integration tests fail on loaded
	// runners, so setup runs under one bounded overall context and the tight
	// per-phase budget starts only once the fixture and driver are live.
	ctx, cancel := context.WithTimeout(overallCtx, 5*time.Second)
	defer cancel()
	ordered, ok := stream.(driver.OrderedStream)
	if !ok {
		t.Fatal("Claude Spawn() stream does not implement OrderedStream")
	}
	if prompt := fixture.WaitForKind(ctx, steertest.EventPrompt); prompt.Err != nil {
		t.Fatalf("prompt event: %v", prompt.Err)
	}
	steerCtx, cancelSteer := context.WithCancel(ctx)
	steerResult := make(chan struct {
		result driver.SteerResult
		err    error
	}, 1)
	request := newSteeringIntegrationRequest(t, "late steer")
	go func() {
		result, err := d.Steer(steerCtx, request)
		steerResult <- struct {
			result driver.SteerResult
			err    error
		}{result: result, err: err}
	}()
	if gate, err := fixture.WaitForNth(ctx, steertest.EventGate, 1); err != nil || gate.Gate != "late-ack" {
		t.Fatalf("late acknowledgement gate = %#v, %v", gate, err)
	}
	cancelSteer()
	select {
	case completed := <-steerResult:
		if completed.err == nil || completed.result.Outcome != driver.SteerOutcomeDeliveryUnknown || !completed.result.WriteAdmitted {
			t.Fatalf("canceled Steer() = %#v, %v, want admitted delivery_unknown", completed.result, completed.err)
		}
	case <-ctx.Done():
		t.Fatalf("canceled Steer() did not return: %v", ctx.Err())
	}
	if err := fixture.Release("late-ack"); err != nil {
		t.Fatalf("release late acknowledgement: %v", err)
	}
	if steer, err := fixture.WaitForNth(ctx, steertest.EventSteer, 1); err != nil || steer.Outcome != steertest.OutcomeInjected {
		t.Fatalf("late steer response = %#v, %v, want injected", steer, err)
	}
	lateObserved := false
	for {
		select {
		case observation, ok := <-ordered.Observations():
			if !ok {
				t.Fatal("ordered observations closed before prompt terminal")
			}
			if steer, ok := observation.(driver.SteerObservation); ok {
				if steer.Outcome == driver.SteerOutcomeInjected {
					t.Fatal("late injected acknowledgement was exposed as a fresh delivery")
				}
				lateObserved = true
			}
		case <-ctx.Done():
			t.Fatalf("late acknowledgement observation: %v", ctx.Err())
		}
		if lateObserved {
			break
		}
	}
	second, err := d.Steer(ctx, newSteeringIntegrationRequest(t, "second"))
	if err != nil || second.Outcome != driver.SteerOutcomeUnsupported {
		t.Fatalf("Steer() after late acknowledgement = %#v, %v, want unsupported", second, err)
	}
	if got := steeringIntegrationCountEvents(fixture, steertest.EventSteer); got != 2 {
		t.Fatalf("fixture steer records = %d, want one request/response pair", got)
	}
	if err := fixture.Release("prompt-terminal"); err != nil {
		t.Fatalf("release prompt terminal: %v", err)
	}
	if terminal := fixture.WaitForKind(ctx, steertest.EventTerminal); terminal.Err != nil {
		t.Fatalf("terminal event: %v", terminal.Err)
	}
	for range ordered.Observations() {
	}
}
