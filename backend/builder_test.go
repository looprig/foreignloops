package backend

import (
	"context"
	"errors"
	"testing"

	"github.com/looprig/core/uuid"
	"github.com/looprig/foreignloops/driver"
	"github.com/looprig/harness/pkg/foreign"
	"github.com/looprig/harness/pkg/loop"
)

func TestBuildWithReturnsHarnessBuilder(t *testing.T) {
	t.Parallel()
	var _ foreign.Builder = BuildWith(builderConfig())
}

func TestBuildWithFailsClosedOnInvalidRuntimeWiring(t *testing.T) {
	t.Parallel()

	build := BuildWith(builderConfig())
	got, sid, err := build(context.Background(), uuid.UUID{}, uuid.UUID{}, loop.Provenance{}, nil, nil, nil, nil)
	if got != nil {
		t.Fatalf("BuildWith error returned non-nil Backend %T", got)
	}
	if sid != "" {
		t.Fatalf("BuildWith error sid = %q, want empty", sid)
	}
	var cfgErr *ConfigError
	if !errors.As(err, &cfgErr) {
		t.Fatalf("BuildWith error = %T %v, want *ConfigError", err, err)
	}
	if cfgErr.Field != "System" {
		t.Fatalf("ConfigError.Field = %q, want System", cfgErr.Field)
	}
}

func TestBuildWithServicesCopiesServicesIntoActorState(t *testing.T) {
	t.Parallel()

	services := foreign.NewServices(
		foreign.NewBrokerDescriptor("unix:///tmp/foreign-loop.sock", []byte("capability")),
		builderDeliveryHook{},
	)
	build := BuildWithServices(builderConfig())
	var _ foreign.ServicesBuilder = build

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	state, sid, err := build(ctx, mustID(t), mustID(t), loop.Provenance{}, &fakePublisher{}, validBoundDefinition(), seqIDGen(), workingFac(), services)
	if err != nil {
		t.Fatalf("BuildWithServices: %v", err)
	}
	if sid == "" {
		t.Fatal("BuildWithServices returned empty prebound sid")
	}
	actor, ok := state.(*Loop)
	if !ok {
		t.Fatalf("BuildWithServices returned %T, want *Loop", state)
	}
	if actor.services.Delivery == nil || actor.services.Delivery != services.Delivery {
		t.Fatal("BuildWithServices did not retain the scoped delivery hook")
	}
	if actor.services.Broker.Endpoint() != services.Broker.Endpoint() {
		t.Fatalf("actor broker endpoint = %q, want %q", actor.services.Broker.Endpoint(), services.Broker.Endpoint())
	}
	if got, want := string(actor.services.Broker.Capability()), string(services.Broker.Capability()); got != want {
		t.Fatalf("actor broker capability = %q, want %q", got, want)
	}

	// The actor owns a service value, so replacing its public fields must not
	// mutate the caller's value or the original capability snapshot.
	actor.services = foreign.NewServices(foreign.NewBrokerDescriptor("changed", []byte("changed")), nil)
	if services.Broker.Endpoint() != "unix:///tmp/foreign-loop.sock" || services.Delivery == nil {
		t.Fatal("BuildWithServices aliases the caller's Services value")
	}
}

func TestBuildRestoredWithServicesCopiesServicesIntoActorState(t *testing.T) {
	t.Parallel()

	services := foreign.NewServices(
		foreign.NewBrokerDescriptor("unix:///tmp/restored-foreign-loop.sock", []byte("restored-capability")),
		builderDeliveryHook{},
	)
	build := BuildRestoredWithServices(restoredBuilderConfig(t))
	var _ foreign.ServicesRestoredBuilder = build

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	state, err := build(ctx, mustID(t), mustID(t), loop.Provenance{}, &fakePublisher{}, validBoundDefinition(), seqIDGen(), workingFac(), validRestoredSeed(), services)
	if err != nil {
		t.Fatalf("BuildRestoredWithServices: %v", err)
	}
	actor, ok := state.(*Loop)
	if !ok {
		t.Fatalf("BuildRestoredWithServices returned %T, want *Loop", state)
	}
	if actor.services.Delivery == nil || actor.services.Delivery != services.Delivery {
		t.Fatal("BuildRestoredWithServices did not retain the scoped delivery hook")
	}
	if actor.services.Broker.Endpoint() != services.Broker.Endpoint() {
		t.Fatalf("restored actor broker endpoint = %q, want %q", actor.services.Broker.Endpoint(), services.Broker.Endpoint())
	}
	if got, want := string(actor.services.Broker.Capability()), string(services.Broker.Capability()); got != want {
		t.Fatalf("restored actor broker capability = %q, want %q", got, want)
	}
}

func TestLegacyBuildersPreserveZeroServices(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	live, _, err := BuildWith(builderConfig())(ctx, mustID(t), mustID(t), loop.Provenance{}, &fakePublisher{}, validBoundDefinition(), seqIDGen(), workingFac())
	if err != nil {
		t.Fatalf("BuildWith: %v", err)
	}
	if actor := live.(*Loop); actor.services.Delivery != nil || actor.services.Broker.Endpoint() != "" || len(actor.services.Broker.Capability()) != 0 {
		t.Fatalf("legacy live builder services = %#v, want zero", actor.services)
	}

	restored, err := BuildRestoredWith(restoredBuilderConfig(t))(ctx, mustID(t), mustID(t), loop.Provenance{}, &fakePublisher{}, validBoundDefinition(), seqIDGen(), workingFac(), validRestoredSeed())
	if err != nil {
		t.Fatalf("BuildRestoredWith: %v", err)
	}
	if actor := restored.(*Loop); actor.services.Delivery != nil || actor.services.Broker.Endpoint() != "" || len(actor.services.Broker.Capability()) != 0 {
		t.Fatalf("legacy restored builder services = %#v, want zero", actor.services)
	}
}

func builderConfig() Config {
	return Config{
		Agent:   &fakeAgent{},
		Cwd:     "/workspace",
		Posture: driver.PostureDefault,
		SIDMode: SIDPrebound,
	}
}

func restoredBuilderConfig(t *testing.T) Config {
	t.Helper()
	cfg := builderConfig()
	cfg.Cwd = t.TempDir()
	return cfg
}

type builderDeliveryHook struct{}

func (builderDeliveryHook) CreateIntent(context.Context, foreign.DeliveryIntent) error { return nil }
func (builderDeliveryHook) Reserve(context.Context, foreign.DeliveryReservation) error { return nil }
func (builderDeliveryHook) QueueFallback(context.Context, foreign.DeliveryFallback) error {
	return nil
}
func (builderDeliveryHook) Resolve(context.Context, foreign.DeliveryResolution) error { return nil }
