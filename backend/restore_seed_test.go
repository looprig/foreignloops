package backend

import (
	"context"
	"testing"

	"github.com/looprig/core/content"
	"github.com/looprig/foreignloops/driver"
	"github.com/looprig/harness/pkg/foreign"
	"github.com/looprig/harness/pkg/loop"
)

func TestRestoredBuilderForwardsSeedForeignSIDAsResumeTurn(t *testing.T) {
	t.Parallel()
	const seedSID = "journaled-agent-session"
	agent := &fakeAgent{
		history: driver.History{Available: true, Steps: []content.AgenticMessages{{aiMessage("continued")}}},
		events:  []driver.Event{{Kind: driver.KindTerminalOK, Message: aiMessage("continued")}},
	}
	cfg := restoredValidConfig(t)
	cfg.Agent = agent
	seed := foreign.RestoredForeign{ForeignSID: seedSID, TurnIndex: 7}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	built, err := BuildRestoredWith(cfg)(ctx, mustID(t), mustID(t), loop.Provenance{}, &fakePublisher{}, validBoundDefinition(), seqIDGen(), workingFac(), seed)
	if err != nil {
		t.Fatalf("BuildRestoredWith: %v", err)
	}
	state := built.(*Loop)
	submit(t, state, "resume")
	waitTurnIndex(t, state, seed.TurnIndex+1)

	turn := agent.lastForeignTurn()
	if turn.ForeignSID != seedSID || turn.StartNew {
		t.Fatalf("restored first turn = {StartNew:%v ForeignSID:%q}, want {StartNew:false ForeignSID:%q}", turn.StartNew, turn.ForeignSID, seedSID)
	}
	shutdown(t, state)
}
