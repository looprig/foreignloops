package acp

import (
	"context"
	"sync"

	"github.com/looprig/foreignloops/driver"
)

// projection is the sole owner of a stream's public channels. Producers never
// send to, or close, Events/Observations directly. The command mailbox is
// intentionally never closed: a stale turn handle can attempt a send after
// the arbiter has retired, and the done case makes that attempt harmless.
type projection struct {
	events       chan driver.Event
	observations chan driver.Observation
	commands     chan projectionCommand
	done         chan struct{}
	abort        chan struct{}

	selectOnce sync.Once
	stopOnce   sync.Once
	abortOnce  sync.Once
}

type projectionCommand struct {
	selectView  streamView
	event       *driver.Event
	observation driver.Observation
	ack         chan struct{}
	stop        bool
}

func newProjection() *projection {
	p := &projection{
		events:       make(chan driver.Event, 4096),
		observations: make(chan driver.Observation, 4096),
		commands:     make(chan projectionCommand, 128),
		done:         make(chan struct{}),
		abort:        make(chan struct{}),
	}
	go p.run()
	return p
}

func (p *projection) stopOn(ctx context.Context) {
	if p == nil || ctx == nil {
		return
	}
	go func() {
		select {
		case <-ctx.Done():
			p.abortOwner()
		case <-p.done:
		}
	}()
}

func (p *projection) eventsView() <-chan driver.Event {
	if p == nil {
		return nil
	}
	p.selectView(viewEvents)
	return p.events
}

func (p *projection) observationsView() <-chan driver.Observation {
	if p == nil {
		return nil
	}
	p.selectView(viewObservations)
	return p.observations
}

func (p *projection) selectView(view streamView) {
	if p == nil || (view != viewEvents && view != viewObservations) {
		return
	}
	p.selectOnce.Do(func() {
		ack := make(chan struct{})
		select {
		case p.commands <- projectionCommand{selectView: view, ack: ack}:
			select {
			case <-ack:
			case <-p.done:
			case <-p.abort:
			}
		case <-p.done:
		case <-p.abort:
		}
	})
}

func (p *projection) emitEvent(event driver.Event) {
	if p == nil {
		return
	}
	copyEvent := event
	p.send(projectionCommand{event: &copyEvent})
}

func (p *projection) emitObservation(observation driver.Observation) <-chan struct{} {
	ack := make(chan struct{})
	if p == nil || observation == nil {
		close(ack)
		return ack
	}
	select {
	case p.commands <- projectionCommand{observation: observation, ack: ack}:
	case <-p.done:
		close(ack)
	case <-p.abort:
		close(ack)
	}
	return ack
}

func (p *projection) send(command projectionCommand) {
	select {
	case p.commands <- command:
	case <-p.done:
	case <-p.abort:
	}
}

func (p *projection) close() {
	if p == nil {
		return
	}
	p.stopOnce.Do(func() {
		select {
		case p.commands <- projectionCommand{stop: true}:
		case <-p.done:
		}
	})
	<-p.done
}

func (p *projection) run() {
	selected := viewUnselected
	closedEvents := false
	closedObservations := false
	stopping := false
	pending := make([]projectionCommand, 0, 32)
	defer func() {
		for _, command := range pending {
			closeProjectionAck(command.ack)
		}
		// The owner is the only goroutine that closes either public channel.
		if !closedEvents {
			close(p.events)
		}
		if !closedObservations {
			close(p.observations)
		}
		close(p.done)
	}()

	for {
		var eventOut chan driver.Event
		var observationOut chan driver.Observation
		var eventValue driver.Event
		var observationValue driver.Observation
		if len(pending) > 0 && selected != viewUnselected {
			command := pending[0]
			switch selected {
			case viewEvents:
				if command.event != nil {
					eventOut = p.events
					eventValue = *command.event
				}
			case viewObservations:
				if command.observation != nil {
					observationOut = p.observations
					observationValue = command.observation
				}
			}
			if eventOut == nil && observationOut == nil {
				closeProjectionAck(command.ack)
				pending = pending[1:]
				continue
			}
			if stopping {
				if eventOut != nil && len(eventOut) == cap(eventOut) {
					return
				}
				if observationOut != nil && len(observationOut) == cap(observationOut) {
					return
				}
			}
		} else if stopping {
			return
		}

		select {
		case command := <-p.commands:
			if command.stop {
				stopping = true
				continue
			}
			if command.selectView != viewUnselected && selected == viewUnselected {
				selected = command.selectView
				if selected == viewEvents {
					close(p.observations)
					closedObservations = true
				} else {
					close(p.events)
					closedEvents = true
				}
				closeProjectionAck(command.ack)
				continue
			}
			if command.event != nil || command.observation != nil {
				pending = append(pending, command)
			}
		case eventOut <- eventValue:
			closeProjectionAck(pending[0].ack)
			pending = pending[1:]
		case observationOut <- observationValue:
			closeProjectionAck(pending[0].ack)
			pending = pending[1:]
		case <-p.abort:
			return
		}
	}
}

func (p *projection) abortOwner() {
	if p == nil {
		return
	}
	p.abortOnce.Do(func() { close(p.abort) })
}

func closeProjectionAck(ack chan struct{}) {
	if ack != nil {
		close(ack)
	}
}
