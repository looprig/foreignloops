package acp

import "sync"

// steeringObservationCapacity is intentionally independent from both public
// projection buffers. Non-steer updates can fill those buffers, but they must
// not make an already accepted steer unable to publish its one observation.
const steeringObservationCapacity = 128

// steerObservationLane reserves one bounded slot for each accepted steer. The
// lane is a counter rather than a channel so Close can release all outstanding
// reservations atomically and racing late completions can safely no-op.
type steerObservationLane struct {
	mu       sync.Mutex
	capacity int
	used     int
	closed   bool
}

type steerObservationReservation struct {
	lane *steerObservationLane
	once sync.Once
}

func newSteerObservationLane(capacity int) *steerObservationLane {
	if capacity < 0 {
		capacity = 0
	}
	return &steerObservationLane{capacity: capacity}
}

func (l *steerObservationLane) reserve() (*steerObservationReservation, bool) {
	if l == nil {
		return nil, false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed || l.used >= l.capacity {
		return nil, false
	}
	l.used++
	return &steerObservationReservation{lane: l}, true
}

func (r *steerObservationReservation) release() {
	if r == nil || r.lane == nil {
		return
	}
	r.once.Do(func() {
		r.lane.mu.Lock()
		if !r.lane.closed && r.lane.used > 0 {
			r.lane.used--
		}
		r.lane.mu.Unlock()
	})
}

func (l *steerObservationLane) close() {
	if l == nil {
		return
	}
	l.mu.Lock()
	l.closed = true
	l.used = 0
	l.mu.Unlock()
}

func (l *steerObservationLane) inUse() int {
	if l == nil {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.used
}
