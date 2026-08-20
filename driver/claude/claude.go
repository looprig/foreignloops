package claude

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/looprig/core/content"
	"github.com/looprig/foreignloops/driver"
)

const closeGrace = 2 * time.Second

var errNilWrappedCmd = errors.New("wrap returned a nil command")

// WrapError reports a wrapper failure while retaining the underlying cause.
type WrapError struct{ Cause error }

func (e *WrapError) Error() string { return "claude: wrap foreign process: " + e.Cause.Error() }
func (e *WrapError) Unwrap() error { return e.Cause }

type agent struct {
	execPath string
	home     string
	model    string
	env      []string
	wrap     CommandWrapper
}

func (a *agent) Spawn(_ context.Context, turn driver.Turn) (driver.Stream, error) {
	if a.execPath == "" {
		return nil, &SpawnConfigError{Field: "ExecPath", Reason: "empty"}
	}
	if !cleanAbsoluteExecPath(a.execPath) {
		return nil, &SpawnConfigError{Field: "ExecPath", Reason: "must be a clean absolute path"}
	}
	if a.model == "" {
		return nil, &SpawnConfigError{Field: "Model", Reason: "empty"}
	}
	if err := platformSupported(); err != nil {
		return nil, err
	}
	cmd, stdout, err := a.start(turn)
	if err != nil {
		return nil, &driver.SpawnError{Cause: err}
	}
	decoded, decodeErr := decodeStream(stdout)
	events, stopEvents, eventsDone := forwardEvents(decoded)
	historyRoot := transcriptRoot(a.home)
	historyPath, historyPathErr := transcriptPath(a.home, turn.Cwd, turn.ForeignSID)
	s := &stream{
		events:         events,
		stopEvents:     stopEvents,
		eventsDone:     eventsDone,
		historyRoot:    historyRoot,
		historyPath:    historyPath,
		historyPathErr: historyPathErr,
		cmd:            cmd,
		decodeErr:      decodeErr,
		pgid:           cmd.Process.Pid,
	}
	return s, nil
}

func (a *agent) start(turn driver.Turn) (*exec.Cmd, io.Reader, error) {
	args := buildArgs(turn, a.model)
	// #nosec G204 -- execPath is operator-configured, and args is a fixed argv
	// list passed positionally; there is no shell and no string splitting.
	cmd := exec.Command(a.execPath, args...)
	cmd.Dir = turn.Cwd
	cmd.Env = append(make([]string, 0, len(a.env)), a.env...)
	cmd.Stdin = strings.NewReader(promptText(turn.Input))
	cmd.Stderr = io.Discard
	configureProcessGroup(cmd)
	if a.wrap != nil {
		wrapped, err := a.wrap(cmd)
		if err != nil {
			return nil, nil, &WrapError{Cause: err}
		}
		if wrapped == nil {
			return nil, nil, &WrapError{Cause: errNilWrappedCmd}
		}
		cmd = wrapped
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, nil, err
	}
	return cmd, stdout, nil
}

type stream struct {
	events         <-chan driver.Event
	stopEvents     func()
	eventsDone     <-chan struct{}
	historyRoot    string
	historyPath    string
	historyPathErr error
	cmd            *exec.Cmd
	decodeErr      func() error
	pgid           int
	once           sync.Once
	closeErr       error
}

func (s *stream) Events() <-chan driver.Event { return s.events }

func (s *stream) History() (driver.History, error) {
	if s.historyPathErr != nil {
		return driver.History{}, &driver.HistoryError{Cause: s.historyPathErr}
	}
	return historyFromContainedPath(s.historyRoot, s.historyPath)
}

func (s *stream) Close() error {
	s.once.Do(func() {
		s.closeErr = s.shutdown()
	})
	return s.closeErr
}

func (s *stream) shutdown() error {
	s.stopEvents()
	exited := make(chan struct{})
	go func() {
		// A failed observer cannot safely distinguish a running leader from an
		// exited one, so treat failure as a request for immediate escalation.
		_ = waitProcessExit(s.pgid)
		close(exited)
	}()

	_ = interruptProcessGroup(s.pgid)
	timer := time.NewTimer(closeGrace)
	select {
	case <-exited:
		if !timer.Stop() {
			<-timer.C
		}
		// The leader has exited but remains unreaped, so its PID still reserves
		// the process-group ID while any surviving descendants are killed.
		_ = killProcessGroup(s.pgid)
	case <-timer.C:
		// Escalate before waiting: the live leader still owns the numeric PGID.
		_ = killProcessGroup(s.pgid)
		<-exited
	}
	// This is the only reap, and no process-group signal can occur after it.
	waitErr := s.cmd.Wait()
	<-s.eventsDone
	decodeErr := s.decodeErr()
	if decodeErr != nil {
		slog.Warn("claude: foreign stream decode error", "error", decodeErr)
	}
	return exitError(waitErr)
}

func forwardEvents(source <-chan driver.Event) (<-chan driver.Event, func(), <-chan struct{}) {
	out := make(chan driver.Event)
	stop := make(chan struct{})
	done := make(chan struct{})
	var stopOnce sync.Once
	go func() {
		defer close(done)
		defer close(out)
		stopped := false
		for event := range source {
			if stopped {
				continue
			}
			select {
			case <-stop:
				stopped = true
			case out <- event:
			}
		}
	}()
	return out, func() { stopOnce.Do(func() { close(stop) }) }, done
}

func promptText(blocks []content.Block) string {
	var prompt strings.Builder
	for _, block := range blocks {
		if text, ok := block.(*content.TextBlock); ok {
			prompt.WriteString(text.Text)
		}
	}
	return prompt.String()
}

func exitError(err error) error {
	if err == nil {
		return nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		// A signalled process reports exit code -1. shutdown always signals
		// this driver's own process group before reaping, so a leader still
		// running at Close dies by our hand; blaming the foreign agent for
		// that teardown is wrong. Only a genuine non-zero exit is an
		// ExitError.
		if code := exitErr.ExitCode(); code > 0 {
			return &driver.ExitError{Code: code}
		}
	}
	return nil
}
