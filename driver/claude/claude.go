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
	"github.com/looprig/foreignloop/driver"
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

func (a *agent) Spawn(ctx context.Context, turn driver.Turn) (driver.Stream, error) {
	if a.execPath == "" {
		return nil, &SpawnConfigError{Field: "ExecPath", Reason: "empty"}
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
	historyPath, historyPathErr := transcriptPath(a.home, turn.Cwd, turn.ForeignSID)
	s := &stream{
		events:         events,
		stopEvents:     stopEvents,
		eventsDone:     eventsDone,
		historyPath:    historyPath,
		historyPathErr: historyPathErr,
		cmd:            cmd,
		decodeErr:      decodeErr,
		pgid:           cmd.Process.Pid,
	}
	if ctx != nil {
		s.stopContext = context.AfterFunc(ctx, func() { _ = s.Close() })
	}
	return s, nil
}

func (a *agent) start(turn driver.Turn) (*exec.Cmd, io.Reader, error) {
	args := buildArgs(turn, a.model)
	// #nosec G204 -- execPath is operator-configured, and args is a fixed argv
	// list passed positionally; there is no shell and no string splitting.
	cmd := exec.Command(a.execPath, args...)
	cmd.Dir = turn.Cwd
	cmd.Env = append([]string(nil), a.env...)
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
	historyPath    string
	historyPathErr error
	cmd            *exec.Cmd
	decodeErr      func() error
	pgid           int
	stopContext    func() bool
	once           sync.Once
	closeErr       error
}

func (s *stream) Events() <-chan driver.Event { return s.events }

func (s *stream) History() (driver.History, error) {
	if s.historyPathErr != nil {
		return driver.History{}, &driver.HistoryError{Cause: s.historyPathErr}
	}
	return historyFromPath(s.historyPath)
}

func (s *stream) Close() error {
	s.once.Do(func() {
		if s.stopContext != nil {
			s.stopContext()
		}
		s.closeErr = s.shutdown()
	})
	return s.closeErr
}

func (s *stream) shutdown() error {
	s.stopEvents()
	_ = interruptProcessGroup(s.pgid)
	kill := time.AfterFunc(closeGrace, func() { _ = killProcessGroup(s.pgid) })
	defer kill.Stop()
	waitErr := s.cmd.Wait()
	<-s.eventsDone
	decodeErr := s.decodeErr()
	if decodeErr != nil {
		slog.Warn("claude: foreign stream decode error", "error", decodeErr)
	}
	return errors.Join(decodeErr, exitError(waitErr))
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
		if code := exitErr.ExitCode(); code != 0 {
			return &driver.ExitError{Code: code}
		}
	}
	return nil
}
