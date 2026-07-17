package codex

import (
	"bufio"
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

// closeGrace is how long the process group has to exit on SIGINT before SIGKILL.
const closeGrace = 2 * time.Second

// maxLineBytes is the scanner's per-line ceiling for Codex JSONL output.
const maxLineBytes = 1 << 20

type agent struct {
	execPath         string
	model            string
	profile          string
	additionalDirs   []string
	sandbox          SandboxMode
	approval         ApprovalPolicy
	env              []string
	ignoreUserConfig bool
	ignoreRules      bool
	skipGitRepoCheck bool
}

// Spawn starts the Codex CLI for one turn in its own process group and returns
// the live decoded stream.
func (a *agent) Spawn(ctx context.Context, turn driver.Turn) (driver.Stream, error) {
	if a.execPath == "" {
		return nil, &SpawnConfigError{Field: "ExecPath", Reason: "empty"}
	}
	if !cleanAbsoluteExecPath(a.execPath) {
		return nil, &SpawnConfigError{Field: "ExecPath", Reason: "must be a clean absolute path"}
	}
	if (!turn.StartNew || turn.ForeignSID != "") && !validSessionID(turn.ForeignSID) {
		return nil, &SpawnConfigError{Field: "ForeignSID", Reason: "must be a UUID"}
	}
	if err := platformSupported(); err != nil {
		return nil, err
	}
	cmd, stdout, err := a.start(turn)
	if err != nil {
		return nil, &driver.SpawnError{Cause: err}
	}
	decoded, decodeErr := decodeJSONL(stdout)
	events, stopEvents, eventsDone := forwardEvents(decoded)
	s := &stream{
		events:     events,
		stopEvents: stopEvents,
		eventsDone: eventsDone,
		cmd:        cmd,
		decodeErr:  decodeErr,
		pgid:       cmd.Process.Pid,
		closed:     make(chan struct{}),
	}
	if ctx != nil {
		go func() {
			select {
			case <-ctx.Done():
				_ = s.Close()
			case <-s.closed:
			}
		}()
	}
	return s, nil
}

// start builds and starts the child process without a shell. stderr is sent to
// io.Discard so child output can never block on an unread stderr pipe.
func (a *agent) start(turn driver.Turn) (*exec.Cmd, io.Reader, error) {
	cfg := runConfig{
		cwd:              turn.Cwd,
		model:            a.model,
		profile:          a.profile,
		additionalDirs:   append([]string(nil), a.additionalDirs...),
		sandbox:          a.sandbox,
		approval:         a.approval,
		ignoreUserConfig: a.ignoreUserConfig,
		ignoreRules:      a.ignoreRules,
		skipGitRepoCheck: a.skipGitRepoCheck,
	}
	prompt := promptText(turn.SystemPrompt, turn.Input)
	var args []string
	if turn.StartNew {
		args = buildStartArgs(turn, cfg, prompt)
	} else {
		args = buildResumeArgs(turn, cfg, prompt)
	}
	// #nosec G204 -- execPath is operator-configured, and args is a fixed argv
	// list passed positionally; there is no shell and no string splitting.
	cmd := exec.Command(a.execPath, args...)
	cmd.Dir = turn.Cwd
	cmd.Env = append(make([]string, 0, len(a.env)), a.env...)
	cmd.Stderr = io.Discard
	configureProcessGroup(cmd)
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
	events     <-chan driver.Event
	stopEvents func()
	eventsDone <-chan struct{}
	cmd        *exec.Cmd
	decodeErr  func() error
	pgid       int
	closed     chan struct{}
	once       sync.Once
	closeErr   error
}

func (s *stream) Events() <-chan driver.Event { return s.events }

func (s *stream) History() (driver.History, error) {
	return driver.History{Available: false}, nil
}

// Close signals the child's process group, waits for the decoder to drain, and
// reaps the child. Repeat calls return the first result.
func (s *stream) Close() error {
	s.once.Do(func() {
		close(s.closed)
		s.closeErr = s.shutdown()
	})
	return s.closeErr
}

func (s *stream) shutdown() error {
	s.stopEvents()
	interruptErr := interruptProcessGroup(s.pgid)
	if !processGroupMissing(interruptErr) {
		// Keep the leader unreaped until escalation. Its PID is the process-group
		// ID, so it cannot be reused while the delayed group signal is pending.
		timer := time.NewTimer(closeGrace)
		<-timer.C
		_ = killProcessGroup(s.pgid)
	}
	waitErr := s.cmd.Wait()
	<-s.eventsDone
	decodeErr := s.decodeErr()
	if decodeErr != nil {
		slog.Warn("codex: foreign stream decode error", "error", decodeErr)
	}
	return errors.Join(decodeErr, exitError(waitErr))
}

func decodeJSONL(r io.Reader) (<-chan driver.Event, func() error) {
	ch := make(chan driver.Event)
	var (
		mu       sync.Mutex
		firstErr error
	)
	setErr := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		if firstErr == nil {
			firstErr = err
		}
	}
	go func() {
		defer close(ch)
		scanner := bufio.NewScanner(r)
		scanner.Buffer(make([]byte, 0, 64*1024), maxLineBytes)
		for scanner.Scan() {
			events, err := decodeLine(scanner.Bytes())
			if err != nil {
				setErr(err)
				continue
			}
			for _, event := range events {
				ch <- event
			}
		}
		if err := scanner.Err(); err != nil {
			setErr(&driver.DecodeError{Cause: err})
		}
	}()
	return ch, func() error {
		mu.Lock()
		defer mu.Unlock()
		return firstErr
	}
}

// forwardEvents permits Close to stop consumer delivery while continuing to
// drain the decoder until the child closes stdout.
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

// promptText flattens text input blocks into the final Codex argv element.
func promptText(system string, blocks []content.Block) string {
	var task strings.Builder
	for _, block := range blocks {
		if text, ok := block.(*content.TextBlock); ok {
			task.WriteString(text.Text)
		}
	}
	return "<looprig-system>" + system + "</looprig-system>\n\n<user-task>" + task.String() + "</user-task>"
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
