package claude

import (
	"reflect"
	"testing"

	"github.com/looprig/foreignloops/driver"
)

const testSID = "11111111-2222-3333-4444-555555555555"

func TestBuildArgsExactOrder(t *testing.T) {
	t.Parallel()
	turn := driver.Turn{
		SystemPrompt: "SYS PROMPT",
		ForeignSID:   testSID,
		StartNew:     true,
		Cwd:          "/work/dir",
		Posture:      driver.PostureAcceptEdits,
	}
	want := []string{
		"-p",
		"--output-format", "stream-json",
		"--include-partial-messages",
		"--verbose",
		"--append-system-prompt", "SYS PROMPT",
		"--model", "the-model",
		"--permission-mode", "acceptEdits",
		"--add-dir", "/work/dir",
		"--session-id", testSID,
	}
	if got := buildArgs(turn, "the-model"); !reflect.DeepEqual(got, want) {
		t.Fatalf("buildArgs() = %#v, want %#v", got, want)
	}
}

func TestBuildArgsSessionSelector(t *testing.T) {
	t.Parallel()
	const sid = testSID
	for _, tt := range []struct {
		name     string
		startNew bool
		wantTail []string
	}{
		{name: "start", startNew: true, wantTail: []string{"--session-id", sid}},
		{name: "resume", startNew: false, wantTail: []string{"--resume", sid}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := buildArgs(driver.Turn{ForeignSID: sid, StartNew: tt.startNew}, "small")
			if !reflect.DeepEqual(got[len(got)-2:], tt.wantTail) {
				t.Fatalf("buildArgs() tail = %#v, want %#v", got[len(got)-2:], tt.wantTail)
			}
		})
	}
}

func TestPostureStringFailsSecure(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name    string
		posture driver.PermissionPosture
		want    string
	}{
		{name: "default", posture: driver.PostureDefault, want: "default"},
		{name: "accept edits", posture: driver.PostureAcceptEdits, want: "acceptEdits"},
		{name: "unknown", posture: driver.PermissionPosture(255), want: "default"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := postureString(tt.posture); got != tt.want {
				t.Fatalf("postureString(%d) = %q, want %q", tt.posture, got, tt.want)
			}
		})
	}
}
