package claude

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestTranscriptPath(t *testing.T) {
	t.Parallel()
	const home = "/home/u"
	const want = "/home/u/.claude/projects/-Users-x-y/11111111-2222-3333-4444-555555555555.jsonl"
	for _, tt := range []struct {
		name    string
		cwd     string
		sid     string
		want    string
		wantErr bool
	}{
		{name: "happy", cwd: "/Users/x/y", sid: testSID, want: want},
		{name: "trailing slash", cwd: "/Users/x/y/", sid: testSID, want: want},
		{name: "parent traversal sid", cwd: "/Users/x/y", sid: "../etc/passwd", wantErr: true},
		{name: "slash sid", cwd: "/Users/x/y", sid: "a/b", wantErr: true},
		{name: "backslash sid", cwd: "/Users/x/y", sid: `a\b`, wantErr: true},
		{name: "non-hex sid", cwd: "/Users/x/y", sid: "zzzzzzzz-2222-3333-4444-555555555555", wantErr: true},
		{name: "empty sid", cwd: "/Users/x/y", sid: "", wantErr: true},
		{name: "uppercase sid", cwd: "/Users/x/y", sid: "AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE", want: "/home/u/.claude/projects/-Users-x-y/AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE.jsonl"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := transcriptPath(home, tt.cwd, tt.sid)
			if (err != nil) != tt.wantErr {
				t.Fatalf("transcriptPath() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				var pathErr *PathError
				if !errors.As(err, &pathErr) {
					t.Fatalf("transcriptPath() error = %T %v, want *PathError", err, err)
				}
				return
			}
			if got != tt.want {
				t.Fatalf("transcriptPath() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestWithinRejectsEscape(t *testing.T) {
	t.Parallel()
	root := filepath.Join("root", "projects")
	if err := within(root, filepath.Join(root, "project", "history.jsonl")); err != nil {
		t.Fatalf("within() contained path error = %v", err)
	}
	err := within(root, filepath.Join("root", "outside", "history.jsonl"))
	var pathErr *PathError
	if !errors.As(err, &pathErr) {
		t.Fatalf("within() escape error = %T %v, want *PathError", err, err)
	}
}
