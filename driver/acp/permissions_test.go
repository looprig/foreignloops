package acp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/looprig/acp/client"
	"github.com/looprig/acp/launch"
	"github.com/looprig/acp/protocol"
	"github.com/looprig/foreignloops/driver"
)

func TestPermissionHandlerSelectsOfferedOptionByKind(t *testing.T) {
	workspaceRoot := t.TempDir()
	outsidePath := filepath.Join(filepath.Dir(workspaceRoot), "outside", "note.txt")

	tests := []struct {
		name          string
		posture       driver.Posture
		request       protocol.RequestPermissionRequest
		wantOptionID  protocol.PermissionOptionID
		wantError     bool
		wantErrorText []string
	}{
		{
			name:    "read-only rejects mutation",
			posture: driver.PostureReadOnly,
			request: permissionRequest(
				protocol.ToolKindEdit,
				[]protocol.ToolCallContent{{Diff: &protocol.Diff{Path: workspaceRoot + "/note.txt"}}},
				permissionOptions(),
			),
			wantOptionID: "reject-by-kind",
		},
		{
			name:    "read-only rejects execution content",
			posture: driver.PostureReadOnly,
			request: permissionRequest(
				protocol.ToolKindExecute,
				[]protocol.ToolCallContent{{Terminal: &protocol.Terminal{TerminalID: "terminal-secret"}}},
				permissionOptions(),
			),
			wantOptionID: "reject-by-kind",
		},
		{
			name:         "read-only rejects network kind",
			posture:      driver.PostureReadOnly,
			request:      permissionRequest(protocol.ToolKindFetch, nil, permissionOptions()),
			wantOptionID: "reject-by-kind",
		},
		{
			name:    "read-only allows protocol read kind",
			posture: driver.PostureReadOnly,
			request: permissionRequest(
				protocol.ToolKindRead,
				nil,
				[]protocol.PermissionOption{
					{Name: "Reject this", Kind: protocol.PermissionOptionKindAllowOnce, OptionID: "allow-by-kind"},
					{Name: "Allow this", Kind: protocol.PermissionOptionKindRejectOnce, OptionID: "reject-by-kind"},
				},
			),
			wantOptionID: "allow-by-kind",
		},
		{
			name:         "read-only rejects ambiguous kind",
			posture:      driver.PostureReadOnly,
			request:      permissionRequest(protocol.ToolKindOther, nil, permissionOptions()),
			wantOptionID: "reject-by-kind",
		},
		{
			name:    "workspace-write allows edit inside root",
			posture: driver.PostureWorkspaceWrite,
			request: permissionRequest(
				protocol.ToolKindEdit,
				[]protocol.ToolCallContent{{Diff: &protocol.Diff{Path: workspaceRoot + "/nested/note.txt"}}},
				permissionOptions(),
			),
			wantOptionID: "allow-by-kind",
		},
		{
			name:    "workspace-write rejects edit outside root",
			posture: driver.PostureWorkspaceWrite,
			request: permissionRequest(
				protocol.ToolKindEdit,
				[]protocol.ToolCallContent{{Diff: &protocol.Diff{Path: outsidePath}}},
				permissionOptions(),
			),
			wantOptionID: "reject-by-kind",
		},
		{
			name:    "workspace-write rejects path traversal",
			posture: driver.PostureWorkspaceWrite,
			request: permissionRequest(
				protocol.ToolKindEdit,
				[]protocol.ToolCallContent{{Diff: &protocol.Diff{Path: workspaceRoot + "/../other/note.txt"}}},
				permissionOptions(),
			),
			wantOptionID: "reject-by-kind",
		},
		{
			name:    "workspace-write allows execution scoped to root",
			posture: driver.PostureWorkspaceWrite,
			request: permissionRequestWithLocations(
				protocol.ToolKindExecute,
				nil,
				permissionOptions(),
				workspaceRoot+"/script.sh",
			),
			wantOptionID: "allow-by-kind",
		},
		{
			name:         "workspace-write rejects network kind",
			posture:      driver.PostureWorkspaceWrite,
			request:      permissionRequest(protocol.ToolKindFetch, nil, permissionOptions()),
			wantOptionID: "reject-by-kind",
		},
		{
			name:         "workspace-write rejects ambiguous request",
			posture:      driver.PostureWorkspaceWrite,
			request:      permissionRequest(protocol.ToolKindEdit, nil, permissionOptions()),
			wantOptionID: "reject-by-kind",
		},
		{
			name:    "no options fails closed",
			posture: driver.PostureReadOnly,
			request: permissionRequest(
				protocol.ToolKindRead,
				nil,
				nil,
			),
			wantError:     true,
			wantErrorText: []string{"/private/secret.txt", "https://secret.example", "token-value", "raw protocol error"},
		},
		{
			name:    "only always allow fails closed",
			posture: driver.PostureReadOnly,
			request: permissionRequest(
				protocol.ToolKindRead,
				nil,
				[]protocol.PermissionOption{{Name: "Allow", Kind: protocol.PermissionOptionKindAllowAlways, OptionID: "always-allow"}},
			),
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := newPermissionHandler(tt.posture, workspaceRoot)
			got, err := handler.RequestPermission(context.Background(), tt.request)
			if tt.wantError {
				if err == nil {
					t.Fatal("RequestPermission() error = nil, want fail-closed error")
				}
				for _, forbidden := range tt.wantErrorText {
					if strings.Contains(err.Error(), forbidden) {
						t.Errorf("RequestPermission() error contains sensitive value %q: %q", forbidden, err)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("RequestPermission() error = %v", err)
			}
			if got.Outcome.Selected == nil {
				t.Fatalf("RequestPermission() selected outcome = nil, want option %q", tt.wantOptionID)
			}
			if got.Outcome.Selected.OptionID != protocol.PermissionOptionID(tt.wantOptionID) {
				t.Fatalf("selected option id = %q, want %q", got.Outcome.Selected.OptionID, tt.wantOptionID)
			}
		})
	}
}

func TestPathWithinWorkspaceRejectsSymlinkEscape(t *testing.T) {
	workspaceRoot := t.TempDir()
	outsideRoot := t.TempDir()
	linkPath := filepath.Join(workspaceRoot, "linked")
	if err := os.Symlink(outsideRoot, linkPath); err != nil {
		t.Fatalf("create test symlink: %v", err)
	}

	if pathWithinWorkspace(workspaceRoot, filepath.Join(linkPath, "nested", "note.txt")) {
		t.Fatal("path through an escaping symlink was accepted")
	}
}

func TestPathWithinWorkspaceAllowsSafeNestedPath(t *testing.T) {
	workspaceRoot := t.TempDir()
	nestedRoot := filepath.Join(workspaceRoot, "nested", "directory")
	if err := os.MkdirAll(nestedRoot, 0o755); err != nil {
		t.Fatalf("create nested workspace directory: %v", err)
	}

	if !pathWithinWorkspace(workspaceRoot, filepath.Join(nestedRoot, "new-note.txt")) {
		t.Fatal("safe nested path was rejected")
	}
}

func TestPermissionHandlerRejectsDeniedRequestWithoutRejectOption(t *testing.T) {
	handler := newPermissionHandler(driver.PostureReadOnly, "/workspace/project")
	request := permissionRequest(
		protocol.ToolKindExecute,
		nil,
		[]protocol.PermissionOption{{Name: "Allow once", Kind: protocol.PermissionOptionKindAllowOnce, OptionID: "allow"}},
	)

	_, err := handler.RequestPermission(context.Background(), request)
	if err == nil {
		t.Fatal("RequestPermission() error = nil, want fail-closed error")
	}
	if !errors.Is(err, errPermissionDenied) {
		t.Fatalf("RequestPermission() error = %v, want permission denial", err)
	}
}

func TestNewAlwaysInstallsPermissionHandler(t *testing.T) {
	cfg := validConfig(HarnessCodex)
	cfg.AgentSessionID = ""
	var gotLaunchConfig launch.Config
	sess := newFakeSession("permission-session")
	owned := &fakeDialedClient{acpClient: &fakeClient{newSession: sess}}

	installDial(t, func(_ context.Context, got launch.Config) (dialedClient, error) {
		gotLaunchConfig = got
		return owned, nil
	})

	d, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if d == nil {
		t.Fatal("New() driver = nil, want driver")
	}
	if gotLaunchConfig.Client.Permissions == nil {
		t.Fatal("launch config client permissions = nil, want always-registered handler")
	}
	handler, ok := gotLaunchConfig.Client.Permissions.(*permissionHandler)
	if !ok {
		t.Fatalf("launch config client permissions = %T, want *permissionHandler", gotLaunchConfig.Client.Permissions)
	}
	if handler.posture != cfg.Posture || handler.workspaceRoot != cfg.WorkspaceRoot {
		t.Fatalf("permission handler = %+v, want posture %q and workspace root %q", handler, cfg.Posture, cfg.WorkspaceRoot)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func permissionOptions() []protocol.PermissionOption {
	return []protocol.PermissionOption{
		{Name: "Allow this operation", Kind: protocol.PermissionOptionKindRejectOnce, OptionID: "reject-by-kind"},
		{Name: "Reject this operation", Kind: protocol.PermissionOptionKindAllowOnce, OptionID: "allow-by-kind"},
	}
}

func permissionRequest(kind protocol.ToolKind, content []protocol.ToolCallContent, options []protocol.PermissionOption) protocol.RequestPermissionRequest {
	return permissionRequestWithLocations(kind, content, options)
}

func permissionRequestWithLocations(kind protocol.ToolKind, content []protocol.ToolCallContent, options []protocol.PermissionOption, paths ...string) protocol.RequestPermissionRequest {
	request := protocol.RequestPermissionRequest{
		Options:   options,
		SessionID: "session-id",
		ToolCall: protocol.ToolCallUpdate{
			Content:  content,
			Kind:     &kind,
			RawInput: []byte(`{"path":"/private/secret.txt","url":"https://secret.example","token":"token-value"}`),
			Title:    stringPtr("raw protocol error"),
		},
	}
	for _, path := range paths {
		request.ToolCall.Locations = append(request.ToolCall.Locations, protocol.ToolCallLocation{Path: path})
	}
	return request
}

func stringPtr(value string) *string { return &value }

var _ client.PermissionHandler = (*permissionHandler)(nil)
