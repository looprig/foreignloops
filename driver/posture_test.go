package driver

import "testing"

func TestPostureValid(t *testing.T) {
	tests := []struct {
		name    string
		posture Posture
		want    bool
	}{
		{name: "read-only", posture: PostureReadOnly, want: true},
		{name: "workspace-write", posture: PostureWorkspaceWrite, want: true},
		{name: "zero value", posture: Posture(""), want: false},
		{name: "unknown", posture: Posture("yolo"), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.posture.Valid(); got != tt.want {
				t.Errorf("Posture(%q).Valid() = %t, want %t", tt.posture, got, tt.want)
			}
		})
	}
}
