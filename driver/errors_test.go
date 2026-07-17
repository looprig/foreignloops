package driver

import (
	"errors"
	"fmt"
	"testing"
)

func TestCauseErrors(t *testing.T) {
	cause := errors.New("low-level failure")
	tests := []struct {
		name string
		err  error
		want string
		as   func(error) bool
	}{
		{
			name: "spawn",
			err:  &SpawnError{Cause: cause},
			want: "foreignloop: spawn: low-level failure",
			as: func(err error) bool {
				var target *SpawnError
				return errors.As(err, &target) && target.Cause == cause
			},
		},
		{
			name: "decode",
			err:  &DecodeError{Cause: cause},
			want: "foreignloop: decode: low-level failure",
			as: func(err error) bool {
				var target *DecodeError
				return errors.As(err, &target) && target.Cause == cause
			},
		},
		{
			name: "history",
			err:  &HistoryError{Cause: cause},
			want: "foreignloop: authoritative history: low-level failure",
			as: func(err error) bool {
				var target *HistoryError
				return errors.As(err, &target) && target.Cause == cause
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.err.Error(); got != tt.want {
				t.Errorf("Error() = %q, want %q", got, tt.want)
			}
			wrapped := fmt.Errorf("outer: %w", tt.err)
			if !errors.Is(wrapped, cause) {
				t.Errorf("errors.Is(%v, cause) = false", wrapped)
			}
			if !tt.as(wrapped) {
				t.Errorf("errors.As(%v, target) = false", wrapped)
			}
		})
	}
}

func TestExitError(t *testing.T) {
	err := &ExitError{Code: 23}
	if got, want := err.Error(), "foreignloop: agent exited 23"; got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}

	wrapped := fmt.Errorf("outer: %w", err)
	var target *ExitError
	if !errors.As(wrapped, &target) {
		t.Fatalf("errors.As(%v, *ExitError) = false", wrapped)
	}
	if target.Code != 23 {
		t.Errorf("ExitError.Code = %d, want 23", target.Code)
	}
}
