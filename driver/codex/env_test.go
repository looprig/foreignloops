package codex

import (
	"reflect"
	"testing"
)

func TestWhitelistEnvIncludesOnlyAllowedParentKeysPlusSortedCredentials(t *testing.T) {
	t.Parallel()
	parent := []string{
		"SECRET_TOKEN=drop",
		"TERM=xterm-256color",
		"PATH=/usr/bin",
		"MALFORMED",
		"HOME=/home/runner",
		"EQUALS=a=b=c",
	}
	want := []string{
		"CODEX_API_KEY=first",
		"EQUALS=a=b=c",
		"HOME=/home/runner",
		"PATH=/credential/bin",
		"ZZZ_TOKEN=last",
	}
	got := whitelistEnv(parent, []string{"PATH", "HOME", "EQUALS"}, map[string]string{
		"ZZZ_TOKEN": "last", "CODEX_API_KEY": "first", "PATH": "/credential/bin",
	})
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("whitelistEnv() = %#v, want %#v", got, want)
	}
}

func TestWhitelistEnvEmptyReturnsOwnedEmptySlice(t *testing.T) {
	t.Parallel()
	if got := whitelistEnv([]string{"PATH=/usr/bin"}, nil, nil); !reflect.DeepEqual(got, []string{}) {
		t.Fatalf("whitelistEnv() = %#v, want empty slice", got)
	}
}
