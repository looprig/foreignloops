package claude

import (
	"reflect"
	"testing"
)

func TestWhitelistEnv(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		parent     []string
		allow      []string
		credential map[string]string
		want       []string
	}{
		{
			name:   "allow-listed keys pass through and secret is excluded",
			parent: []string{"PATH=/usr/bin", "SECRET_TOKEN=shh", "HOME=/home/u"},
			allow:  []string{"PATH", "HOME"},
			want:   []string{"PATH=/usr/bin", "HOME=/home/u"},
		},
		{
			name:       "credentials override parent and sort after parent entries",
			parent:     []string{"PATH=/parent/bin", "HOME=/home/u"},
			allow:      []string{"PATH", "HOME"},
			credential: map[string]string{"PATH": "/credential/bin", "ANTHROPIC_API_KEY": "sk-test"},
			want:       []string{"HOME=/home/u", "ANTHROPIC_API_KEY=sk-test", "PATH=/credential/bin"},
		},
		{
			name:       "empty allow-list yields sorted credentials only",
			parent:     []string{"PATH=/usr/bin", "HOME=/home/u"},
			credential: map[string]string{"B": "2", "A": "1"},
			want:       []string{"A=1", "B=2"},
		},
		{
			name:   "value containing equals is retained",
			parent: []string{"FOO=a=b=c"},
			allow:  []string{"FOO"},
			want:   []string{"FOO=a=b=c"},
		},
		{
			name:   "malformed parent element is skipped",
			parent: []string{"BROKEN", "TERM=xterm"},
			allow:  []string{"BROKEN", "TERM"},
			want:   []string{"TERM=xterm"},
		},
		{name: "empty inputs return nil", allow: []string{"PATH"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := whitelistEnv(tt.parent, tt.allow, tt.credential)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("whitelistEnv() = %#v, want %#v", got, tt.want)
			}
		})
	}
}
