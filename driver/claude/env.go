package claude

import (
	"sort"
	"strings"
)

// whitelistEnv returns only allowed parent entries plus explicit credentials.
// Credentials override parent entries with the same key. Parent entries retain
// their input ordering and credential keys are sorted, making the result stable.
func whitelistEnv(parent, allow []string, credential map[string]string) []string {
	allowed := make(map[string]struct{}, len(allow))
	for _, key := range allow {
		allowed[key] = struct{}{}
	}
	out := make([]string, 0, len(parent)+len(credential))
	for _, entry := range parent {
		i := strings.IndexByte(entry, '=')
		if i < 0 {
			continue
		}
		key := entry[:i]
		if _, ok := allowed[key]; !ok {
			continue
		}
		if _, overridden := credential[key]; overridden {
			continue
		}
		out = append(out, entry)
	}
	keys := make([]string, 0, len(credential))
	for key := range credential {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		out = append(out, key+"="+credential[key])
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
