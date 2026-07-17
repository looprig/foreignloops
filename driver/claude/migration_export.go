//go:build migration

package claude

import "github.com/looprig/core/content"

// DecodeTranscriptForMigration exposes the extracted decoder only to migration
// parity tests. It must not become a permanent public API.
func DecodeTranscriptForMigration(path string) ([]content.AgenticMessages, error) {
	return decodeTranscript(path)
}
