package claude

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func seedFixtureLines(f *testing.F, dir string) {
	f.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		f.Fatalf("read fixture directory %s: %v", dir, err)
	}
	f.Add([]byte{})
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".jsonl" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			f.Fatalf("read fixture %s: %v", entry.Name(), err)
		}
		for _, line := range bytes.Split(data, []byte{'\n'}) {
			f.Add(line)
		}
	}
}

func FuzzDecodeStreamLine(f *testing.F) {
	seedFixtureLines(f, filepath.Join("testdata", "stream"))
	f.Fuzz(func(t *testing.T, line []byte) {
		_, _ = decodeStreamLine(line)
	})
}

func FuzzDecodeTranscriptLine(f *testing.F) {
	seedFixtureLines(f, filepath.Join("testdata", "transcript"))
	f.Fuzz(func(t *testing.T, line []byte) {
		_, _, _ = decodeTranscriptLine(line)
	})
}
