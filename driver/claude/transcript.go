package claude

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// PathError reports fail-closed transcript-path derivation.
type PathError struct{ Reason string }

func (e *PathError) Error() string { return "claude: transcript path: " + e.Reason }

var sidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

const (
	claudeDir     = ".claude"
	projectsDir   = "projects"
	transcriptExt = ".jsonl"
)

func transcriptPath(home, cwd, sid string) (string, error) {
	if !sidPattern.MatchString(sid) {
		return "", &PathError{Reason: "sid is not a plain UUID"}
	}
	encoded := strings.ReplaceAll(filepath.Clean(cwd), string(filepath.Separator), "-")
	root := transcriptRoot(home)
	full := filepath.Clean(filepath.Join(root, encoded, sid+transcriptExt))
	if err := within(root, full); err != nil {
		return "", err
	}
	return full, nil
}

func transcriptRoot(home string) string {
	return filepath.Clean(filepath.Join(home, claudeDir, projectsDir))
}

// openContainedTranscript resolves and validates the provider path before any
// transcript bytes are read. The Go standard library has no portable openat
// traversal for both macOS and Linux, so this resolves once before open, then
// resolves again and compares file identity after open. The opened descriptor
// pins subsequent reads and the second check detects ordinary path-swap races;
// eliminating every same-inode hard-link race would require a non-stdlib,
// platform-specific open-relative implementation.
func openContainedTranscript(root, path string) (_ *os.File, err error) {
	root = filepath.Clean(root)
	path = filepath.Clean(path)
	if err := within(root, path); err != nil {
		return nil, err
	}
	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return nil, fmt.Errorf("resolve projects root: %w", err)
	}
	resolvedPath, err := filepath.EvalSymlinks(path)
	if err != nil {
		return nil, fmt.Errorf("resolve transcript path: %w", err)
	}
	if err := within(resolvedRoot, resolvedPath); err != nil {
		return nil, err
	}

	file, err := os.Open(path) // #nosec G304 -- path is provider-derived and containment is checked before and after open.
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = file.Close()
		}
	}()

	resolvedAfterOpen, err := filepath.EvalSymlinks(path)
	if err != nil {
		return nil, fmt.Errorf("re-resolve transcript path: %w", err)
	}
	if err := within(resolvedRoot, resolvedAfterOpen); err != nil {
		return nil, err
	}
	openedInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat opened transcript: %w", err)
	}
	resolvedInfo, err := os.Stat(resolvedAfterOpen)
	if err != nil {
		return nil, fmt.Errorf("stat resolved transcript: %w", err)
	}
	if !os.SameFile(openedInfo, resolvedInfo) {
		return nil, &PathError{Reason: "transcript path changed while opening"}
	}
	return file, nil
}

func within(root, full string) error {
	rel, err := filepath.Rel(root, full)
	if err != nil {
		return &PathError{Reason: "path is not relative to projects root"}
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return &PathError{Reason: "path escapes projects root"}
	}
	return nil
}
