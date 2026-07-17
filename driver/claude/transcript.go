package claude

import (
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
	root := filepath.Clean(filepath.Join(home, claudeDir, projectsDir))
	full := filepath.Clean(filepath.Join(root, encoded, sid+transcriptExt))
	if err := within(root, full); err != nil {
		return "", err
	}
	return full, nil
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
