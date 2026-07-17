package claude

import "github.com/looprig/foreignloop/driver"

const (
	flagPrint           = "-p"
	flagOutputFormat    = "--output-format"
	valStreamJSON       = "stream-json"
	flagPartialMessages = "--include-partial-messages"
	flagVerbose         = "--verbose"
	flagSystemPrompt    = "--append-system-prompt"
	flagModel           = "--model"
	flagPermissionMode  = "--permission-mode"
	flagAddDir          = "--add-dir"
	flagSessionID       = "--session-id"
	flagResume          = "--resume"
)

const (
	permModeDefault     = "default"
	permModeAcceptEdits = "acceptEdits"
)

func postureString(posture driver.PermissionPosture) string {
	switch posture {
	case driver.PostureAcceptEdits:
		return permModeAcceptEdits
	default:
		return permModeDefault
	}
}

func buildArgs(turn driver.Turn, model string) []string {
	args := []string{
		flagPrint,
		flagOutputFormat, valStreamJSON,
		flagPartialMessages,
		flagVerbose,
		flagSystemPrompt, turn.SystemPrompt,
		flagModel, model,
		flagPermissionMode, postureString(turn.Posture),
		flagAddDir, turn.Cwd,
	}
	if turn.StartNew {
		return append(args, flagSessionID, turn.ForeignSID)
	}
	return append(args, flagResume, turn.ForeignSID)
}
