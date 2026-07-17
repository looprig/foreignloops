package claude

import (
	"errors"

	"github.com/looprig/foreignloop/driver"
)

func historyFromContainedPath(root, path string) (driver.History, error) {
	file, err := openContainedTranscript(root, path)
	if err != nil {
		return driver.History{}, asHistoryError(err)
	}
	defer func() { _ = file.Close() }()
	return driver.History{Available: true, Steps: foldTranscript(file)}, nil
}

func historyFromPath(path string) (driver.History, error) {
	steps, err := decodeTranscript(path)
	if err != nil {
		return driver.History{}, asHistoryError(err)
	}
	return driver.History{Available: true, Steps: steps}, nil
}

func asHistoryError(err error) error {
	var historyErr *driver.HistoryError
	if errors.As(err, &historyErr) {
		return err
	}
	return &driver.HistoryError{Cause: err}
}
