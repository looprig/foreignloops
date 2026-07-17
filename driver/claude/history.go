package claude

import (
	"errors"

	"github.com/looprig/foreignloop/driver"
)

func historyFromPath(path string) (driver.History, error) {
	steps, err := decodeTranscript(path)
	if err != nil {
		var historyErr *driver.HistoryError
		if errors.As(err, &historyErr) {
			return driver.History{}, err
		}
		return driver.History{}, &driver.HistoryError{Cause: err}
	}
	return driver.History{Available: true, Steps: steps}, nil
}
