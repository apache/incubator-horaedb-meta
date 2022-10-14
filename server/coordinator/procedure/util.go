package procedure

import (
	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/looplab/fsm"
	"go.uber.org/zap"
)

type LogLevel int

const (
	LogLevelInfo = iota
	LogLevelWarn
	LogLevelError
)

func CancelEventWithLog(event *fsm.Event, err error, level LogLevel, msg string, fields ...zap.Field) {
	logByLevel(level, msg, fields...)
	event.Cancel(err)
}

func logByLevel(level LogLevel, msg string, fields ...zap.Field) {
	switch level {
	case LogLevelInfo:
		log.Info(msg, fields...)
		break
	case LogLevelWarn:
		log.Warn(msg, fields...)
		break
	case LogLevelError:
		log.Error(msg, fields...)
		break
	}
}
