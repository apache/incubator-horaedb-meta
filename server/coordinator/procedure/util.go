// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/looplab/fsm"
	"github.com/pkg/errors"
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
	event.Cancel(errors.WithMessage(err, msg))
}

func logByLevel(level LogLevel, msg string, fields ...zap.Field) {
	switch level {
	case LogLevelInfo:
		log.Info(msg, fields...)
	case LogLevelWarn:
		log.Warn(msg, fields...)
	case LogLevelError:
		log.Error(msg, fields...)
	}
}
