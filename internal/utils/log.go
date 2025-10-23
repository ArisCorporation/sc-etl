package utils

import (
	"context"
	"log/slog"
	"os"
	"sync"
)

var (
	loggerOnce sync.Once
	logger     *slog.Logger
)

// Logger returns a process wide JSON logger.
func Logger() *slog.Logger {
	loggerOnce.Do(func() {
		handler := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		})
		logger = slog.New(handler)
	})
	return logger
}

// Info logs an informational message.
func Info(msg string, args ...any) {
	Logger().Info(msg, argsToKV(args)...)
}

// Warn logs a warning message.
func Warn(msg string, args ...any) {
	Logger().Warn(msg, argsToKV(args)...)
}

// Error logs an error message.
func Error(msg string, err error, args ...any) {
	if err != nil {
		args = append(args, "error", err)
	}
	Logger().Error(msg, argsToKV(args)...)
}

// With returns a contextual logger carrying additional attributes.
func With(ctx context.Context, args ...any) *slog.Logger {
	return Logger().With(argsToKV(args)...)
}

func argsToKV(args []any) []any {
	if len(args)%2 != 0 {
		args = append(args, "(missing)")
	}
	kv := make([]any, 0, len(args))
	for i := 0; i+1 < len(args); i += 2 {
		key, ok := args[i].(string)
		if !ok {
			key = "arg"
		}
		kv = append(kv, key, args[i+1])
	}
	return kv
}
