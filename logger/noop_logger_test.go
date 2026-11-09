package logger

import "testing"

func TestNoOpLogger(t *testing.T) {
	logger := NewNoOpLogger()

	// Test that all logging methods can be called without panicking
	logger.Debugw("debug message", "key", "value")
	logger.Infow("info message", "key", "value")
	logger.Warnw("warn message", "key", "value")
	logger.Errorw("error message", "key", "value")

	// NoOpLogger.Fatalw should not terminate the process
	logger.Fatalw("fatal message", "key", "value")

	// Test context enrichment methods
	enriched := logger.With("key", "value")
	enriched.Infow("enriched message")

	nodeLogger := logger.WithNodeID("1")
	nodeLogger.Infow("node message")

	termLogger := logger.WithTerm(5)
	termLogger.Infow("term message")

	compLogger := logger.WithComponent("test")
	compLogger.Infow("component message")

	// Test chaining of context enrichment methods
	chainedLogger := logger.WithNodeID("1").WithTerm(5).WithComponent("test").With("key", "value")
	chainedLogger.Infow("chained message")

	// Test custom function callbacks on NoOpLogger
	noop := logger.(*NoOpLogger)
	var called int
	noop.DebugwFunc = func(msg string, kvs ...any) { called++ }
	noop.InfowFunc = func(msg string, kvs ...any) { called++ }
	noop.WarnwFunc = func(msg string, kvs ...any) { called++ }
	noop.ErrorwFunc = func(msg string, kvs ...any) { called++ }
	noop.FatalwFunc = func(msg string, kvs ...any) { called++ }

	noop.Debugw("debug")
	noop.Infow("info")
	noop.Warnw("warn")
	noop.Errorw("error")
	noop.Fatalw("fatal")

	if called != 5 {
		t.Errorf("Expected 5 callback invocations, got %d", called)
	}
}
