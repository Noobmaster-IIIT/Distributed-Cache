package graceful

import (
	"os"
	"os/signal"
	"syscall"
)

// Shutdown listens for interrupt signals and executes a cleanup function.
func Shutdown(cleanup func()) {
	// Create a channel to receive OS signals.
	sigCh := make(chan os.Signal, 1)

	// Notify the channel for SIGINT (Ctrl+C) and SIGTERM.
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Block until a signal is received.
	<-sigCh

	// Execute the cleanup function.
	cleanup()
}
