// Package main is a production-shaped, OpenAI-compatible streaming backend.
//
// It exists to show what a real service looks like on top of photon: the
// streaming concerns photon owns, the domain concerns an application owns, and
// the seam between them.
//
// See README.md for what is real, what is stubbed, and what is deliberately
// absent.
package main

import (
	"log/slog"
	"os"
)

func main() {
	// The logger is built first and passed down, rather than reaching for the
	// global default in every file. A global logger is convenient until two
	// components need different levels, and then it is a refactor.
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// Configuration is parsed before anything is constructed, and a bad value is
	// a startup failure rather than a runtime surprise. A server that boots with
	// a nonsense quota and silently accepts everything is worse than one that
	// refuses to start.
	cfg, err := LoadConfig(os.Getenv)
	if err != nil {
		// Logged to stderr and exited, because the JSON logger is not configured
		// yet and a misconfigured logger is a confusing way to report a
		// configuration error.
		slog.Error("invalid configuration", "err", err)
		os.Exit(1)
	}

	logger.Info("starting",
		"addr", cfg.Addr,
		"max_streams", cfg.MaxStreams,
		"max_stream_buffer_bytes", cfg.MaxStreamBufferBytes,
		"stall_timeout", cfg.StallTimeout.String(),
		"tenants", len(cfg.Tenants),
		"upstream", cfg.UpstreamURL)

	srv, err := New(cfg, logger)
	if err != nil {
		logger.Error("could not build server", "err", err)
		os.Exit(1)
	}

	if err := srv.Run(); err != nil {
		logger.Error("server exited with error", "err", err)
		os.Exit(1)
	}

	logger.Info("stopped")
}
