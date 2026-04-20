/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package whisper

import (
	"log"
	"os"
	"strings"
)

// DebugLogger provides conditional debug logging for Whisper subsystems.
// Enabled by setting the DEBUG environment variable to a comma-separated
// list of subsystem names (e.g., "whisper.gossip,whisper.rumor").
// Use "whisper.*" to enable all Whisper debug output.
type DebugLogger struct {
	name    string
	enabled bool
	logger  *log.Logger
}

var debugEnabled = parseDebugEnv()

func parseDebugEnv() map[string]bool {
	env := os.Getenv("DEBUG")
	if env == "" {
		return nil
	}
	m := make(map[string]bool)
	for _, s := range strings.Split(env, ",") {
		m[strings.TrimSpace(s)] = true
	}
	return m
}

func isDebugEnabled(name string) bool {
	if debugEnabled == nil {
		return false
	}
	if debugEnabled["whisper.*"] || debugEnabled["*"] {
		return true
	}
	// Check exact match and parent prefixes
	if debugEnabled[name] {
		return true
	}
	parts := strings.Split(name, ".")
	for i := len(parts) - 1; i > 0; i-- {
		prefix := strings.Join(parts[:i], ".") + ".*"
		if debugEnabled[prefix] {
			return true
		}
	}
	return false
}

func newDebug(name string) *DebugLogger {
	return &DebugLogger{
		name:    name,
		enabled: isDebugEnabled(name),
		logger:  log.New(os.Stderr, "["+name+"] ", log.LstdFlags),
	}
}

// Printf logs a formatted message if this subsystem is enabled.
func (d *DebugLogger) Printf(format string, args ...interface{}) {
	if d.enabled {
		d.logger.Printf(format, args...)
	}
}

// Package-level debug loggers for each subsystem.
var (
	dbgGossip     = newDebug("whisper.gossip")
	dbgHypercube  = newDebug("whisper.hypercube")
	dbgRumor      = newDebug("whisper.rumor")
	dbgPeer       = newDebug("whisper.peer")
	dbgSync       = newDebug("whisper.sync")
)
