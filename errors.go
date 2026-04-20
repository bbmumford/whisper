/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package whisper

import "errors"

var (
	// ErrTopicNotFound is returned when a topic is not registered.
	ErrTopicNotFound = errors.New("whisper: topic not found")

	// ErrTopicExists is returned when registering a duplicate topic.
	ErrTopicExists = errors.New("whisper: topic already registered")

	// ErrStoreMissing is returned when a StatefulMerge topic has no StateStore.
	ErrStoreMissing = errors.New("whisper: StatefulMerge topic requires a StateStore")

	// ErrHandlerMissing is returned when a RequestResponse topic has no TopicHandler.
	ErrHandlerMissing = errors.New("whisper: RequestResponse topic requires a TopicHandler")
)
