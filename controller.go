package main

import (
	"bytes"
	"encoding/gob"
	"log/slog"
)

const (
	getCommand   = "get"
	purgeCommand = "purge"
)

type getRequest struct{ Key []byte }
type getResponse struct{ Value []byte }
type purgeRequest struct{ Key []byte }

type controller struct {
	onGet   func(key []byte) []byte
	onPurge func(key []byte)

	sendQuery     func(string, []byte) []byte
	sendUserEvent func(string, []byte)
}

func (c *controller) handleQuery(_ string, payload []byte) []byte {
	var req getRequest
	if err := gob.NewDecoder(bytes.NewReader(payload)).Decode(&req); err != nil {
		slog.Error("Failed to decode getKeyRequest", "error", err)
		return nil
	}

	slog.Info("Received query", "key", req.Key)

	if value := c.onGet(req.Key); value != nil {
		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(getResponse{Value: value}); err != nil {
			slog.Error("Failed to encode getKeyResponse", "error", err)
			return nil
		}
		return buf.Bytes()
	}

	return nil
}

func (c *controller) handleUserEvent(_ string, payload []byte) {
	var req purgeRequest
	if err := gob.NewDecoder(bytes.NewReader(payload)).Decode(&req); err != nil {
		slog.Error("Failed to decode purgeKeyRequest", "error", err)
		return
	}

	slog.Info("Received purge", "key", req.Key)

	c.onPurge(req.Key)
}

func (c *controller) get(key []byte) []byte {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(getRequest{key}); err != nil {
		slog.Error("Failed to encode getKeyRequest", "error", err)
		return nil
	}

	slog.Info("Sending query", "key", key)

	if nodeRes := c.sendQuery(getCommand, buf.Bytes()); nodeRes != nil {
		var res getResponse
		if err := gob.NewDecoder(bytes.NewReader(nodeRes)).Decode(&res); err != nil {
			slog.Error("failed to decode response", "err", err)
			return nil
		}
		slog.Info("Received response", "value", res.Value)
		return res.Value
	}

	return nil
}

func (c *controller) purge(key []byte) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(purgeRequest{key}); err != nil {
		slog.Error("Failed to encode purgeKeyRequest", "error", err)
		return
	}

	slog.Info("Sending purge", "key", key)

	c.sendUserEvent(purgeCommand, buf.Bytes())
}
