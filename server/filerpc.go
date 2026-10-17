package server

import (
	"context"
	"encoding/json"

	agentv1 "github.com/easydev-org/easyagent/gen/agent/v1"
	"github.com/easydev-org/easybus-sdk/bus"
	"github.com/easydev-org/easybus-sdk/protocol"
)

// ServeFileRPC wires the agent's file RPCs onto the bus, so a DB-less /
// store-less EXTENSION can persist bytes through the agent (the same path as an
// in-process ingest) and read them back. The extension side is
// `easybus-sdk/extension.{Ingest,Get}FileViaAgent`.
//
// BYTES NEVER RIDE THE MESSAGE: the caller first puts the bytes in the
// transient object store and sends only the object reference; the agent reads
// that object, stores it (db + blob), and replies with the canonical
// `file:<code>`. For get, the agent writes the bytes to the transient object
// store and returns the reference; the caller reads it.
//
// Returns a stop func; call it on shutdown.
func (s *Server) ServeFileRPC(ctx context.Context, b bus.Bus) (func(), error) {
	ingest, err := b.Subscribe(ctx, protocol.FileIngestWildcard, bus.SubscribeOpts{})
	if err != nil {
		return nil, err
	}
	get, err := b.Subscribe(ctx, protocol.FileGetWildcard, bus.SubscribeOpts{})
	if err != nil {
		_ = ingest.Close()
		return nil, err
	}
	go s.ingestLoop(ctx, b, ingest)
	go s.getLoop(ctx, b, get)
	return func() { _ = ingest.Close(); _ = get.Close() }, nil
}

// ingestLoop answers `easybus.<tenant>.file.ingest`: read the referenced
// object, store it, reply {ok, code, mime}.
func (s *Server) ingestLoop(ctx context.Context, b bus.Bus, sub bus.Subscription) {
	for {
		env, ok := sub.Next(ctx)
		if !ok {
			return
		}
		if env.ReplyTo == nil {
			continue
		}
		replyTo := *env.ReplyTo
		tenant := env.Tenant
		if tenant == "" {
			tenant = protocol.SubjectTenant(env.Ch)
		}
		var req protocol.FileIngestRequest
		if !protocol.Coerce(env.Payload, &req) {
			_ = s.replyFile(ctx, b, replyTo, tenant, protocol.FileIngestResponse{Ok: false, Error: &protocol.FileErrorPayload{Code: "invalid_argument", Message: "malformed file.ingest request"}})
			continue
		}
		data, err := b.ObjectGet(ctx, protocol.TenantObjectName(tenant, req.Object))
		if err != nil || data == nil {
			_ = s.replyFile(ctx, b, replyTo, tenant, protocol.FileIngestResponse{Ok: false, Error: &protocol.FileErrorPayload{Code: "not_found", Message: "object not found"}})
			continue
		}
		// Reuse the in-process ingest path (store + derived mime + media probe).
		ctxT := context.WithValue(ctx, tenantKey{}, tenant)
		res, err := s.IngestFile(ctxT, &agentv1.IngestFileRequest{Code: req.Code, Data: data, Name: req.Name})
		if err != nil {
			_ = s.replyFile(ctx, b, replyTo, tenant, protocol.FileIngestResponse{Ok: false, Error: &protocol.FileErrorPayload{Code: "internal", Message: err.Error()}})
			continue
		}
		_ = s.replyFile(ctx, b, replyTo, tenant, protocol.FileIngestResponse{Ok: true, Code: res.Code, Mime: res.Mime})
	}
}

// getLoop answers `easybus.<tenant>.file.get`: look the file up, write its bytes
// to the transient object store, reply {ok, meta, object}.
func (s *Server) getLoop(ctx context.Context, b bus.Bus, sub bus.Subscription) {
	for {
		env, ok := sub.Next(ctx)
		if !ok {
			return
		}
		if env.ReplyTo == nil {
			continue
		}
		replyTo := *env.ReplyTo
		tenant := env.Tenant
		if tenant == "" {
			tenant = protocol.SubjectTenant(env.Ch)
		}
		var req protocol.FileGetRequest
		if !protocol.Coerce(env.Payload, &req) {
			_ = s.replyFile(ctx, b, replyTo, tenant, protocol.FileGetResponse{Ok: false, Error: &protocol.FileErrorPayload{Code: "invalid_argument", Message: "malformed file.get request"}})
			continue
		}
		rec, found, err := s.db.FileByCode(tenant, req.Code)
		if err != nil || !found {
			_ = s.replyFile(ctx, b, replyTo, tenant, protocol.FileGetResponse{Ok: false, Error: &protocol.FileErrorPayload{Code: "not_found", Message: "file not found"}})
			continue
		}
		data, _ := s.getBlob(ctx, rec.Code)
		obj := protocol.NewID() + ".get"
		if err := b.ObjectPut(ctx, protocol.TenantObjectName(tenant, obj), data); err != nil {
			_ = s.replyFile(ctx, b, replyTo, tenant, protocol.FileGetResponse{Ok: false, Error: &protocol.FileErrorPayload{Code: "internal", Message: err.Error()}})
			continue
		}
		meta := &protocol.FileMetaWire{Code: rec.Code, Sha256: rec.SHA256, Name: rec.Name, Mime: rec.Mime, Size: rec.Size}
		_ = s.replyFile(ctx, b, replyTo, tenant, protocol.FileGetResponse{Ok: true, Meta: meta, Object: obj})
	}
}

// replyFile publishes a file RPC reply as a Go-compatible JSON payload.
func (s *Server) replyFile(ctx context.Context, b bus.Bus, replyTo, tenant string, v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	var payload any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return err
	}
	return b.Publish(ctx, replyTo, payload, bus.PublishOpts{Tenant: tenant})
}
