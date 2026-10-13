package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	"connectrpc.com/connect/v2"
	agentv1 "github.com/easydev-org/easyagent/gen/agent/v1"
	"github.com/easydev-org/easyagent/gen/agent/v1/agentv1connect"
	"github.com/easydev-org/easyagent/media"
	"github.com/easydev-org/easyagent/store"
)

// IngestFile stores bytes and returns the (derived) content type. Metadata lives
// in the store; the mime is DERIVED from the bytes (magic-byte sniff).
func (s *Server) IngestFile(ctx context.Context, req *agentv1.IngestFileRequest) (*agentv1.IngestFileResponse, error) {
	tenant := tenantOf(ctx)
	code := req.Code
	if code == "" {
		code = newUUID()
	}
	sum := sha256.Sum256(req.Data)
	sha := hex.EncodeToString(sum[:])
	facts := media.Probe(req.Data)
	mime := facts.Mime
	if mime == "" {
		mime = media.Mime(req.Data)
	}
	rec, err := s.db.UpsertFile(store.AgentFile{
		Code: code, Tenant: tenant, SHA256: sha, Name: req.Name,
		Mime: mime, Size: int64(len(req.Data)),
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	s.putBlob(ctx, rec.Code, req.Data)
	// Best-effort media facts (image dims/thumb; mp4 duration/dims).
	_ = s.db.UpdateFileMedia(rec.Code, facts.Width, facts.Height, facts.DurationMS, nil, nil)
	if len(facts.Thumb) > 0 {
		thumbCode := rec.Code + "-thumb"
		s.putBlob(ctx, thumbCode, facts.Thumb)
		_, _ = s.db.UpsertFile(store.AgentFile{
			Code: thumbCode, Tenant: tenant, SHA256: sha + ":thumb",
			Name: rec.Name + ".thumb.png", Mime: "image/png", Size: int64(len(facts.Thumb)),
		})
		_ = s.db.UpdateFileMedia(rec.Code, facts.Width, facts.Height, facts.DurationMS, &thumbCode, nil)
	}
	return &agentv1.IngestFileResponse{Ok: true, Code: rec.Code, Mime: rec.Mime}, nil
}

// UploadFile is the base64 upload path (same store as IngestFile).
func (s *Server) UploadFile(ctx context.Context, req *agentv1.UploadFileRequest) (*agentv1.UploadFileResponse, error) {
	data := []byte(req.Data)
	name := ""
	if req.File != nil {
		name = req.File.Name
	}
	res, err := s.IngestFile(ctx, &agentv1.IngestFileRequest{Data: data, Name: name})
	if err != nil {
		return nil, err
	}
	return &agentv1.UploadFileResponse{Ok: true, Code: res.Code, Mime: res.Mime}, nil
}

// GetFile returns a file's bytes + metadata.
func (s *Server) GetFile(ctx context.Context, req *agentv1.GetFileRequest) (*agentv1.GetFileResponse, error) {
	rec, ok, err := s.db.FileByCode(tenantOf(ctx), req.Code)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, "file not found")
	}
	data, _ := s.getBlob(ctx, rec.Code)
	return &agentv1.GetFileResponse{Data: data, Name: rec.Name, Mime: rec.Mime}, nil
}

// GetFileMeta returns a file's metadata (identity + media facts).
func (s *Server) GetFileMeta(ctx context.Context, req *agentv1.GetFileMetaRequest) (*agentv1.GetFileMetaResponse, error) {
	rec, ok, err := s.db.FileByCode(tenantOf(ctx), req.Code)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, "file not found")
	}
	out := &agentv1.GetFileMetaResponse{Name: rec.Name, Mime: rec.Mime, Size: int32(rec.Size)}
	if rec.Width != nil {
		w := int32(*rec.Width)
		out.Width = &w
	}
	if rec.Height != nil {
		h := int32(*rec.Height)
		out.Height = &h
	}
	out.DurationMs = rec.DurationMS
	out.ThumbCode = rec.ThumbCode
	out.Thumbhash = rec.Thumbhash
	return out, nil
}

// GetFileStream streams a file's bytes in chunks.
func (s *Server) GetFileStream(ctx context.Context, req *agentv1.GetFileRequest, stream agentv1connect.AgentServiceGetFileStreamServerStream) error {
	rec, ok, err := s.db.FileByCode(tenantOf(ctx), req.Code)
	if err != nil {
		return connect.NewError(connect.CodeInternal, err.Error())
	}
	if !ok {
		return connect.NewError(connect.CodeNotFound, "file not found")
	}
	data, _ := s.getBlob(ctx, rec.Code)
	const chunk = 64 * 1024
	var off uint64
	for off < uint64(len(data)) {
		end := off + chunk
		if end > uint64(len(data)) {
			end = uint64(len(data))
		}
		if err := stream.Send(&agentv1.FileChunk{
			Data: data[off:end], Offset: off, Total: uint64(len(data)),
			Last: end == uint64(len(data)),
		}); err != nil {
			return err
		}
		off = end
	}
	if len(data) == 0 {
		return stream.Send(&agentv1.FileChunk{Total: 0, Last: true})
	}
	return nil
}

// putBlob stores bytes in the durable backend when configured, else in memory.
func (s *Server) putBlob(ctx context.Context, code string, data []byte) {
	if s.blobStore != nil {
		_ = s.blobStore.Put(ctx, code, data)
		return
	}
	s.blobMu.Lock()
	s.blobs[code] = append([]byte(nil), data...)
	s.blobMu.Unlock()
}

// getBlob reads stored bytes from the durable backend when configured.
func (s *Server) getBlob(ctx context.Context, code string) ([]byte, bool) {
	if s.blobStore != nil {
		b, ok, _ := s.blobStore.Get(ctx, code)
		return b, ok
	}
	s.blobMu.RLock()
	defer s.blobMu.RUnlock()
	return s.blobs[code], s.blobs[code] != nil
}
