package server

import (
	"github.com/easydev-org/easyagent/agent"
	agentv1 "github.com/easydev-org/easyagent/gen/agent/v1"
	"github.com/easydev-org/easyagent/store"
)

// sessionToMsg maps a store row to the proto Session.
func sessionToMsg(s store.Session, status string) *agentv1.Session {
	msg := &agentv1.Session{
		Name:             s.Name,
		Model:            s.Model,
		Preset:           s.Preset,
		MaxTurns:         int32(s.MaxTurns),
		SystemPrompt:     s.SystemPrompt,
		LastInputTokens:  int32(s.LastInputTokens),
		LastOutputTokens: int32(s.LastOutputTokens),
		CreatedAt:        s.CreatedAt,
		UpdatedAt:        s.UpdatedAt,
		Locale:           s.Locale,
		Group:            s.Group,
		MessageSeq:       int32(s.MessageSeq),
		UnreadCount:      int32(unread(s)),
		Status:           status,
		Variant:          s.Variant,
	}
	if s.TipID != nil {
		msg.TipId = *s.TipID
	}
	if s.LastUsedAt != nil {
		msg.LastUsedAt = *s.LastUsedAt
	}
	msg.LastMessagePreview = s.LastMessagePreview
	if s.LastMessageAt != nil {
		msg.LastMessageAt = *s.LastMessageAt
	}
	msg.LastTurnReason = s.LastTurnReason
	if s.LastTurnAt != nil {
		msg.LastTurnAt = *s.LastTurnAt
	}
	return msg
}

func unread(s store.Session) int64 {
	if s.MessageSeq > s.ReadSeq {
		return s.MessageSeq - s.ReadSeq
	}
	return 0
}

// messagesToProto hydrates a chain segment into proto messages with parts.
func messagesToProto(msgs []store.Message, parts []store.Part) []*agentv1.Message {
	byMsg := map[string][]*agentv1.Part{}
	for _, p := range parts {
		byMsg[p.MessageID] = append(byMsg[p.MessageID], &agentv1.Part{
			Id: p.ID, MessageId: p.MessageID, Type: p.Type, Seq: int32(p.Seq), Data: p.Data,
		})
	}
	out := make([]*agentv1.Message, 0, len(msgs))
	for _, m := range msgs {
		pm := &agentv1.Message{
			Id: m.ID, Role: m.Role, Source: m.Source, CreatedAt: m.CreatedAt,
			Parts: byMsg[m.ID],
		}
		if m.PrevID != nil {
			pm.PrevId = *m.PrevID
		}
		out = append(out, pm)
	}
	return out
}

// providerToProto maps a store provider row (masking the api key).
func providerToProto(p store.Provider, models []*agentv1.ProviderModel, headers map[string]string) *agentv1.Provider {
	return &agentv1.Provider{
		ProviderId: p.ProviderID,
		ApiType:    p.APIType,
		BaseUrl:    p.BaseURL,
		ApiKey:     agent.MaskSecret(p.APIKey),
		Headers:    headers,
		Models:     models,
		UpdatedAt:  p.UpdatedAt,
		Capability: p.Capability,
	}
}
