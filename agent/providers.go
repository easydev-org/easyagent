package agent

import (
	"fmt"
	"strings"
)

// ModelCapability is a provider's single modality. A provider serves EXACTLY
// one capability; a host serving several registers one provider per modality.
type ModelCapability string

const (
	CapText          ModelCapability = "text"
	CapImage         ModelCapability = "image"
	CapVideo         ModelCapability = "video"
	CapSpeech        ModelCapability = "speech"
	CapTranscription ModelCapability = "transcription"
	CapEmbedding     ModelCapability = "embedding"
	CapRerank        ModelCapability = "rerank"
	CapRealtime      ModelCapability = "realtime"
)

// ModelCapabilities lists every supported modality.
var ModelCapabilities = []ModelCapability{
	CapText, CapImage, CapVideo, CapSpeech, CapTranscription, CapEmbedding, CapRerank, CapRealtime,
}

// ParseCapability validates a client-supplied capability.
//
// EMPTY IS AN ERROR, not "text". A default here made two different situations indistinguishable: a caller
// that forgot the field, and a caller that asked for a text model. The first is a client bug that should be
// reported while the request is in hand; the second is an intention that should be stated. Anything that
// reads a row written before this rule must resolve its own default explicitly, where it can be seen.
func ParseCapability(raw string) (ModelCapability, error) {
	v := strings.ToLower(strings.TrimSpace(raw))
	if v == "" {
		return "", fmt.Errorf("capability is required (one of %s)", joinCaps(ModelCapabilities))
	}
	for _, c := range ModelCapabilities {
		if string(c) == v {
			return c, nil
		}
	}
	return "", fmt.Errorf("unknown capability: %s (expected %s)", raw, joinCaps(ModelCapabilities))
}

// The single supported api type: the AI SDK v4 gateway protocol. Every provider
// is a v4 mount; the base URL distinguishes them.
const APITypeV4 = "v4-gateway"

// CapabilityMatrix is the single source of truth for registration validation:
// which capabilities each api type may serve. With only the v4 gateway, every
// modality is supported (the gateway routes internally).
var CapabilityMatrix = map[string]map[ModelCapability]bool{
	APITypeV4: {
		CapText: true, CapImage: true, CapVideo: true, CapSpeech: true,
		CapTranscription: true, CapEmbedding: true, CapRerank: true, CapRealtime: true,
	},
}

// ValidateAPIType rejects an unknown api type.
func ValidateAPIType(apiType string) error {
	if _, ok := CapabilityMatrix[strings.ToLower(apiType)]; ok {
		return nil
	}
	return fmt.Errorf("unknown api type: %s (expected %s)", apiType, APITypeV4)
}

// SupportsCapability reports whether apiType may serve capability.
func SupportsCapability(apiType string, capability ModelCapability) bool {
	m, ok := CapabilityMatrix[strings.ToLower(apiType)]
	return ok && m[capability]
}

func joinCaps(cs []ModelCapability) string {
	parts := make([]string, len(cs))
	for i, c := range cs {
		parts[i] = string(c)
	}
	return strings.Join(parts, "|")
}
