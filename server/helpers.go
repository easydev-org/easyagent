package server

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"time"

	"connectrpc.com/connect/v2"

	"google.golang.org/protobuf/types/known/structpb"

	"github.com/easydev-org/easyagent/agent"
	agentv1 "github.com/easydev-org/easyagent/gen/agent/v1"
	"github.com/easydev-org/easyagent/store"
)

func marshalJSON(v any) ([]byte, error) { return json.Marshal(v) }

// now is the server clock (injectable in tests).
func (s *Server) now() time.Time {
	if s.nowFn != nil {
		return s.nowFn()
	}
	return time.Now()
}

// storeMessage aliases the store row used by the hydrate helpers.
type storeMessage = store.Message

// newUUID mints a random UUID-v4-shaped id.
func newUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	s := hex.EncodeToString(b[:])
	return s[0:8] + "-" + s[8:12] + "-" + s[12:16] + "-" + s[16:20] + "-" + s[20:32]
}

func deref(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func asStr(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

// structFromMap builds a protobuf Struct from a map (best-effort).
func structFromMap(m map[string]any) *structpb.Struct {
	s, err := structpb.NewStruct(m)
	if err != nil {
		return &structpb.Struct{}
	}
	return s
}

// connectErr maps a plain error to an Internal Connect error.
func connectErr(err error) error { return connect.NewError(connect.CodeInternal, err.Error()) }

// eventToPrompt maps an internal event to a PromptResponse. PromptResponse.params
// is map<string,string>, so non-string values are JSON-encoded.
func eventToPrompt(ev agent.Event) *agentv1.PromptResponse {
	params := map[string]string{}
	for k, v := range ev.Params {
		params[k] = asStr(v)
		if params[k] == "" {
			params[k] = jsonScalar(v)
		}
	}
	return &agentv1.PromptResponse{Event: ev.Name, Params: params}
}

// eventParams maps an internal event to a protobuf Struct (WatchSession).
func eventParams(ev agent.Event) *structpb.Struct {
	m := map[string]any{}
	for k, v := range ev.Params {
		m[k] = jsonSafe(v)
	}
	return structFromMap(m)
}

func jsonScalar(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	default:
		b, err := marshalJSON(v)
		if err != nil {
			return ""
		}
		return string(b)
	}
}

// jsonSafe coerces a value into something structpb.NewStruct accepts.
func jsonSafe(v any) any {
	switch t := v.(type) {
	case nil, string, bool, float64, int, int64:
		return t
	case map[string]any:
		out := map[string]any{}
		for k, x := range t {
			out[k] = jsonSafe(x)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, x := range t {
			out[i] = jsonSafe(x)
		}
		return out
	default:
		return jsonScalar(v)
	}
}
