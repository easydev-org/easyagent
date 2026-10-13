package llm

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// Vision asks a vision-capable TEXT model a question about an image (the
// `image-read` tool). It builds a single-turn v4 language-model call whose user
// content carries an image file part + the question.
func (c *Client) Vision(ctx context.Context, model, prompt string, image []byte, mediaType string) (string, error) {
	if mediaType == "" {
		mediaType = "image/png"
	}
	body := map[string]any{
		"prompt": []any{map[string]any{
			"role": "user",
			"content": []any{
				map[string]any{"type": "text", "text": prompt},
				map[string]any{
					"type": "file", "mediaType": mediaType,
					"data": map[string]any{"type": "data", "data": base64.StdEncoding.EncodeToString(image)},
				},
			},
		}},
	}
	raw, err := c.postV4(ctx, "/language-model", "ai-language-model-id", model, "ai-language-model-specification-version", body)
	if err != nil {
		return "", err
	}
	var out struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return "", fmt.Errorf("llm: vision response: %w", err)
	}
	return strings.TrimSpace(out.Text), nil
}

// GeneratedFile is one produced media object (image/video/audio).
type GeneratedFile struct {
	MediaType string
	Data      []byte
}

// postV4 POSTs a v4 call-options body to one modality endpoint and returns the
// raw response body (non-streaming). modelHeader names the model header for the
// modality (e.g. ai-model-id).
func (c *Client) postV4(ctx context.Context, path, modelHeader, model, specVersion string, body map[string]any) ([]byte, error) {
	return c.postV4Streaming(ctx, path, modelHeader, model, specVersion, body, true)
}

// postV4Streaming is postV4 with an explicit streaming flag (the language-model
// header the gateway dispatches on).
func (c *Client) postV4Streaming(ctx context.Context, path, modelHeader, model, specVersion string, body map[string]any, allowStreaming bool) ([]byte, error) {
	buf, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("llm: marshal: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}
	req.Header.Set("content-type", "application/json")
	req.Header.Set("authorization", "Bearer "+c.apiKey)
	req.Header.Set(modelHeader, model)
	if specVersion != "" {
		req.Header.Set(specVersion, "4")
	}
	if path == "/language-model" {
		// Vision is the only language-model caller here and is non-streaming.
		req.Header.Set("ai-language-model-streaming", "false")
	}
	for k, v := range c.headers {
		req.Header.Set(k, v)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if resp.StatusCode != http.StatusOK {
		return nil, &APIError{StatusCode: resp.StatusCode, Header: resp.Header, Body: string(raw), Message: extractErrMessage(raw)}
	}
	return raw, nil
}

// GenerateImage calls `/v4/ai/image-model` (generateImage). Returns the produced
// images (base64-decoded). `images` are optional reference images (image edit).
func (c *Client) GenerateImage(ctx context.Context, model, prompt string, n int, size string, images [][]byte) ([]GeneratedFile, error) {
	body := map[string]any{"prompt": prompt}
	if n > 0 {
		body["n"] = n
	}
	if size != "" {
		body["size"] = size
	}
	if len(images) > 0 {
		// image-edit: the AI SDK image prompt is {images: [bytes], text}.
		parts := make([]map[string]any, 0, len(images))
		for _, img := range images {
			parts = append(parts, map[string]any{
				"type": "file", "mediaType": "image/png",
				"data": map[string]any{"type": "data", "data": base64.StdEncoding.EncodeToString(img)},
			})
		}
		body["prompt"] = map[string]any{"images": parts, "text": prompt}
	}
	raw, err := c.postV4(ctx, "/image-model", "ai-model-id", model, "ai-image-model-specification-version", body)
	if err != nil {
		return nil, err
	}
	var out struct {
		Images []string `json:"images"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("llm: image response: %w", err)
	}
	files := make([]GeneratedFile, 0, len(out.Images))
	for _, b64 := range out.Images {
		data, derr := decodeMaybeDataURL(b64)
		if derr != nil {
			return nil, derr
		}
		files = append(files, GeneratedFile{MediaType: "image/png", Data: data})
	}
	return files, nil
}

// GenerateSpeech calls `/v4/ai/speech-model` (generateSpeech). `voice` is empty
// for a preset-voice model, or a `data:` URL for a voice-cloning model.
func (c *Client) GenerateSpeech(ctx context.Context, model, text, voice, refText string) (GeneratedFile, error) {
	body := map[string]any{"text": text}
	if voice != "" {
		body["voice"] = voice
	}
	if refText != "" {
		body["providerOptions"] = map[string]any{"gateway": map[string]any{"ref_text": refText}}
	}
	raw, err := c.postV4(ctx, "/speech-model", "ai-model-id", model, "ai-speech-model-specification-version", body)
	if err != nil {
		return GeneratedFile{}, err
	}
	var out struct {
		Audio     string `json:"audio"`
		MediaType string `json:"mediaType"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return GeneratedFile{}, fmt.Errorf("llm: speech response: %w", err)
	}
	data, err := decodeMaybeDataURL(out.Audio)
	if err != nil {
		return GeneratedFile{}, err
	}
	mt := out.MediaType
	if mt == "" {
		mt = "audio/mpeg"
	}
	return GeneratedFile{MediaType: mt, Data: data}, nil
}

// Transcribe calls `/v4/ai/transcription-model` (transcribe). Returns the text.
func (c *Client) Transcribe(ctx context.Context, model string, audio []byte, mediaType string) (string, error) {
	body := map[string]any{"audio": base64.StdEncoding.EncodeToString(audio)}
	if mediaType != "" {
		body["mediaType"] = mediaType
	}
	raw, err := c.postV4(ctx, "/transcription-model", "ai-model-id", model, "ai-transcription-model-specification-version", body)
	if err != nil {
		return "", err
	}
	var out struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return "", fmt.Errorf("llm: transcription response: %w", err)
	}
	return strings.TrimSpace(out.Text), nil
}

// GenerateVideo calls `/v4/ai/video-model` (an SSE stream) and returns the
// produced videos. The gateway's video endpoint streams
// `{type:"result",videos:[{type:"base64",data,mediaType}]}` or `{type:"error",…}`.
func (c *Client) GenerateVideo(ctx context.Context, model string, body map[string]any) ([]GeneratedFile, error) {
	buf, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/video-model", bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}
	req.Header.Set("content-type", "application/json")
	req.Header.Set("authorization", "Bearer "+c.apiKey)
	req.Header.Set("ai-model-id", model)
	req.Header.Set("ai-video-model-specification-version", "4")
	for k, v := range c.headers {
		req.Header.Set(k, v)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
		return nil, &APIError{StatusCode: resp.StatusCode, Header: resp.Header, Body: string(raw), Message: extractErrMessage(raw)}
	}
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64*1024), 64<<20)
	var files []GeneratedFile
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if data == "" || data == "[DONE]" {
			continue
		}
		var ev struct {
			Type   string `json:"type"`
			Error  string `json:"message"`
			Videos []struct {
				Type      string `json:"type"`
				Data      string `json:"data"`
				MediaType string `json:"mediaType"`
			} `json:"videos"`
		}
		if json.Unmarshal([]byte(data), &ev) != nil {
			continue
		}
		if ev.Type == "error" {
			return nil, fmt.Errorf("llm: video: %s", ev.Error)
		}
		if ev.Type == "result" {
			for _, v := range ev.Videos {
				b, derr := decodeMaybeDataURL(v.Data)
				if derr != nil {
					return nil, derr
				}
				mt := v.MediaType
				if mt == "" {
					mt = "video/mp4"
				}
				files = append(files, GeneratedFile{MediaType: mt, Data: b})
			}
		}
	}
	return files, sc.Err()
}

// decodeMaybeDataURL decodes plain base64 or a `data:<mime>;base64,<data>` URL.
func decodeMaybeDataURL(s string) ([]byte, error) {
	if s == "" {
		return nil, nil
	}
	if strings.HasPrefix(s, "data:") {
		if i := strings.IndexByte(s, ','); i >= 0 {
			s = s[i+1:]
		}
	}
	return base64.StdEncoding.DecodeString(s)
}
