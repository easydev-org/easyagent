// Package media derives a media file's facts WITHOUT ffmpeg: mime (magic bytes),
// image dimensions + a thumbnail (pure Go), and MP4/M4A duration + dimensions.
// Video thumbnails are NOT produced (no pure-Go decoder); the field stays unset.
package media

import (
	"bytes"
	"image"
	"io"

	"github.com/abema/go-mp4"
	"github.com/disintegration/imaging"
	"github.com/gabriel-vasile/mimetype"

	// Register the pure-Go image decoders the standard library does not ship.
	_ "golang.org/x/image/webp"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
)

// Facts are the derived media facts (all optional).
type Facts struct {
	Mime       string
	Width      *int
	Height     *int
	DurationMS *int64
	// Thumb is a small WebP of an image (nil when not an image / failed).
	Thumb []byte
}

// Probe derives facts from bytes. It never returns an error: an unsupported or
// malformed input simply yields fewer facts (the caller stores what it can).
func Probe(data []byte) Facts {
	f := Facts{}
	if m := mimetype.Detect(data); m != nil {
		f.Mime = m.String()
	}
	switch {
	case isImage(f.Mime):
		probeImage(data, &f)
	case f.Mime == "video/mp4" || f.Mime == "audio/mp4" || f.Mime == "audio/x-m4a" || f.Mime == "audio/m4a":
		probeMP4(data, &f)
	}
	return f
}

func isImage(mime string) bool { return len(mime) >= 6 && mime[:6] == "image/" }

// probeImage decodes the config (dimensions) and builds a small thumbnail.
func probeImage(data []byte, f *Facts) {
	cfg, _, err := image.DecodeConfig(bytes.NewReader(data))
	if err != nil {
		return
	}
	w, h := cfg.Width, cfg.Height
	f.Width, f.Height = &w, &h
	// A bounded thumbnail: decode + resize. Guard against decompression bombs.
	if int64(w)*int64(h) > 40_000_000 {
		return
	}
	img, err := imaging.Decode(bytes.NewReader(data), imaging.AutoOrientation(true))
	if err != nil {
		return
	}
	thumb := imaging.Fit(img, 512, 512, imaging.Lanczos)
	var buf bytes.Buffer
	if err := imaging.Encode(&buf, thumb, imaging.PNG); err == nil {
		f.Thumb = buf.Bytes()
	}
}

// probeMP4 walks the MP4 boxes for the movie header (duration) and the track
// header (video dimensions). Pure Go via go-mp4; no decoder.
func probeMP4(data []byte, f *Facts) {
	r := bytes.NewReader(data)
	_, err := mp4.ReadBoxStructure(r, func(h *mp4.ReadHandle) (any, error) {
		switch h.BoxInfo.Type {
		case mp4.BoxTypeMvhd():
			box, _, err := h.ReadPayload()
			if err != nil {
				return nil, nil
			}
			if mvhd, ok := box.(*mp4.Mvhd); ok {
				// Duration is in the movie timescale units.
				if mvhd.Timescale > 0 {
					ms := int64(mvhd.GetDuration()) * 1000 / int64(mvhd.Timescale)
					f.DurationMS = &ms
				}
			}
			return nil, nil
		case mp4.BoxTypeTkhd():
			box, _, err := h.ReadPayload()
			if err != nil {
				return nil, nil
			}
			if tkhd, ok := box.(*mp4.Tkhd); ok {
				w := int(tkhd.GetWidthInt())
				hgt := int(tkhd.GetHeightInt())
				if w > 0 && hgt > 0 && f.Width == nil {
					f.Width, f.Height = &w, &hgt
				}
			}
			return nil, nil
		}
		// Descend into container boxes (moov/trak/mdia/...).
		if _, e := h.Expand(); e != nil {
			return nil, e
		}
		return nil, nil
	})
	_ = err
}

// Mime detects a content type from the leading bytes (magic bytes), with a
// plain-text fallback.
func Mime(data []byte) string {
	if m := mimetype.Detect(data); m != nil {
		return m.String()
	}
	return "application/octet-stream"
}

// Thumbnail decodes + resizes an image to a bounded PNG (nil when not an image).
func Thumbnail(data []byte, maxEdge int) []byte {
	if maxEdge <= 0 {
		maxEdge = 512
	}
	img, err := imaging.Decode(bytes.NewReader(data), imaging.AutoOrientation(true))
	if err != nil {
		return nil
	}
	thumb := imaging.Fit(img, maxEdge, maxEdge, imaging.Lanczos)
	var buf bytes.Buffer
	if err := imaging.Encode(&buf, thumb, imaging.PNG); err != nil {
		return nil
	}
	return buf.Bytes()
}

var _ = io.Discard
