package media

import (
	"bytes"
	"image"
	"image/color"
	"image/png"
	"testing"
)

func TestProbePNG(t *testing.T) {
	img := image.NewRGBA(image.Rect(0, 0, 40, 24))
	for y := 0; y < 24; y++ {
		for x := 0; x < 40; x++ {
			img.Set(x, y, color.RGBA{uint8(x * 6), uint8(y * 10), 128, 255})
		}
	}
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		t.Fatal(err)
	}
	f := Probe(buf.Bytes())
	if f.Mime != "image/png" {
		t.Errorf("mime = %q", f.Mime)
	}
	if f.Width == nil || *f.Width != 40 || f.Height == nil || *f.Height != 24 {
		t.Errorf("dims = %v x %v", f.Width, f.Height)
	}
	if len(f.Thumb) == 0 {
		t.Error("expected a thumbnail")
	} else if _, _, err := image.DecodeConfig(bytes.NewReader(f.Thumb)); err != nil {
		t.Errorf("thumbnail is not a decodable image: %v", err)
	}
}

func TestProbeUnknown(t *testing.T) {
	f := Probe([]byte("not a media file"))
	// A plain-text blob still gets a mime (text/plain), and no media facts.
	if f.Width != nil || f.Height != nil || f.DurationMS != nil || len(f.Thumb) != 0 {
		t.Errorf("unexpected facts for non-media: %+v", f)
	}
}
