package protocol

import (
	"bytes"
	"errors"
	"io"
	"math"
	"strings"
	"testing"

	"xo/internal/game"
)

func TestReader(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		data string
		ok   bool
	}{
		{"valid", "{\"type\":\"ping\"}\n", true},
		{"broken_json", "{no}\n", false},
		{"missing_type", "{}\n", false},
		{"long_id", "{\"type\":\"move\",\"id\":\"" + strings.Repeat("a", 65) + "\"}\n", false},
		{"oversized", strings.Repeat("x", MaxMessageSize+1) + "\n", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewReader(strings.NewReader(tc.data)).Read()
			if (err == nil) != tc.ok {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

type failedWriter struct {
	err error
}

func (w failedWriter) Write(p []byte) (int, error) { return 0, w.err }

func TestWriteFailures(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("write failed")
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"writer_error", wantErr},
		{"zero_write", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := tc.err
			if want == nil {
				want = io.ErrShortWrite
			}
			if err := Write(failedWriter{tc.err}, Message{Type: "ping"}); !errors.Is(err, want) {
				t.Fatalf("write error not propagated: %v", err)
			}
		})
	}
	percent := math.NaN()
	if err := Write(io.Discard, Message{Type: "invitation", Invitation: &game.Invitation{
		Comparison: game.Comparison{WorseThan: &percent},
	}}); err == nil {
		t.Fatal("unencodable message accepted")
	}
}

type shortWriter struct{ bytes.Buffer }

func (w *shortWriter) Write(p []byte) (int, error) {
	if len(p) > 3 {
		p = p[:3]
	}
	return w.Buffer.Write(p)
}

func TestWrite(t *testing.T) {
	t.Parallel()
	var writer shortWriter
	want := []Message{
		{Type: "hello", Name: "Имя\nс переносом"},
		{Type: "move", ID: "request", GameID: "game", Cell: 5},
	}
	for _, message := range want {
		if err := Write(&writer, message); err != nil {
			t.Fatal(err)
		}
	}
	reader := NewReader(&writer)
	for _, message := range want {
		got, err := reader.Read()
		if err != nil || got.Type != message.Type || got.Name != message.Name || got.Cell != message.Cell {
			t.Fatalf("round trip failed: %v", err)
		}
	}
	if _, err := reader.Read(); err != io.EOF {
		t.Fatalf("want EOF, got %v", err)
	}
	if err := Write(io.Discard, Message{Type: "error", Error: strings.Repeat("x", MaxMessageSize)}); err == nil {
		t.Fatal("oversized message accepted")
	}
}
