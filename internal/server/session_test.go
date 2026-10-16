package server

import (
	"net"
	"testing"
	"time"

	"xo/internal/protocol"
)

func TestSessionBackpressure(t *testing.T) {
	t.Parallel()
	local, remote := net.Pipe()
	defer remote.Close()
	p := &session{conn: local, out: make(chan protocol.Message, 1), done: make(chan struct{})}
	defer p.close()
	p.send(protocol.Message{Type: "first"})
	p.send(protocol.Message{Type: "overflow"})
	select {
	case <-p.done:
	default:
		t.Fatal("slow client left its full output queue connected")
	}
	p.send(protocol.Message{Type: "after_close"})
	if len(p.out) != 1 || (<-p.out).Type != "first" {
		t.Fatal("closed session accepted more messages")
	}
}

func TestSessionWriteFailure(t *testing.T) {
	t.Parallel()
	for _, state := range []string{"local_closed", "remote_closed"} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			local, remote := net.Pipe()
			defer remote.Close()
			p := &session{conn: local, out: make(chan protocol.Message, 1), done: make(chan struct{})}
			defer p.close()
			if state == "local_closed" {
				local.Close()
			} else {
				remote.Close()
			}
			p.send(protocol.Message{Type: "pong"})
			done := make(chan struct{})
			go func() { p.write(); close(done) }()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("writer did not stop after connection failure")
			}
			select {
			case <-p.done:
			default:
				t.Fatal("failed writer left the session active")
			}
		})
	}
}
