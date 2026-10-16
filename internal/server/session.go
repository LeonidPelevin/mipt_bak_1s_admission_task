package server

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"xo/internal/protocol"
)

type session struct {
	playerID string
	conn     net.Conn
	out      chan protocol.Message
	done     chan struct{}
	once     sync.Once
}

func (p *session) close() {
	p.once.Do(func() {
		close(p.done)
		p.conn.Close()
	})
}

func (p *session) send(message protocol.Message) {
	select {
	case <-p.done:
		return
	default:
	}
	select {
	case p.out <- message:
	case <-p.done:
	default:
		p.close()
	}
}

func (p *session) write() {
	for {
		select {
		case <-p.done:
			return
		case message := <-p.out:
			if err := p.conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
				p.close()
				return
			}
			if err := protocol.Write(p.conn, message); err != nil {
				p.close()
				return
			}
		}
	}
}

func (s *Server) serveConnection(ctx context.Context, conn net.Conn) {
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	defer conn.Close()
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return
	}
	reader := protocol.NewReader(conn)
	hello, err := reader.Read()
	if err != nil || hello.Type != "hello" {
		return
	}
	peer := &session{
		conn: conn, out: make(chan protocol.Message, 64), done: make(chan struct{}),
	}
	var writer sync.WaitGroup
	defer func() {
		peer.close()
		writer.Wait()
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer cancel()
		s.disconnect(cleanup, peer)
	}()
	if err := s.register(ctx, peer, hello); err != nil {
		code := "registration"
		if errors.Is(err, errSessionActive) {
			code = "session_active"
		}
		if conn.SetWriteDeadline(time.Now().Add(5*time.Second)) == nil {
			protocol.Write(conn, protocol.Message{Type: "error", Code: code, Error: err.Error()})
		}
		return
	}
	writer.Add(1)
	go func() {
		defer writer.Done()
		peer.write()
	}()
	for {
		if err := conn.SetReadDeadline(time.Now().Add(35 * time.Second)); err != nil {
			return
		}
		request, err := reader.Read()
		if err != nil {
			return
		}
		peer.send(s.handle(ctx, peer, request))
	}
}
