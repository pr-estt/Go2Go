// Command server is a concurrent TCP file-transfer server.
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type config struct {
	addr     string
	maxConns int
}

func main() {
	var cfg config
	flag.StringVar(&cfg.addr, "addr", ":9000", "listen address")
	flag.IntVar(&cfg.maxConns, "max-conns", 100, "maximum concurrent connections")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	srv := &Server{cfg: cfg, sem: make(chan struct{}, cfg.maxConns)}
	if err := srv.ListenAndServe(ctx); err != nil {
		log.Fatalf("server: %v", err)
	}
	log.Println("server stopped")
}

// Server accepts connections and serves the file-transfer protocol.
type Server struct {
	cfg   config
	sem   chan struct{} // limits concurrent connections
	wg    sync.WaitGroup
	mu    sync.Mutex
	conns map[net.Conn]struct{}
}

func (s *Server) ListenAndServe(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.cfg.addr)
	if err != nil {
		return err
	}
	log.Printf("listening on %s", ln.Addr())

	// Stop accepting on shutdown signal.
	go func() {
		<-ctx.Done()
		log.Println("shutting down...")
		ln.Close()
	}()

	for {
		// Acquire a slot before accepting so excess clients wait in the backlog.
		select {
		case s.sem <- struct{}{}:
		case <-ctx.Done():
			return s.shutdown()
		}

		conn, err := ln.Accept()
		if err != nil {
			<-s.sem
			if ctx.Err() != nil {
				return s.shutdown()
			}
			log.Printf("accept error: %v", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		s.track(conn, true)
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer func() { <-s.sem }()
			defer s.track(conn, false)
			s.handle(conn)
		}()
	}
}

// shutdown waits briefly for in-flight transfers, then force-closes them.
func (s *Server) shutdown() error {
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		log.Println("grace period over, closing remaining connections")
		s.mu.Lock()
		for c := range s.conns {
			c.Close()
		}
		s.mu.Unlock()
		<-done
	}
	return nil
}

func (s *Server) track(c net.Conn, add bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conns == nil {
		s.conns = make(map[net.Conn]struct{})
	}
	if add {
		s.conns[c] = struct{}{}
	} else {
		delete(s.conns, c)
	}
}

func (s *Server) handle(conn net.Conn) {
	defer conn.Close()
	log.Printf("[%s] connected", conn.RemoteAddr())
	defer log.Printf("[%s] disconnected", conn.RemoteAddr())
}
