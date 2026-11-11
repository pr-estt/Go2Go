// Command server is a concurrent TCP file-transfer server.
package main

import (
	"flag"
	"log"
	"net"
)

type config struct {
	addr string
}

func main() {
	var cfg config
	flag.StringVar(&cfg.addr, "addr", ":9000", "listen address")
	flag.Parse()

	srv := &Server{cfg: cfg}
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("server: %v", err)
	}
	log.Println("server stopped")
}

// Server accepts connections and serves the file-transfer protocol.
type Server struct {
	cfg config
}

func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.cfg.addr)
	if err != nil {
		return err
	}
	log.Printf("listening on %s", ln.Addr())

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("accept error: %v", err)
			continue
		}
		log.Printf("connection from %s", conn.RemoteAddr())
		conn.Close()
	}
}
