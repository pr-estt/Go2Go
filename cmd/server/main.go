// Command server is a concurrent TCP file-transfer server.
//
// Protocol (text commands terminated by '\n', raw bytes for file data):
//
//	AUTH <token>                      -> OK | ERR <msg>      (only if -token is set)
//	LIST                              -> OK <n>\n<n bytes of "name\tsize\n" lines>
//	UPLOAD <name> <size> <sha256>     -> OK (ready) ... client sends <size> bytes ... -> OK | ERR <msg>
//	DOWNLOAD <name>                   -> OK <size> <sha256>\n<size bytes> | ERR <msg>
//	DELETE <name>                     -> OK | ERR <msg>
//	QUIT                              -> OK (connection closes)
package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type config struct {
	addr        string
	dir         string
	token       string
	maxSize     int64
	maxConns    int
	idleTimeout time.Duration
}

func main() {
	var cfg config
	var maxSizeMB int64
	flag.StringVar(&cfg.addr, "addr", ":9000", "listen address")
	flag.StringVar(&cfg.dir, "dir", "./storage", "directory where files are stored")
	flag.StringVar(&cfg.token, "token", "", "optional shared secret clients must present (AUTH)")
	flag.Int64Var(&maxSizeMB, "max-size", 1024, "maximum upload size in MiB")
	flag.IntVar(&cfg.maxConns, "max-conns", 100, "maximum concurrent connections")
	flag.DurationVar(&cfg.idleTimeout, "idle", 60*time.Second, "close connection after this much I/O inactivity")
	flag.Parse()
	cfg.maxSize = maxSizeMB << 20

	if err := os.MkdirAll(cfg.dir, 0o755); err != nil {
		log.Fatalf("create storage dir: %v", err)
	}

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
	log.Printf("listening on %s, storing files in %s", ln.Addr(), s.cfg.dir)

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
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue
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

// timeoutConn refreshes the deadline before each Read/Write, giving an
// inactivity timeout that works for transfers of any size.
type timeoutConn struct {
	net.Conn
	timeout time.Duration
}

func (c timeoutConn) Read(p []byte) (int, error) {
	c.Conn.SetReadDeadline(time.Now().Add(c.timeout))
	return c.Conn.Read(p)
}

func (c timeoutConn) Write(p []byte) (int, error) {
	c.Conn.SetWriteDeadline(time.Now().Add(c.timeout))
	return c.Conn.Write(p)
}

type session struct {
	srv    *Server
	conn   net.Conn
	r      *bufio.Reader
	w      io.Writer
	authed bool
	remote string
}

var errLineTooLong = errors.New("command line too long")

func (s *Server) handle(raw net.Conn) {
	defer raw.Close()
	tc := timeoutConn{Conn: raw, timeout: s.cfg.idleTimeout}
	c := &session{
		srv:    s,
		conn:   raw,
		r:      bufio.NewReaderSize(tc, 4096),
		w:      tc,
		authed: s.cfg.token == "",
		remote: raw.RemoteAddr().String(),
	}
	log.Printf("[%s] connected", c.remote)
	defer log.Printf("[%s] disconnected", c.remote)

	for {
		line, err := c.readLine()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				log.Printf("[%s] read command: %v", c.remote, err)
			}
			return
		}
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		cmd := strings.ToUpper(fields[0])
		args := fields[1:]

		if !c.authed && cmd != "AUTH" && cmd != "QUIT" {
			c.errf("authentication required")
			continue
		}

		var herr error // non-nil means the connection is unusable
		switch cmd {
		case "AUTH":
			herr = c.cmdAuth(args)
		case "LIST":
			herr = c.cmdList()
		case "UPLOAD":
			herr = c.cmdUpload(args)
		case "DOWNLOAD":
			herr = c.cmdDownload(args)
		case "DELETE":
			herr = c.cmdDelete(args)
		case "QUIT":
			c.reply("OK")
			return
		default:
			herr = c.errf("unknown command %q", cmd)
		}
		if herr != nil {
			log.Printf("[%s] %s failed: %v", c.remote, cmd, herr)
			return
		}
	}
}

func (c *session) readLine() (string, error) {
	line, err := c.r.ReadSlice('\n')
	if err != nil {
		if errors.Is(err, bufio.ErrBufferFull) {
			return "", errLineTooLong
		}
		return "", err
	}
	return strings.TrimRight(string(line), "\r\n"), nil
}

func (c *session) reply(format string, a ...any) error {
	_, err := fmt.Fprintf(c.w, format+"\n", a...)
	return err
}

// errf sends an ERR line. The returned error is only a write failure.
func (c *session) errf(format string, a ...any) error {
	msg := strings.ReplaceAll(fmt.Sprintf(format, a...), "\n", " ")
	return c.reply("ERR %s", msg)
}

func (c *session) cmdList() error {
	entries, err := os.ReadDir(c.srv.cfg.dir)
	if err != nil {
		return c.errf("cannot list files")
	}
	var lines []string
	for _, e := range entries {
		if !e.Type().IsRegular() || strings.HasPrefix(e.Name(), ".") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		lines = append(lines, fmt.Sprintf("%s\t%d\n", e.Name(), info.Size()))
	}
	sort.Strings(lines)
	payload := strings.Join(lines, "")
	if err := c.reply("OK %d", len(payload)); err != nil {
		return err
	}
	_, err = io.WriteString(c.w, payload)
	return err
}

func (c *session) cmdUpload(args []string) error {
	if len(args) != 3 {
		return c.errf("usage: UPLOAD <name> <size> <sha256>")
	}
	name := args[0]
	if !validName(name) {
		return c.errf("invalid file name")
	}
	size, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil || size < 0 {
		return c.errf("invalid size")
	}
	if size > c.srv.cfg.maxSize {
		return c.errf("file too large (max %d bytes)", c.srv.cfg.maxSize)
	}
	wantSum := strings.ToLower(args[2])
	if b, err := hex.DecodeString(wantSum); err != nil || len(b) != sha256.Size {
		return c.errf("invalid sha256")
	}

	// Write to a temp file first so readers never see partial uploads.
	tmp, err := os.CreateTemp(c.srv.cfg.dir, ".upload-*")
	if err != nil {
		return c.errf("cannot store file")
	}
	committed := false
	defer func() {
		if !committed {
			tmp.Close()
			os.Remove(tmp.Name())
		}
	}()

	if err := c.reply("OK"); err != nil { // tell client it may send data
		return err
	}

	h := sha256.New()
	if _, err := io.CopyN(io.MultiWriter(tmp, h), c.r, size); err != nil {
		return fmt.Errorf("receiving data: %w", err) // stream is broken; drop connection
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != wantSum {
		return c.errf("checksum mismatch")
	}
	if err := tmp.Sync(); err != nil {
		return c.errf("cannot store file")
	}
	if err := tmp.Close(); err != nil {
		return c.errf("cannot store file")
	}
	if err := os.Chmod(tmp.Name(), 0o644); err != nil {
		return c.errf("cannot store file")
	}
	if err := os.Rename(tmp.Name(), filepath.Join(c.srv.cfg.dir, name)); err != nil {
		return c.errf("cannot store file")
	}
	committed = true
	log.Printf("[%s] uploaded %s (%d bytes)", c.remote, name, size)
	return c.reply("OK")
}

// validName accepts only plain file names: no paths, no hidden files,
// no whitespace/control characters (names are space-delimited on the wire).
func validName(n string) bool {
	if n == "" || len(n) > 255 || strings.HasPrefix(n, ".") {
		return false
	}
	for _, r := range n {
		if r < 0x21 || r == 0x7f || r == '/' || r == '\\' || r == ':' {
			return false
		}
	}
	return filepath.Base(n) == n
}

func (c *session) cmdAuth(args []string) error {
	if c.srv.cfg.token == "" {
		return c.reply("OK")
	}
	if len(args) != 1 ||
		subtle.ConstantTimeCompare([]byte(args[0]), []byte(c.srv.cfg.token)) != 1 {
		time.Sleep(500 * time.Millisecond) // slow down guessing
		return c.errf("invalid token")
	}
	c.authed = true
	return c.reply("OK")
}

func (c *session) cmdDownload(args []string) error {
	if len(args) != 1 || !validName(args[0]) {
		return c.errf("usage: DOWNLOAD <name>")
	}
	f, err := os.Open(filepath.Join(c.srv.cfg.dir, args[0]))
	if err != nil {
		return c.errf("file not found")
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return c.errf("file not found")
	}

	// Hash first so the checksum can go in the header.
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return c.errf("cannot read file")
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return c.errf("cannot read file")
	}
	if err := c.reply("OK %d %s", n, hex.EncodeToString(h.Sum(nil))); err != nil {
		return err
	}
	// Send exactly n bytes (the file could grow while we send).
	if _, err := io.CopyN(c.w, f, n); err != nil {
		return fmt.Errorf("sending data: %w", err)
	}
	log.Printf("[%s] downloaded %s (%d bytes)", c.remote, args[0], n)
	return nil
}

func (c *session) cmdDelete(args []string) error {
	if len(args) != 1 || !validName(args[0]) {
		return c.errf("usage: DELETE <name>")
	}
	if err := os.Remove(filepath.Join(c.srv.cfg.dir, args[0])); err != nil {
		return c.errf("file not found")
	}
	log.Printf("[%s] deleted %s", c.remote, args[0])
	return c.reply("OK")
}
