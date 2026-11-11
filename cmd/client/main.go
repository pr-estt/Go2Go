// Command client talks to the file-transfer server.
//
// Usage:
//
//	client [-addr host:port] [-token secret] list
package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func main() {
	addr := flag.String("addr", "localhost:9000", "server address")
	token := flag.String("token", "", "shared secret, if the server requires one")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `Usage: %s [flags] <command> [args]

Commands:
  list
  upload   <local-path> [remote-name]
  download <remote-name> [local-path]
  delete   <remote-name>

Flags:
`, os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		flag.Usage()
		os.Exit(2)
	}

	c, err := dial(*addr, *token)
	if err != nil {
		fatal(err)
	}
	defer c.close()

	switch strings.ToLower(args[0]) {
	case "list":
		err = c.list()
	case "upload":
		if len(args) < 2 || len(args) > 3 {
			flag.Usage()
			os.Exit(2)
		}
		remote := filepath.Base(args[1])
		if len(args) == 3 {
			remote = args[2]
		}
		err = c.upload(args[1], remote)
	case "download":
		if len(args) < 2 || len(args) > 3 {
			flag.Usage()
			os.Exit(2)
		}
		local := args[1]
		if len(args) == 3 {
			local = args[2]
		}
		err = c.download(args[1], local)
	case "delete":
		if len(args) != 2 {
			flag.Usage()
			os.Exit(2)
		}
		_, err = c.cmd("DELETE %s", args[1])
	default:
		flag.Usage()
		os.Exit(2)
	}
	if err != nil {
		fatal(err)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}

type client struct {
	conn net.Conn
	r    *bufio.Reader
}

func dial(addr, token string) (*client, error) {
	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		return nil, err
	}
	c := &client{conn: conn, r: bufio.NewReader(conn)}
	if token != "" {
		if _, err := c.cmd("AUTH %s", token); err != nil {
			conn.Close()
			return nil, err
		}
	}
	return c, nil
}

func (c *client) close() {
	fmt.Fprint(c.conn, "QUIT\n")
	c.conn.Close()
}

// cmd sends one command line and returns the fields following "OK".
func (c *client) cmd(format string, a ...any) ([]string, error) {
	if _, err := fmt.Fprintf(c.conn, format+"\n", a...); err != nil {
		return nil, err
	}
	return c.readStatus()
}

func (c *client) readStatus() ([]string, error) {
	line, err := c.r.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("reading response: %w", err)
	}
	line = strings.TrimRight(line, "\r\n")
	fields := strings.Fields(line)
	switch {
	case len(fields) > 0 && fields[0] == "OK":
		return fields[1:], nil
	case len(fields) > 0 && fields[0] == "ERR":
		return nil, errors.New("server: " + strings.TrimSpace(strings.TrimPrefix(line, "ERR")))
	default:
		return nil, fmt.Errorf("unexpected response %q", line)
	}
}

func (c *client) list() error {
	f, err := c.cmd("LIST")
	if err != nil {
		return err
	}
	n, err := parseSize(f, 0)
	if err != nil {
		return err
	}
	_, err = io.CopyN(os.Stdout, c.r, n)
	return err
}

func (c *client) upload(local, remote string) error {
	f, err := os.Open(local)
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file", local)
	}

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	size := info.Size()

	// First OK = server accepted the header and is ready for data.
	if _, err := c.cmd("UPLOAD %s %d %s", remote, size, hex.EncodeToString(h.Sum(nil))); err != nil {
		return err
	}
	if _, err := io.CopyN(c.conn, f, size); err != nil {
		return err
	}
	// Second OK = file stored and checksum verified.
	_, err = c.readStatus()
	if err == nil {
		fmt.Printf("uploaded %s as %s (%d bytes)\n", local, remote, size)
	}
	return err
}

func (c *client) download(remote, local string) error {
	f, err := c.cmd("DOWNLOAD %s", remote)
	if err != nil {
		return err
	}
	size, err := parseSize(f, 0)
	if err != nil {
		return err
	}
	if len(f) < 2 {
		return errors.New("malformed server response")
	}
	wantSum := f[1]

	tmp, err := os.CreateTemp(filepath.Dir(local), ".download-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name()) // no-op after successful rename
	defer tmp.Close()

	h := sha256.New()
	if _, err := io.CopyN(io.MultiWriter(tmp, h), c.r, size); err != nil {
		return err
	}
	if hex.EncodeToString(h.Sum(nil)) != wantSum {
		return errors.New("checksum mismatch, download discarded")
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmp.Name(), 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), local); err != nil {
		return err
	}
	fmt.Printf("downloaded %s to %s (%d bytes)\n", remote, local, size)
	return nil
}

func parseSize(fields []string, i int) (int64, error) {
	if len(fields) <= i {
		return 0, errors.New("malformed server response")
	}
	n, err := strconv.ParseInt(fields[i], 10, 64)
	if err != nil || n < 0 {
		return 0, errors.New("malformed size in server response")
	}
	return n, nil
}
