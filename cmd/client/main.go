// Command client talks to the file-transfer server.
//
// Usage:
//
//	client [-addr host:port] [-token secret] list
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
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
