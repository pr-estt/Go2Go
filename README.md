# go2go

**go2do** is a simple, fast TCP file transfer server and client written in Go. Run the server on any machine, then upload, download, list and delete files from anywhere on the network using the bundled CLI, or from your own programs using a small text-based protocol.

It uses only the Go standard library, so there are no dependencies to install and it builds into a single binary.

## Features

- **Simple protocol:** plain-text commands over TCP, with raw bytes for file data. Easy to implement in any language.
- **Concurrent:** every connection is handled in its own goroutine, with a configurable connection limit.
- **Integrity checks:** SHA-256 is verified on every upload and download.
- **Atomic uploads:** files are written to a temporary file and renamed into place, so nobody ever sees a half-written file.
- **Safe by default:** strict file-name validation prevents path traversal, upload size is capped, and stalled clients are dropped.
- **Optional authentication:** protect the server with a shared token.
- **Graceful shutdown:** in-flight transfers get a grace period when the server stops.
- **Cross-platform:** runs anywhere Go does (Linux, macOS, Windows).

## Quick start

Requires Go 1.21 or newer.

```bash
# Build
go build -o go2do-server ./cmd/server
go build -o go2do-client ./cmd/client

# Start the server
./go2do-server -addr :9000 -dir ./storage -token s3cret

# In another terminal, use the client
./go2do-client -token s3cret upload ./photo.jpg
./go2do-client -token s3cret list
./go2do-client -token s3cret download photo.jpg ./copy.jpg
./go2do-client -token s3cret delete photo.jpg
```

## Server

```bash
./go2do-server [flags]
```

| Flag         | Default     | Description                                          |
|--------------|-------------|------------------------------------------------------|
| `-addr`      | `:9000`     | Address and port to listen on                        |
| `-dir`       | `./storage` | Directory where files are stored                     |
| `-token`     | *(none)*    | Shared secret clients must present (disabled if empty) |
| `-max-size`  | `1024`      | Maximum upload size in MiB                           |
| `-max-conns` | `100`       | Maximum concurrent connections                       |
| `-idle`      | `60s`       | Drop a connection after this much I/O inactivity     |

Stop it with `Ctrl+C` or `SIGTERM`. The server stops accepting new connections, waits up to 10 seconds for active transfers, then closes the rest.

## Client

```bash
./go2do-client [flags] <command> [args]
```

Flags must come before the command.

| Flag     | Default           | Description                              |
|----------|-------------------|------------------------------------------|
| `-addr`  | `localhost:9000`  | Server address                           |
| `-token` | *(none)*          | Shared secret, if the server requires it |

| Command                            | Description                                              |
|------------------------------------|----------------------------------------------------------|
| `list`                             | List files on the server with their sizes                |
| `upload <local> [remote-name]`     | Upload a file (remote name defaults to the local name)   |
| `download <remote> [local-path]`   | Download a file (local path defaults to the remote name) |
| `delete <remote>`                  | Delete a file from the server                            |

## Deploying

To make go2do available to other machines, run the server on a host they can reach and open its TCP port in the firewall.

Example `systemd` unit (`/etc/systemd/system/go2do.service`):

```ini
[Unit]
Description=go2do file transfer server
After=network.target

[Service]
ExecStart=/opt/go2do/go2do-server -addr :9000 -dir /var/lib/go2do -token YOUR_SECRET
Restart=on-failure
User=go2do

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable --now go2do
```

> **Security note:** the protocol is not encrypted, so data and the token travel in plaintext. Before exposing go2do to the internet, add TLS (replace `net.Listen` with `tls.Listen` in the server) or put it behind a VPN or SSH tunnel.

