# go2do

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
