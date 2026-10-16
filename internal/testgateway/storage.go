package testgateway

import (
	"bufio"
	"context"
	"io"
	"sync"

	"github.com/eidonix-ai/eidonix-connector/protocol"
)

// Storage sends a typed operation to an accepted storage-capable test session.
// Callers must read a get body to EOF to verify its checksum, then close it.
func (g *Gateway) Storage(ctx context.Context, header protocol.StreamHeader, upload io.Reader) (protocol.StorageResult, io.ReadCloser, error) {
	header.V = protocol.Version
	header.Kind = "storage"
	if header.RequestID == "" {
		header.RequestID, _ = protocol.NewInstanceID()
	}
	if err := protocol.ValidateStorageHeader(header, protocol.MaxStorageObjectBytes, 0); err != nil {
		return protocol.StorageResult{}, nil, err
	}
	c, finish, err := g.openHeader(ctx, header)
	if err != nil {
		return protocol.StorageResult{}, nil, err
	}
	var once sync.Once
	closeUpload := func() {
		once.Do(func() {
			if closer, ok := upload.(io.Closer); ok {
				closer.Close()
			}
		})
	}
	stopUpload := context.AfterFunc(ctx, closeUpload)
	closeAll := func() { stopUpload(); closeUpload(); finish() }
	// Reading concurrently permits immediate errors to interrupt an upload.
	go func() {
		defer closeUpload()
		if header.Op == "put" {
			body := protocol.NewStorageBodyWriter(c, protocol.MaxStorageObjectBytes)
			if upload != nil {
				if _, e := io.Copy(body, upload); e != nil {
					finish()
					return
				}
			}
			if e := body.Finish(); e != nil {
				finish()
				return
			}
		}
		// Only put uses FIN as a completion marker; other operations keep it for cancellation.
		if header.Op == "put" {
			c.Close()
		}
	}()
	reader := bufio.NewReader(c)
	result, err := protocol.ReadStorageResult(reader, header.Op)
	if err != nil {
		closeAll()
		return result, nil, err
	}
	if !result.OK {
		closeAll()
		return result, nil, result.Error
	}
	if header.Op != "get" {
		closeAll()
		return result, nil, nil
	}
	expected := *result.Size
	if len(result.Range) == 2 {
		expected = result.Range[1] - result.Range[0] + 1
	}
	body := protocol.NewStorageBodyReader(reader, expected, protocol.MaxStorageObjectBytes, c.SetReadDeadline)
	return result, &storageBody{Reader: body, close: closeAll}, nil
}

type storageBody struct {
	io.Reader
	close func()
}

func (b *storageBody) Close() error { b.close(); return nil }
