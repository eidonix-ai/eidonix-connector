package protocol

import "context"

type sessionKey struct{}

func WithSessionID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, sessionKey{}, id)
}
func SessionID(ctx context.Context) string { id, _ := ctx.Value(sessionKey{}).(string); return id }
