package protocol

// The closed error-code set. Every error the connector reports, on HTTP
// streams (in HeaderError) and in storage results, is one of these codes with
// its fixed message. HTTPStatus is the status a connector-generated HTTP
// response uses; storage codes are mapped by the gateway (see PROTOCOL.md).
type errorCode struct {
	message string
	status  int
}

var errorCodes = map[string]errorCode{
	// Shared and HTTP-stream codes.
	"invalid_request":          {"invalid request", 400},
	"model_required":           {"request must name a model", 400},
	"path_not_allowed":         {"request not allowed for this service", 403},
	"content_type_not_allowed": {"content type not allowed for this service", 403},
	"model_not_allowed":        {"model not allowed for this service", 403},
	"route_not_found":          {"service not found", 404},
	"request_too_large":        {"request exceeds size limit", 413},
	"busy":                     {"service is busy", 429},
	"rate_limited":             {"rate limit exceeded", 429},
	"upstream_unavailable":     {"service is unavailable", 502},
	"upstream_error":           {"service request failed", 502},
	"response_too_large":       {"response exceeds size limit", 502},
	"upstream_timeout":         {"service timed out", 504},
	// Storage codes.
	"invalid_key":         {"invalid object key", 400},
	"key_too_long":        {"object key is too long", 400},
	"not_found":           {"object not found", 404},
	"precondition_failed": {"object changed", 412},
	"too_large":           {"object exceeds size limit", 413},
	"size_mismatch":       {"object size does not match", 400},
	"checksum_mismatch":   {"object checksum does not match", 502},
	"forbidden":           {"storage refused the request", 502},
	"backend_unavailable": {"storage is unavailable", 502},
	"backend_invalid":     {"invalid storage response", 502},
	"timeout":             {"storage operation timed out", 504},
	"wrong_route_type":    {"wrong service type", 502},
	"unsupported_kind":    {"unsupported stream kind", 502},
	"internal":            {"internal connector error", 500},
}

// ValidErrorCode reports whether code is in the closed set.
func ValidErrorCode(code string) bool { _, ok := errorCodes[code]; return ok }

// ErrorMessage returns the fixed message for code ("internal" for unknown codes).
func ErrorMessage(code string) string {
	if c, ok := errorCodes[code]; ok {
		return c.message
	}
	return errorCodes["internal"].message
}

// HTTPStatus returns the HTTP status for code (500 for unknown codes).
func HTTPStatus(code string) int {
	if c, ok := errorCodes[code]; ok {
		return c.status
	}
	return 500
}
