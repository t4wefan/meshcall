package meshcall

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// HTTPRouterOptions configures an HTTPRouter.
type HTTPRouterOptions struct {
	// Token is a Bearer token ("mk_…" API key or an issued token).
	Token string
	// Username and Password select Basic authentication instead.
	Username string
	Password string
	// Headers are added to every request (a gateway in front of the Router).
	Headers map[string]string
	// Retry is the unary retry policy; nil uses DefaultRetry.
	Retry *RetryPolicy
	// Reconnect drives subscription resume backoff; zero uses DefaultReconnect.
	Reconnect ReconnectPolicy
	// Timeout bounds each request when the call sets no timeout.
	Timeout time.Duration
	Logger  *slog.Logger
	// Client overrides the HTTP client (tests, custom transports).
	Client *http.Client
}

// HTTPRouter is a client-only Router that speaks the Router's HTTP v1 API.
//
// Pass it to generated clients exactly like a WebSocket Router
// (NewChatClient(httpRouter)); typed unary, server-stream and client-stream
// calls work, duplex calls are refused (the Router answers 405) and there is
// no server side.
type HTTPRouter struct {
	base      string
	options   HTTPRouterOptions
	defaults  CallOptions
	logger    *slog.Logger
	retry     RetryPolicy
	reconnect ReconnectPolicy
	client    *http.Client
}

// NewHTTPRouter creates a client for the Router at endpoint (http(s)://, or
// ws(s):// which is converted).
func NewHTTPRouter(endpoint string, opts ...HTTPRouterOption) *HTTPRouter {
	options := HTTPRouterOptions{}
	for _, opt := range opts {
		if opt != nil {
			opt(&options)
		}
	}
	return newHTTPRouter(endpoint, options)
}

// HTTPRouterOption configures NewHTTPRouter.
type HTTPRouterOption func(*HTTPRouterOptions)

// WithHTTPToken authenticates with a Bearer token.
func WithHTTPToken(token string) HTTPRouterOption {
	return func(o *HTTPRouterOptions) { o.Token, o.Username, o.Password = token, "", "" }
}

// WithHTTPBasic authenticates with a username and password.
func WithHTTPBasic(username, password string) HTTPRouterOption {
	return func(o *HTTPRouterOptions) { o.Username, o.Password, o.Token = username, password, "" }
}

// WithHTTPHeaders adds request headers.
func WithHTTPHeaders(headers map[string]string) HTTPRouterOption {
	return func(o *HTTPRouterOptions) { o.Headers = headers }
}

// WithHTTPRetry sets the unary retry policy.
func WithHTTPRetry(policy RetryPolicy) HTTPRouterOption {
	return func(o *HTTPRouterOptions) { o.Retry = &policy }
}

// WithHTTPTimeout bounds requests that set no timeout.
func WithHTTPTimeout(timeout time.Duration) HTTPRouterOption {
	return func(o *HTTPRouterOptions) { o.Timeout = timeout }
}

// WithHTTPClient sets the underlying http.Client.
func WithHTTPClient(client *http.Client) HTTPRouterOption {
	return func(o *HTTPRouterOptions) { o.Client = client }
}

// WithHTTPLogger sets the logger used for subscription resume warnings.
func WithHTTPLogger(logger *slog.Logger) HTTPRouterOption {
	return func(o *HTTPRouterOptions) { o.Logger = logger }
}

func newHTTPRouter(endpoint string, options HTTPRouterOptions) *HTTPRouter {
	base := strings.TrimRight(endpoint, "/")
	switch {
	case strings.HasPrefix(base, "ws://"):
		base = "http://" + strings.TrimPrefix(base, "ws://")
	case strings.HasPrefix(base, "wss://"):
		base = "https://" + strings.TrimPrefix(base, "wss://")
	}
	retry := DefaultRetry
	if options.Retry != nil {
		retry = *options.Retry
		if retry.MaxAttempts == 0 {
			retry.MaxAttempts = DefaultRetry.MaxAttempts
		}
		if retry.InitialDelay == 0 {
			retry.InitialDelay, retry.MaxDelay, retry.Multiplier, retry.Jitter = DefaultRetry.InitialDelay, DefaultRetry.MaxDelay, DefaultRetry.Multiplier, DefaultRetry.Jitter
		}
		if retry.RetryCodes == nil {
			retry.RetryCodes = DefaultRetry.RetryCodes
		}
	}
	logger := options.Logger
	if logger == nil {
		logger = slog.Default()
	}
	client := options.Client
	if client == nil {
		client = http.DefaultClient
	}
	reconnect := options.Reconnect
	if reconnect == (ReconnectPolicy{}) {
		reconnect = DefaultReconnect
	}
	return &HTTPRouter{
		base:      base,
		options:   options,
		logger:    logger,
		retry:     retry,
		reconnect: reconnect,
		client:    client,
	}
}

// WithOptions returns a shallow copy whose calls apply opts by default.
func (h *HTTPRouter) WithOptions(opts CallOptions) Caller {
	clone := *h
	clone.defaults = mergeCallOptions(h.defaults, opts)
	return &clone
}

func mergeCallOptions(base, over CallOptions) CallOptions {
	merged := base
	if over.Timeout != 0 {
		merged.Timeout = over.Timeout
	}
	if over.IdleTimeout != 0 {
		merged.IdleTimeout = over.IdleTimeout
	}
	if over.Metadata != nil {
		metadata := map[string]string{}
		for k, v := range base.Metadata {
			metadata[k] = v
		}
		for k, v := range over.Metadata {
			metadata[k] = v
		}
		merged.Metadata = metadata
	}
	if over.IdempotencyKey != "" {
		merged.IdempotencyKey = over.IdempotencyKey
	}
	if over.Instance != "" {
		merged.Instance = over.Instance
	}
	merged.Idempotent = merged.Idempotent || over.Idempotent
	return merged
}

// Close releases idle connections; HTTPRouter has no persistent connection.
func (h *HTTPRouter) Close() error {
	h.client.CloseIdleConnections()
	return nil
}

func (h *HTTPRouter) reconnectDelays() func() (time.Duration, bool) { return h.reconnect.delays() }
func (h *HTTPRouter) log() *slog.Logger                             { return h.logger }
func (h *HTTPRouter) WithCallOptions(opts CallOptions) Caller {
	return h.WithOptions(opts)
}

func (h *HTTPRouter) optionsFor(opts []CallOption) CallOptions {
	merged := h.defaults
	for _, opt := range opts {
		if opt != nil {
			opt(&merged)
		}
	}
	return merged
}

func (h *HTTPRouter) headers(options CallOptions, contentType string) http.Header {
	headers := http.Header{}
	for key, value := range h.options.Headers {
		headers.Set(key, value)
	}
	if h.options.Token != "" {
		headers.Set("Authorization", "Bearer "+h.options.Token)
	} else if h.options.Username != "" {
		pair := h.options.Username + ":" + h.options.Password
		headers.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(pair)))
	}
	if contentType != "" {
		headers.Set("Content-Type", contentType)
	}
	if options.Timeout > 0 {
		headers.Set("MeshCall-Timeout-Ms", fmt.Sprint(options.Timeout.Milliseconds()))
	}
	if options.IdleTimeout > 0 {
		headers.Set("MeshCall-Idle-Timeout-Ms", fmt.Sprint(options.IdleTimeout.Milliseconds()))
	}
	if options.IdempotencyKey != "" {
		headers.Set("Idempotency-Key", options.IdempotencyKey)
	}
	if options.Instance != "" {
		headers.Set("MeshCall-Instance", options.Instance)
	}
	for key, value := range options.Metadata {
		headers.Set("MeshCall-Meta-"+key, value)
	}
	return headers
}

func (h *HTTPRouter) callURL(service, method string) string {
	return h.base + "/v1/call/" + url.PathEscape(service) + "/" + url.PathEscape(method)
}

func (h *HTTPRouter) unaryRaw(ctx context.Context, service, method string, request json.RawMessage, opts []CallOption) (json.RawMessage, map[string]string, error) {
	options := h.optionsFor(opts)
	mayRetry := !h.retry.Disabled && (options.Idempotent || options.IdempotencyKey != "")
	var deadline time.Time
	if options.Timeout > 0 {
		deadline = time.Now().Add(options.Timeout)
	}
	for attempt := 1; ; attempt++ {
		callCtx, cancel := h.withTimeout(ctx, options)
		httpRequest, err := http.NewRequestWithContext(callCtx, http.MethodPost, h.callURL(service, method), bytes.NewReader(request))
		if err != nil {
			cancel()
			return nil, nil, detailed(CodeInvalidArgument, err.Error(), "client", "", false)
		}
		httpRequest.Header = h.headers(options, "application/json")
		response, err := h.client.Do(httpRequest)
		if err != nil {
			cancel()
			if mayRetry && attempt < h.retry.MaxAttempts && ctx.Err() == nil {
				select {
				case <-time.After(h.retry.delay(attempt)):
					continue
				case <-ctx.Done():
					return nil, nil, contextError(ctx)
				}
			}
			return nil, nil, detailed(CodeUnavailable, "HTTP request failed: "+err.Error(), "client", "", true)
		}
		body, readErr := io.ReadAll(response.Body)
		response.Body.Close()
		cancel()
		if readErr != nil {
			return nil, nil, detailed(CodeUnavailable, "HTTP response unreadable: "+readErr.Error(), "client", "", true)
		}
		trailers := trailersOf(response.Header)
		if response.StatusCode >= 400 {
			failure := errorFromHTTP(response.StatusCode, body)
			if mayRetry && failure.Retryable && containsString(h.retry.RetryCodes, failure.Code) && attempt < h.retry.MaxAttempts {
				delay := h.retry.delay(attempt)
				if !deadline.IsZero() && time.Until(deadline) <= delay {
					return nil, trailers, errAttempts(failure, attempt)
				}
				select {
				case <-time.After(delay):
					continue
				case <-ctx.Done():
					return nil, trailers, contextError(ctx)
				}
			}
			return nil, trailers, errAttempts(failure, attempt)
		}
		return json.RawMessage(body), trailers, nil
	}
}

func (h *HTTPRouter) withTimeout(ctx context.Context, options CallOptions) (context.Context, context.CancelFunc) {
	if options.Timeout > 0 {
		return context.WithTimeout(ctx, options.Timeout)
	}
	if h.options.Timeout > 0 {
		return context.WithTimeout(ctx, h.options.Timeout)
	}
	return context.WithCancel(ctx)
}

func (h *HTTPRouter) serverStreamRaw(ctx context.Context, service, method string, request json.RawMessage, opts []CallOption) (streamSource, error) {
	options := h.optionsFor(opts)
	callCtx, cancel := h.withTimeout(ctx, options)
	httpRequest, err := http.NewRequestWithContext(callCtx, http.MethodPost, h.callURL(service, method), bytes.NewReader(request))
	if err != nil {
		cancel()
		return nil, detailed(CodeInvalidArgument, err.Error(), "client", "", false)
	}
	httpRequest.Header = h.headers(options, "application/json")
	response, err := h.client.Do(httpRequest)
	if err != nil {
		cancel()
		return nil, detailed(CodeUnavailable, "HTTP request failed: "+err.Error(), "client", "", true)
	}
	if response.StatusCode >= 400 {
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		cancel()
		return nil, errorFromHTTP(response.StatusCode, body)
	}
	scanner := bufio.NewScanner(response.Body)
	scanner.Buffer(make([]byte, 64*1024), 64<<20)
	return &httpStream{response: response, cancel: cancel, scanner: scanner}, nil
}

func (h *HTTPRouter) clientStreamRaw(ctx context.Context, service, method string, request json.RawMessage, items func(yield func(json.RawMessage) bool), opts []CallOption) (json.RawMessage, map[string]string, error) {
	options := h.optionsFor(opts)
	callCtx, cancel := h.withTimeout(ctx, options)
	defer cancel()
	reader, writer := io.Pipe()
	go func() {
		encoder := json.NewEncoder(writer)
		if err := encoder.Encode(request); err != nil {
			_ = writer.CloseWithError(err)
			return
		}
		items(func(item json.RawMessage) bool {
			if err := encoder.Encode(item); err != nil {
				_ = writer.CloseWithError(err)
				return false
			}
			return true
		})
		_ = writer.Close()
	}()
	httpRequest, err := http.NewRequestWithContext(callCtx, http.MethodPost, h.callURL(service, method), reader)
	if err != nil {
		return nil, nil, detailed(CodeInvalidArgument, err.Error(), "client", "", false)
	}
	httpRequest.Header = h.headers(options, "application/x-ndjson")
	response, err := h.client.Do(httpRequest)
	if err != nil {
		return nil, nil, detailed(CodeUnavailable, "HTTP request failed: "+err.Error(), "client", "", true)
	}
	defer response.Body.Close()
	body, readErr := io.ReadAll(response.Body)
	if readErr != nil {
		return nil, nil, detailed(CodeUnavailable, "HTTP response unreadable: "+readErr.Error(), "client", "", true)
	}
	if response.StatusCode >= 400 {
		return nil, nil, errorFromHTTP(response.StatusCode, body)
	}
	return json.RawMessage(body), trailersOf(response.Header), nil
}

func (h *HTTPRouter) duplexRaw(ctx context.Context, service, method string, request json.RawMessage, opts []CallOption) (rawDuplex, error) {
	return nil, &Error{
		Code:    CodeMethodNotFound,
		Message: "duplex methods are WebSocket-only",
		Details: map[string]any{"source": "client", "reason": "duplex_over_http"},
	}
}

// Built-in Router services, shared with *Router's implementations.

// ListServices lists the services visible to this caller.
func (h *HTTPRouter) ListServices(ctx context.Context, opts ...CallOption) ([]RegistryService, error) {
	return listServices(ctx, h, opts...)
}

// DescribeService describes one registered service.
func (h *HTTPRouter) DescribeService(ctx context.Context, service string, opts ...CallOption) (RegistryServiceDetail, error) {
	return describeService(ctx, h, service, opts...)
}

// Whoami returns the caller's identity and scopes.
func (h *HTTPRouter) Whoami(ctx context.Context, opts ...CallOption) (WhoamiResponse, error) {
	return whoami(ctx, h, opts...)
}

// IssueToken mints a scoped bearer token.
func (h *HTTPRouter) IssueToken(ctx context.Context, scopes []RouterScope, ttlSeconds int64, opts ...CallOption) (IssuedToken, error) {
	return issueToken(ctx, h, scopes, ttlSeconds, opts...)
}

// RevokeToken revokes an issued token by its ID.
func (h *HTTPRouter) RevokeToken(ctx context.Context, tokenID string, opts ...CallOption) (RevokeTokenResponse, error) {
	return revokeToken(ctx, h, tokenID, opts...)
}

// httpStream reads NDJSON envelopes from one streaming response.
type httpStream struct {
	response *http.Response
	cancel   context.CancelFunc
	scanner  *bufio.Scanner
	done     bool
}

func (s *httpStream) next() (frame, error) {
	if s.done {
		return frame{}, io.EOF
	}
	for s.scanner.Scan() {
		line := bytes.TrimSpace(s.scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var envelope struct {
			Item json.RawMessage `json:"item"`
			End  *struct {
				Trailers map[string]string `json:"trailers"`
			} `json:"end"`
			Error *errorPayload `json:"error"`
		}
		if err := json.Unmarshal(line, &envelope); err != nil {
			s.abort(nil, "invalid_response")
			return frame{}, detailed(CodeProtocolError, "Invalid HTTP stream envelope", "client", "invalid_response", false)
		}
		switch {
		case envelope.Item != nil:
			return frame{Kind: "stream.item", Payload: envelope.Item}, nil
		case envelope.Error != nil:
			s.abort(nil, "stream_error")
			return frame{Kind: "call.error", Error: envelope.Error}, nil
		case envelope.End != nil:
			s.done = true
			s.cancel()
			return frame{Kind: "call.result", Metadata: envelope.End.Trailers}, nil
		default:
			s.abort(nil, "invalid_response")
			return frame{}, detailed(CodeProtocolError, "Unknown HTTP stream envelope", "client", "invalid_response", false)
		}
	}
	if err := s.scanner.Err(); err != nil && !errors.Is(err, io.EOF) {
		s.done = true
		s.cancel()
		return frame{}, detailed(CodeUnavailable, "HTTP stream ended: "+err.Error(), "client", "", true)
	}
	s.done = true
	s.cancel()
	return frame{}, io.EOF
}

func (s *httpStream) consumed() {}
func (s *httpStream) abort(err error, reason string) {
	if s.done {
		return
	}
	s.done = true
	s.response.Body.Close()
	s.cancel()
}

func trailersOf(headers http.Header) map[string]string {
	trailers := map[string]string{}
	for name, values := range headers {
		if len(values) > 0 && strings.HasPrefix(strings.ToLower(name), "meshcall-trailer-") {
			trailers[name[len("MeshCall-Trailer-"):]] = values[0]
		}
	}
	return trailers
}

func errorFromHTTP(status int, body []byte) *Error {
	var envelope struct {
		Error *errorPayload `json:"error"`
	}
	if err := json.Unmarshal(body, &envelope); err == nil && envelope.Error != nil {
		return &Error{Code: envelope.Error.Code, Message: envelope.Error.Message, Retryable: envelope.Error.Retryable, Details: envelope.Error.Details}
	}
	return detailed(CodeUnavailable, fmt.Sprintf("HTTP %d without a MeshCall error", status), "client", "", status >= 500)
}

func errAttempts(err *Error, attempts int) *Error {
	if attempts <= 1 {
		return err
	}
	details := map[string]any{}
	for key, value := range err.Details {
		details[key] = value
	}
	details["attempts"] = attempts
	return &Error{Code: err.Code, Message: err.Message, Retryable: err.Retryable, Details: details}
}

func containsString(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}
