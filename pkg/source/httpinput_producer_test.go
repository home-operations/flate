package source

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/home-operations/flate/internal/fetcherr"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func TestHttpGetURL_RejectsOversizedBody(t *testing.T) {
	t.Parallel()
	c := newTestHTTPInputs(t, "")
	c.httpClient.Transport = remoteRoundTrip(func(*http.Request) (*http.Response, error) {
		body := remoteBody{Reader: remoteRead(func(p []byte) (int, error) { clear(p); return len(p), nil }), close: func() error { return nil }}
		return &http.Response{StatusCode: http.StatusOK, Body: body, Header: make(http.Header)}, nil
	})
	_, err := c.httpGetURL(t.Context(), "https://example.invalid/big.yaml", sourcecache.ProducerInput{Writer: io.Discard})
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("oversized-body error=%v", err)
	}
}

func TestHttpGetURL_AcceptsBodyAtCap(t *testing.T) {
	t.Parallel()
	c := newTestHTTPInputs(t, "")
	c.httpClient.Transport = remoteRoundTrip(func(*http.Request) (*http.Response, error) {
		reader := io.LimitReader(remoteRead(func(p []byte) (int, error) { clear(p); return len(p), nil }), remoteFetchMaxBytes)
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(reader), Header: make(http.Header)}, nil
	})
	desc, err := c.httpGetURL(t.Context(), "https://example.invalid/atcap.yaml", sourcecache.ProducerInput{Writer: io.Discard})
	if err != nil || desc.Size != remoteFetchMaxBytes {
		t.Fatalf("body exactly at cap: size=%d, error=%v", desc.Size, err)
	}
}

func TestHttpGetURL_BodyCloseErrors(t *testing.T) {
	t.Parallel()
	readErr, closeErr := errors.New("body read"), errors.New("body close")
	for _, outcome := range []string{"success", "created", "last-success", "status", "read", "limit"} {
		t.Run(outcome, func(t *testing.T) {
			t.Parallel()
			c := newTestHTTPInputs(t, "")
			closes := 0
			c.httpClient.Transport = remoteRoundTrip(func(*http.Request) (*http.Response, error) {
				var reader io.Reader = strings.NewReader("complete")
				status := http.StatusOK
				switch outcome {
				case "created":
					status = http.StatusCreated
				case "last-success":
					status = 299
				case "status":
					status = http.StatusNotFound
				case "read":
					reader = remoteRead(func([]byte) (int, error) { return 0, readErr })
				case "limit":
					reader = remoteRead(func(p []byte) (int, error) { clear(p); return len(p), nil })
				}
				return &http.Response{StatusCode: status, Header: make(http.Header), Body: remoteBody{Reader: reader, close: func() error { closes++; return closeErr }}}, nil
			})
			_, err := c.httpGetURL(t.Context(), "https://example.invalid/body", sourcecache.ProducerInput{Writer: io.Discard})
			if !errors.Is(err, closeErr) || closes != 1 || strings.Count(err.Error(), closeErr.Error()) != 1 {
				t.Fatalf("body cleanup: %v closes=%d", err, closes)
			}
			if outcome == "read" && !errors.Is(err, readErr) || outcome == "status" && fetcherr.Classes(err)&fetcherr.Client == 0 || outcome == "limit" && !strings.Contains(err.Error(), "exceeds") {
				t.Fatalf("operation identity: %v", err)
			}
			if outcome == "status" {
				status, ok := errors.AsType[*fetcherr.ResponseError](err)
				if !ok || status.StatusCode != http.StatusNotFound || status.Op != http.MethodGet {
					t.Fatalf("response facts=%+v error=%v", status, err)
				}
			}
			if (outcome == "status" || outcome == "read" || outcome == "limit") && !strings.HasSuffix(err.Error(), "\n"+closeErr.Error()) {
				t.Fatalf("operation must precede cleanup: %v", err)
			}
		})
	}
}

func TestHttpGetURL_RequestLifetime(t *testing.T) {
	t.Parallel()
	for _, stage := range []string{"headers", "body", "caller-deadline", "previous", "close"} {
		t.Run(stage, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				c := newTestHTTPInputs(t, "")
				started := time.Now()
				ctx := t.Context()
				bound := 5 * time.Second
				if stage == "caller-deadline" {
					var cancel context.CancelFunc
					bound = time.Second
					ctx, cancel = context.WithTimeout(ctx, bound)
					defer cancel()
				}
				if stage == "previous" {
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				}
				var requestCtx context.Context
				closes := 0
				c.httpClient.Transport = remoteRoundTrip(func(r *http.Request) (*http.Response, error) {
					if stage == "previous" {
						t.Error("request issued after Previous rejected cancellation")
					}
					requestCtx = r.Context()
					deadline, ok := requestCtx.Deadline()
					if !ok || deadline.Sub(started) != bound {
						t.Fatalf("request deadline = %v, present = %t, want %v", deadline.Sub(started), ok, bound)
					}
					if stage == "headers" {
						<-requestCtx.Done()
						return nil, requestCtx.Err()
					}
					var reader io.Reader = remoteRead(func([]byte) (int, error) {
						<-requestCtx.Done()
						return 0, requestCtx.Err()
					})
					if stage == "close" {
						reader = strings.NewReader("complete")
					}
					return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: remoteBody{Reader: reader, close: func() error {
						closes++
						if stage == "close" && requestCtx.Err() != nil {
							t.Errorf("request context canceled before body close: %v", requestCtx.Err())
						}
						return nil
					}}}, nil
				})
				_, err := c.httpGetURL(ctx, "https://example.invalid/resource", sourcecache.ProducerInput{Writer: io.Discard})
				switch stage {
				case "close":
					if err != nil || closes != 1 || !errors.Is(requestCtx.Err(), context.Canceled) {
						t.Fatalf("completed lifetime: err=%v closes=%d context=%v", err, closes, requestCtx.Err())
					}
				case "previous":
					if !errors.Is(err, context.Canceled) || requestCtx != nil {
						t.Fatalf("Previous cancellation: err=%v request=%v", err, requestCtx)
					}
				default:
					if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) != bound {
						t.Fatalf("request lifetime: err=%v elapsed=%v want=%v", err, time.Since(started), bound)
					}
					if stage != "headers" && closes != 1 {
						t.Fatalf("expired body closes=%d", closes)
					}
				}
			})
		})
	}
	t.Run("network-body-cancellation", func(t *testing.T) {
		t.Parallel()
		entered, canceled := make(chan struct{}), make(chan struct{})
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/healthy" {
				_, _ = io.WriteString(w, "healthy")
				return
			}
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			<-r.Context().Done()
			close(canceled)
		}))
		defer srv.Close()
		var network Network
		defer network.Close()
		c := newTestHTTPInputs(t, "")
		c.httpClient.Transport = remoteRoundTrip(func(r *http.Request) (*http.Response, error) {
			resp, err := network.RoundTrip(r)
			if err != nil || r.URL.Path == "/healthy" {
				return resp, err
			}
			body := resp.Body
			enteredBody := sync.OnceFunc(func() { close(entered) })
			resp.Body = remoteBody{Reader: remoteRead(func(p []byte) (int, error) {
				enteredBody()
				return body.Read(p)
			}), close: body.Close}
			return resp, nil
		})
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		go func() {
			_, err := c.httpGetURL(ctx, srv.URL+"/stalled", sourcecache.ProducerInput{Writer: io.Discard})
			done <- err
		}()
		<-entered
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("wrapped Network body cancellation: %v", err)
		}
		<-canceled
		var body strings.Builder
		if _, err := c.httpGetURL(t.Context(), srv.URL+"/healthy", sourcecache.ProducerInput{Writer: &body}); err != nil || body.String() != "healthy" {
			t.Fatalf("Network after caller cancellation: %q %v", body.String(), err)
		}
	})
}
