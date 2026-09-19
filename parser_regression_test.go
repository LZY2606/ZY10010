package gofeed_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/mmcdole/gofeed"
	"github.com/stretchr/testify/require"
)

const (
	regressionRSS  = `<rss version="2.0"><channel><title>rss-title</title></channel></rss>`
	regressionAtom = `<feed xmlns="http://www.w3.org/2005/Atom"><title>atom-title</title></feed>`
	regressionJSON = `{"version":"https://jsonfeed.org/version/1.1","title":"json-title","items":[]}`
)

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func regressionResponse(status int, body io.ReadCloser) *http.Response {
	return &http.Response{
		StatusCode: status,
		Status:     http.StatusText(status),
		Header:     make(http.Header),
		Body:       body,
	}
}

// TestRegressionConcurrentSharedParserFirstUse shares one Parser with no
// explicit Client across goroutines whose very first calls race on ParseURL
// and ParseURLWithContext for all three feed formats. Lazy write-back of the
// default client used to make the race detector report concurrent read/write
// on the Parser's Client field; warming up once hid it, which is why the
// start barrier matters here.
func TestRegressionConcurrentSharedParserFirstUse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/rss":
			io.WriteString(w, regressionRSS)
		case "/atom":
			io.WriteString(w, regressionAtom)
		case "/json":
			io.WriteString(w, regressionJSON)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	want := map[string]string{"/rss": "rss-title", "/atom": "atom-title", "/json": "json-title"}
	p := gofeed.NewParser()

	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for path, title := range want {
				feed, err := p.ParseURL(srv.URL + path)
				if err != nil {
					t.Errorf("ParseURL(%s): %v", path, err)
					continue
				}
				if feed.Title != title {
					t.Errorf("ParseURL(%s): title %q, want %q", path, feed.Title, title)
				}
				feed, err = p.ParseURLWithContext(srv.URL+path, context.Background())
				if err != nil {
					t.Errorf("ParseURLWithContext(%s): %v", path, err)
					continue
				}
				if feed.Title != title {
					t.Errorf("ParseURLWithContext(%s): title %q, want %q", path, feed.Title, title)
				}
			}
		}()
	}
	close(start)
	wg.Wait()
}

// TestRegressionCustomClientUsedEveryTime pins that an explicitly provided
// Client is used for every request and never replaced by the default.
func TestRegressionCustomClientUsedEveryTime(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	client := &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		mu.Lock()
		calls++
		mu.Unlock()
		return regressionResponse(http.StatusOK, io.NopCloser(strings.NewReader(regressionRSS))), nil
	})}

	p := gofeed.NewParser()
	p.Client = client
	for i := 0; i < 3; i++ {
		feed, err := p.ParseURL("https://example.com/feed")
		require.NoError(t, err)
		require.Equal(t, "rss-title", feed.Title)
	}
	require.Equal(t, 3, calls, "custom client must serve every request")
	require.Same(t, client, p.Client, "explicit Client must not be replaced by the default")
}

// errorWithDataReader delivers its remaining data and a non-EOF error in the
// same Read, like a chunked response whose final chunk arrives with a
// transport failure.
type errorWithDataReader struct {
	data []byte
	err  error
}

func (r *errorWithDataReader) Read(p []byte) (int, error) {
	if r.data == nil {
		return 0, r.err
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	if len(r.data) == 0 {
		r.data = nil
		return n, r.err
	}
	return n, nil
}

type closeTrackingBody struct {
	io.Reader
	closeErr error
	closes   int
	read     int64
}

func (b *closeTrackingBody) Read(p []byte) (int, error) {
	n, err := b.Reader.Read(p)
	b.read += int64(n)
	return n, err
}

func (b *closeTrackingBody) Close() error {
	b.closes++
	return b.closeErr
}

// TestRegressionLimitBeatsTransportError covers the chunked-body case where
// the final Read delivers both the bytes crossing MaxByteSize and a non-EOF
// transport error: the caller must see ErrResponseTooLarge, and the body must
// not be probed more than one byte past the limit.
func TestRegressionLimitBeatsTransportError(t *testing.T) {
	boom := errors.New("chunked transfer failed")
	// Exactly limit+1 bytes: the read that crosses MaxByteSize is also the
	// final one, so it delivers the overflowing data and boom together.
	doc := `<rss version="2.0"><channel><title>` + strings.Repeat("x", 61) + `</title></channel></rss>`

	body := &closeTrackingBody{Reader: &errorWithDataReader{data: []byte(doc), err: boom}}
	p := gofeed.NewParser()
	p.MaxByteSize = int64(len(doc) - 1)
	p.Client = &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		return regressionResponse(http.StatusOK, body), nil
	})}

	feed, err := p.ParseURL("https://example.com/feed")
	require.ErrorIs(t, err, gofeed.ErrResponseTooLarge)
	require.Nil(t, feed)
	require.LessOrEqual(t, body.read, p.MaxByteSize+1, "read at most one extra byte to detect overflow")
	require.Equal(t, 1, body.closes)
}

// TestRegressionUnderlyingReadErrorPreserved confirms the flip side: when the
// response stays within MaxByteSize, the underlying read error reaches the
// caller unchanged.
func TestRegressionUnderlyingReadErrorPreserved(t *testing.T) {
	boom := errors.New("connection reset")
	doc := `<rss version="2.0"><channel><title>ok</title></channel></rss>`

	body := &closeTrackingBody{Reader: &errorWithDataReader{data: []byte(doc), err: boom}}
	p := gofeed.NewParser()
	p.MaxByteSize = 10000
	p.Client = &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		return regressionResponse(http.StatusOK, body), nil
	})}

	feed, err := p.ParseURLWithContext("https://example.com/feed", context.Background())
	require.ErrorIs(t, err, boom)
	require.NotErrorIs(t, err, gofeed.ErrResponseTooLarge)
	require.Nil(t, feed)
	require.Equal(t, 1, body.closes)
}

// TestRegressionCloseErrorDoesNotMask verifies that a failing Body.Close
// never replaces an error already produced by the status check, the size
// limit, or the parser, and only surfaces when the request otherwise
// succeeded. The body must be closed exactly once on every path.
func TestRegressionCloseErrorDoesNotMask(t *testing.T) {
	closeErr := errors.New("body close failed")

	newParser := func(body *closeTrackingBody, status int) *gofeed.Parser {
		p := gofeed.NewParser()
		p.Client = &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			return regressionResponse(status, body), nil
		})}
		return p
	}

	t.Run("status error wins", func(t *testing.T) {
		body := &closeTrackingBody{Reader: strings.NewReader(regressionRSS), closeErr: closeErr}
		p := newParser(body, http.StatusServiceUnavailable)
		feed, err := p.ParseURL("https://example.com/feed")
		require.Nil(t, feed)
		var httpErr gofeed.HTTPError
		require.ErrorAs(t, err, &httpErr)
		require.Equal(t, http.StatusServiceUnavailable, httpErr.StatusCode)
		require.NotErrorIs(t, err, closeErr)
		require.Equal(t, 1, body.closes)
	})

	t.Run("limit error wins", func(t *testing.T) {
		big := `<rss version="2.0"><channel><title>` + strings.Repeat("x", 5000) + `</title></channel></rss>`
		body := &closeTrackingBody{Reader: strings.NewReader(big), closeErr: closeErr}
		p := newParser(body, http.StatusOK)
		p.MaxByteSize = 100
		feed, err := p.ParseURL("https://example.com/feed")
		require.Nil(t, feed)
		require.ErrorIs(t, err, gofeed.ErrResponseTooLarge)
		require.NotErrorIs(t, err, closeErr)
		require.Equal(t, 1, body.closes)
	})

	t.Run("parse error wins", func(t *testing.T) {
		body := &closeTrackingBody{Reader: strings.NewReader("this is not a feed"), closeErr: closeErr}
		p := newParser(body, http.StatusOK)
		feed, err := p.ParseURL("https://example.com/feed")
		require.Nil(t, feed)
		require.ErrorIs(t, err, gofeed.ErrFeedTypeNotDetected)
		require.NotErrorIs(t, err, closeErr)
		require.Equal(t, 1, body.closes)
	})

	t.Run("close error surfaces on success", func(t *testing.T) {
		body := &closeTrackingBody{Reader: strings.NewReader(regressionRSS), closeErr: closeErr}
		p := newParser(body, http.StatusOK)
		_, err := p.ParseURL("https://example.com/feed")
		require.ErrorIs(t, err, closeErr)
		require.Equal(t, 1, body.closes)
	})

	t.Run("success untouched", func(t *testing.T) {
		body := &closeTrackingBody{Reader: strings.NewReader(regressionRSS)}
		p := newParser(body, http.StatusOK)
		feed, err := p.ParseURL("https://example.com/feed")
		require.NoError(t, err)
		require.Equal(t, "rss-title", feed.Title)
		require.Equal(t, 1, body.closes)
	})
}
