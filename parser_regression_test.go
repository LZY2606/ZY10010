package gofeed_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/mmcdole/gofeed"
	"github.com/stretchr/testify/require"
)

const (
	regressionRSS   = `<rss version="2.0"><channel><title>r</title><item><title>i</title></item></channel></rss>`
	regressionAtom  = `<feed xmlns="http://www.w3.org/2005/Atom"><title>a</title></feed>`
	regressionJSON  = `{"version":"https://jsonfeed.org/version/1.1","title":"j","items":[]}`
	regressionBogus = `this is not a feed at all`
)

// roundTripperFunc adapts a function to http.RoundTripper.
type roundTripperFunc func(*http.Request) (*http.Response, error)

func (fn roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

// regressionBody is a response body whose reads and closes are fully
// controlled by the test.
type regressionBody struct {
	reader  io.Reader
	closeN  int32
	closeFn func() error
}

func (b *regressionBody) Read(p []byte) (int, error) { return b.reader.Read(p) }

func (b *regressionBody) Close() error {
	atomic.AddInt32(&b.closeN, 1)
	if b.closeFn != nil {
		return b.closeFn()
	}
	return nil
}

func (b *regressionBody) closes() int { return int(atomic.LoadInt32(&b.closeN)) }

// regressionClient returns a client whose transport serves status with body
// for every request.
func regressionClient(status int, body *regressionBody) *http.Client {
	return &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: status,
			Status:     http.StatusText(status),
			Header:     make(http.Header),
			Body:       body,
		}, nil
	})}
}

// TestRegressionConcurrentSharedParserFirstUse shares one read-only Parser
// across goroutines with no explicit Client. The first ParseURL calls used to
// race on the lazy Client default; ParseURLWithContext and Parse of all three
// feed formats must be equally race-free. Requests go to a closed loopback
// port so no network is involved; the assertion is the race detector's.
func TestRegressionConcurrentSharedParserFirstUse(t *testing.T) {
	p := gofeed.NewParser()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(4)
		go func() {
			defer wg.Done()
			_, _ = p.ParseURL("http://127.0.0.1:1/feed")
		}()
		go func() {
			defer wg.Done()
			_, _ = p.ParseURLWithContext("http://127.0.0.1:1/feed", context.Background())
		}()
		go func() {
			defer wg.Done()
			for _, doc := range []string{regressionRSS, regressionAtom, regressionJSON} {
				if _, err := p.ParseString(doc); err != nil {
					t.Error(err)
				}
			}
		}()
		go func() {
			defer wg.Done()
			if _, err := p.Parse(strings.NewReader(regressionRSS)); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
}

// TestRegressionCustomClientUsedEveryTime ensures an explicitly provided
// Client is used for every call and never replaced by the package default.
func TestRegressionCustomClientUsedEveryTime(t *testing.T) {
	var calls int32
	client := &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		atomic.AddInt32(&calls, 1)
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(regressionRSS)),
		}, nil
	})}
	p := gofeed.NewParser()
	p.Client = client

	const n = 4
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			feed, err := p.ParseURL("https://example.com/feed")
			require.NoError(t, err)
			require.Equal(t, "r", feed.Title)
		}()
	}
	wg.Wait()
	require.Equal(t, int32(n), atomic.LoadInt32(&calls))
	require.Same(t, client, p.Client)
}

// finalChunkErrorReader delivers its remaining data together with a non-EOF
// error on the last Read, mimicking a chunked body that fails mid-trailer.
type finalChunkErrorReader struct {
	*strings.Reader
	err error
}

func (r finalChunkErrorReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if n > 0 && r.Len() == 0 {
		return n, r.err
	}
	return n, err
}

type countingReader struct {
	r    io.Reader
	read *int64
}

func (c countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	*c.read += int64(n)
	return n, err
}

// TestRegressionTooLargePriorityOverReadError verifies that when the final
// Read both crosses MaxByteSize and carries a transport error, the caller
// sees ErrResponseTooLarge, and that no more than one byte past the limit is
// consumed. Without overflow the underlying error stays visible.
func TestRegressionTooLargePriorityOverReadError(t *testing.T) {
	boom := errors.New("chunked transfer failed")
	doc := `<rss version="2.0"><channel><title>` + strings.Repeat("x", 5000) + `</title></channel></rss>`

	t.Run("overflow wins", func(t *testing.T) {
		var read int64
		body := &regressionBody{reader: countingReader{finalChunkErrorReader{strings.NewReader(doc), boom}, &read}}
		p := gofeed.NewParser()
		// Limit one byte short of the document so the final Read delivers
		// both the overflowing byte and the transport error together.
		p.MaxByteSize = int64(len(doc)) - 1
		p.Client = regressionClient(http.StatusOK, body)
		feed, err := p.ParseURL("https://example.com/feed")
		require.ErrorIs(t, err, gofeed.ErrResponseTooLarge)
		require.NotErrorIs(t, err, boom)
		require.Nil(t, feed)
		require.LessOrEqual(t, read, p.MaxByteSize+1)
		require.Equal(t, 1, body.closes())
	})

	t.Run("underlying error survives without overflow", func(t *testing.T) {
		body := &regressionBody{reader: finalChunkErrorReader{strings.NewReader(doc), boom}}
		p := gofeed.NewParser()
		p.MaxByteSize = int64(len(doc)) + 100
		p.Client = regressionClient(http.StatusOK, body)
		feed, err := p.ParseURL("https://example.com/feed")
		require.ErrorIs(t, err, boom)
		require.NotErrorIs(t, err, gofeed.ErrResponseTooLarge)
		require.Nil(t, feed)
		require.Equal(t, 1, body.closes())
	})
}

// TestRegressionCloseErrorNeverOverwrites checks the error priority around
// response body Close: an existing status/read/parse error always wins, a
// Close error only surfaces on an otherwise successful call, and the body is
// closed exactly once on every path.
func TestRegressionCloseErrorNeverOverwrites(t *testing.T) {
	closeErr := errors.New("body close failed")

	t.Run("status error wins", func(t *testing.T) {
		body := &regressionBody{reader: strings.NewReader(regressionRSS), closeFn: func() error { return closeErr }}
		p := gofeed.NewParser()
		p.Client = regressionClient(http.StatusServiceUnavailable, body)
		feed, err := p.ParseURL("https://example.com/feed")
		require.Nil(t, feed)
		require.NotErrorIs(t, err, closeErr)
		var httpErr gofeed.HTTPError
		require.ErrorAs(t, err, &httpErr)
		require.Equal(t, http.StatusServiceUnavailable, httpErr.StatusCode)
		require.Equal(t, 1, body.closes())
	})

	t.Run("parse error wins", func(t *testing.T) {
		body := &regressionBody{reader: strings.NewReader(regressionBogus), closeFn: func() error { return closeErr }}
		p := gofeed.NewParser()
		p.Client = regressionClient(http.StatusOK, body)
		feed, err := p.ParseURL("https://example.com/feed")
		require.Nil(t, feed)
		require.ErrorIs(t, err, gofeed.ErrFeedTypeNotDetected)
		require.NotErrorIs(t, err, closeErr)
		require.Equal(t, 1, body.closes())
	})

	t.Run("too large wins", func(t *testing.T) {
		body := &regressionBody{reader: strings.NewReader(regressionRSS), closeFn: func() error { return closeErr }}
		p := gofeed.NewParser()
		p.MaxByteSize = 10
		p.Client = regressionClient(http.StatusOK, body)
		feed, err := p.ParseURL("https://example.com/feed")
		require.Nil(t, feed)
		require.ErrorIs(t, err, gofeed.ErrResponseTooLarge)
		require.NotErrorIs(t, err, closeErr)
		require.Equal(t, 1, body.closes())
	})

	t.Run("close error surfaces on success", func(t *testing.T) {
		body := &regressionBody{reader: strings.NewReader(regressionRSS), closeFn: func() error { return closeErr }}
		p := gofeed.NewParser()
		p.Client = regressionClient(http.StatusOK, body)
		feed, err := p.ParseURL("https://example.com/feed")
		require.ErrorIs(t, err, closeErr)
		require.NotNil(t, feed)
		require.Equal(t, 1, body.closes())
	})

	t.Run("clean success closes once", func(t *testing.T) {
		body := &regressionBody{reader: strings.NewReader(regressionRSS)}
		p := gofeed.NewParser()
		p.Client = regressionClient(http.StatusOK, body)
		feed, err := p.ParseURL("https://example.com/feed")
		require.NoError(t, err)
		require.Equal(t, "r", feed.Title)
		require.Equal(t, 1, body.closes())
	})
}
