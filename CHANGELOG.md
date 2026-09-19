# Changelog

## Unreleased

### Fixed: data race on first use of a shared Parser

Symptom: a `Parser` configured once and then shared across goroutines (the
standard crawler pattern) tripped the race detector when the first batch of
`ParseURL` calls landed concurrently. Warming the parser up with a single
call made the reports disappear.

Call chain: `ParseURL` / `ParseURLWithContext` resolved the HTTP client
through an accessor that lazily wrote the shared default back into
`Parser.Client` (`f.Client = defaultClient`). Concurrent first calls raced
on that write. After the first call completed, the field was set and every
later call only read it, which is why the race vanished after warm-up.

Fix: the accessor now returns the shared default client (and the shared
default translators, on the `Parse` path for RSS, Atom, and JSON feeds)
without ever writing back to the `Parser`. A parser that is only read after
configuration is safe for concurrent use from the very first call. An
explicitly assigned `Parser.Client` is still used for every request and is
never replaced by the default.

### Fixed: MaxByteSize overflow lost to a simultaneous transport error

Symptom: against a chunked upstream whose final `Read` delivered both the
bytes crossing `MaxByteSize` and a non-EOF transport error, callers received
the transport error and `errors.Is(err, ErrResponseTooLarge)` was false.

Call chain: `ParseURLWithContext` wraps the response body in a
`limitedReader`. When a single `Read` returned more bytes than the remaining
budget together with an error, the reader kept the underlying error instead
of reporting the overflow.

Fix: once the limit is exceeded, `ErrResponseTooLarge` is the definitive
outcome and takes priority over any error attached to the same `Read`. The
reader still asks the body for at most one byte past the limit to make that
determination. When the limit is not exceeded, an underlying read error is
returned unchanged and remains matchable with `errors.Is`. A zero
`MaxByteSize` still means no limit.

### Fixed: response body Close error masked the real failure

Symptom: when the server returned an error status (e.g. 503) and closing the
response body also failed, the caller saw the close error instead of the
`HTTPError` carrying the status code. The same overwrite happened when a 2xx
response had already failed with `ErrResponseTooLarge` or a parse error.

Call chain: `ParseURLWithContext` closed the body in a deferred function
that assigned the close error to the named return value unconditionally,
clobbering any error produced by the status check, the limited read, or the
feed parse.

Fix: the deferred close only becomes the returned error when the call has
succeeded up to that point; status, read, and parse errors always take
priority. The body is still closed exactly once on every return path,
including context cancellation.

### Why ordinary successful responses never exposed these

All three bugs sit on paths that a healthy response never touches: the lazy
default write only happens while `Client` is still nil, the error-priority
branch in `limitedReader` only runs when the limit is actually crossed, and
the close-error assignment only matters when `Close` itself fails. A
successful parse of an in-limit body from a well-behaved server exercises
none of them, which is why the symptoms only appeared under concurrency,
chunked transfer failures, and misbehaving upstreams.
