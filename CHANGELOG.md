# Changelog

## Unreleased

### Fixed: data race on first use of a shared Parser

A `Parser` that was fully configured and then only read is documented to be
safe for concurrent use, and crawlers routinely share one instance across
goroutines. But `ParseURL`/`ParseURLWithContext` resolved the HTTP client
through a lazy write-back: when `Parser.Client` was unset, the first call
stored the shared default client into the `Client` field. Concurrent first
calls therefore raced on that field (reported by the race detector as
concurrent read/write on the parser), while any call after the field had been
populated only read it — which is why warming the parser up once made the
symptom disappear. The same lazy write-back pattern on the translator fields
had already been removed; the client accessor now follows the same rule and
returns the shared default without writing it back. An explicitly provided
custom `Client` is still used for every request and is never replaced.

Public call chain: `Parser.ParseURL` / `Parser.ParseURLWithContext` →
`Parser.httpClient`.

### Fixed: ErrResponseTooLarge loses to a transport error on the same read

With a positive `Parser.MaxByteSize`, `ParseURL`/`ParseURLWithContext` wrap
the response body in a `limitedReader` that reads at most one byte past the
limit to distinguish overflow from an exact-size response. A chunked upstream
can deliver the final chunk together with a non-EOF transport error from the
same `Read`. Previously the underlying error won in that case, so callers
saw the transport error and `errors.Is(err, ErrResponseTooLarge)` was false
even though the response had genuinely exceeded the limit. Now crossing the
limit always reports `ErrResponseTooLarge`, taking priority over any error
attached to the same read; when the response stays within the limit, the
underlying read error is still returned unchanged and remains discoverable
via `errors.Is`. Ordinary successful responses never hit this path because
their final read returns `io.EOF` (or nothing) at or below the limit, so no
priority decision ever arises.

Public call chain: `Parser.ParseURL` / `Parser.ParseURLWithContext` →
`limitedReader.Read` → `ErrResponseTooLarge`.

### Fixed: Body.Close error masking the real result

`ParseURL`/`ParseURLWithContext` close the response body via a deferred call
that unconditionally assigned a close failure to the returned error. A server
replying 503 whose body also failed to close surfaced the close error instead
of the `HTTPError` carrying the status code, and the same overwrite happened
when a 2xx response had already failed with `ErrResponseTooLarge` or a parse
error. The deferred close now only reports its error when nothing else went
wrong, so the priority is: status/read/parse error first, close error only on
an otherwise fully successful request. The body is still closed exactly once
on every return path. Ordinary successful responses were unaffected because
their `Close` returns nil, leaving nothing to overwrite.

Public call chain: `Parser.ParseURL` / `Parser.ParseURLWithContext` →
deferred `resp.Body.Close`.
