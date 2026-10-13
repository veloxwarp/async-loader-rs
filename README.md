# Async Loader

Load and cache data from any network service.

## Stewardship

This repository is an independent continuation of [fpco/async-loader-rs](https://github.com/fpco/async-loader-rs), imported with its full Git history. It is maintained by Velox Warp. Original authorship, attribution, and licensing remain preserved; the original FP Complete copyright notice is retained in [LICENSE](LICENSE).

The fpco repository remains separate: its issues and pull requests have not been transferred, and its URLs do not redirect here.

## Using this repository

For a Git dependency, point Cargo at the maintained repository:

```toml
[dependencies]
async-loader = { git = "https://github.com/veloxwarp/async-loader-rs", branch = "main" }
```

Existing consumers pinned to a commit can retain their `rev` when changing the repository URL. The imported `newer-rust` and `report-errors-on-stale-old` branches are preserved for historical compatibility; new development targets `main`.

## Cache behavior and operational limits

- `get_value` returns a cached value immediately while it is within its usable lifetime. Once `reload_delay` has elapsed, the first caller starts a background refresh but still receives that cached value; concurrent callers share the refresh. `get_latest_value` waits for a new load (or an already-running refresh), even when usable cache exists.
- With a finite `stale_delay`, the usable lifetime is `reload_delay + stale_delay`; beyond that, callers wait for loading and receive an error if it fails. `get_value_allow_stale` can fall back to an expired cached value on failure. `StaleDelay::AllowStale` lets ordinary reads continue using the last successful value indefinitely while attempting refreshes.
- Load errors do not replace the last successful cache entry. An unwinding panic during loader invocation or future polling is reported as a load error, releases waiting callers, and allows another attempt. The normal panic hook still runs; process-aborting panics cannot be recovered.
- There is no built-in load timeout. A loader that never returns keeps that key's load in flight. Callers should apply a suitable timeout, such as `tokio::time::timeout`, and arrange I/O deadlines or cancellation inside their loader. Timing out or dropping a caller does **not** cancel the shared background load.

### Lifetime is not retention

Freshness deadlines determine whether a value can be returned, **not** when it is evicted. With `store_cache: true`, per-key state and cached values remain until the loader is dropped; even failed keys can retain state. There is no eviction policy or key-count bound. Use a bounded, validated key space and account for the size of retained values. Do not feed arbitrary request URLs, user-controlled identifiers, or an unbounded stream of keys directly into a long-lived loader. Include all authorization/tenant distinctions that affect the result in the key, or use separate loaders, to prevent cross-user data reuse.

With `store_cache: false`, callers can still share an in-flight load, but completed values are not cached and the completed per-key entry is removed. This is separate from HTTP `Cache-Control: no-store`, which does not configure this in-process cache.

### HTTP cache policy

The library does not add `Cache-Control` headers. Headers returned by `CacheLoader::into_response` are preserved, including `private` and `no-store`; the default JSON response has no explicit HTTP cache policy. Set the appropriate policy in your loader or response middleware, especially for authenticated or sensitive responses. In-process cache freshness does not establish that a response is safe for a shared HTTP cache. Applications that previously relied on automatically generated `public, max-age=...` must now set their policy explicitly.

## License

MIT; see [LICENSE](LICENSE).
