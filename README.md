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

## License

MIT; see [LICENSE](LICENSE).
