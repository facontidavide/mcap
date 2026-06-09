# mcap-repackage

A C++ command-line tool that re-chunks an MCAP file for better compression and
per-topic read locality. It is a port of the Rust `mcap repackage` command and
performs the same conversion, but uses the multithreaded `mcap::ParallelReader`
for the read path.

## What it does

For each fixed **log-time window** (default 1 second), messages are:

1. grouped and sorted by **topic** (then channel id, log time, sequence, publish
   time),
2. packed into chunks with **one chunk per topic run**,
3. split when a chunk would exceed `--chunk-size`, and
4. written as **single-message chunks** when a message is `>=`
   `--large-message-threshold`.

Metadata and attachments are copied through unchanged. The output is a fully
indexed MCAP file.

The read path uses the `ParallelReader` (log-time order) when the input has the
required chunk + message indexes; otherwise it falls back to a serial read.

## Usage

```
mcap-repackage <input.mcap> --output <output.mcap> [OPTIONS]

OPTIONS:
  -o, --output <path>             Destination file (alias: --output-file)
  --window-duration-secs <n>      Log-time window in seconds (default: 1)
  --large-message-threshold <n>   Bytes; messages >= this get their own chunk
                                  (default: 262144)
  --chunk-size <n>                Target uncompressed chunk size in bytes for
                                  small-message groups (default: 4194304)
  --compression <c>               zstd | lz4 | none (default: zstd)
  --compression-level <n>         0 = compressor default (default: 0)
  --include-crc[=<bool>]          Include CRCs in the output (default: true)
  -h, --help                      Print help
```

> **Note on `--compression-level`:** the C++ MCAP writer exposes only five
> levels, so the numeric value is mapped onto the `CompressionLevel` enum
> (`0`/`3` = Default, `1` = Fastest, `2` = Fast, `4` = Slow, `>=5` = Slowest).
> Level `0` keeps the Rust semantic of "use the compressor default".

## Differences from the Rust command

The conversion (windowing, per-topic grouping, chunk-size splitting, large-message
isolation, metadata/attachment passthrough) matches the Rust `repackage` command.
Two differences stem from the C++ MCAP library:

- **Chunk-size ceiling.** The Rust writer disables size-based auto-flush
  (`chunk_size(None)`); the C++ writer cannot, and its chunk buffer is reserved to
  the chunk size. The tool therefore controls boundaries explicitly but caps the
  writer's reserve/auto-flush threshold at 2 GiB. A `--chunk-size` (or
  `--large-message-threshold`) above that cap, or pathologically tiny messages,
  may produce an extra chunk boundary at the ceiling — a benign layout difference,
  never data loss.
- **Unindexed log-time reads.** A chunked input that has chunk indexes but no
  message indexes cannot be read in log-time order by this library, so it is read
  in file order (which assumes the input is approximately log-time ordered, as
  Rust's own linear path does). Files with full indexes use the `ParallelReader`;
  unchunked files use the serial reader.

## Building

From the `cpp/` directory (mirrors `build.sh`):

```bash
conan editable add ./mcap mcap/2.1.3   # if not already registered
conan install repackage --install-folder repackage/build/Release \
  -s compiler.cppstd=17 -s build_type=Release --build missing
conan build repackage --build-folder repackage/build/Release
```

The resulting binary is `cpp/repackage/build/Release/bin/mcap-repackage`.
