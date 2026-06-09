# mcap-repackage

A C++ command-line tool that re-chunks an MCAP file for smaller size and faster
reads, using the multithreaded `mcap::ParallelReader` for the read path.

## What it does

Messages are read in **log-time order** and:

1. packed into chunks bounded by `--chunk-size`;
2. sorted by **channel** within each chunk (topic adjacency → better compression
   and faster selective reads);
3. written as **single-message chunks** when a message is `>=`
   `--large-message-threshold`.

Because messages are buffered in log-time order, each chunk spans a contiguous
time slice, so the chunks are **time-disjoint (low overlap)** — a full log-time
read needs no cross-chunk merge, and a reader can still skip chunks for selective
reads. Metadata and attachments are copied through unchanged; the output is a
fully indexed MCAP file.

The read path uses the `ParallelReader` (log-time order) when the input has the
required chunk + message indexes; otherwise it falls back to a serial read.

## Usage

```
mcap-repackage <input.mcap> --output <output.mcap> [OPTIONS]

OPTIONS:
  -o, --output <path>             Destination file (alias: --output-file)
  --large-message-threshold <n>   Bytes; messages >= this get their own chunk
                                  (default: 262144)
  --chunk-size <n>                Target uncompressed chunk size in bytes
                                  (default: 4194304)
  --compression <c>               zstd | lz4 | none (default: zstd)
  --compression-level <n>         0 = compressor default (default: 0)
  --include-crc[=<bool>]          Include CRCs in the output (default: true)
  --read-threads <n>              Parallel-read worker threads; 0 = default (4),
                                  capped at 8
  --stats                         Print parallel-read chunk counters to stderr
  -h, --help                      Print help
```

> **Note on `--compression-level`:** the C++ MCAP writer exposes five discrete
> levels, mapped onto the `CompressionLevel` enum → zstd `{Fastest=1, Fast=3,
> Default=1, Slow=5, Slowest=7}`. Level `0` = Default = zstd 1. zstd-1 is the
> recommended default: with the parallel reader, decompression is fully hidden, so
> a higher level only shrinks the file marginally at a read-speed cost that the
> reader already absorbs.

## Notes / library quirks

- **Chunk-size ceiling.** The C++ writer's chunk buffer is reserved to its chunk
  size, so the tool can't set it to "infinite" to fully disable size-based
  auto-flush; it caps the reserve/auto-flush threshold at 2 GiB. A `--chunk-size`
  (or `--large-message-threshold`) above that cap, or pathologically tiny
  messages, may add an extra chunk boundary at the ceiling — a benign layout
  difference, never data loss. Keep `--chunk-size` around the 4 MiB default
  (sweeps showed 2–8 MiB is the size/speed sweet spot).
- **Unindexed log-time reads.** A chunked input that has chunk indexes but no
  message indexes cannot be read in log-time order by this library, so it is read
  in file order (which assumes the input is approximately log-time ordered).
  Files with full indexes use the `ParallelReader`; unchunked files use the serial
  reader.

## Building

From the `cpp/` directory (mirrors `build.sh`):

```bash
conan editable add ./mcap mcap/2.1.3   # if not already registered
conan install repackage --install-folder repackage/build/Release \
  -s compiler.cppstd=17 -s build_type=Release --build missing
conan build repackage --build-folder repackage/build/Release
```

The resulting binary is `cpp/repackage/build/Release/bin/mcap-repackage`.
