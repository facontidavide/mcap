// mcap-repackage: re-chunk an MCAP file into the "sorted" layout, using the
// multithreaded mcap::ParallelReader for reads.
//
// Messages are read in log-time order and packed into chunks bounded by a target
// chunk size; within each chunk they are sorted by channel (topic adjacency ->
// better compression and faster selective reads). Because messages are buffered
// in log-time order, each chunk spans a contiguous time slice -> chunks are
// time-disjoint (low overlap), so a full read needs no cross-chunk merge.
// Messages >= a threshold are isolated into single-message chunks. Metadata and
// attachments are copied through unchanged. The read path uses the ParallelReader
// (log-time order) when the input has the required chunk + message indexes, and
// falls back to a serial read otherwise.

#define MCAP_IMPLEMENTATION
#include <mcap/mcap.hpp>

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace {

namespace fs = std::filesystem;

// ---------------------------------------------------------------------------
// Options + CLI parsing
// ---------------------------------------------------------------------------

struct Options {
  std::string inputPath;
  std::string outputPath;
  uint64_t largeMessageThreshold = 256 * 1024;
  uint64_t chunkSize = 4 * 1024 * 1024;
  mcap::Compression compression = mcap::Compression::Zstd;
  uint32_t compressionLevel = 0;
  bool includeCrc = true;
  uint32_t readThreads = 0;  // 0 = default (4, capped at 8); parallel read path only
  bool stats = false;        // --stats: report parallel-read chunk decompression counters
};

void printUsage(const char* argv0) {
  std::cerr
    << "mcap-repackage - re-chunk an MCAP file into time-ordered chunks, grouped\n"
       "                 by topic within each chunk (better compression + lazy reads),\n"
       "                 using the multithreaded ParallelReader.\n\n"
       "USAGE:\n"
       "    "
    << argv0
    << " <input.mcap> --output <output.mcap> [OPTIONS]\n\n"
       "ARGS:\n"
       "    <input.mcap>                  Local path to the source MCAP file\n\n"
       "OPTIONS:\n"
       "    -o, --output <path>           Destination repackaged MCAP file\n"
       "                                  (alias: --output-file)\n"
       "    --large-message-threshold <n> Messages >= this data size are written as\n"
       "                                  single-message chunks (default: 262144)\n"
       "    --chunk-size <n>              Target uncompressed chunk size in bytes\n"
       "                                  (default: 4194304)\n"
       "    --compression <c>             Output chunk compression: zstd, lz4, or none\n"
       "                                  (default: zstd)\n"
       "    --compression-level <n>       0 = compressor default. Mapped to the C++\n"
       "                                  CompressionLevel enum: 0/3=Default, 1=Fastest,\n"
       "                                  2=Fast, 4=Slow, >=5=Slowest (default: 0)\n"
       "    --include-crc[=<bool>]        Include CRCs in the output MCAP (default: true)\n"
       "    --stats                       Print parallel-read chunk stats to stderr\n"
       "                                  (scheduled / decompressed / forced / peak resident)\n"
       "    --read-threads <n>            Parallel-read worker threads. 0 = default (4);\n"
       "                                  any value is capped at 8 (ignored on the serial\n"
       "                                  fallback path)\n"
       "    -h, --help                    Print help\n";
}

bool parseUint64(const std::string& s, uint64_t& out) {
  // Require a leading ASCII digit: std::stoull would otherwise silently accept a
  // leading '+'/'-' (wrapping negatives modulo 2^64) or leading whitespace,
  // whereas clap (the Rust CLI) rejects those for a u64 argument.
  if (s.empty() || s[0] < '0' || s[0] > '9') {
    return false;
  }
  try {
    size_t pos = 0;
    unsigned long long v = std::stoull(s, &pos);
    if (pos != s.size()) {
      return false;
    }
    out = static_cast<uint64_t>(v);
    return true;
  } catch (const std::exception&) {
    return false;
  }
}

bool parseUint32(const std::string& s, uint32_t& out) {
  uint64_t v = 0;
  if (!parseUint64(s, v) || v > std::numeric_limits<uint32_t>::max()) {
    return false;
  }
  out = static_cast<uint32_t>(v);
  return true;
}

std::optional<Options> parseArgs(int argc, char** argv) {
  Options o;
  bool haveInput = false;
  bool haveOutput = false;

  for (int i = 1; i < argc; ++i) {
    const std::string raw = argv[i];

    // Accept both "--flag value" and "--flag=value" (matching clap). Split a
    // long-form "--name=value" into the flag name and an inline value.
    std::string arg = raw;
    std::optional<std::string> inlineValue;
    if (raw.rfind("--", 0) == 0) {
      const auto eq = raw.find('=');
      if (eq != std::string::npos) {
        arg = raw.substr(0, eq);
        inlineValue = raw.substr(eq + 1);
      }
    }

    // Helper: this flag's value, preferring an inline "=value" over the next token.
    auto getValue = [&](const std::string& flag) -> std::optional<std::string> {
      if (inlineValue) {
        return inlineValue;
      }
      if (i + 1 >= argc) {
        std::cerr << "error: " << flag << " requires a value\n";
        return std::nullopt;
      }
      return std::string(argv[++i]);
    };

    if (arg == "-h" || arg == "--help") {
      printUsage(argv[0]);
      std::exit(0);
    } else if (arg == "-o" || arg == "--output" || arg == "--output-file") {
      auto v = getValue(arg);
      if (!v) {
        return std::nullopt;
      }
      o.outputPath = *v;
      haveOutput = true;
    } else if (arg == "--large-message-threshold") {
      auto v = getValue(arg);
      if (!v || !parseUint64(*v, o.largeMessageThreshold)) {
        std::cerr << "error: --large-message-threshold expects a non-negative integer\n";
        return std::nullopt;
      }
    } else if (arg == "--chunk-size") {
      auto v = getValue(arg);
      if (!v || !parseUint64(*v, o.chunkSize)) {
        std::cerr << "error: --chunk-size expects a non-negative integer\n";
        return std::nullopt;
      }
    } else if (arg == "--compression") {
      auto v = getValue(arg);
      if (!v) {
        return std::nullopt;
      }
      if (*v == "zstd") {
        o.compression = mcap::Compression::Zstd;
      } else if (*v == "lz4") {
        o.compression = mcap::Compression::Lz4;
      } else if (*v == "none") {
        o.compression = mcap::Compression::None;
      } else {
        std::cerr << "error: --compression must be one of: zstd, lz4, none\n";
        return std::nullopt;
      }
    } else if (arg == "--compression-level") {
      auto v = getValue(arg);
      if (!v || !parseUint32(*v, o.compressionLevel)) {
        std::cerr << "error: --compression-level expects a 32-bit non-negative integer\n";
        return std::nullopt;
      }
    } else if (arg == "--read-threads") {
      auto v = getValue(arg);
      if (!v || !parseUint32(*v, o.readThreads)) {
        std::cerr << "error: --read-threads expects a 32-bit non-negative integer\n";
        return std::nullopt;
      }
    } else if (arg == "--stats") {
      o.stats = true;
    } else if (arg == "--include-crc") {
      // Bare "--include-crc" means true; otherwise "--include-crc=<bool>".
      if (!inlineValue) {
        o.includeCrc = true;
      } else if (*inlineValue == "true") {
        o.includeCrc = true;
      } else if (*inlineValue == "false") {
        o.includeCrc = false;
      } else {
        std::cerr << "error: expected --include-crc or --include-crc=<true|false>\n";
        return std::nullopt;
      }
    } else if (!arg.empty() && arg[0] == '-') {
      std::cerr << "error: unknown flag '" << raw << "'\n";
      return std::nullopt;
    } else {
      if (haveInput) {
        std::cerr << "error: multiple input files specified ('" << o.inputPath << "' and '" << raw
                  << "')\n";
        return std::nullopt;
      }
      o.inputPath = raw;
      haveInput = true;
    }
  }

  if (!haveInput) {
    std::cerr << "error: missing required <input.mcap> argument\n";
    return std::nullopt;
  }
  if (!haveOutput) {
    std::cerr << "error: missing required --output <path> argument\n";
    return std::nullopt;
  }
  return o;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

uint64_t saturatingAdd(uint64_t a, uint64_t b) {
  return (a > std::numeric_limits<uint64_t>::max() - b) ? std::numeric_limits<uint64_t>::max()
                                                        : a + b;
}

// Map the numeric --compression-level onto the C++ CompressionLevel enum. The
// C++ writer only exposes five levels, so this is a best-effort mapping; 0 means
// "use the compressor default" (matching the Rust semantics).
mcap::CompressionLevel mapCompressionLevel(uint32_t level) {
  switch (level) {
    case 0:
    case 3:
      return mcap::CompressionLevel::Default;
    case 1:
      return mcap::CompressionLevel::Fastest;
    case 2:
      return mcap::CompressionLevel::Fast;
    case 4:
      return mcap::CompressionLevel::Slow;
    default:
      return mcap::CompressionLevel::Slowest;
  }
}

// Replicates the Rust ensure_distinct_input_output: the input must exist; if the
// output already exists it must not resolve to the same file.
bool ensureDistinctInputOutput(const Options& o) {
  std::error_code ec;
  const fs::path in = fs::canonical(o.inputPath, ec);
  if (ec) {
    std::cerr << "error: failed to canonicalize input '" << o.inputPath << "': " << ec.message()
              << "\n";
    return false;
  }
  if (!fs::exists(o.outputPath)) {
    return true;
  }
  const fs::path out = fs::canonical(o.outputPath, ec);
  if (ec) {
    std::cerr << "error: failed to canonicalize output '" << o.outputPath << "': " << ec.message()
              << "\n";
    return false;
  }
  if (in == out) {
    std::cerr << "error: input and output paths resolve to the same file\n";
    return false;
  }
  return true;
}

// ---------------------------------------------------------------------------
// Repackager: buffers messages into size-bounded chunks, sorts each chunk by
// channel before writing, and isolates large messages -- the "sorted" layout.
// ---------------------------------------------------------------------------

struct OwnedMessage {
  mcap::ChannelId channelId = 0;  // output (re-registered) channel id
  uint32_t sequence = 0;
  mcap::Timestamp logTime = 0;
  mcap::Timestamp publishTime = 0;
  std::vector<std::byte> data;
};

class Repackager {
public:
  Repackager(mcap::McapWriter& writer, const Options& opts)
      : writer_(writer)
      , opts_(opts) {}

  // Buffer one message into the current chunk, registering its schema/channel on
  // the output writer the first time that source channel is seen. A large message
  // is isolated into its own chunk; otherwise the buffered chunk is flushed
  // (sorted by channel, then closed) once it reaches the target chunk size.
  void push(const mcap::MessageView& mv) {
    const mcap::ChannelId channelId = ensureChannel(mv.channel, mv.schema);
    const uint64_t dataSize = mv.message.dataSize;

    if (dataSize >= opts_.largeMessageThreshold) {
      flushChunk();
      writer_.closeLastChunk();
      emitMessage(channelId, mv.message.sequence, mv.message.logTime, mv.message.publishTime,
                  mv.message.data, dataSize);
      writer_.closeLastChunk();
      return;
    }
    if (payloadSize_ > 0 && saturatingAdd(payloadSize_, dataSize) > opts_.chunkSize) {
      flushChunk();
    }
    OwnedMessage om;
    om.channelId = channelId;
    om.sequence = mv.message.sequence;
    om.logTime = mv.message.logTime;
    om.publishTime = mv.message.publishTime;
    om.data.assign(mv.message.data, mv.message.data + dataSize);
    buffer_.push_back(std::move(om));
    payloadSize_ = saturatingAdd(payloadSize_, dataSize);
  }

  // Flushes the final chunk. Returns the first write error (if any).
  mcap::Status finish() {
    flushChunk();
    return lastStatus_;
  }

  bool ok() const {
    return lastStatus_.ok();
  }

private:
  // Register a source channel (and its schema) on the output writer the first
  // time it is seen; returns the new output channel id. Fails fast if the channel
  // references a schema that is not present in the file.
  mcap::ChannelId ensureChannel(const mcap::ChannelPtr& channel, const mcap::SchemaPtr& schema) {
    auto it = channelMap_.find(channel->id);
    if (it != channelMap_.end()) {
      return it->second;
    }

    mcap::SchemaId newSchemaId = 0;
    if (channel->schemaId != 0) {
      if (!schema) {
        // Channel references a schema that is not present in the file. Fail fast
        // rather than silently writing a schema-less channel (Rust aborts with
        // UnknownSchema here).
        if (lastStatus_.ok()) {
          lastStatus_ = mcap::Status(mcap::StatusCode::InvalidSchemaId,
                                     "channel '" + channel->topic + "' references missing schema " +
                                       std::to_string(channel->schemaId));
          std::cerr << "error: " << lastStatus_.message << "\n";
        }
      } else {
        auto sit = schemaMap_.find(channel->schemaId);
        if (sit != schemaMap_.end()) {
          newSchemaId = sit->second;
        } else {
          mcap::Schema outSchema(schema->name, schema->encoding, schema->data);
          writer_.addSchema(outSchema);
          newSchemaId = outSchema.id;
          schemaMap_.emplace(channel->schemaId, newSchemaId);
        }
      }
    }

    mcap::Channel outChannel(channel->topic, channel->messageEncoding, newSchemaId,
                             channel->metadata);
    writer_.addChannel(outChannel);
    channelMap_.emplace(channel->id, outChannel.id);
    return outChannel.id;
  }

  void emitMessage(mcap::ChannelId channelId, uint32_t sequence, mcap::Timestamp logTime,
                   mcap::Timestamp publishTime, const std::byte* data, uint64_t dataSize) {
    mcap::Message msg;
    msg.channelId = channelId;
    msg.sequence = sequence;
    msg.logTime = logTime;
    msg.publishTime = publishTime;
    msg.dataSize = dataSize;
    msg.data = data;
    const mcap::Status st = writer_.write(msg);
    if (!st.ok() && lastStatus_.ok()) {
      lastStatus_ = st;
      std::cerr << "error: failed to write message: " << st.message << "\n";
    }
  }

  // Sort the buffered chunk by (channel, log time, sequence) and write it as one
  // chunk, then close it. Messages were buffered in arrival (log-time) order, so
  // each chunk spans a contiguous time slice (chunks are time-disjoint); the
  // within-chunk channel grouping improves compression and selective reads
  // without making chunks overlap in time. Large messages are isolated by push().
  void flushChunk() {
    if (buffer_.empty()) {
      return;
    }
    std::stable_sort(buffer_.begin(), buffer_.end(),
                     [](const OwnedMessage& a, const OwnedMessage& b) {
                       if (a.channelId != b.channelId) {
                         return a.channelId < b.channelId;
                       }
                       if (a.logTime != b.logTime) {
                         return a.logTime < b.logTime;
                       }
                       return a.sequence < b.sequence;
                     });
    for (const OwnedMessage& m : buffer_) {
      if (!lastStatus_.ok()) {
        break;
      }
      emitMessage(m.channelId, m.sequence, m.logTime, m.publishTime, m.data.data(), m.data.size());
    }
    buffer_.clear();
    payloadSize_ = 0;
    writer_.closeLastChunk();
  }

  mcap::McapWriter& writer_;
  const Options& opts_;
  std::unordered_map<mcap::SchemaId, mcap::SchemaId> schemaMap_;
  std::unordered_map<mcap::ChannelId, mcap::ChannelId> channelMap_;
  std::vector<OwnedMessage> buffer_;  // messages buffered for the current chunk
  uint64_t payloadSize_ = 0;          // running size of the buffered chunk
  mcap::Status lastStatus_;
};

// ---------------------------------------------------------------------------
// Metadata + attachment copy (after messages, before close).
// ---------------------------------------------------------------------------

// Copy records identified by their summary indexes (metadata or attachments)
// through to the writer, in original file order. The metadata and attachment
// paths differ only in record type, parse function, and the noun in error
// messages, so they share this one implementation.
template <typename IndexMap, typename Record>
bool copyRecordsFromIndexes(mcap::McapReader& reader, mcap::McapWriter& writer, const char* noun,
                            const IndexMap& indexMap,
                            mcap::Status (*parse)(const mcap::Record&, Record*)) {
  using Index = typename IndexMap::mapped_type;
  std::vector<Index> indexes;
  indexes.reserve(indexMap.size());
  for (const auto& kv : indexMap) {
    indexes.push_back(kv.second);
  }
  std::sort(indexes.begin(), indexes.end(), [](const Index& a, const Index& b) {
    return a.offset < b.offset;
  });
  for (const auto& index : indexes) {
    mcap::Record record;
    mcap::Status st = mcap::McapReader::ReadRecord(*reader.dataSource(), index.offset, &record);
    if (!st.ok()) {
      std::cerr << "error: failed to read " << noun << " at offset " << index.offset << ": "
                << st.message << "\n";
      return false;
    }
    Record parsed;
    st = parse(record, &parsed);
    if (!st.ok()) {
      std::cerr << "error: failed to parse " << noun << " '" << index.name << "': " << st.message
                << "\n";
      return false;
    }
    st = writer.write(parsed);
    if (!st.ok()) {
      std::cerr << "error: failed to write " << noun << " '" << parsed.name << "': " << st.message
                << "\n";
      return false;
    }
  }
  return true;
}

bool copyMetadataFromIndexes(mcap::McapReader& reader, mcap::McapWriter& writer) {
  return copyRecordsFromIndexes(reader, writer, "metadata", reader.metadataIndexes(),
                                mcap::McapReader::ParseMetadata);
}

bool copyAttachmentsFromIndexes(mcap::McapReader& reader, mcap::McapWriter& writer) {
  return copyRecordsFromIndexes(reader, writer, "attachment", reader.attachmentIndexes(),
                                mcap::McapReader::ParseAttachment);
}

bool indexesComplete(size_t indexCount, bool haveStats, uint32_t statCount) {
  if (haveStats) {
    return indexCount == static_cast<size_t>(statCount);
  }
  return indexCount > 0;
}

// Copies all metadata then all attachments exactly once. Uses the (already
// parsed) summary indexes when they are provably complete; otherwise re-scans
// the file to rebuild complete indexes for the missing kind(s).
bool copyMetadataAndAttachments(mcap::McapReader& reader, const std::string& inputPath,
                                mcap::McapWriter& writer) {
  const bool haveStats = reader.statistics().has_value();
  uint32_t metaCount = 0;
  uint32_t attCount = 0;
  if (haveStats) {
    metaCount = reader.statistics()->metadataCount;
    attCount = reader.statistics()->attachmentCount;
  }
  const bool metaComplete = indexesComplete(reader.metadataIndexes().size(), haveStats, metaCount);
  const bool attComplete = indexesComplete(reader.attachmentIndexes().size(), haveStats, attCount);

  if (metaComplete && !copyMetadataFromIndexes(reader, writer)) {
    return false;
  }
  if (attComplete && !copyAttachmentsFromIndexes(reader, writer)) {
    return false;
  }

  if (!metaComplete || !attComplete) {
    // Rebuild complete indexes via a forced scan, then copy only what is missing.
    mcap::McapReader scanReader;
    mcap::Status st = scanReader.open(inputPath);
    if (!st.ok()) {
      std::cerr << "error: failed to reopen '" << inputPath
                << "' for metadata/attachment scan: " << st.message << "\n";
      return false;
    }
    st = scanReader.readSummary(mcap::ReadSummaryMethod::ForceScan);
    if (!st.ok()) {
      std::cerr << "error: failed to scan '" << inputPath
                << "' for metadata/attachments: " << st.message << "\n";
      return false;
    }
    if (!metaComplete && !copyMetadataFromIndexes(scanReader, writer)) {
      return false;
    }
    if (!attComplete && !copyAttachmentsFromIndexes(scanReader, writer)) {
      return false;
    }
  }
  return true;
}

}  // namespace

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

int main(int argc, char** argv) {
  const auto parsed = parseArgs(argc, argv);
  if (!parsed) {
    return 1;
  }
  const Options& opts = *parsed;

  if (!ensureDistinctInputOutput(opts)) {
    return 1;
  }

  // Open the input via the ParallelReader (builds an MmapReader + parses the
  // summary). The composed McapReader is reused for the serial fallback and for
  // copying metadata/attachments.
  mcap::ParallelReader reader;
  mcap::Status st = reader.open(opts.inputPath);
  if (!st.ok()) {
    std::cerr << "error: failed to open input '" << opts.inputPath << "': " << st.message << "\n";
    return 1;
  }

  // Configure the writer. The repackager controls EVERY chunk boundary explicitly
  // via closeLastChunk() (the chunk-size flush and large-message isolation).
  // The C++ writer has no flag to disable its own size-based auto-flush, and its
  // chunk buffer is reserved to writeOpts.chunkSize, so we set a ceiling that is
  // (a) at least as large as any boundary we request (so the writer does not
  // auto-cut before our explicit closeLastChunk()) and (b) bounded, so reserve()
  // neither throws (as UINT64_MAX would) nor reserves an unreasonable amount. For
  // a --chunk-size / --large-message-threshold above the cap, or pathologically
  // tiny messages, the writer may insert an extra boundary at the ceiling -- a
  // benign chunk-layout difference, never data loss. Compression/CRC come from
  // the CLI; profile/library are copied from the input header.
  std::string profile;
  std::optional<std::string> library;
  if (reader.reader().header().has_value()) {
    profile = reader.reader().header()->profile;
    library = reader.reader().header()->library;
  }
  mcap::McapWriterOptions writeOpts(profile);
  if (library.has_value()) {
    writeOpts.library = *library;
  }
  writeOpts.noChunking = false;
  constexpr uint64_t kBaseCeiling = 256ull * 1024 * 1024;      // 256 MiB
  constexpr uint64_t kMaxCeiling = 2ull * 1024 * 1024 * 1024;  // 2 GiB reserve cap
  uint64_t ceiling = std::max({kBaseCeiling, opts.chunkSize, opts.largeMessageThreshold});
  ceiling = std::min(ceiling, kMaxCeiling);
  writeOpts.chunkSize = ceiling;
  writeOpts.compression = opts.compression;
  writeOpts.compressionLevel = mapCompressionLevel(opts.compressionLevel);
  writeOpts.noChunkCRC = !opts.includeCrc;
  writeOpts.enableDataCRC = opts.includeCrc;  // note: opt-in polarity
  writeOpts.noSummaryCRC = !opts.includeCrc;
  writeOpts.noAttachmentCRC = !opts.includeCrc;

  // Open the output through our own stream so I/O failures (e.g. disk full) are
  // observable via the stream state -- the McapWriter's FILE* sink swallows them.
  std::ofstream output(opts.outputPath, std::ios::binary | std::ios::trunc);
  if (!output.is_open()) {
    std::cerr << "error: failed to open output '" << opts.outputPath << "'\n";
    return 1;
  }
  mcap::McapWriter writer;
  try {
    writer.open(output, writeOpts);  // void overload; may reserve the chunk buffer
  } catch (const std::exception& e) {
    std::cerr << "error: failed to initialize writer: " << e.what() << "\n";
    return 1;
  }

  // Any read problem (truncated/corrupt record, decompression failure, message
  // referencing a missing channel, ...) is fatal: the serial LinearMessageView
  // surfaces such errors only through this callback, so we record them and abort
  // rather than emit a silently-truncated output (matching Rust's `message?`).
  bool readProblem = false;
  const auto onProblem = [&readProblem](const mcap::Status& s) {
    std::cerr << "error: " << s.message << "\n";
    readProblem = true;
  };

  Repackager repackager(writer, opts);

  // Read messages: parallel (log-time order) when the input has the required
  // chunk + message indexes; otherwise a serial read. NOTE: a chunked file that
  // has chunk indexes but no message indexes cannot be read in log-time order by
  // this library (unlike the Rust reader, which re-derives order by decoding
  // chunks), so it falls back to FileOrder and therefore assumes the input is
  // approximately log-time ordered -- as does Rust's own linear path.
  const auto& chunkIndexes = reader.chunkIndexes();
  const bool indexed =
    !chunkIndexes.empty() &&
    std::any_of(chunkIndexes.begin(), chunkIndexes.end(), [](const mcap::ChunkIndex& c) {
      return c.messageIndexLength != 0;
    });

  bool readDone = false;
  if (indexed) {
    mcap::ParallelReadOptions readOpts;
    readOpts.read.readOrder = mcap::ReadMessageOptions::ReadOrder::LogTimeOrder;
    readOpts.threadCount = opts.readThreads;  // 0 -> default (4, capped at 8)
    // Opt into the chunk-count cap (the library default is the precise byte budget);
    // it is neutral-to-faster for the repackager's full read of the input.
    readOpts.memoryCap = mcap::MemoryCapMode::ChunkCount;
    mcap::ParallelMessageView view = reader.readMessages(onProblem, readOpts);
    if (view.status().ok()) {
      for (const auto& mv : view) {
        repackager.push(mv);
        if (!repackager.ok()) {
          writer.terminate();
          return 1;
        }
      }
      if (!view.status().ok()) {
        std::cerr << "error: parallel read failed: " << view.status().message << "\n";
        writer.terminate();
        return 1;
      }
      if (opts.stats) {
        const auto& rs = view.stats();
        const uint64_t scheduled = rs.chunksScheduled.load();
        const uint64_t decompressed = rs.chunksDecompressed.load();
        const uint64_t forced = rs.chunksForced.load();
        const double peakMB = static_cast<double>(rs.peakDecompressedBytes.load()) / 1e6;
        const size_t total = chunkIndexes.size();
        const bool onceEach = (decompressed == scheduled) && (decompressed <= total);
        std::cerr << "[stats] read path: parallel\n"
                  << "[stats]   chunks: total=" << total << " scheduled=" << scheduled
                  << " decompressed=" << decompressed << " forced=" << forced << "\n"
                  << "[stats]   each chunk decompressed at most once: "
                  << (onceEach ? "yes" : "NO (re-decompression detected!)") << "\n"
                  << "[stats]   peak decompressed resident: " << peakMB << " MB\n";
      }
      readDone = true;
    }
  }
  if (!readDone) {
    mcap::ReadMessageOptions readOpts;
    readOpts.readOrder = indexed ? mcap::ReadMessageOptions::ReadOrder::LogTimeOrder
                                 : mcap::ReadMessageOptions::ReadOrder::FileOrder;
    for (const auto& mv : reader.reader().readMessages(onProblem, readOpts)) {
      repackager.push(mv);
      if (!repackager.ok()) {
        writer.terminate();
        return 1;
      }
    }
  }

  if (opts.stats && !readDone) {
    std::cerr << "[stats] read path: serial (no parallel decompression stats available)\n";
  }

  if (readProblem) {
    std::cerr << "error: aborting due to read error(s); output not written\n";
    writer.terminate();
    return 1;
  }

  st = repackager.finish();
  if (!st.ok()) {
    std::cerr << "error: " << st.message << "\n";
    writer.terminate();
    return 1;
  }

  if (!copyMetadataAndAttachments(reader.reader(), opts.inputPath, writer)) {
    writer.terminate();
    return 1;
  }

  writer.close();
  output.flush();
  if (!output) {
    std::cerr << "error: failed to write output '" << opts.outputPath << "' (I/O error)\n";
    return 1;
  }
  std::cout << "repackaged '" << opts.inputPath << "' -> '" << opts.outputPath << "'\n";
  return 0;
}
