// mcap-repackage: re-chunk an MCAP file by grouping messages per fixed log-time
// window and per topic, using the multithreaded mcap::ParallelReader for reads.
//
// This is a C++ port of the Rust `mcap repackage` CLI command. It preserves the
// same conversion: within each fixed log-time window, messages are sorted by
// topic, each topic run is packed into its own chunk(s) split at a target chunk
// size, and large messages are isolated into single-message chunks; metadata and
// attachments are copied through unchanged. The read path
// uses the ParallelReader (log-time order) when the input has the required chunk
// + message indexes, and falls back to a serial read otherwise.
//
// Behavioral reference: rust/cli/src/commands/repackage.rs on branch
// feature/repackage-cli-stacked.

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

constexpr uint64_t kNanosPerSecond = 1000000000ULL;

// ---------------------------------------------------------------------------
// Options + CLI parsing
// ---------------------------------------------------------------------------

struct Options {
  std::string inputPath;
  std::string outputPath;
  uint64_t windowDurationSecs = 1;
  uint64_t largeMessageThreshold = 256 * 1024;
  uint64_t chunkSize = 4 * 1024 * 1024;
  mcap::Compression compression = mcap::Compression::Zstd;
  uint32_t compressionLevel = 0;
  bool includeCrc = true;

  uint64_t windowNs() const {
    return windowDurationSecs * kNanosPerSecond;
  }
};

void printUsage(const char* argv0) {
  std::cerr
    << "mcap-repackage - re-chunk an MCAP file (per log-time window, grouped by\n"
       "                 topic) using the multithreaded ParallelReader.\n\n"
       "USAGE:\n"
       "    "
    << argv0
    << " <input.mcap> --output <output.mcap> [OPTIONS]\n\n"
       "ARGS:\n"
       "    <input.mcap>                  Local path to the source MCAP file\n\n"
       "OPTIONS:\n"
       "    -o, --output <path>           Destination repackaged MCAP file\n"
       "                                  (alias: --output-file)\n"
       "    --window-duration-secs <n>    Log-time window in seconds (default: 1)\n"
       "    --large-message-threshold <n> Messages >= this data size are written as\n"
       "                                  single-message chunks (default: 262144)\n"
       "    --chunk-size <n>              Target uncompressed chunk size in bytes for\n"
       "                                  small-message groups (default: 4194304)\n"
       "    --compression <c>             Output chunk compression: zstd, lz4, or none\n"
       "                                  (default: zstd)\n"
       "    --compression-level <n>       0 = compressor default. Mapped to the C++\n"
       "                                  CompressionLevel enum: 0/3=Default, 1=Fastest,\n"
       "                                  2=Fast, 4=Slow, >=5=Slowest (default: 0)\n"
       "    --include-crc[=<bool>]        Include CRCs in the output MCAP (default: true)\n"
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
    } else if (arg == "--window-duration-secs") {
      auto v = getValue(arg);
      if (!v || !parseUint64(*v, o.windowDurationSecs)) {
        std::cerr << "error: --window-duration-secs expects a non-negative integer\n";
        return std::nullopt;
      }
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
        std::cerr << "error: multiple input files specified ('" << o.inputPath
                  << "' and '" << raw << "')\n";
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
  if (o.windowDurationSecs == 0) {
    std::cerr << "error: --window-duration-secs must be greater than zero\n";
    return std::nullopt;
  }
  if (o.windowDurationSecs > std::numeric_limits<uint64_t>::max() / kNanosPerSecond) {
    std::cerr << "error: window duration overflows nanoseconds\n";
    return std::nullopt;
  }
  return o;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

uint64_t saturatingAdd(uint64_t a, uint64_t b) {
  return (a > std::numeric_limits<uint64_t>::max() - b)
           ? std::numeric_limits<uint64_t>::max()
           : a + b;
}

// Map the numeric --compression-level onto the C++ CompressionLevel enum. The
// C++ writer only exposes five levels, so this is a best-effort mapping; 0 means
// "use the compressor default" (matching the Rust semantics).
mcap::CompressionLevel mapCompressionLevel(uint32_t level) {
  switch (level) {
    case 0:
      return mcap::CompressionLevel::Default;
    case 1:
      return mcap::CompressionLevel::Fastest;
    case 2:
      return mcap::CompressionLevel::Fast;
    case 3:
      return mcap::CompressionLevel::Default;
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
    std::cerr << "error: failed to canonicalize input '" << o.inputPath
              << "': " << ec.message() << "\n";
    return false;
  }
  if (!fs::exists(o.outputPath)) {
    return true;
  }
  const fs::path out = fs::canonical(o.outputPath, ec);
  if (ec) {
    std::cerr << "error: failed to canonicalize output '" << o.outputPath
              << "': " << ec.message() << "\n";
    return false;
  }
  if (in == out) {
    std::cerr << "error: input and output paths resolve to the same file\n";
    return false;
  }
  return true;
}

// ---------------------------------------------------------------------------
// Repackager: windows messages, sorts by topic within a window, and writes them
// with tool-controlled chunk boundaries.
// ---------------------------------------------------------------------------

struct ChannelInfo {
  mcap::ChannelId newId = 0;
  std::string topic;
};

struct OwnedMessage {
  mcap::ChannelId srcChannelId = 0;
  const std::string* topic = nullptr;  // points into stable ChannelInfo storage
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

  // Copies one message into the current window (registering its schema/channel
  // on the output writer the first time that source channel is seen).
  void push(const mcap::MessageView& mv) {
    const ChannelInfo& info = ensureChannel(mv.channel, mv.schema);

    const uint64_t window =
      (mv.message.logTime / opts_.windowNs()) * opts_.windowNs();
    if (currentWindow_.has_value()) {
      if (*currentWindow_ != window) {
        flushWindow();
        currentWindow_ = window;
      }
    } else {
      currentWindow_ = window;
    }

    OwnedMessage om;
    om.srcChannelId = mv.channel->id;
    om.topic = &info.topic;
    om.sequence = mv.message.sequence;
    om.logTime = mv.message.logTime;
    om.publishTime = mv.message.publishTime;
    om.data.assign(mv.message.data, mv.message.data + mv.message.dataSize);
    windowMessages_.push_back(std::move(om));
  }

  // Flushes the final window. Returns the first write error encountered (if any).
  mcap::Status finish() {
    flushWindow();
    return lastStatus_;
  }

  bool ok() const {
    return lastStatus_.ok();
  }

private:
  const ChannelInfo& ensureChannel(const mcap::ChannelPtr& channel,
                                   const mcap::SchemaPtr& schema) {
    auto it = channelInfo_.find(channel->id);
    if (it != channelInfo_.end()) {
      return it->second;
    }

    mcap::SchemaId newSchemaId = 0;
    if (channel->schemaId != 0) {
      if (!schema) {
        // Channel references a schema that is not present in the file. Fail fast
        // rather than silently writing a schema-less channel (Rust aborts with
        // UnknownSchema here).
        if (lastStatus_.ok()) {
          lastStatus_ = mcap::Status(
            mcap::StatusCode::InvalidSchemaId,
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

    ChannelInfo info;
    info.newId = outChannel.id;
    info.topic = channel->topic;
    auto res = channelInfo_.emplace(channel->id, std::move(info));
    return res.first->second;
  }

  void writeMessage(const OwnedMessage& m) {
    const auto it = channelInfo_.find(m.srcChannelId);
    mcap::Message msg;
    msg.channelId = it->second.newId;
    msg.sequence = m.sequence;
    msg.logTime = m.logTime;
    msg.publishTime = m.publishTime;
    msg.dataSize = m.data.size();
    msg.data = m.data.data();
    const mcap::Status st = writer_.write(msg);
    if (!st.ok() && lastStatus_.ok()) {
      lastStatus_ = st;
      std::cerr << "error: failed to write message: " << st.message << "\n";
    }
  }

  void flushWindow() {
    if (windowMessages_.empty()) {
      return;
    }

    std::vector<OwnedMessage> msgs = std::move(windowMessages_);
    windowMessages_.clear();

    // Sort by (topic, channel id, log time, sequence, publish time). stable_sort
    // mirrors Rust's stable Vec::sort_by for fully-equal keys.
    std::stable_sort(msgs.begin(), msgs.end(),
                     [](const OwnedMessage& a, const OwnedMessage& b) {
                       if (*a.topic != *b.topic) {
                         return *a.topic < *b.topic;
                       }
                       if (a.srcChannelId != b.srcChannelId) {
                         return a.srcChannelId < b.srcChannelId;
                       }
                       if (a.logTime != b.logTime) {
                         return a.logTime < b.logTime;
                       }
                       if (a.sequence != b.sequence) {
                         return a.sequence < b.sequence;
                       }
                       return a.publishTime < b.publishTime;
                     });

    std::optional<mcap::ChannelId> currentChannel;
    uint64_t payloadSize = 0;
    for (const OwnedMessage& m : msgs) {
      if (!lastStatus_.ok()) {
        return;
      }

      // (1) Channel change forces a fresh chunk so each topic run is isolated.
      const bool channelChanged =
        !currentChannel.has_value() || *currentChannel != m.srcChannelId;
      if (channelChanged) {
        writer_.closeLastChunk();
        currentChannel = m.srcChannelId;
        payloadSize = 0;
      }

      const uint64_t dataSize = m.data.size();

      // (2) Large messages get their own single-message chunk.
      if (dataSize >= opts_.largeMessageThreshold) {
        writer_.closeLastChunk();
        writeMessage(m);
        writer_.closeLastChunk();
        payloadSize = 0;
        continue;
      }

      // (3) Split the chunk before it would exceed the target chunk size. Only
      //     when payload > 0, so the first message of a chunk is never pre-split.
      if (payloadSize > 0 && saturatingAdd(payloadSize, dataSize) > opts_.chunkSize) {
        writer_.closeLastChunk();
        payloadSize = 0;
      }

      writeMessage(m);
      payloadSize = saturatingAdd(payloadSize, dataSize);
    }

    // Final boundary for this window.
    writer_.closeLastChunk();
  }

  mcap::McapWriter& writer_;
  const Options& opts_;
  std::unordered_map<mcap::SchemaId, mcap::SchemaId> schemaMap_;
  std::unordered_map<mcap::ChannelId, ChannelInfo> channelInfo_;
  std::optional<uint64_t> currentWindow_;
  std::vector<OwnedMessage> windowMessages_;
  mcap::Status lastStatus_;
};

// ---------------------------------------------------------------------------
// Metadata + attachment copy (after messages, before close).
// ---------------------------------------------------------------------------

bool copyMetadataFromIndexes(mcap::McapReader& reader, mcap::McapWriter& writer) {
  std::vector<mcap::MetadataIndex> indexes;
  for (const auto& kv : reader.metadataIndexes()) {
    indexes.push_back(kv.second);
  }
  std::sort(indexes.begin(), indexes.end(),
            [](const mcap::MetadataIndex& a, const mcap::MetadataIndex& b) {
              return a.offset < b.offset;
            });
  for (const auto& index : indexes) {
    mcap::Record record;
    mcap::Status st = mcap::McapReader::ReadRecord(*reader.dataSource(), index.offset, &record);
    if (!st.ok()) {
      std::cerr << "error: failed to read metadata at offset " << index.offset
                << ": " << st.message << "\n";
      return false;
    }
    mcap::Metadata metadata;
    st = mcap::McapReader::ParseMetadata(record, &metadata);
    if (!st.ok()) {
      std::cerr << "error: failed to parse metadata '" << index.name
                << "': " << st.message << "\n";
      return false;
    }
    st = writer.write(metadata);
    if (!st.ok()) {
      std::cerr << "error: failed to write metadata '" << metadata.name
                << "': " << st.message << "\n";
      return false;
    }
  }
  return true;
}

bool copyAttachmentsFromIndexes(mcap::McapReader& reader, mcap::McapWriter& writer) {
  std::vector<mcap::AttachmentIndex> indexes;
  for (const auto& kv : reader.attachmentIndexes()) {
    indexes.push_back(kv.second);
  }
  std::sort(indexes.begin(), indexes.end(),
            [](const mcap::AttachmentIndex& a, const mcap::AttachmentIndex& b) {
              return a.offset < b.offset;
            });
  for (const auto& index : indexes) {
    mcap::Record record;
    mcap::Status st = mcap::McapReader::ReadRecord(*reader.dataSource(), index.offset, &record);
    if (!st.ok()) {
      std::cerr << "error: failed to read attachment at offset " << index.offset
                << ": " << st.message << "\n";
      return false;
    }
    mcap::Attachment attachment;
    st = mcap::McapReader::ParseAttachment(record, &attachment);
    if (!st.ok()) {
      std::cerr << "error: failed to parse attachment '" << index.name
                << "': " << st.message << "\n";
      return false;
    }
    st = writer.write(attachment);
    if (!st.ok()) {
      std::cerr << "error: failed to write attachment '" << attachment.name
                << "': " << st.message << "\n";
      return false;
    }
  }
  return true;
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
  const bool metaComplete =
    indexesComplete(reader.metadataIndexes().size(), haveStats, metaCount);
  const bool attComplete =
    indexesComplete(reader.attachmentIndexes().size(), haveStats, attCount);

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
    std::cerr << "error: failed to open input '" << opts.inputPath << "': "
              << st.message << "\n";
    return 1;
  }

  // Configure the writer. The repackager controls EVERY chunk boundary
  // explicitly via closeLastChunk() (window, topic-run, large-message, and the
  // chunk-size split), matching the Rust chunk_size(None) + manual-flush design.
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
  constexpr uint64_t kBaseCeiling = 256ull * 1024 * 1024;     // 256 MiB
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
    std::any_of(chunkIndexes.begin(), chunkIndexes.end(),
                [](const mcap::ChunkIndex& c) { return c.messageIndexLength != 0; });

  bool readDone = false;
  if (indexed) {
    mcap::ParallelReadOptions readOpts;
    readOpts.read.readOrder = mcap::ReadMessageOptions::ReadOrder::LogTimeOrder;
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
    std::cerr << "error: failed to write output '" << opts.outputPath
              << "' (I/O error)\n";
    return 1;
  }
  std::cout << "repackaged '" << opts.inputPath << "' -> '" << opts.outputPath
            << "'\n";
  return 0;
}
