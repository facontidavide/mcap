use std::borrow::Cow;
use std::cmp::Ordering;
use std::io::{Seek, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context, Result};

use crate::cli::{CompressionFormat, RepackageCommand};
use crate::commands::common;
use crate::context::CommandContext;

#[derive(Debug, Clone)]
struct RepackageOptions {
    compression: Option<mcap::Compression>,
    compression_level: u32,
    window_duration_ns: u64,
    large_message_threshold: u64,
    chunk_size: u64,
    include_crc: bool,
}

#[derive(Debug)]
struct OwnedMessage {
    channel: Arc<mcap::Channel<'static>>,
    sequence: u32,
    log_time: u64,
    publish_time: u64,
    data: Vec<u8>,
}

struct WindowRepackager<'a, W: Write + Seek> {
    writer: &'a mut mcap::Writer<W>,
    opts: &'a RepackageOptions,
    current_window: Option<u64>,
    window_messages: Vec<OwnedMessage>,
}

pub fn run(_ctx: &CommandContext, args: RepackageCommand) -> Result<()> {
    ensure_distinct_input_output(&args.file, &args.output_file)?;
    let opts = build_repackage_options(&args)?;
    let input = common::map_file(&args.file)?;
    let output = std::fs::File::create(&args.output_file)
        .with_context(|| format!("failed to open output '{}'", args.output_file.display()))?;
    repackage_to_writer(input.as_ref(), output, &opts)
}

fn ensure_distinct_input_output(input: &Path, output: &Path) -> Result<()> {
    let input_path = std::fs::canonicalize(input)
        .with_context(|| format!("failed to canonicalize input '{}'", input.display()))?;

    if !output.exists() {
        return Ok(());
    }

    let output_path = std::fs::canonicalize(output)
        .with_context(|| format!("failed to canonicalize output '{}'", output.display()))?;

    if input_path == output_path {
        bail!("input and output paths resolve to the same file");
    }

    Ok(())
}

fn build_repackage_options(args: &RepackageCommand) -> Result<RepackageOptions> {
    let window_duration_ns = args
        .window_duration_secs
        .checked_mul(1_000_000_000)
        .context("window duration overflows nanoseconds")?;
    if window_duration_ns == 0 {
        bail!("--window-duration-secs must be greater than zero");
    }

    Ok(RepackageOptions {
        compression: convert_compression(args.compression),
        compression_level: args.compression_level,
        window_duration_ns,
        large_message_threshold: args.large_message_threshold,
        chunk_size: args.chunk_size,
        include_crc: args.include_crc,
    })
}

fn convert_compression(value: CompressionFormat) -> Option<mcap::Compression> {
    match value {
        CompressionFormat::Zstd => Some(mcap::Compression::Zstd),
        CompressionFormat::Lz4 => Some(mcap::Compression::Lz4),
        CompressionFormat::None => None,
    }
}

fn repackage_to_writer<W: Write + Seek>(
    input: &[u8],
    sink: W,
    opts: &RepackageOptions,
) -> Result<()> {
    let header = common::read_header(input)?;
    let summary = mcap::Summary::read(input).context("failed to read file index")?;

    let mut write_options = mcap::WriteOptions::new()
        .use_chunks(true)
        .chunk_size(None)
        .compression(opts.compression)
        .compression_level(opts.compression_level)
        .compression_threads(0)
        .calculate_chunk_crcs(opts.include_crc)
        .calculate_data_section_crc(opts.include_crc)
        .calculate_summary_section_crc(opts.include_crc)
        .calculate_attachment_crcs(opts.include_crc);

    if let Some(header) = header {
        write_options = write_options
            .profile(header.profile)
            .library(header.library);
    }

    let mut writer = write_options
        .create(sink)
        .context("failed to create mcap writer")?;

    if let Some(summary) = summary
        .as_ref()
        .filter(|summary| !summary.chunk_indexes.is_empty())
    {
        repackage_indexed(input, summary, &mut writer, opts)?;
    } else {
        repackage_linear(input, &mut writer, opts)?;
    }

    copy_metadata_and_attachments(input, summary.as_ref(), &mut writer)?;
    writer.finish().context("failed to finish mcap writer")?;
    Ok(())
}

fn repackage_indexed<W: Write + Seek>(
    input: &[u8],
    summary: &mcap::Summary,
    writer: &mut mcap::Writer<W>,
    opts: &RepackageOptions,
) -> Result<()> {
    let mut indexed_reader = mcap::sans_io::IndexedReader::new_with_options(
        summary,
        mcap::sans_io::IndexedReaderOptions::new()
            .with_order(mcap::sans_io::indexed_reader::ReadOrder::LogTime),
    )?;
    let mut repackager = WindowRepackager::new(writer, opts);

    while let Some(event) = indexed_reader.next_event() {
        match event? {
            mcap::sans_io::IndexedReadEvent::ReadChunkRequest { offset, length } => {
                let chunk_data = checked_slice(input, offset, length)?;
                indexed_reader.insert_chunk_record_data(offset, chunk_data)?;
            }
            mcap::sans_io::IndexedReadEvent::Message { header, data } => {
                let channel = summary.channels.get(&header.channel_id).ok_or_else(|| {
                    anyhow::anyhow!("message references unknown channel {}", header.channel_id)
                })?;
                repackager.push(OwnedMessage {
                    channel: channel.clone(),
                    sequence: header.sequence,
                    log_time: header.log_time,
                    publish_time: header.publish_time,
                    data: data.to_vec(),
                })?;
            }
        }
    }

    repackager.finish()
}

fn repackage_linear<W: Write + Seek>(
    input: &[u8],
    writer: &mut mcap::Writer<W>,
    opts: &RepackageOptions,
) -> Result<()> {
    let mut repackager = WindowRepackager::new(writer, opts);
    for message in mcap::MessageStream::new(input)? {
        let message = message?;
        repackager.push(OwnedMessage {
            channel: message.channel,
            sequence: message.sequence,
            log_time: message.log_time,
            publish_time: message.publish_time,
            data: message.data.into_owned(),
        })?;
    }
    repackager.finish()
}

fn checked_slice(input: &[u8], offset: u64, length: usize) -> Result<&[u8]> {
    let start = usize::try_from(offset)
        .with_context(|| format!("chunk offset out of range for this platform: {offset}"))?;
    let end = start
        .checked_add(length)
        .ok_or_else(|| anyhow::anyhow!("chunk read overflow at offset {offset}"))?;
    input.get(start..end).ok_or_else(|| {
        anyhow::anyhow!("chunk read out of bounds at offset {offset} length {length}")
    })
}

impl<'a, W: Write + Seek> WindowRepackager<'a, W> {
    fn new(writer: &'a mut mcap::Writer<W>, opts: &'a RepackageOptions) -> Self {
        Self {
            writer,
            opts,
            current_window: None,
            window_messages: Vec::new(),
        }
    }

    fn push(&mut self, message: OwnedMessage) -> Result<()> {
        let window = window_start(message.log_time, self.opts.window_duration_ns);
        match self.current_window {
            Some(current) if current != window => {
                self.flush_window()?;
                self.current_window = Some(window);
            }
            None => self.current_window = Some(window),
            Some(_) => {}
        }
        self.window_messages.push(message);
        Ok(())
    }

    fn finish(mut self) -> Result<()> {
        self.flush_window()
    }

    fn flush_window(&mut self) -> Result<()> {
        if self.window_messages.is_empty() {
            return Ok(());
        }

        let mut messages = std::mem::take(&mut self.window_messages);
        messages.sort_by(compare_repackaged_messages);

        let mut current_channel_id = None;
        let mut current_payload_size = 0_u64;
        for message in messages {
            let channel_changed = current_channel_id != Some(message.channel.id);
            if channel_changed {
                self.writer.flush()?;
                current_channel_id = Some(message.channel.id);
                current_payload_size = 0;
            }

            let data_size = message.data.len() as u64;
            if data_size >= self.opts.large_message_threshold {
                self.writer.flush()?;
                write_owned_message(self.writer, &message)?;
                self.writer.flush()?;
                current_payload_size = 0;
                continue;
            }

            if current_payload_size > 0
                && current_payload_size.saturating_add(data_size) > self.opts.chunk_size
            {
                self.writer.flush()?;
                current_payload_size = 0;
            }

            write_owned_message(self.writer, &message)?;
            current_payload_size = current_payload_size.saturating_add(data_size);
        }

        self.writer.flush()?;
        Ok(())
    }
}

fn window_start(log_time: u64, window_duration_ns: u64) -> u64 {
    (log_time / window_duration_ns) * window_duration_ns
}

fn compare_repackaged_messages(lhs: &OwnedMessage, rhs: &OwnedMessage) -> Ordering {
    lhs.channel
        .topic
        .cmp(&rhs.channel.topic)
        .then_with(|| lhs.channel.id.cmp(&rhs.channel.id))
        .then_with(|| lhs.log_time.cmp(&rhs.log_time))
        .then_with(|| lhs.sequence.cmp(&rhs.sequence))
        .then_with(|| lhs.publish_time.cmp(&rhs.publish_time))
}

fn write_owned_message<W: Write + Seek>(
    writer: &mut mcap::Writer<W>,
    message: &OwnedMessage,
) -> Result<()> {
    writer
        .write(&mcap::Message {
            channel: message.channel.clone(),
            sequence: message.sequence,
            log_time: message.log_time,
            publish_time: message.publish_time,
            data: Cow::Borrowed(message.data.as_slice()),
        })
        .context("failed to write message")
}

fn copy_metadata_and_attachments<W: Write + Seek>(
    input: &[u8],
    summary: Option<&mcap::Summary>,
    writer: &mut mcap::Writer<W>,
) -> Result<()> {
    let indexed_metadata_complete = summary.is_some_and(metadata_indexes_complete);
    let indexed_attachments_complete = summary.is_some_and(attachment_indexes_complete);

    if let Some(summary) = summary {
        if indexed_metadata_complete {
            copy_metadata_from_summary(input, summary, writer)?;
        }
        if indexed_attachments_complete {
            copy_attachments_from_summary(input, summary, writer)?;
        }
    }

    if !indexed_metadata_complete || !indexed_attachments_complete {
        copy_metadata_and_attachments_linear(
            input,
            writer,
            !indexed_metadata_complete,
            !indexed_attachments_complete,
        )?;
    }

    Ok(())
}

fn metadata_indexes_complete(summary: &mcap::Summary) -> bool {
    summary
        .stats
        .as_ref()
        .map_or(!summary.metadata_indexes.is_empty(), |stats| {
            summary.metadata_indexes.len() as u32 == stats.metadata_count
        })
}

fn attachment_indexes_complete(summary: &mcap::Summary) -> bool {
    summary
        .stats
        .as_ref()
        .map_or(!summary.attachment_indexes.is_empty(), |stats| {
            summary.attachment_indexes.len() as u32 == stats.attachment_count
        })
}

fn copy_metadata_from_summary<W: Write + Seek>(
    input: &[u8],
    summary: &mcap::Summary,
    writer: &mut mcap::Writer<W>,
) -> Result<()> {
    let mut indexes = summary.metadata_indexes.clone();
    indexes.sort_by_key(|index| index.offset);
    for index in &indexes {
        let metadata = mcap::read::metadata(input, index)
            .with_context(|| format!("failed to read metadata at offset {}", index.offset))?;
        writer
            .write_metadata(&metadata)
            .with_context(|| format!("failed to write metadata {}", metadata.name))?;
    }
    Ok(())
}

fn copy_attachments_from_summary<W: Write + Seek>(
    input: &[u8],
    summary: &mcap::Summary,
    writer: &mut mcap::Writer<W>,
) -> Result<()> {
    let mut indexes = summary.attachment_indexes.clone();
    indexes.sort_by_key(|index| index.offset);
    for index in &indexes {
        let attachment = mcap::read::attachment(input, index)
            .with_context(|| format!("failed to read attachment at offset {}", index.offset))?;
        writer
            .attach(&attachment)
            .with_context(|| format!("failed to write attachment {}", attachment.name))?;
    }
    Ok(())
}

fn copy_metadata_and_attachments_linear<W: Write + Seek>(
    input: &[u8],
    writer: &mut mcap::Writer<W>,
    include_metadata: bool,
    include_attachments: bool,
) -> Result<()> {
    for record in mcap::read::LinearReader::new(input)? {
        match record? {
            mcap::records::Record::Metadata(metadata) if include_metadata => {
                writer.write_metadata(&metadata)?;
            }
            mcap::records::Record::Attachment { header, data, .. } if include_attachments => {
                writer.attach(&mcap::Attachment {
                    log_time: header.log_time,
                    create_time: header.create_time,
                    name: header.name,
                    media_type: header.media_type,
                    data: Cow::Borrowed(data.as_ref()),
                })?;
            }
            _ => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::Cursor;

    use super::{repackage_to_writer, RepackageOptions};

    fn default_options() -> RepackageOptions {
        RepackageOptions {
            compression: None,
            compression_level: 0,
            window_duration_ns: 1_000_000_000,
            large_message_threshold: 256 * 1024,
            chunk_size: 4 * 1024 * 1024,
            include_crc: true,
        }
    }

    fn write_test_input() -> Vec<u8> {
        let mut output = Cursor::new(Vec::new());
        {
            let mut writer = mcap::WriteOptions::new()
                .compression(None)
                .chunk_size(Some(128))
                .create(&mut output)
                .expect("writer");
            let schema_id = writer
                .add_schema("Example", "jsonschema", br#"{"type":"object"}"#)
                .expect("schema");
            let points = writer
                .add_channel(schema_id, "/points", "json", &BTreeMap::new())
                .expect("points channel");
            let imu = writer
                .add_channel(schema_id, "/imu", "json", &BTreeMap::new())
                .expect("imu channel");

            for (channel_id, sequence, log_time, data) in [
                (points, 1, 1_000_000_000, vec![1, 1]),
                (imu, 2, 2_000_000_000, vec![2, 2]),
                (points, 3, 3_000_000_000, vec![3, 3]),
                (imu, 4, 4_000_000_000, vec![4, 4]),
                (imu, 5, 6_000_000_000, vec![5, 5]),
            ] {
                writer
                    .write_to_known_channel(
                        &mcap::records::MessageHeader {
                            channel_id,
                            sequence,
                            log_time,
                            publish_time: log_time,
                        },
                        data.as_slice(),
                    )
                    .expect("message");
            }

            writer
                .write_metadata(&mcap::records::Metadata {
                    name: "demo".to_string(),
                    metadata: BTreeMap::from([("key".to_string(), "value".to_string())]),
                })
                .expect("metadata");
            writer
                .attach(&mcap::Attachment {
                    log_time: 7_000_000_000,
                    create_time: 7_000_000_000,
                    name: "attachment.bin".to_string(),
                    media_type: "application/octet-stream".to_string(),
                    data: std::borrow::Cow::Borrowed(&[9, 8, 7]),
                })
                .expect("attachment");
            writer.finish().expect("finish");
        }
        output.into_inner()
    }

    fn message_fingerprints(input: &[u8]) -> Vec<(String, u32, u64, u64, Vec<u8>)> {
        let mut messages: Vec<_> = mcap::MessageStream::new(input)
            .expect("message stream")
            .map(|message| {
                let message = message.expect("message");
                (
                    message.channel.topic.clone(),
                    message.sequence,
                    message.log_time,
                    message.publish_time,
                    message.data.into_owned(),
                )
            })
            .collect();
        messages.sort();
        messages
    }

    fn chunk_topics(input: &[u8]) -> Vec<Vec<String>> {
        let summary = mcap::Summary::read(input)
            .expect("summary")
            .expect("summary should exist");
        let mut chunk_indexes = summary.chunk_indexes.clone();
        chunk_indexes.sort_by_key(|index| index.chunk_start_offset);
        chunk_indexes
            .iter()
            .map(|index| {
                summary
                    .stream_chunk(input, index)
                    .expect("chunk stream")
                    .map(|message| message.expect("message").channel.topic.clone())
                    .collect()
            })
            .collect()
    }

    #[test]
    fn repackage_preserves_messages_metadata_and_attachments() {
        let input = write_test_input();
        let mut output = Cursor::new(Vec::new());
        repackage_to_writer(&input, &mut output, &default_options()).expect("repackage");
        let output = output.into_inner();

        assert_eq!(message_fingerprints(&output), message_fingerprints(&input));
        let summary = mcap::Summary::read(&output)
            .expect("summary")
            .expect("summary should exist");
        assert_eq!(summary.metadata_indexes.len(), 1);
        assert_eq!(summary.attachment_indexes.len(), 1);
    }

    #[test]
    fn repackage_groups_topics_inside_each_window() {
        let input = write_test_input();
        let mut opts = default_options();
        opts.window_duration_ns = 5_000_000_000;
        let mut output = Cursor::new(Vec::new());
        repackage_to_writer(&input, &mut output, &opts).expect("repackage");

        assert_eq!(
            chunk_topics(&output.into_inner()),
            vec![
                vec!["/imu".to_string(), "/imu".to_string()],
                vec!["/points".to_string(), "/points".to_string()],
                vec!["/imu".to_string()],
            ]
        );
    }

    #[test]
    fn large_messages_are_written_as_single_message_chunks() {
        let input = write_test_input();
        let mut opts = default_options();
        opts.large_message_threshold = 2;
        let mut output = Cursor::new(Vec::new());
        repackage_to_writer(&input, &mut output, &opts).expect("repackage");

        let chunks = chunk_topics(&output.into_inner());
        assert_eq!(chunks.iter().filter(|chunk| chunk.len() == 1).count(), 5);
    }

    #[test]
    fn chunk_size_splits_small_message_groups() {
        let input = write_test_input();
        let mut opts = default_options();
        opts.window_duration_ns = 5_000_000_000;
        opts.chunk_size = 2;
        opts.large_message_threshold = 100;
        let mut output = Cursor::new(Vec::new());
        repackage_to_writer(&input, &mut output, &opts).expect("repackage");

        assert_eq!(
            chunk_topics(&output.into_inner()),
            vec![
                vec!["/imu".to_string()],
                vec!["/imu".to_string()],
                vec!["/points".to_string()],
                vec!["/points".to_string()],
                vec!["/imu".to_string()],
            ]
        );
    }
}
