// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/*!
Download Amazon EBS snapshots.
*/

use crate::block_device::get_block_device_size;
use async_trait::async_trait;
use aws_sdk_ebs::Client as EbsClient;
use base64::engine::general_purpose::STANDARD as base64_engine;
use base64::Engine as _;
use futures::stream::{self, StreamExt};
use indicatif::ProgressBar;
use log::debug;
use sha2::{Digest, Sha256};
use snafu::{ensure, OptionExt, ResultExt, Snafu};
use std::collections::{BTreeMap, HashSet};
use std::convert::TryFrom;
use std::fs as stdfs;
use std::io::Write as _;
use std::io::SeekFrom;
use std::os::unix::fs::FileTypeExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

#[derive(Debug, Snafu)]
pub struct Error(error::Error);
type Result<T> = std::result::Result<T, Error>;

const GIBIBYTE: i64 = 1024 * 1024 * 1024;
const SNAPSHOT_BLOCK_WORKERS: usize = 64;
const SNAPSHOT_BLOCK_ATTEMPTS: u8 = 3;
const SHA256_ALGORITHM: &str = "SHA256";

// ListSnapshotBlocks allows us to specify how many blocks are returned in each
// query, from the default of 100 to the maximum of 10000. Since we fetch all
// the block information up front in a loop, we ask for the maximum so that we
// need fewer API calls.
const LIST_REQUEST_MAX_RESULTS: i32 = 10000;

pub struct SnapshotDownloader {
    ebs_client: EbsClient,
}

impl SnapshotDownloader {
    pub fn new(ebs_client: EbsClient) -> Self {
        SnapshotDownloader { ebs_client }
    }

    /// Download a snapshot into the file at the specified path.
    /// * `snapshot_id` is the snapshot to download.
    /// * `path` is the destination file for the snapshot. It will be extended to the volume size
    ///   of the snapshot. If the snapshot is sparse, i.e. not all blocks are present, then the file
    ///   will contain holes that return zeroes when read.
    /// * `progress_bar` is optional, since output to the terminal may not be wanted.
    pub async fn download_to_file<P: AsRef<Path>>(
    &self,
    snapshot_id: &str,
    path: P,
    progress_bar: Option<ProgressBar>,
) -> Result<()> {
    self.download_to_file_resume(snapshot_id, path, progress_bar, false)
        .await
}

/// Download a snapshot into the file at the specified path, optionally resuming a partial
/// download. When `resume` is true and the destination is a normal filesystem file, ColdSnap
/// will write to a stable partial file `<path>.coldsnap.part` and track completed block indices
/// in `<path>.coldsnap.state`.
///
/// Resume is intentionally **not** supported for block devices.
pub async fn download_to_file_resume<P: AsRef<Path>>(
    &self,
    snapshot_id: &str,
    path: P,
    progress_bar: Option<ProgressBar>,
    resume: bool,
) -> Result<()> {
    let path = path.as_ref();
    let _ = path
        .file_name()
        .context(error::ValidateFileNameSnafu { path })?;

    // Find the overall volume size, the block size, and the metadata we need for each block:
    // the index, which lets us calculate the offset into the volume; and the token, which we
    // need to retrieve it.
    let snapshot: Snapshot = self.list_snapshot_blocks(snapshot_id).await?;

    let is_block_device = BlockDeviceTarget::is_valid(path).await?;
    if resume && is_block_device {
        return error::ResumeNotSupportedForBlockDeviceSnafu {
            path: path.to_path_buf(),
        }
        .fail()
        .map_err(Error::from);
    }

    let resume_state = if resume && !is_block_device {
        Some(ResumeState::load_or_create(path, &snapshot)?)
    } else {
        None
    };

    let mut target = if is_block_device {
        BlockDeviceTarget::new_target(path)?
    } else {
        // If not block assume file for now
        FileTarget::new_target(path, resume)?
    };

    debug!("Writing {}G to {}...", snapshot.volume_size, path.display());
    target.grow(snapshot.volume_size * GIBIBYTE).await?;
    self.write_snapshot_blocks(snapshot, target.write_path()?, progress_bar, resume_state)
        .await?;
    target.finalize()?;

    // Best-effort cleanup of the resume state file after a successful download.
    if resume && !is_block_device {
        let _ = stdfs::remove_file(state_path_for(path));
    }

    Ok(())
}

    async fn write_snapshot_blocks(
        &self,
        snapshot: Snapshot,
        write_path: &Path,
        progress_bar: Option<ProgressBar>,
        resume_state: Option<ResumeState>,
    ) -> Result<()> {
        // Collect errors encountered while downloading blocks, since we can't
        // return a result directly through `for_each_concurrent`.
        let block_errors = Arc::new(Mutex::new(BTreeMap::new()));

        // We may have a progress bar to update.
        let progress_bar = match progress_bar {
            Some(pb) => {
                let pb_length = snapshot.blocks.len();
                let pb_length =
                    u64::try_from(pb_length).with_context(|_| error::ConvertNumberSnafu {
                        what: "progress bar length",
                        number: pb_length.to_string(),
                        target: "u64",
                    })?;
                pb.set_length(pb_length);
                Arc::new(Some(pb))
            }
            None => Arc::new(None),
        };


// Resume support: optionally skip blocks already downloaded.
let resume_done: Arc<Mutex<HashSet<i32>>> = match resume_state.as_ref() {
    Some(rs) => Arc::clone(&rs.done),
    None => Arc::new(Mutex::new(HashSet::new())),
};
let resume_append_lock: Option<Arc<Mutex<()>>> =
    resume_state.as_ref().map(|rs| Arc::clone(&rs.append_lock));
let resume_state_path: Option<PathBuf> = resume_state.as_ref().map(|rs| rs.state_path.clone());

        // Create a context for each block that can be moved to another thread.
        let mut block_contexts = Vec::new();
        
for SnapshotBlock { index, token } in snapshot.blocks {
    // If resuming, skip blocks we already completed.
    if resume_state.is_some() {
        let done = resume_done.lock().expect("poisoned");
        if done.contains(&index) {
            if let Some(ref progress_bar) = *progress_bar {
                progress_bar.inc(1);
            }
            continue;
        }
    }

    block_contexts.push(BlockContext {
        path: write_path.to_path_buf(),
        block_index: index,
        block_token: token,
        block_size: snapshot.block_size,
        snapshot_id: snapshot.snapshot_id.clone(),
        block_errors: Arc::clone(&block_errors),
        progress_bar: Arc::clone(&progress_bar),
        ebs_client: self.ebs_client.clone(),

        resume_done: Arc::clone(&resume_done),
        resume_state_path: resume_state_path.clone(),
        resume_append_lock: resume_append_lock.clone(),
    });
}

        // Distribute the work across a fixed number of concurrent workers.
        // New threads will be created by the runtime as needed, but we'll
        // only process this many blocks at once to limit resource usage.
        let download = stream::iter(block_contexts).for_each_concurrent(
            SNAPSHOT_BLOCK_WORKERS,
            |context| async move {
                for i in 0..SNAPSHOT_BLOCK_ATTEMPTS {
                    let block_result = self.download_block(&context).await;
                    let mut block_errors = context.block_errors.lock().expect("poisoned");
                    if let Err(e) = block_result {
                        debug!(
                            "Error downloading block, attempt {} of {}",
                            i + 1,
                            SNAPSHOT_BLOCK_ATTEMPTS
                        );
                        block_errors.insert(context.block_index, e);
                        continue;
                    }
                    block_errors.remove(&context.block_index);
                    break;
                }
            },
        );
        download.await;

        // At this point, all the concurrent jobs have finished, so all of the Arcs we copied have
        // been dropped. Hence there's exactly one strong reference and it's safe to `try_unwrap`
        // and `unwrap` the result to recover the contents. Any of the Mutexes inside are safe to
        // unwrap unless they've been poisoned by a panic, in which case we also panic.

        // Summarize any fatal errors.
        let block_errors = Arc::try_unwrap(block_errors)
            .expect("referenced")
            .into_inner()
            .expect("poisoned");
        let block_errors_count = block_errors.keys().len();
        if block_errors_count != 0 {
            let error_report: String = block_errors.values().map(|e| e.to_string()).collect();
            error::GetSnapshotBlocksSnafu {
                error_count: block_errors_count,
                snapshot_id: snapshot.snapshot_id,
                error_report,
            }
            .fail()?;
        }

        Ok(())
    }

    /// Retrieve the index and token for all snapshot blocks.
    async fn list_snapshot_blocks(&self, snapshot_id: &str) -> Result<Snapshot> {
        let mut blocks = Vec::new();
        let max_results = LIST_REQUEST_MAX_RESULTS;
        let mut next_token = None;
        let mut volume_size;
        let mut block_size;

        loop {
            let response = self
                .ebs_client
                .list_snapshot_blocks()
                .snapshot_id(snapshot_id)
                .set_next_token(next_token)
                .max_results(max_results)
                .send()
                .await
                .context(error::ListSnapshotBlocksSnafu { snapshot_id })?;

            volume_size = response
                .volume_size
                .context(error::FindVolumeSizeSnafu { snapshot_id })?;

            block_size = response
                .block_size
                .context(error::FindBlockSizeSnafu { snapshot_id })?;

            for block in response.blocks.unwrap_or_default().iter() {
                let index = block
                    .block_index
                    .context(error::FindBlockIndexSnafu { snapshot_id })?;

                let token = String::from(block.block_token.as_ref().context(
                    error::FindBlockPropertySnafu {
                        snapshot_id,
                        block_index: index,
                        property: "token",
                    },
                )?);

                blocks.push(SnapshotBlock { index, token });
            }

            next_token = response.next_token;
            if next_token.is_none() {
                break;
            }
        }

        Ok(Snapshot {
            snapshot_id: snapshot_id.to_string(),
            volume_size,
            block_size,
            blocks,
        })
    }

    /// Download a single block from the snapshot in context and write it to the file.
    async fn download_block(&self, context: &BlockContext) -> Result<()> {
        let snapshot_id = &context.snapshot_id;
        let block_index = context.block_index;
        let block_token = &context.block_token;
        let block_size = context.block_size;

        let response = context
            .ebs_client
            .get_snapshot_block()
            .snapshot_id(snapshot_id)
            .block_index(block_index)
            .block_token(block_token)
            .send()
            .await
            .context(error::GetSnapshotBlockSnafu {
                snapshot_id,
                block_index,
            })?;

        let expected_hash = response.checksum.context(error::FindBlockPropertySnafu {
            snapshot_id,
            block_index,
            property: "checksum",
        })?;

        let checksum_algorithm = response
            .checksum_algorithm
            .context(error::FindBlockPropertySnafu {
                snapshot_id,
                block_index,
                property: "checksum algorithm",
            })?
            .as_str()
            .to_string();

        let data_length = response
            .data_length
            .context(error::FindBlockPropertySnafu {
                snapshot_id,
                block_index,
                property: "data length",
            })?;

        let block_data_stream =
            response
                .block_data
                .collect()
                .await
                .context(error::CollectByteStreamSnafu {
                    snapshot_id,
                    block_index,
                    property: "data",
                })?;

        let block_data = block_data_stream.into_bytes();

        ensure!(
            checksum_algorithm == SHA256_ALGORITHM,
            error::UnexpectedBlockChecksumAlgorithmSnafu {
                snapshot_id,
                block_index,
                checksum_algorithm,
            }
        );
        let block_data_length = block_data.len();
        let block_data_length =
            i32::try_from(block_data_length).with_context(|_| error::ConvertNumberSnafu {
                what: "block data length",
                number: block_data_length.to_string(),
                target: "i32",
            })?;

        ensure!(
            data_length > 0 && data_length <= block_size && data_length == block_data_length,
            error::UnexpectedBlockDataLengthSnafu {
                snapshot_id,
                block_index,
                data_length,
            }
        );

        let mut block_digest = Sha256::new();
        block_digest.update(&block_data);
        let hash_bytes = block_digest.finalize();
        let block_hash = base64_engine.encode(hash_bytes);

        ensure!(
            block_hash == expected_hash,
            error::BadBlockChecksumSnafu {
                snapshot_id,
                block_index,
                block_hash,
                expected_hash,
            }
        );

        // Blocks of all zeroes can be omitted from the file.
        let sparse = block_data.iter().all(|&byte| byte == 0u8);
        if sparse {
            // Even if sparse, mark the block as completed for resume purposes.
            if let Some(ref state_path) = context.resume_state_path {
                if let Some(ref lock) = context.resume_append_lock {
                    let _g = lock.lock().expect("poisoned");
                    append_done_index(state_path, block_index)?;
                }
                let mut done = context.resume_done.lock().expect("poisoned");
                done.insert(block_index);
            }

            if let Some(ref progress_bar) = *context.progress_bar {
                progress_bar.inc(1);
            }
            return Ok(());
        }

        let path: &Path = context.path.as_ref();
        let mut f = OpenOptions::new()
            .write(true)
            .open(path)
            .await
            .context(error::OpenFileSnafu { path })?;

        // Calculate the offset to write the block into the target file
        let block_index_u64 =
            u64::try_from(context.block_index).with_context(|_| error::ConvertNumberSnafu {
                what: "block index",
                number: context.block_index.to_string(),
                target: "u64",
            })?;
        let block_size_u64 =
            u64::try_from(block_size).with_context(|_| error::ConvertNumberSnafu {
                what: "block size",
                number: block_size.to_string(),
                target: "u64",
            })?;
        let offset = block_index_u64 * block_size_u64;

        f.seek(SeekFrom::Start(offset))
            .await
            .context(error::SeekFileOffsetSnafu { path, offset })?;

        let count = usize::try_from(data_length).with_context(|_| error::ConvertNumberSnafu {
            what: "byte count",
            number: data_length.to_string(),
            target: "usize",
        })?;

        f.write_all(&block_data)
            .await
            .context(error::WriteFileBytesSnafu { path, count })?;

        f.flush().await.context(error::FlushFileSnafu { path })?;

        // Mark completion for resume support.
        if let Some(ref state_path) = context.resume_state_path {
            if let Some(ref lock) = context.resume_append_lock {
                let _g = lock.lock().expect("poisoned");
                append_done_index(state_path, block_index)?;
            }
            let mut done = context.resume_done.lock().expect("poisoned");
            done.insert(block_index);
        }

        if let Some(ref progress_bar) = *context.progress_bar {
            progress_bar.inc(1);
        }

        Ok(())
    }
}

/// Stores the metadata about the snapshot contents.
struct Snapshot {
    snapshot_id: String,
    volume_size: i64,
    block_size: i32,
    blocks: Vec<SnapshotBlock>,
}

/// Stores the metadata about a snapshot block.
struct SnapshotBlock {
    index: i32,
    token: String,
}

/// Stores the context needed to download a snapshot block.
struct BlockContext {
    path: PathBuf,
    block_index: i32,
    block_token: String,
    block_size: i32,
    snapshot_id: String,
    block_errors: Arc<Mutex<BTreeMap<i32, Error>>>,
    progress_bar: Arc<Option<ProgressBar>>,
    ebs_client: EbsClient,

    // Resume support
    resume_done: Arc<Mutex<HashSet<i32>>>,
    resume_state_path: Option<PathBuf>,
    resume_append_lock: Option<Arc<Mutex<()>>>,
}

/// Shared interface for write targets.
#[async_trait]
trait SnapshotWriteTarget {
    // grow the target to the desired length
    async fn grow(&mut self, length: i64) -> Result<()>;

    // returns the file path to which blocks must be written
    fn write_path(&self) -> Result<&Path>;

    // persist the contents to disk
    fn finalize(&mut self) -> Result<()>;
}

/// Implements file operations for block devices.
struct BlockDeviceTarget {
    path: PathBuf,
}

impl BlockDeviceTarget {
    fn new_target<P: AsRef<Path>>(path: P) -> Result<Box<dyn SnapshotWriteTarget>> {
        let path = path.as_ref();
        Ok(Box::new(BlockDeviceTarget { path: path.into() }))
    }

    async fn is_valid<P: AsRef<Path>>(path: P) -> Result<bool> {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(false);
        }

        let file_meta = fs::metadata(path)
            .await
            .context(error::ReadFileMetadataSnafu { path })?;

        if file_meta.file_type().is_block_device() {
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

#[async_trait]
impl SnapshotWriteTarget for BlockDeviceTarget {
    // ensures existing size >= length, but otherwise leaves untouched
    async fn grow(&mut self, length: i64) -> Result<()> {
        let path = self.path.as_path();
        let block_device_size =
            get_block_device_size(path).context(error::GetBlockDeviceSizeSnafu)?;

        // Make sure the block device is big enough to hold the snapshot
        ensure!(
            block_device_size >= length,
            error::BlockDeviceTooSmallSnafu {
                block_device_size: block_device_size / GIBIBYTE,
                needed: length / GIBIBYTE,
            }
        );

        Ok(())
    }

    // returns the file path to which blocks must be written
    fn write_path(&self) -> Result<&Path> {
        Ok(self.path.as_path())
    }

    // no-op
    fn finalize(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Implements file operations for filesystem files.
///
/// For atomic writes and resume support, we write blocks into a stable "partial" file
/// `<final>.coldsnap.part` and then rename it to the final destination on success.
struct FileTarget {
    final_path: PathBuf,
    part_path: PathBuf,
}

impl FileTarget {
    fn new_target<P: AsRef<Path>>(path: P, _resume: bool) -> Result<Box<dyn SnapshotWriteTarget>> {
        let final_path = path.as_ref().to_path_buf();
        let part_path = part_path_for(&final_path);
        Ok(Box::new(FileTarget {
            final_path,
            part_path,
        }))
    }
}

#[async_trait]
impl SnapshotWriteTarget for FileTarget {
    // Create (or reuse) the partial file and extend/truncate it to the desired size.
    async fn grow(&mut self, length: i64) -> Result<()> {
        let path = self.part_path.as_path();

        let file_len =
            u64::try_from(length).with_context(|_| error::ConvertNumberSnafu {
                what: "file length",
                number: length.to_string(),
                target: "u64",
            })?;

        let f = OpenOptions::new()
            .create(true)
            .write(true)
            .open(path)
            .await
            .context(error::OpenFileSnafu { path })?;

        f.set_len(file_len)
            .await
            .context(error::ExtendFileSnafu { path })?;

        Ok(())
    }

    fn write_path(&self) -> Result<&Path> {
        Ok(self.part_path.as_path())
    }

    // Persist file to destination by renaming the partial file into place.
    fn finalize(&mut self) -> Result<()> {
        stdfs::rename(&self.part_path, &self.final_path).context(error::RenamePartFileSnafu {
            from: self.part_path.clone(),
            to: self.final_path.clone(),
        })?;
        Ok(())
    }
}

#[derive(Clone)]
struct ResumeState {
    state_path: PathBuf,
    done: Arc<Mutex<HashSet<i32>>>,
    append_lock: Arc<Mutex<()>>,
}

fn part_path_for(final_path: &Path) -> PathBuf {
    let mut p = final_path.as_os_str().to_os_string();
    p.push(".coldsnap.part");
    PathBuf::from(p)
}

fn state_path_for(final_path: &Path) -> PathBuf {
    let mut p = final_path.as_os_str().to_os_string();
    p.push(".coldsnap.state");
    PathBuf::from(p)
}

impl ResumeState {
    fn load_or_create(final_path: &Path, snapshot: &Snapshot) -> Result<Self> {
        let state_path = state_path_for(final_path);

        let (header_ok, done_set) = if state_path.exists() {
            load_resume_state(&state_path, snapshot)?
        } else {
            (false, HashSet::new())
        };

        if !header_ok {
            // Create/overwrite with the correct header (fresh resume file).
            write_resume_header(&state_path, snapshot)?;
        }

        Ok(ResumeState {
            state_path,
            done: Arc::new(Mutex::new(done_set)),
            append_lock: Arc::new(Mutex::new(())),
        })
    }
}

fn write_resume_header(state_path: &Path, snapshot: &Snapshot) -> std::result::Result<(), error::Error> {
    let mut f = stdfs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(state_path)
        .context(error::OpenResumeStateSnafu { path: state_path })?;

    writeln!(
        f,
        "coldsnap-resume-v1 snapshot_id={} volume_size={} block_size={}",
        snapshot.snapshot_id, snapshot.volume_size, snapshot.block_size
    )
    .context(error::WriteResumeStateSnafu { path: state_path })?;

    Ok(())
}

fn load_resume_state(state_path: &Path, snapshot: &Snapshot) -> std::result::Result<(bool, HashSet<i32>), error::Error> {
    let content = stdfs::read_to_string(state_path)
        .context(error::ReadResumeStateSnafu { path: state_path })?;

    let mut lines = content.lines();
    let header = match lines.next() {
        Some(h) => h,
        None => return Ok((false, HashSet::new())),
    };

    let header_ok = header.contains("coldsnap-resume-v1")
        && header.contains(&format!("snapshot_id={}", snapshot.snapshot_id))
        && header.contains(&format!("volume_size={}", snapshot.volume_size))
        && header.contains(&format!("block_size={}", snapshot.block_size));

    if !header_ok {
        return Ok((false, HashSet::new()));
    }

    let mut done = HashSet::new();
    for line in lines {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Ok(idx) = line.parse::<i32>() {
            done.insert(idx);
        }
    }

    Ok((true, done))
}

fn append_done_index(state_path: &Path, block_index: i32) -> std::result::Result<(), error::Error> {
    let mut f = stdfs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(state_path)
        .context(error::OpenResumeStateSnafu { path: state_path })?;

    writeln!(f, "{}", block_index).context(error::WriteResumeStateSnafu { path: state_path })?;
    Ok(())
}

/// Potential errors while downloading a snapshot and writing to a local file.
mod error {
    use aws_sdk_ebs::{
        self,
        operation::{
            get_snapshot_block::GetSnapshotBlockError,
            list_snapshot_blocks::ListSnapshotBlocksError,
        },
    };
    use snafu::Snafu;
    use std::path::PathBuf;

    #[derive(Debug, Snafu)]
    #[snafu(visibility(pub(super)))]
    pub(super) enum Error {
        #[snafu(display("Failed to read metadata for '{}': {}", path.display(), source))]
        ReadFileMetadata {
            path: PathBuf,
            source: std::io::Error,
        },

        #[snafu(display("{}", source))]
        GetBlockDeviceSize { source: crate::block_device::Error },

        #[snafu(display(
            "Block device too small: block device size {} GiB, needed at least {} GiB",
            block_device_size,
            needed
        ))]
        BlockDeviceTooSmall { block_device_size: i64, needed: i64 },

        #[snafu(display("Failed to validate file name '{}'", path.display()))]
        ValidateFileName { path: PathBuf },

        #[snafu(display("Resume is not supported when writing directly to block device '{}'", path.display()))]
        ResumeNotSupportedForBlockDevice { path: PathBuf },

        #[snafu(display("Failed to list snapshot blocks '{snapshot_id}': {source}", source = crate::error_stack(source, 2)))]
        ListSnapshotBlocks {
            snapshot_id: String,
            #[snafu(source(from(aws_sdk_ebs::error::SdkError<ListSnapshotBlocksError>, Box::new)))]
            source: Box<aws_sdk_ebs::error::SdkError<ListSnapshotBlocksError>>,
        },

        #[snafu(display("Failed to find volume size for '{}'", snapshot_id))]
        FindVolumeSize { snapshot_id: String },

        #[snafu(display("Failed to find index for block in '{}'", snapshot_id))]
        FindBlockIndex { snapshot_id: String },

        #[snafu(display(
            "Failed to find {} for block {} in '{}'",
            property,
            block_index,
            snapshot_id
        ))]
        FindBlockProperty {
            snapshot_id: String,
            block_index: i32,
            property: String,
        },

        #[snafu(display(
            "Failed to find {} for block {} in '{}'",
            property,
            block_index,
            snapshot_id
        ))]
        CollectByteStream {
            snapshot_id: String,
            block_index: i32,
            property: String,
            #[snafu(source(from(aws_sdk_ebs::primitives::ByteStreamError, Box::new)))]
            source: Box<aws_sdk_ebs::primitives::ByteStreamError>,
        },

        #[snafu(display("Failed to find block size for '{}'", snapshot_id))]
        FindBlockSize { snapshot_id: String },

        #[snafu(display(
            "Found unexpected checksum algorithm '{}' for block {} in '{}'",
            checksum_algorithm,
            block_index,
            snapshot_id
        ))]
        UnexpectedBlockChecksumAlgorithm {
            snapshot_id: String,
            block_index: i64,
            checksum_algorithm: String,
        },

        #[snafu(display(
            "Found unexpected data length {} for block {} in '{}'",
            data_length,
            block_index,
            snapshot_id
        ))]
        UnexpectedBlockDataLength {
            snapshot_id: String,
            block_index: i64,
            data_length: i64,
        },

        #[snafu(display(
            "Bad checksum for block {} in '{}': expected '{}', got '{}'",
            block_index,
            snapshot_id,
            expected_hash,
            block_hash,
        ))]
        BadBlockChecksum {
            snapshot_id: String,
            block_index: i64,
            block_hash: String,
            expected_hash: String,
        },

        #[snafu(display(
            "Failed to get block {} for snapshot '{}': {}",
            block_index,
            snapshot_id,
            source
        ))]
        GetSnapshotBlock {
            snapshot_id: String,
            block_index: i64,
            #[snafu(source(from(aws_sdk_ebs::error::SdkError<GetSnapshotBlockError>, Box::new)))]
            source: Box<aws_sdk_ebs::error::SdkError<GetSnapshotBlockError>>,
        },

        #[snafu(display(
            "Failed to get {} blocks for snapshot '{}': {}",
            error_count,
            snapshot_id,
            error_report
        ))]
        GetSnapshotBlocks {
            error_count: usize,
            snapshot_id: String,
            error_report: String,
        },

        #[snafu(display("Failed to flush '{}': {}", path.display(), source))]
        FlushFile {
            path: PathBuf,
            source: std::io::Error,
        },

        #[snafu(display("Failed to open '{}': {}", path.display(), source))]
        OpenFile {
            path: PathBuf,
            source: std::io::Error,
        },

        #[snafu(display("Failed to extend '{}': {}", path.display(), source))]
        ExtendFile {
            path: PathBuf,
            source: std::io::Error,
        },

        #[snafu(display("Failed to seek to {} in '{}': {}", offset, path.display(), source))]
        SeekFileOffset {
            path: PathBuf,
            offset: u64,
            source: std::io::Error,
        },

        #[snafu(display("Failed to write {} bytes to '{}': {}", count, path.display(), source))]
        WriteFileBytes {
            path: PathBuf,
            count: usize,
            source: std::io::Error,
        },

        #[snafu(display("Failed to open resume state file '{}': {}", path.display(), source))]
        OpenResumeState {
            path: PathBuf,
            source: std::io::Error,
        },

        #[snafu(display("Failed to read resume state file '{}': {}", path.display(), source))]
        ReadResumeState {
            path: PathBuf,
            source: std::io::Error,
        },

        #[snafu(display("Failed to write resume state file '{}': {}", path.display(), source))]
        WriteResumeState {
            path: PathBuf,
            source: std::io::Error,
        },

        #[snafu(display("Failed to rename partial file '{}' -> '{}': {}", from.display(), to.display(), source))]
        RenamePartFile {
            from: PathBuf,
            to: PathBuf,
            source: std::io::Error,
        },

        #[snafu(display("Failed to convert {} {} to {}: {}", what, number, target, source))]
        ConvertNumber {
            what: String,
            number: String,
            target: String,
            source: std::num::TryFromIntError,
        },
    }
}
