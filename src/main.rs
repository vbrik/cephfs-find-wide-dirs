//! Traverse a cephfs directory tree and identify directories with many files.
//! Very fast because multithreaded and uses ceph.dir.rfiles xattrs to skip subtrees
//! that don't contain enough files.
//!
//! This tool was conceived as an exercise for learning rust, so it's a bit over-engineered
//! (e.g. multithreading is an overkill)

use clap::Parser;
use crossbeam_channel::{Receiver, Sender, unbounded};
use log::error;
use std::{
    fs, io,
    io::ErrorKind,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    thread,
};
use xattr;

#[derive(Debug, Clone, Copy)]
struct DebugFlags {
    /// Match all dirs without looking at xattrs
    no_xattrs: bool,
}

/// Work items in the directory traversal pipeline.
/// `Shutdown` is a *pass-along* sentinel: any worker that receives it re-sends it
/// and then exits. This lets us shut down all workers with a single token, without
/// requiring workers to know the total worker count.
#[derive(Debug)]
enum WorkItem {
    Dir(PathBuf), // process this dir
    Shutdown,     // everybody shut down
}

#[derive(Debug, Clone, Copy)]
enum MainLoopCtl {
    Continue,
    Break,
}

/// Errors for "xattr read + parse" operations.
#[derive(Debug)]
#[allow(dead_code)]
enum XattrError {
    IoError(io::Error),
    Utf8Error(std::str::Utf8Error),
    ParseError(String),
    AttrNotFound,
}

impl From<io::Error> for XattrError {
    fn from(e: io::Error) -> Self {
        Self::IoError(e)
    }
}

impl From<std::str::Utf8Error> for XattrError {
    fn from(e: std::str::Utf8Error) -> Self {
        Self::Utf8Error(e)
    }
}

/// Read an xattr and parse it as a numeric type.
fn get_xattr_numeric<T>(path: &Path, attr: &str) -> Result<T, XattrError>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    let bytes = xattr::get(path, attr)?.ok_or(XattrError::AttrNotFound)?;
    let s = std::str::from_utf8(&bytes)?;
    let s = s.trim_matches(|c: char| c.is_whitespace() || c == '\0');
    s.parse::<T>()
        .map_err(|e| XattrError::ParseError(e.to_string()))
}

fn get_file_counts_numeric(dir: &Path, dbg: DebugFlags) -> Result<(u32, u32), XattrError> {
    if dbg.no_xattrs {
        Ok((0, 0))
    } else {
        let num_files: u32 = get_xattr_numeric(dir, "ceph.dir.files")?;
        let num_rfiles: u32 = get_xattr_numeric(dir, "ceph.dir.rfiles")?;
        Ok((num_files, num_rfiles))
    }
}

/// Main worker loop
fn worker_loop(
    dir_rx: Receiver<WorkItem>,        // incoming work queue
    dir_tx: Sender<WorkItem>,          // outgoing work queue
    match_tx: Sender<(PathBuf, u32)>,  // outgoing match queue
    pending_dir_count: Arc<AtomicU64>, // upper limit on pending and in-flight dirs (when to stop)
    min_file_count: u32,
    dbg: DebugFlags,
) {
    loop {
        let Ok(work) = dir_rx.recv() else {
            error!("Fatal: dir_rx disconnected");
            break;
        };
        let dir = match work {
            WorkItem::Dir(dir) => dir,
            WorkItem::Shutdown => {
                break;
            }
        };
        match process_dir(
            &dir,
            &dir_tx,
            &match_tx,
            &pending_dir_count,
            min_file_count,
            dbg,
        ) {
            MainLoopCtl::Continue => continue,
            MainLoopCtl::Break => break,
        }
    }
    let _ = dir_tx.send(WorkItem::Shutdown);
}

/// Process a single directory
fn process_dir(
    dir: &PathBuf,
    dir_tx: &Sender<WorkItem>,
    match_tx: &Sender<(PathBuf, u32)>,
    pending_dir_count: &Arc<AtomicU64>,
    min_file_count: u32,
    dbg: DebugFlags,
) -> MainLoopCtl {
    // This scope guard is to ensure that pending_dir_count is correctly decremented
    // upon completion of processing of every directory, even if we need bail early.
    // No-more-work condition must also always be checked even if processing fails,
    // and we return early (otherwise we may get stuck).
    let loop_ctl_signal = (|| -> MainLoopCtl {
        let (num_files, num_rfiles) = match get_file_counts_numeric(&dir, dbg) {
            Ok(v) => v,
            Err(XattrError::IoError(e)) if e.kind() == ErrorKind::NotFound => {
                // directory disappeared -- this is normal
                return MainLoopCtl::Continue;
            }
            Err(e) => {
                error!("Fatal: failed to process {:?}: {:?}", dir, e);
                return MainLoopCtl::Break;
            }
        };

        if num_files >= min_file_count {
            if match_tx.send((dir.clone(), num_files)).is_err() {
                error!("Fatal: match_tx disconnected");
                return MainLoopCtl::Break;
            }
        }

        // subtrees may contain directories with many files, so need to descend
        if num_rfiles.saturating_sub(num_files) >= min_file_count {
            let rd = match fs::read_dir(dir) {
                Ok(rd) => rd,
                Err(e) if e.kind() == ErrorKind::NotFound => {
                    // directory disappearances are normal
                    return MainLoopCtl::Continue;
                }
                Err(e) => {
                    error!("Fatal: failed to process {:?}: {:?}", dir, e);
                    return MainLoopCtl::Break;
                }
            };
            for entry_res in rd {
                let direntry = match entry_res {
                    Ok(e) => e,
                    Err(e) => {
                        error!("Skipping entry in {:?}: {:?}", dir, e);
                        return MainLoopCtl::Break;
                    }
                };
                let Ok(ft) = direntry.file_type() else {
                    continue;
                };
                if !ft.is_dir() {
                    continue;
                }
                // Semantically, pending_dir_count counts queued + in-flight directories.
                // More precisely, the invariant is: pending_dir_count >= queued + in-flight,
                // so increment before enqueue.
                pending_dir_count.fetch_add(1, Ordering::Relaxed); // no idea why Relaxed; Release?
                if dir_tx.send(WorkItem::Dir(direntry.path())).is_err() {
                    error!("Fatal: dir_tx disconnected");
                    return MainLoopCtl::Break;
                }
            };
        }
        MainLoopCtl::Continue
    })();

    let prev = pending_dir_count.fetch_sub(1, Ordering::AcqRel); // don't know why AcqRel
    if prev == 1 {
        // Finishing this directory made pending go 1 -> 0. This means no more work.
        return MainLoopCtl::Break;
    }
    loop_ctl_signal
}

#[derive(Parser, Debug)]
#[command(about = "Quickly find cephfs dirs with many files using ceph.dir.{files,rfiles} xattrs.")]
struct Args {
    /// Search root directory.
    #[arg(value_name = "PATH")]
    search_root: String,

    /// Minimum number of files (0 => don't read xattrs, match all).
    #[arg(short, long = "min-num-files", value_name = "NUMBER",
    value_parser = clap::value_parser!(u32).range(0..))]
    min_files: u32,

    /// Number of threads (1-64).
    #[arg(short, long = "threads", value_name = "NUMBER", default_value_t = 16,
    value_parser = clap::value_parser!(u32).range(1..=64))]
    num_threads: u32,
}

fn main() {
    env_logger::init();
    let args = Args::parse();

    let dbg = DebugFlags {
        no_xattrs: args.min_files == 0,
    };
    if dbg.no_xattrs {
        eprintln!("Debug mode: ignoring xattrs");
    }

    let root = PathBuf::from(args.search_root);
    if !root.is_dir() {
        eprintln!("error: not a directory or permission denied: {:?}", root);
        std::process::exit(2);
    }

    let n_workers: usize = args.num_threads.try_into().expect("conversion");

    // Channel for directories that need to be processed (workers send and receive)
    let (dir_tx, dir_rx) = unbounded::<WorkItem>();
    // Channel for directories that match the criteria (workers send to main)
    let (match_tx, match_rx) = unbounded::<(PathBuf, u32)>();

    // A counter to detect when there is no more work to be done.
    // dirs_pending >= dirs pending processing + dirs being processed
    let dirs_pending = Arc::new(AtomicU64::new(0));

    dirs_pending.fetch_add(1, Ordering::Relaxed);
    dir_tx
        .send(WorkItem::Dir(root))
        .expect("Must be able to enqueue root");

    let mut workers = Vec::with_capacity(n_workers);
    for _ in 0..n_workers {
        workers.push({
            let dir_rx = dir_rx.clone();
            let dir_tx = dir_tx.clone();
            let match_tx = match_tx.clone();
            let pending = Arc::clone(&dirs_pending);
            thread::spawn(move || {
                worker_loop(dir_rx, dir_tx, match_tx, pending, args.min_files, dbg)
            })
        });
    }

    // Close our match sender so match_rx disconnects when all workers exit,
    // so that we know when there is no more work to be done.
    drop(match_tx);
    while let Ok((path, num_files)) = match_rx.recv() {
        println!("{}/ {}", path.display(), num_files);
    }

    for h in workers {
        let _ = h.join();
    }
}
