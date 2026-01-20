//! Traverse a cephfs directory tree and identify directories with too many files.
//! Very fast because it's multithreaded and uses ceph.dir.rfiles xattrs to skip
//! subtrees with too few files.
//!
//! Except for disappearing directories all errors treated as fatal (e.g. permissions).
//!
//! This tool was conceived as an exercise for learning Rust, so it's a bit
//! over-engineered for what it is.

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
use thiserror::Error;
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
#[derive(Debug, Error)]
enum XattrError {
    #[error("I/O error while reading xattr `{attr}` on {path}: {source}")]
    Io {
        path: PathBuf,
        attr: String,
        #[source]
        source: io::Error,
    },

    #[error("xattr `{attr}` missing on {path}")]
    MissingXattr { path: PathBuf, attr: String },

    #[error("xattr `{attr}` on {path} is not valid UTF-8: {source}")]
    Utf8 {
        path: PathBuf,
        attr: String,
        #[source]
        source: std::str::Utf8Error,
    },

    #[error("xattr `{attr}` on {path} failed to parse as {ty}: {msg}")]
    Parse {
        path: PathBuf,
        attr: String,
        ty: &'static str,
        msg: String,
    },
}

impl XattrError {
    pub fn is_path_not_found(&self) -> bool {
        matches!(self, XattrError::Io { source, .. } if source.kind() == ErrorKind::NotFound)
    }
}

/// Read an xattr and parse it as a numeric type.
fn get_xattr_numeric<T>(path: &Path, attr: &str) -> Result<T, XattrError>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    let bytes = xattr::get(path, attr).map_err(|e| XattrError::Io {
        path: path.to_owned(),
        attr: attr.to_owned(),
        source: e,
    })?;
    let bytes = bytes.ok_or_else(|| XattrError::MissingXattr {
        path: path.to_owned(),
        attr: attr.to_owned(),
    })?;

    let s = std::str::from_utf8(&bytes).map_err(|e| XattrError::Utf8 {
        path: path.to_owned(),
        attr: attr.to_owned(),
        source: e,
    })?;

    let s = s.trim_matches(|c: char| c.is_whitespace() || c == '\0');
    s.parse::<T>().map_err(|e| XattrError::Parse {
        path: path.to_owned(),
        attr: attr.to_owned(),
        ty: std::any::type_name::<T>(),
        msg: e.to_string(),
    })
}

fn get_file_counts_numeric(dir: &Path) -> Result<(u32, u32), XattrError> {
    let num_files: u32 = get_xattr_numeric(dir, "ceph.dir.files")?;
    let num_rfiles: u32 = get_xattr_numeric(dir, "ceph.dir.rfiles")?;
    Ok((num_files, num_rfiles))
}

fn get_file_counts_numeric_ctl(dir: &Path) -> Result<(u32, u32), MainLoopCtl> {
    let counts = match get_file_counts_numeric(&dir) {
        Ok(v) => v,
        Err(e) if e.is_path_not_found() => {
            // directory disappeared -- this is expected
            return Err(MainLoopCtl::Continue);
        }
        Err(e) => {
            error!("Fatal: failed to process {:?}: {:?}", dir, e);
            return Err(MainLoopCtl::Break);
        }
    };
    Ok(counts)
}

fn get_subdirs_ctl(dir: &Path) -> Result<Vec<PathBuf>, MainLoopCtl> {
    let read_dir = match fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == ErrorKind::NotFound => {
            // directory disappearances are normal
            return Err(MainLoopCtl::Continue);
        }
        Err(e) => {
            error!("Fatal: failed to process {:?}: {:?}", dir, e);
            return Err(MainLoopCtl::Break);
        }
    };
    let mut subdirs = Vec::<PathBuf>::new();
    for entry_res in read_dir {
        let direntry = match entry_res {
            Ok(e) => e,
            Err(e) => {
                error!("Skipping entry in {:?}: {:?}", dir, e);
                return Err(MainLoopCtl::Break);
            }
        };
        let ft = match direntry.file_type() {
            Ok(ft) => ft,
            Err(e) => {
                error!("File type error {:?} {:?} {:?}", dir, direntry.path(), e);
                return Err(MainLoopCtl::Break);
            }
        };
        if !ft.is_dir() {
            continue;
        }
        subdirs.push(direntry.path());
    }
    Ok(subdirs)
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
        let ctl = process_dir(
            &dir,
            &dir_tx,
            &match_tx,
            &pending_dir_count,
            min_file_count,
            dbg,
        );
        match ctl {
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
    // This function must not return before updating pending_dir_count,
    // even if problems are encountered. To accomplish this, actual directory
    // processing is done inside the closure below.
    let loop_ctl_signal = (|| -> MainLoopCtl {
        let (num_files, num_rfiles) = if !dbg.no_xattrs {
            match get_file_counts_numeric_ctl(&dir) {
                Ok(v) => v,
                Err(loop_signal) => return loop_signal,
            }
        } else {
            (0, 0)
        };

        // Are we a match?
        if num_files >= min_file_count || dbg.no_xattrs {
            if match_tx.send((dir.clone(), num_files)).is_err() {
                error!("Fatal: match_tx disconnected");
                return MainLoopCtl::Break;
            }
        }

        // Do we need to traverse subdirs?
        if num_rfiles.saturating_sub(num_files) >= min_file_count || dbg.no_xattrs {
            let subdir_paths = match get_subdirs_ctl(&dir) {
                Ok(v) => v,
                Err(loop_signal) => return loop_signal,
            };
            for path in subdir_paths {
                // Semantically, pending_dir_count counts queued + in-flight directories.
                // More precisely, the invariant is: pending_dir_count >= queued + in-flight,
                // so increment before enqueue.
                pending_dir_count.fetch_add(1, Ordering::Relaxed);
                if dir_tx.send(WorkItem::Dir(path)).is_err() {
                    error!("Fatal: dir_tx disconnected");
                    return MainLoopCtl::Break;
                }
            }
        }
        MainLoopCtl::Continue
    })();

    if pending_dir_count.fetch_sub(1, Ordering::AcqRel) == 1 {
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
