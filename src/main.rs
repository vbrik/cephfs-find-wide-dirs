use clap::Parser;
use crossbeam_channel::{Receiver, Sender, unbounded};
use log::error;
use std::fs;
use std::io;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::thread;
use xattr;

#[derive(Debug)]
enum WorkItem {
    Dir(PathBuf), // process this dir
    Shutdown,     // shut down
}

#[derive(Debug, Clone, Copy)]
enum MainLoopCtl {
    Continue,
    Break,
}

#[derive(Debug)]
#[allow(dead_code)]
enum XattrError {
    Io(io::Error),
    Utf8(std::str::Utf8Error),
    Parse(String),
    NotFound,
}
impl From<io::Error> for XattrError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}
impl From<std::str::Utf8Error> for XattrError {
    fn from(e: std::str::Utf8Error) -> Self {
        Self::Utf8(e)
    }
}

/// Ensures decrement happens even in the case of early bail-outs
pub struct PendingCountGuard<'a> {
    count: &'a AtomicU64,
    active: bool,
}

impl<'a> PendingCountGuard<'a> {
    pub fn new(pending: &'a AtomicU64) -> Self {
        Self {
            count: pending,
            active: true,
        }
    }
    pub fn normal_finish(mut self) -> u64 {
        self.active = false;
        self.count.fetch_sub(1, Ordering::AcqRel)
    }
}

impl Drop for PendingCountGuard<'_> {
    fn drop(&mut self) {
        if self.active {
            self.count.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

fn get_xattr_numeric<T>(path: &Path, attr: &str) -> Result<T, XattrError>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    let bytes = xattr::get(path, attr)?.ok_or(XattrError::NotFound)?;
    let s = std::str::from_utf8(&bytes)?;
    let s = s.trim_matches(|c: char| c.is_whitespace() || c == '\0');
    s.parse::<T>().map_err(|e| XattrError::Parse(e.to_string()))
}

fn get_file_counts_numeric(dir: &Path) -> Result<(i64, i64), XattrError> {
    let num_files: i64 = get_xattr_numeric(dir, "ceph.dir.files")?;
    let num_rfiles: i64 = get_xattr_numeric(dir, "ceph.dir.rfiles")?;
    Ok((num_files, num_rfiles))
}

fn crawler_worker_loop(
    dir_rx: Receiver<WorkItem>,        // incoming work queue
    dir_tx: Sender<WorkItem>,          // outgoing work queue
    match_tx: Sender<(PathBuf, i64)>,  // outgoing match queue
    pending_dir_count: Arc<AtomicU64>, // upper limit on pending and in-flight dirs (when to stop)
    min_file_count: i64,
) {
    loop {
        let Ok(work) = dir_rx.recv() else {
            error!("dir_rx error");
            break;
        };
        let dir = match work {
            WorkItem::Dir(dir) => dir,
            WorkItem::Shutdown => {
                break;
            }
        };
        match process_dir(&dir, &dir_tx, &match_tx, &pending_dir_count, min_file_count) {
            MainLoopCtl::Continue => continue,
            MainLoopCtl::Break => break,
        }
    }
    let _ = dir_tx.send(WorkItem::Shutdown);
}

fn process_dir(
    dir: &PathBuf,
    dir_tx: &Sender<WorkItem>,
    match_tx: &Sender<(PathBuf, i64)>,
    pending_dir_count: &Arc<AtomicU64>,
    min_file_count: i64,
) -> MainLoopCtl {
    // the guard guarantees pending_dir_count will be decremented even if we return early
    let guard = PendingCountGuard::new(&pending_dir_count);

    let (num_files, num_rfiles) = match get_file_counts_numeric(&dir) {
        Ok(v) => v,
        Err(XattrError::Io(e)) if e.kind() == ErrorKind::NotFound => {
            // directory disappeared
            return MainLoopCtl::Continue;
        }
        Err(e) => {
            error!("Failed to process {:?} with error {:?}", dir, e);
            return MainLoopCtl::Break;
        }
    };

    if num_files >= min_file_count {
        if match_tx.send((dir.clone(), num_files)).is_err() {
            error!("match_tx error");
            return MainLoopCtl::Break;
        }
    }

    if num_rfiles - num_files >= min_file_count {
        for direntry in fs::read_dir(&dir).into_iter().flatten().flatten() {
            let Ok(ft) = direntry.file_type() else {
                continue;
            };
            if !ft.is_dir() {
                continue;
            }

            // Semantically, pending_dir_count counts queued + in-flight directories.
            // More precisely, the invariant is: pending_dir_count >= queued + in-flight,
            // so increment before enqueue.
            pending_dir_count.fetch_add(1, Ordering::Relaxed);
            if dir_tx.send(WorkItem::Dir(direntry.path())).is_err() {
                error!("dir_tx error");
                return MainLoopCtl::Break; //disconnected
            }
        }
    }
    // Finish this directory: .finish() will deactivate the guard
    // so no double decrement will happen
    let prev = guard.normal_finish();
    if prev == 1 {
        // Finishing this directory made pending go 1 -> 0. This means no more work.
        return MainLoopCtl::Break;
    }
    MainLoopCtl::Continue
}

#[derive(Parser, Debug)]
#[command(
    about = "Quickly find cephfs directories that have more than a certain number of files. \
                    Uses cephfs extended attributes ceph.dir.{files,rfiles}."
)]
struct Args {
    /// Search root
    #[arg(value_name = "PATH")]
    search_root: String,

    /// Minimum number of files
    #[arg(short, long = "min-num-files", value_name = "NUMBER")]
    min_files: i64,

    /// Number of threads
    #[arg(short, long = "threads", value_name = "NUMBER", default_value_t = 16)]
    num_threads: usize,
}

fn main() {
    env_logger::init();
    let args = Args::parse();

    let root = PathBuf::from(args.search_root);
    if !root.is_dir() {
        eprintln!("error: not a directory: {:?}", root);
        std::process::exit(2);
    }

    let n_workers = args.num_threads;

    let (dir_tx, dir_rx) = unbounded::<WorkItem>();
    let (match_tx, match_rx) = unbounded::<(PathBuf, i64)>();

    let dirs_pending = Arc::new(AtomicU64::new(0));

    dirs_pending.fetch_add(1, Ordering::Relaxed);
    dir_tx
        .send(WorkItem::Dir(root))
        .expect("failed to enqueue root");

    let mut workers = Vec::with_capacity(n_workers);
    for _ in 0..n_workers {
        workers.push({
            let dir_rx = dir_rx.clone();
            let dir_tx = dir_tx.clone();
            let match_tx = match_tx.clone();
            let pending = Arc::clone(&dirs_pending);
            thread::spawn(move || {
                crawler_worker_loop(dir_rx, dir_tx, match_tx, pending, args.min_files)
            })
        });
    }

    // Close match sender clone so match_rx ends when workers exit
    drop(match_tx);
    // Drain matches until workers are done (all match_tx clones dropped)
    while let Ok((path, num_files)) = match_rx.recv() {
        println!("{}/ {}", path.display(), num_files);
    }

    for h in workers {
        let _ = h.join();
    }
}
