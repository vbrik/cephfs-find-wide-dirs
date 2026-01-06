use clap::Parser;
use crossbeam_channel::{Receiver, Sender, unbounded};
use log::{error, info, warn};
use std::fs;
use std::io;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::thread;
use xattr;

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

#[derive(Debug)]
enum Work {
    Dir(PathBuf), // process this dir
    Shutdown,     // shut down
}

/// Parse and return the value of an integer extended attribute
fn get_xattr_i64(path: &Path, attr: &str) -> io::Result<i64> {
    let bytes = xattr::get(path, attr)?.ok_or_else(|| {
        io::Error::new( ErrorKind::Other, format!("xattr error path={:?}, attr={}", path, attr))
    })?;
    let s = std::str::from_utf8(&bytes).map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?;
    let s = s.trim_matches(|c: char| c.is_whitespace() || c == '\0');
    s.parse::<i64>()
        .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))
}

fn get_file_counts(dir: &Path) -> io::Result<(i64, i64)> {
    let num_files = get_xattr_i64(dir, "ceph.dir.files")?;
    let num_rfiles = get_xattr_i64(dir, "ceph.dir.rfiles")?;
    Ok((num_files, num_rfiles))
}


/// Thread worker
fn crawler_worker(
    dir_rx: Receiver<Work>,            // incoming work queue
    dir_tx: Sender<Work>,              // outgoing work queue
    match_tx: Sender<(PathBuf, i64)>,         // outgoing match queue
    pending_dir_count: Arc<AtomicU64>, // upper limit on pending and in-flight dirs (when to stop)
    min_file_count: i64,
) {
    loop {
        //print!(".");
        let Ok(work) = dir_rx.recv() else {
            error!("dir_rx error");
            break; // disconnected
        };
        let dir = match work {
            Work::Dir(dir) => dir,
            Work::Shutdown => {
                //error!("shutting down");
                // Keep the shutdown signal in the queue for the remaining workers.
                let _ = dir_tx.send(Work::Shutdown);
                break;
            }
        };

        let (num_files, num_rfiles) = match get_file_counts(&dir) {
            Ok(v) => v,
            Err(e) if e.kind() == ErrorKind::NotFound => {
                continue;
            }
            Err(_) => {
                error!("Failed to process {:?} with error {}", dir, min_file_count);
                let _ = dir_tx.send(Work::Shutdown);
                break;
            }
        };

        if num_files >= min_file_count {
            if match_tx.send((dir.clone(), num_files)).is_err() {
                error!("match_tx error");
                let _ = dir_tx.send(Work::Shutdown);
                break;
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
                if dir_tx.send(Work::Dir(direntry.path())).is_err() {
                    error!("dir_tx error");
                    let _ = dir_tx.send(Work::Shutdown);
                    break; // disconnected
                }
            }
        }

        // Finish this directory.
        let prev = pending_dir_count.fetch_sub(1, Ordering::AcqRel);
        if prev == 1 {
            //error!("stopping {}", prev);
            // Finishing this directory made pending go 1 -> 0. This means no more work.
            let _ = dir_tx.send(Work::Shutdown);
            break;
        }
        //error!("{}", prev);
    }
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

    let (dir_tx, dir_rx) = unbounded::<Work>();
    let (match_tx, match_rx) = unbounded::<(PathBuf, i64)>();

    let dirs_pending = Arc::new(AtomicU64::new(0));

    dirs_pending.fetch_add(1, Ordering::Relaxed);
    dir_tx
        .send(Work::Dir(root))
        .expect("failed to enqueue root");

    let mut workers = Vec::with_capacity(n_workers);
    for _ in 0..n_workers {
        workers.push({
            let dir_rx = dir_rx.clone();
            let dir_tx = dir_tx.clone();
            let match_tx = match_tx.clone();
            let pending = Arc::clone(&dirs_pending);
            thread::spawn(move || crawler_worker(dir_rx, dir_tx, match_tx, pending, args.min_files))
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
