use anyhow::{Context, Result};
use clap::Parser;
use crossbeam_channel::{Receiver, Sender, bounded};
use log::error;
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
    #[arg(value_name = "NUMBER")]
    min_files: i64,

    /// Number of threads
    #[arg(long = "threads", value_name = "NUMBER", default_value_t = 16)]
    num_threads: usize,
}

/// How a directory should be handled after examining its xattrs.
#[derive(Debug, Clone)]
pub enum Decision {
    /// Directory matches. Do NOT traverse its subdirectories.
    MatchPrune(i64),
    /// Directory matches. DO traverse its subdirectories.
    MatchContinue(i64),
    /// Directory does NOT match. Do NOT traverse its subdirectories.
    Prune,
    /// Directory does NOT match. DO traverse its subdirectories.
    Continue,
}

#[derive(Debug)]
enum Work {
    Dir(PathBuf), // process this dir
    Stop,         // shut down
}

fn get_xattr_numeric(path: &Path, attr: &str) -> io::Result<i64> {
    let bytes = xattr::get(path, attr)?
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "xattr not present"))?;

    let s =
        std::str::from_utf8(&bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let s = s.trim_matches(|c: char| c.is_whitespace() || c == '\0');

    if s.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "xattr value is empty after trimming",
        ));
    }

    s.parse::<i64>()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

fn classify_dir(dir: &Path, min_file_count: i64) -> io::Result<Decision> {
    let num_files = get_xattr_numeric(dir, "ceph.dir.files")?;
    let num_rfiles = get_xattr_numeric(dir, "ceph.dir.rfiles")?;
    // number of files in all subdirectories
    let num_sub_rfiles = num_rfiles - num_files;

    if num_files >= min_file_count {
        // match
        if num_sub_rfiles >= min_file_count {
            Ok(Decision::MatchContinue(num_files))
        } else {
            Ok(Decision::MatchPrune(num_files))
        }
    } else {
        // no match
        if num_sub_rfiles >= min_file_count {
            Ok(Decision::Continue)
        } else {
            Ok(Decision::Prune)
        }
    }
}

fn crawler_worker(
    dir_rx: Receiver<Work>,
    dir_tx: Sender<Work>,
    match_tx: Sender<PathBuf>,
    pending_dir_count: Arc<AtomicU64>,
    min_file_count: i64,
) {
    loop {
        let Ok(work) = dir_rx.recv() else {
            error!("dir_rx error");
            break; // disconnected
        };
        let dir = match work {
            Work::Dir(dir) => dir,
            Work::Stop => {
                // Keep the shutdown signal in the queue for the remaining workers.
                let _ = dir_tx.send(Work::Stop);
                break;
            }
        };

        let decision = match classify_dir(&dir, min_file_count) {
            Ok(v) => v,
            Err(e) if e.kind() == ErrorKind::NotFound => {
                continue;
            }
            Err(_) => {
                error!("Failed to process {:?} with error {}", dir, min_file_count);
                let _ = dir_tx.send(Work::Stop);
                break;
            }
        };

        if matches!(
            decision,
            Decision::MatchPrune(_) | Decision::MatchContinue(_)
        ) {
            if match_tx.send(dir.clone()).is_err() {
                error!("match_tx error");
                break;
            }
        }

        if matches!(decision, Decision::MatchContinue(_) | Decision::Continue) {
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
                    break; // disconnected
                }
            }
        }

        // Finish this directory.
        let prev = pending_dir_count.fetch_sub(1, Ordering::AcqRel);
        if prev == 1 {
            // Finishing this directory made pending go 1 -> 0. This means no more work.
            let _ = dir_tx.send(Work::Stop);
            break;
        }
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
    let queue_capacity: usize = 100_000;

    let (dir_tx, dir_rx) = bounded::<Work>(queue_capacity);
    let (match_tx, match_rx) = bounded::<PathBuf>(queue_capacity);

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
    while let Ok(p) = match_rx.recv() {
        println!("{}/", p.display());
    }

    for h in workers {
        let _ = h.join();
    }
}
