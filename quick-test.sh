#!/bin/bash

cargo build --release
sudo $(pwd)/target/release/cephfs-find-wide-dirs -m 0 / | grep -v '^/proc' | sed 's/ 0$//' | sort > me.test
sudo fd --unrestricted --type d . / | grep -v '^/proc' | sort > fd.test

diff -u me.test fd.test
