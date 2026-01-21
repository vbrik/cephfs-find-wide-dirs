#!/bin/bash

cargo build --release
sudo $(pwd)/target/release/cephfs-find-wide-dirs -m 0 /home | grep -v '^/proc' | sed 's/ 0$//' | sort > me.test
sudo fd --unrestricted --type d . /home | grep -v '^/proc' | sort > fd.test

diff -u me.test fd.test
