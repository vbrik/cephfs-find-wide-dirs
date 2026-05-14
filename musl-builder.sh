#!/bin/bash
set -ex
OUTPUT_DIR=musl-static-bins
NAME_BASE=cephfs-find-wide-dirs
podman build -f Dockerfile.musl-builder -t $NAME_BASE-musl-builder .
podman create --name $NAME_BASE-extract $NAME_BASE-musl-builder
mkdir -p $OUTPUT_DIR
podman cp $NAME_BASE-extract:/build/target/x86_64-unknown-linux-musl/release/cephfs-find-wide-dirs $OUTPUT_DIR
podman rm $NAME_BASE-extract
set +x
echo
echo
echo Results are in $OUTPUT_DIR/
echo
echo
