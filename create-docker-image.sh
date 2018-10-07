#!/bin/sh
set -E
docker build -t local/raft-peer -f Dockerfile .
