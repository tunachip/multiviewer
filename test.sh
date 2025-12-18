#!/usr/bin/env bash
# Quick RTP test of the multiviewer mosaic.

set -euo pipefail

python -m multiviewer.live \
  --registry example_registry.csv \
  --width 1280 \
  --height 720 \
  --font-size 32 \
  --rtp-out 127.0.0.1:5004 \
  --rtp-fps 30 \
  --rtp-sdp-file mosaic.sdp \
  --rtp-ffmpeg-arg "-loglevel info"
