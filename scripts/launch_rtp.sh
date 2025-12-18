#!/usr/bin/env bash
# Launch the multiviewer headless and stream the mosaic over RTP to a client IP.

set -euo pipefail

if [ $# -lt 2 ]; then
  echo "Usage: $0 <client_ip> <registry_csv> [port]"
  exit 1
fi

CLIENT_IP="$1"
REGISTRY="$2"
PORT="${3:-5004}"

WIDTH="${WIDTH:-1280}"
HEIGHT="${HEIGHT:-720}"
FONT_SIZE="${FONT_SIZE:-32}"
FPS="${FPS:-30}"
ENCODER="${ENCODER:-mpeg4}" # choose mpeg4 for broad compatibility
SDP_FILE="/tmp/mosaic_${CLIENT_IP//./_}_${PORT}.sdp"

python -m multiviewer.live \
  --registry "$REGISTRY" \
  --width "$WIDTH" \
  --height "$HEIGHT" \
  --font-size "$FONT_SIZE" \
  --rtp-out "${CLIENT_IP}:${PORT}" \
  --rtp-fps "$FPS" \
  --rtp-encoder "$ENCODER" \
  --rtp-sdp-file "$SDP_FILE" \
  --rtp-ffmpeg-arg "-vf format=yuv420p" \
  --rtp-ffmpeg-arg "-loglevel info" \
  --no-window

echo "RTP stream sent to ${CLIENT_IP}:${PORT}"
echo "SDP written to ${SDP_FILE}"
