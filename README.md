# Multiviewer

Prototype multiviewer that composes many RTP feeds onto a grid.

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# generate static mosaic
python -m multiviewer.cli --registry example_registry.csv --width 1280 --height 720 --font-size 32 --output mosaic.png

# run live multiviewer (press q or Esc to quit)
python -m multiviewer.live --registry example_registry.csv --width 1280 --height 720 --font-size 32
```

Registry entries should include `ipAddress`; plain IPs get prefixed with `rtp://`. If you already have full URLs, include the scheme and they will be used as-is. Use `--font /path/to/font.ttf` to select a specific TTF.
- Pass extra ffmpeg input options with `--ffmpeg-opt key=value` (repeatable), e.g. `--ffmpeg-opt rtpflags=send_bye`.
- To stream the composed mosaic over RTP: add `--rtp-out <dest>:6910 --rtp-fps 30`. Example:
  ```bash
  python -m multiviewer.live --registry example_registry.csv --width 1280 --height 720 --font-size 32 --rtp-out 192.168.1.50:6910 --rtp-fps 30
  ```
  Use `--rtp-ffmpeg-arg` to pass extra ffmpeg args for the output (repeatable, quote groups), e.g.:
  ```bash
  --rtp-ffmpeg-arg "-sdp_file mosaic.sdp" --rtp-ffmpeg-arg "-loglevel info"
  ```
  If your ffmpeg build lacks `libx264`, pick another encoder with `--rtp-encoder mpeg4` (or `h264_nvenc` if available).

### GUI note
The live viewer uses OpenCV’s HighGUI (`cv2.namedWindow`/`imshow`). Install the non-headless package (`opencv-python`) and make sure your environment has GUI backends (GTK/Qt/X11) available; otherwise you’ll see a `cvNamedWindow` “re-run cmake” error. On headless servers, run under Xvfb/VNC or redirect to a machine with a display.
If you only need RTP output, run with `--no-window` to skip GUI entirely.

## Channel selection GUI
Launch a simple Tk-based selector with fuzzy search and only start the chosen channels:
```bash
python -m multiviewer.selector --registry example_registry.csv --width 1280 --height 720 --font-size 32
```
- Type to filter; select multiple entries; click “Launch Selected.”
- Behind the scenes it calls `multiviewer.live` with `--channel <name>` for each selection. You can also pass `--channel` flags directly to `multiviewer.live` if you prefer the CLI.

## Remote RTP to a Windows client
Use `scripts/launch_rtp.sh` on the server to stream to a Windows client, then copy the SDP over and open it in VLC:
```bash
# on the server
./scripts/launch_rtp.sh <windows_ip> <registry_csv> 5004

# from Windows (PowerShell) to fetch SDP, assuming OpenSSH client is installed:
scp user@<server_ip>:/tmp/mosaic_<windows_ip_with_underscores>_5004.sdp $HOME\\Downloads\\mosaic.sdp
# open the downloaded mosaic.sdp in VLC (Media -> Open File)
```
If the Windows box runs the sender command via SSH, ensure SSH keys/creds are set up; adjust paths as needed.

## Web launcher (enter IP, pick channels)
A lightweight web UI serves channel selection and starts an RTP stream, exposing the SDP for download:
```bash
python -m multiviewer.web --host 0.0.0.0 --port 8080 --registry example_registry.csv
```
Then browse to `http://<server_ip>:8080`, enter your receiving IP/port, select channels, click “Start Stream,” and download the SDP link to open in VLC. The server runs `multiviewer.live` headless for that request.
The UI also offers an “Open in VLC” link using the `vlc://` protocol; browsers may prompt/ask for permission.

Browser playback (HLS): check “Play in browser (HLS)” in the web UI. It will start an HLS stream, expose `index.m3u8`, and play it inline using hls.js if supported by the browser.
If the HLS stream fails to start, the server will return an error instead of serving a missing playlist.
The web UI also shows active sessions and lets you stop them; only one session per target IP is kept, and sessions auto-expire after a configurable duration (default 5 minutes). Advanced settings in the UI let you tweak encoder/bitrate/FPS/HLS segment size.
