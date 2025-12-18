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

## Channel selection GUI
Launch a simple Tk-based selector with fuzzy search and only start the chosen channels:
```bash
python -m multiviewer.selector --registry example_registry.csv --width 1280 --height 720 --font-size 32
```
- Type to filter; select multiple entries; click “Launch Selected.”
- Behind the scenes it calls `multiviewer.live` with `--channel <name>` for each selection. You can also pass `--channel` flags directly to `multiviewer.live` if you prefer the CLI.
