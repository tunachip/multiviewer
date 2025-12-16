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
