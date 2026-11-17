"""Contrast and console-cleanliness defects found in the live Chrome pass."""
import re
from pathlib import Path

DASH = Path(__file__).resolve().parents[2] / "src/ton_sentinel/dashboard"
CSS = (DASH / "app.css").read_text()


def _lum(hexcolor):
    h = hexcolor.lstrip("#")
    ch = [int(h[i:i + 2], 16) / 255 for i in (0, 2, 4)]
    ch = [c / 12.92 if c <= 0.03928 else ((c + 0.055) / 1.055) ** 2.4 for c in ch]
    return 0.2126 * ch[0] + 0.7152 * ch[1] + 0.0722 * ch[2]


def _ratio(a, b):
    hi, lo = sorted((_lum(a), _lum(b)), reverse=True)
    return (hi + 0.05) / (lo + 0.05)


def _decl(selector, prop):
    m = re.search(re.escape(selector) + r"\s*\{[^}]*?\b" + prop + r":\s*(#[0-9a-fA-F]{6})", CSS)
    return m[1] if m else None


def test_muted_text_inherits_dark_colour_inside_light_tiles():
    # .muted is light grey for the dark theme; the light-background tiles must override it.
    muted = re.search(r"--color-muted:\s*(#[0-9a-f]{6})", CSS)[1]
    for tile in (".tile-unavailable", ".pipe-not-measured"):
        bg = _decl(tile, "background") or _decl(".pipe-tile" + tile, "background")
        if tile == ".pipe-not-measured":
            bg = re.search(r"\.pipe-not-measured \.pipe-tile[^{]*\{[^}]*background:\s*(#[0-9a-f]{6})", CSS)[1]
        assert _ratio(muted, bg) < 4.5  # confirms the override is needed
        rule = re.search(re.escape(tile) + r"[^{]*\.muted[^{]*\{([^}]*)\}", CSS)
        assert rule, f"no .muted override inside {tile}"
        assert "color: inherit" in rule[1] or "color:inherit" in rule[1]


def test_favicon_request_is_204_not_a_console_404():
    import io
    from ton_sentinel.api import create_app
    seen = {}
    environ = {"REQUEST_METHOD": "GET", "PATH_INFO": "/favicon.ico", "QUERY_STRING": "",
               "wsgi.input": io.BytesIO()}
    body = b"".join(create_app(dsn="postgresql://unused/none")(
        environ, lambda status, headers, *exc: seen.update(status=status)))
    assert seen["status"].startswith("204") and body == b""
