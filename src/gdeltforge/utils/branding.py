"""GdeltForge's terminal voice: the ASCII banner, status glyphs, and the
color palette from the Brand & Identity System v1.0.

Colors are applied as 24-bit ANSI escapes, gated on both an actual TTY
(never on piped/redirected output, matching the system's own rule) and the
NO_COLOR convention (https://no-color.org/). No new dependency: Windows'
own virtual-terminal processing is enabled directly via ctypes so the
escapes render in a plain console, not just Windows Terminal.
"""

from __future__ import annotations

import os
import sys

# Named colors, straight from the brand system's palette section.
FORGE = (0xE8, 0x91, 0x2A)
SLATE = (0x6C, 0x7C, 0x9E)
NEARWHITE = (0xE8, 0xEC, 0xF5)
PASS = (0x7F, 0xCF, 0x9A)
FAIL = (0xE8, 0x6A, 0x4A)

CHECK = "✓"  # success
CROSS = "✗"  # error
ARROW = "→"  # step / trace


def _enable_windows_vt_mode() -> None:
    """Best-effort: turn on ANSI escape support in a plain Windows console.
    Windows Terminal already supports it; legacy conhost.exe needs this
    flag set once per process. Silently gives up on anything unexpected,
    since this is a cosmetic feature, never worth failing a real command
    over.
    """
    if sys.platform != "win32":
        return
    try:
        import ctypes

        kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
        handle = kernel32.GetStdHandle(-11)  # STD_OUTPUT_HANDLE
        mode = ctypes.c_uint32()
        if not kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
            return
        enable_virtual_terminal_processing = 0x0004
        kernel32.SetConsoleMode(handle, mode.value | enable_virtual_terminal_processing)
    except Exception:
        pass


def supports_color() -> bool:
    """True only on a real, interactive terminal that hasn't opted out via
    NO_COLOR. False for piped/redirected output, matching the brand
    system's "never on piped output" rule, and for CI logs, dumb
    terminals, and anything else that isn't a genuine TTY.
    """
    if os.environ.get("NO_COLOR") is not None:
        return False
    if not sys.stdout.isatty():
        return False
    if sys.platform == "win32":
        _enable_windows_vt_mode()
    return True


def colorize(text: str, rgb: tuple[int, int, int]) -> str:
    """Wrap text in a 24-bit ANSI foreground color, or return it unchanged
    when color is unavailable, so callers never need their own branch.
    """
    if not supports_color():
        return text
    r, g, b = rgb
    return f"\x1b[38;2;{r};{g};{b}m{text}\x1b[0m"


def full_banner(version: str, tagline: str) -> str:
    """The 4-line ASCII banner: G and F, joined by the meridian rule.
    Plain ASCII/box-drawing only, no typographic dashes. Reserved for
    --version; routine interactive starts get compact_emblem instead.
    """
    return (
        f"  ___  {colorize('__', FORGE)}\n"
        f" / __| {colorize('| _|', FORGE)}   "
        f"{colorize('GdeltForge', NEARWHITE)} {colorize(version, SLATE)}\n"
        f"{colorize('─', SLATE)}| (_ |{colorize('─', SLATE)}{colorize('| |', FORGE)}"
        f"{colorize('─' * 22, SLATE)}\n"
        f" \\___| {colorize('|_|', FORGE)}   {colorize(tagline.lower(), SLATE)}"
    )


def compact_emblem(version: str) -> str:
    """The one-line emblem for routine interactive starts."""
    return (
        f"G{colorize('━', SLATE)}{colorize('F', FORGE)}  "
        f"{colorize('gdeltforge', SLATE)} {colorize('·', SLATE)} "
        f"{colorize(version, FORGE)}"
    )


def status_line(glyph: str, rgb: tuple[int, int, int], message: str) -> str:
    """A single status line: colored glyph, plain message. Shared shape for
    success/error/step reporting anywhere in the CLI that wants the
    brand's terminal voice instead of a bare log line.
    """
    return f"{colorize(glyph, rgb)} {message}"


def safe_print(text: str) -> None:
    """print(), but never lets a decorative banner crash the CLI. A legacy
    Windows console using a non-UTF-8 codepage (cp1252 and similar) can't
    encode the box-drawing/glyph characters here and raises
    UnicodeEncodeError on a plain print, confirmed directly against this
    module's own output, not assumed. Falls back to the same text with
    those characters replaced rather than skipping output entirely.
    """
    try:
        print(text)
    except UnicodeEncodeError:
        encoding = sys.stdout.encoding or "ascii"
        print(text.encode(encoding, errors="replace").decode(encoding))
