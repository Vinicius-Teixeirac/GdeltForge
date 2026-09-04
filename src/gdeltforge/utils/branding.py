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
VOID = (0x07, 0x0B, 0x16)  # the docs site's own dark background; used here as
# badge text, dark-on-light/dark-on-orange, not as a background of its own

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


def colorize(
    text: str, rgb: tuple[int, int, int], *, bg: tuple[int, int, int] | None = None
) -> str:
    """Wrap text in a 24-bit ANSI foreground color, optionally with a
    background fill too (for a solid badge rather than colored text), or
    return it unchanged when color is unavailable, so callers never need
    their own branch. A single trailing reset clears both.
    """
    if not supports_color():
        return text
    r, g, b = rgb
    codes = f"\x1b[38;2;{r};{g};{b}m"
    if bg is not None:
        br, bg_g, bb = bg
        codes += f"\x1b[48;2;{br};{bg_g};{bb}m"
    return f"{codes}{text}\x1b[0m"


# Big block G/F monogram, 6 rows x 18 columns, each row split at column 9:
# the left half draws G, the right half draws F. Verified character-by-
# character to be exactly 18 columns wide on every row before the color
# split was added, so the two halves always land on the same boundary
# regardless of which row's glyph strokes happen to be there. Echoes the
# real brand monogram (lockup-e-monogram.png: a bisected G/F with a
# meridian line through the middle) at CLI scale, in place of the old
# banner's thin single-line G/F, which read as illegible punctuation
# rather than either letter.
_MONOGRAM_ROWS = (
    "   _____   ______ ",
    "  / ____| |  ____|",
    " | |  __  | |__   ",
    "─| | |_ |─|  __|──",
    " | |__| | | |     ",
    "  \\_____| |_|     ",
)
_MONOGRAM_SPLIT = 9


def full_banner(version: str, tagline: str) -> str:
    """The big block G/F monogram (see _MONOGRAM_ROWS), G in near-white and
    F in forge orange matching the real lockup's own two-tone treatment,
    with the wordmark and tagline set beside it rather than below: the
    glyphs alone are already 6 rows tall, and stacking more text under
    them would push the whole banner past a comfortable single screenful.
    Plain ASCII/box-drawing only, no typographic dashes. Reserved for
    --version; routine interactive starts get compact_emblem instead.
    """
    lines = [
        colorize(row[:_MONOGRAM_SPLIT], NEARWHITE) + colorize(row[_MONOGRAM_SPLIT:], FORGE)
        for row in _MONOGRAM_ROWS
    ]
    lines[2] += f"  {colorize('GdeltForge', NEARWHITE)}  {colorize(version, FORGE)}"
    lines[3] += f"  {colorize(tagline.lower(), SLATE)}"
    return "\n".join(lines)


def compact_emblem(version: str) -> str:
    """The one-line emblem for routine interactive starts: a solid two-tone
    pill, " G" on a near-white fill and "F " on a forge-orange fill, split
    right at the G/F boundary, both letters set in Void (the docs site's
    own dark background color, #070B16) for contrast against either fill.
    A literal, filled-in miniature of the real lockup (a bisected G/F)
    rather than line art trying to draw the same letters out of thin
    strokes at a scale with no room for them, which is why two earlier
    one-line designs failed: first as thin ASCII that read as illegible
    punctuation, then as "G━F" that read as a dash between two stray
    capitals, then as an abstract orb-and-meridian mark that dropped
    letterforms entirely and, in turn, read as too little presence. Actual
    background fill gives this one real visual weight without needing to
    render letterforms out of strokes; full_banner still reserves the
    large-scale line-art version for --version, where there's room for it
    to read cleanly. The rest of the line stays muted, deliberately: this
    prints on every single command, not just --version, so only the pill
    itself should draw the eye."""
    return (
        f"{colorize(' G', VOID, bg=NEARWHITE)}{colorize('F ', VOID, bg=FORGE)} "
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
