from unittest import mock

from gdeltforge.utils import branding


class TestSupportsColor:
    def test_false_when_not_a_tty(self, monkeypatch):
        monkeypatch.delenv("NO_COLOR", raising=False)
        with mock.patch("sys.stdout.isatty", return_value=False):
            assert branding.supports_color() is False

    def test_false_when_no_color_is_set_even_on_a_real_tty(self, monkeypatch):
        monkeypatch.setenv("NO_COLOR", "1")
        with mock.patch("sys.stdout.isatty", return_value=True):
            assert branding.supports_color() is False

    def test_true_on_a_real_tty_with_no_color_unset(self, monkeypatch):
        monkeypatch.delenv("NO_COLOR", raising=False)
        with mock.patch("sys.stdout.isatty", return_value=True), \
             mock.patch("gdeltforge.utils.branding._enable_windows_vt_mode"):
            assert branding.supports_color() is True


class TestColorize:
    def test_returns_plain_text_without_color_support(self, monkeypatch):
        monkeypatch.delenv("NO_COLOR", raising=False)
        with mock.patch("sys.stdout.isatty", return_value=False):
            assert branding.colorize("hello", branding.FORGE) == "hello"

    def test_wraps_in_ansi_truecolor_escape_when_supported(self, monkeypatch):
        monkeypatch.delenv("NO_COLOR", raising=False)
        with mock.patch("sys.stdout.isatty", return_value=True), \
             mock.patch("gdeltforge.utils.branding._enable_windows_vt_mode"):
            result = branding.colorize("hello", (1, 2, 3))
            assert result == "\x1b[38;2;1;2;3mhello\x1b[0m"


class TestFullBanner:
    def test_contains_the_version_and_lowercased_tagline(self, monkeypatch):
        monkeypatch.delenv("NO_COLOR", raising=False)
        with mock.patch("sys.stdout.isatty", return_value=False):
            text = branding.full_banner("1.2.3", "Global Event Data Pipeline")
        assert "1.2.3" in text
        assert "global event data pipeline" in text
        assert "GdeltForge" in text

    def test_is_six_lines(self, monkeypatch):
        # The big block G/F monogram (see _MONOGRAM_ROWS) is 6 rows tall
        # on its own; this replaced the old 4-line thin single-line G/F,
        # which read as illegible punctuation rather than either letter.
        monkeypatch.delenv("NO_COLOR", raising=False)
        with mock.patch("sys.stdout.isatty", return_value=False):
            text = branding.full_banner("1.2.3", "tagline")
        assert len(text.splitlines()) == 6

    def test_every_line_is_the_same_visual_width(self, monkeypatch):
        # Regression guard for the monogram itself: every row of
        # _MONOGRAM_ROWS was verified character-by-character to be
        # exactly 18 columns wide before the wordmark/tagline text (which
        # only ever appends past column 18, never inserts before it) was
        # added, so the G/F split at column 9 lands on the same boundary
        # regardless of which row's glyph strokes are there. Checked with
        # color off, since colorize() with no color support returns the
        # plain text unchanged, so line length here is real column width,
        # not ANSI escape byte length.
        monkeypatch.delenv("NO_COLOR", raising=False)
        with mock.patch("sys.stdout.isatty", return_value=False):
            text = branding.full_banner("1.2.3", "tagline")
        lines = text.splitlines()
        widths = [len(line) for line in lines[:2]] + [len(lines[4]), len(lines[5])]
        assert len(set(widths)) == 1  # the 4 rows with no appended text

    def test_monogram_reads_as_g_and_f_blocks(self, monkeypatch):
        # Not a pixel-perfect render check (that's what the width test
        # above and eyeballing the real output are for), just confirming
        # the actual glyph strokes survive unchanged: this is the one
        # part of the banner that isn't generated from a template, and a
        # future edit nearby could silently corrupt it without any other
        # test noticing.
        monkeypatch.delenv("NO_COLOR", raising=False)
        with mock.patch("sys.stdout.isatty", return_value=False):
            text = branding.full_banner("1.2.3", "tagline")
        lines = text.splitlines()
        assert "_____" in lines[0]
        assert "______" in lines[0]
        assert lines[5].startswith("  \\_____|")

    def test_never_contains_a_typographic_dash(self, monkeypatch):
        # Regression guard: the brand system's own mockup slipped in a real
        # em-dash elsewhere in the document; this banner must stay
        # box-drawing/ASCII only.
        monkeypatch.delenv("NO_COLOR", raising=False)
        with mock.patch("sys.stdout.isatty", return_value=False):
            text = branding.full_banner("1.2.3", "tagline")
        assert "—" not in text
        assert "–" not in text


class TestCompactEmblem:
    def test_contains_the_version_and_package_name(self, monkeypatch):
        monkeypatch.delenv("NO_COLOR", raising=False)
        with mock.patch("sys.stdout.isatty", return_value=False):
            text = branding.compact_emblem("1.2.3")
        assert "1.2.3" in text
        assert "gdeltforge" in text
        assert len(text.splitlines()) == 1


class TestSafePrint:
    def test_prints_normally_when_encoding_succeeds(self, capsys):
        branding.safe_print("hello")
        assert capsys.readouterr().out == "hello\n"

    def test_falls_back_instead_of_raising_on_a_real_encoding_failure(self):
        # Reproduces the actual crash found testing on a legacy Windows
        # console: cp1252 stdout can't encode box-drawing characters, so
        # the first write raises; the fallback re-encodes with that same
        # (real, lossy) encoding under errors="replace", which is what
        # actually turns the unencodable character into "?" before the
        # retry succeeds, not a synthetic always-succeeds mock.
        written = []

        def flaky_write(text):
            if "─" in text:
                raise UnicodeEncodeError("cp1252", text, 0, 1, "character maps to <undefined>")
            written.append(text)

        fake_stdout = mock.MagicMock()
        fake_stdout.encoding = "cp1252"
        fake_stdout.write.side_effect = flaky_write

        with mock.patch("sys.stdout", fake_stdout):
            branding.safe_print("a─b")  # must not raise

        assert written  # the retry actually got through
        assert "─" not in "".join(written)
