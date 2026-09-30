"""
filter.py

Deprecated: the filter stage is now the clean stage,
gdeltforge.cleaning.cleaner (GDELTCleaner, run_cleaner). This module keeps
the pre-0.12 names importable for the 0.12.x series and warns on import.
"""

import warnings

from gdeltforge.cleaning.cleaner import GDELTCleaner, run_cleaner

warnings.warn(
    "gdeltforge.filtering.filter is deprecated: use gdeltforge.cleaning.cleaner "
    "(GDELTCleaner, run_cleaner). The old names will be removed in a future release.",
    DeprecationWarning,
    stacklevel=2,
)


class GDELTFilter(GDELTCleaner):
    """Deprecated name for GDELTCleaner, with the pre-0.12 method names."""

    def filter_all_files(self, pattern: str = "*.parquet") -> tuple[int, int]:
        return self.clean_all_files(pattern)

    def filter_single_file(self, parquet_path, output_path=None) -> tuple[int, int]:
        return self.clean_single_file(parquet_path, output_path)


run_filter = run_cleaner

__all__ = ["GDELTFilter", "run_filter"]
