from collections.abc import Sequence

import numpy as np

from gdeltforge.utils.logging import get_logger

logger = get_logger(__name__)


class ReproducibleRNG:
    """
    Thin wrapper around numpy.random.Generator adding logging
    and exposing commonly used operations: choice, multinomial, randint.
    """
    def __init__(self, seed: int | None):
        # A concrete seed is kept on self even when the caller passed None:
        # CalendarSampler.get_calendar_samples and FilteredSampler.
        # get_stratified_sample each derive one independent child generator
        # per group they see (see samplers._group_rng), and need a real
        # int to derive from either way. Drawing one here when seed is
        # None doesn't make that case any more reproducible than before:
        # np.random.default_rng(None) below still seeds from OS entropy,
        # this just captures the value it would otherwise have thrown away.
        if seed is None:
            seed = int(np.random.SeedSequence().generate_state(1)[0])
        self.seed = seed
        self.rng = np.random.default_rng(seed)
        logger.info(f"random seed set: {seed}")

    def choice(self, n, size, replace=False):
        return self.rng.choice(n, size=size, replace=replace)

    def multinomial(self, n: int, pvals: Sequence[float]) -> np.ndarray:
        """
        Draw a single sample from a multinomial distribution.
        Returns an array of counts summing to n, one per category in pvals.
        pvals must sum to 1.0.
        """
        return self.rng.multinomial(n, pvals)

    def randint(self, low, high=None):
        """
        Wrapper for Generator.integers().
        - If only 'low' is provided: behaves like numpy randint(high).
        - If 'low' and 'high': integers in [low, high].
        """
        if high is None:
            # emulate numpy.randint(high) -> [0, high)
            return self.rng.integers(0, low)
        else:
            # emulate numpy.randint(low, high) -> [low, high]
            return self.rng.integers(low, high + 1)
