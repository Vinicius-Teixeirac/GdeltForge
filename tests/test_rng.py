from gdeltforge.sampling.rng import ReproducibleRNG


class TestChoice:
    def test_returns_requested_size(self):
        rng = ReproducibleRNG(42)
        result = rng.choice(100, size=10, replace=False)
        assert len(result) == 10

    def test_no_duplicates_without_replacement(self):
        rng = ReproducibleRNG(42)
        result = rng.choice(100, size=50, replace=False)
        assert len(set(result)) == 50

    def test_deterministic_with_same_seed(self):
        r1 = ReproducibleRNG(123).choice(1000, size=20, replace=False)
        r2 = ReproducibleRNG(123).choice(1000, size=20, replace=False)
        assert list(r1) == list(r2)

    def test_different_seeds_diverge(self):
        r1 = ReproducibleRNG(1).choice(1000, size=20, replace=False)
        r2 = ReproducibleRNG(2).choice(1000, size=20, replace=False)
        assert list(r1) != list(r2)

    def test_values_stay_within_range(self):
        rng = ReproducibleRNG(7)
        result = rng.choice(50, size=50, replace=False)
        assert set(result) == set(range(50))


class TestMultinomial:
    def test_counts_sum_to_n(self):
        rng = ReproducibleRNG(1)
        counts = rng.multinomial(100, [0.2, 0.3, 0.5])
        assert counts.sum() == 100
        assert len(counts) == 3

    def test_deterministic_with_same_seed(self):
        c1 = ReproducibleRNG(9).multinomial(50, [0.5, 0.5])
        c2 = ReproducibleRNG(9).multinomial(50, [0.5, 0.5])
        assert list(c1) == list(c2)


class TestRandint:
    def test_single_arg_is_exclusive_upper_bound(self):
        rng = ReproducibleRNG(5)
        for _ in range(50):
            v = rng.randint(10)
            assert 0 <= v < 10

    def test_two_args_is_inclusive_range(self):
        rng = ReproducibleRNG(5)
        for _ in range(50):
            v = rng.randint(5, 10)
            assert 5 <= v <= 10
