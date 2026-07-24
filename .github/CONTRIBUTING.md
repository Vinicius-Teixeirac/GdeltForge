# Contributing to GdeltForge

Thanks for considering a contribution. This project is small and welcomes issues, bug fixes, documentation improvements, and new features alike.

By participating, you're expected to follow the [Code of Conduct](CODE_OF_CONDUCT.md). For security issues, see [SECURITY.md](SECURITY.md) instead of opening a public issue.

## Getting set up

This project uses [uv](https://docs.astral.sh/uv/) for dependency management.

```
git clone https://github.com/Vinicius-Teixeirac/GdeltForge.git
cd GdeltForge
uv sync --group dev
cp config/settings.example.yaml config/settings.yaml
```

Run the test suite (pure unit tests: no network, no browser, no real GDELT data required):

```
uv run pytest
```

To work on the documentation site:

```
uv sync --group docs
uv run mkdocs serve
```

See the [full documentation](https://vinicius-teixeirac.github.io/GdeltForge/) for how the pipeline itself is organized and configured.

## Making a change

1. **Open an issue first for anything non-trivial** (new features, behavior changes, architectural changes) so we can agree on the approach before you put work into it. Small fixes and docs corrections can go straight to a PR.
2. **Branch off `main`** — don't commit directly to it. Name the branch for what it does (`feat/...`, `fix/...`, `docs/...`).
3. **Keep commits atomic and typed**: one logical change per commit, with a conventional-commits-style prefix describing its nature —
   `feat`, `fix`, `perf`, `docs`, `chore`, `refactor`, `test`, or `ci`. Look at the existing git history for the style this repo follows.
4. **Add or update tests** for any behavior change. `uv run pytest` should pass before you open a PR.
5. **Update the docs** (`docs/`) if you're changing anything user-facing — a new CLI flag, a new config key, a changed default.
6. **Open a pull request against `main`** using the PR template. CI (GitHub Actions) runs the test suite and a package build check on Python 3.10 and 3.12 automatically.

## Code style

There's no enforced linter/formatter yet — match the style of the surrounding code (see the modules under `src/gdeltforge/`). Prefer clarity and small, focused functions over cleverness. Comments should explain *why*, not *what* — the code should already say what it does.

## Reporting bugs / requesting features

Use the issue templates (Bug Report / Feature Request) when opening an issue on GitHub — they ask for the details that are usually needed to act on a report (GdeltForge version, Python version, config relevant to the issue, steps to reproduce).

## License

By contributing, you agree that your contributions will be licensed under this project's [Apache License 2.0](../LICENSE).
