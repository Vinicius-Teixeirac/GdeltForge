try:
    from gdeltforge._version import __version__
except ImportError:
    # _version.py is generated at build/install time by hatch-vcs and
    # won't exist yet in a fresh checkout that hasn't been built.
    __version__ = "0.0.0.dev0"
