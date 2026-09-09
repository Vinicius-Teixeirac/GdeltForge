import tqdm

# tqdm starts a background monitor thread (checking for stalled bars) the
# first time any tqdm instance is created anywhere in the process, and
# that thread stays alive for the rest of the process's lifetime. Every
# convert/filter/scrape stage here forks a real ProcessPoolExecutor
# worker pool on Linux (the platform default start method), and fork()
# only clones the calling thread: if the monitor thread happens to hold
# tqdm's own class-level lock at the instant of fork, the child inherits
# that lock already held, with no thread left in the child that could
# ever release it. Any tqdm instance created inside a worker afterward
# deadlocks permanently, hanging the whole run with no error, no
# traceback, nothing in the log.
#
# This never surfaces from a single CLI invocation, since that's a fresh
# process with no tqdm history yet, but it does the moment a longer-lived
# process runs more than one gdeltforge stage back to back, e.g. this
# project's own pytest suite: whichever test first creates a real
# ProcessPoolExecutor after an earlier test already started a tqdm bar
# hangs, invisible on Windows (always spawn, never fork, so nothing is
# ever inherited mid-lock) but permanent on Linux CI. Setting
# monitor_interval to 0 stops tqdm from ever starting that thread at all
# (its own documented switch for exactly this class of problem), which
# costs nothing real here: the monitor only exists to warn about a bar
# that stopped updating, not to drive any bar's own rendering.
tqdm.tqdm.monitor_interval = 0

try:
    from gdeltforge._version import __version__
except ImportError:
    # _version.py is generated at build/install time by hatch-vcs and
    # won't exist yet in a fresh checkout that hasn't been built.
    __version__ = "0.0.0.dev0"
