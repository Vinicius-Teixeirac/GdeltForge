"""
storage.py

Whether polars should memory-map the Parquet files a command reads
(io.memory_map), decided before polars is imported: polars reads
POLARS_FORCE_ASYNC once, when it loads, and with it set reads files
through its object-store path, without mapping them. So this module
imports neither polars nor pyarrow.

A mapped read that fails on network storage (an NFS server that stops
answering for a moment) kills the process with SIGBUS, which Python can't
catch, so it can't be retried either. An unmapped read raises an OSError
instead, which gdeltforge.utils.io retries (io.read_retries). On local
disks mapping costs nothing and saves about a tenth of peak memory, so
"auto" maps unless one of the configured data directories is on a
network filesystem.

Provides:
    - network_filesystem: the network filesystem type a path is on, or None
    - memory_map_decision: whether to map, and why, for a loaded config
"""

from __future__ import annotations

import os
import sys
from collections.abc import Iterable, Mapping
from pathlib import Path, PurePosixPath

from gdeltforge.utils.config import get_dict, resolve_memory_map

POLARS_FORCE_ASYNC = "POLARS_FORCE_ASYNC"

# Whether the gdeltforge launcher set POLARS_FORCE_ASYNC itself (from the
# config), so the command reports the config's reason, not "it is set".
_set_by_launcher = False


def mark_set_by_launcher() -> None:
    """Record that the launcher set POLARS_FORCE_ASYNC for this process."""
    global _set_by_launcher
    _set_by_launcher = True


def environ_without_launcher_setting() -> dict[str, str]:
    """os.environ as the user left it: without the POLARS_FORCE_ASYNC the
    launcher set, so memory_map_decision gives the config's reason."""
    environ = dict(os.environ)
    if _set_by_launcher:
        environ.pop(POLARS_FORCE_ASYNC, None)
    return environ

# Linux filesystem types that live on another machine. 9p and virtiofs are
# left out: they carry a VM's host disk (WSL's /mnt/c, for one), which is
# local storage as far as a failing read goes.
_NETWORK_TYPES = frozenset({
    "nfs", "nfs4", "cifs", "smb3", "smbfs", "ncpfs", "afs", "ceph", "glusterfs",
    "lustre", "gpfs", "beegfs", "fuse.glusterfs", "fuse.cephfs", "fuse.sshfs",
    "sshfs", "fuse.s3fs", "fuse.rclone", "fuse.gcsfuse", "davfs", "fuse.davfs2",
})

# GetDriveTypeW's answer for a network drive.
_DRIVE_REMOTE = 4


def _unescape_mount_field(field: str) -> str:
    """/proc/mounts writes a space, tab, newline or backslash in a path
    as an octal escape (\\040 for a space)."""
    out, i = [], 0
    while i < len(field):
        if field[i] == "\\" and field[i + 1:i + 4].isdigit():
            out.append(chr(int(field[i + 1:i + 4], 8)))
            i += 4
        else:
            out.append(field[i])
            i += 1
    return "".join(out)


def _linux_mounts(mounts_text: str) -> list[tuple[str, str]]:
    """(mount point, filesystem type) for each line of /proc/mounts."""
    mounts = []
    for line in mounts_text.splitlines():
        fields = line.split()
        if len(fields) >= 3:
            mounts.append((_unescape_mount_field(fields[1]), fields[2]))
    return mounts


def _existing_ancestor(path: Path) -> Path:
    """path made absolute, or its nearest ancestor that exists: an output
    directory a command creates later is on its parent's filesystem."""
    path = Path(os.path.abspath(path))
    while not path.exists() and path.parent != path:
        path = path.parent
    try:
        return path.resolve()
    except OSError:
        return path


def network_filesystem(path: str | Path, mounts_text: str | None = None) -> str | None:
    """
    The network filesystem type path is on ("nfs4", "cifs", ...), or None
    when it is local or the platform gives no way to tell. Linux reads
    /proc/mounts (mounts_text stands in for it in tests) and takes the
    longest mount point holding the path; Windows treats a UNC path and a
    mapped network drive as network storage. Other platforms report None.
    """
    if mounts_text is not None or sys.platform.startswith("linux"):
        if mounts_text is None:
            try:
                mounts_text = Path("/proc/mounts").read_text(encoding="utf-8")
            except OSError:
                return None
            text = _existing_ancestor(Path(path)).as_posix()
        else:
            # Given mounts (tests): the path is taken as the absolute POSIX
            # path it names, on any platform.
            text = PurePosixPath(path).as_posix()
        best: tuple[str, str] | None = None
        for point, fstype in _linux_mounts(mounts_text):
            inside = text == point or text.startswith(point.rstrip("/") + "/")
            if inside and (best is None or len(point) > len(best[0])):
                best = (point, fstype)
        return best[1] if best is not None and best[1] in _NETWORK_TYPES else None
    if sys.platform == "win32":
        text = str(_existing_ancestor(Path(path)))
        if text.startswith("\\\\"):
            return "network share"
        import ctypes

        root = os.path.splitdrive(text)[0] + "\\"
        if ctypes.windll.kernel32.GetDriveTypeW(root) == _DRIVE_REMOTE:  # type: ignore[attr-defined]
            return "network drive"
    return None


def _configured_directories(config: dict) -> Iterable[tuple[str, str]]:
    """(paths.<key>, value) for every non-empty string under paths."""
    for key, value in get_dict(config, "paths").items():
        if isinstance(value, str) and value:
            yield f"paths.{key}", value


def memory_map_decision(
    config: dict, environ: Mapping[str, str] | None = None,
    mounts_text: str | None = None,
) -> tuple[bool, str]:
    """
    (map the files?, why) for this config. An exported POLARS_FORCE_ASYNC
    decides on its own: polars reads it whatever the config says. true and
    false are taken as given; auto maps unless a configured directory is
    on a network filesystem, and names the first one found.
    """
    environ = os.environ if environ is None else environ
    exported = environ.get(POLARS_FORCE_ASYNC)
    if exported is not None:
        mapped = exported != "1"
        return mapped, f"{POLARS_FORCE_ASYNC}={exported} is exported"
    setting = resolve_memory_map(config)
    if setting is not True and setting is not False:
        for key, value in _configured_directories(config):
            fstype = network_filesystem(value, mounts_text)
            if fstype is not None:
                return False, f"io.memory_map is auto and {key} is on {fstype}"
        return True, "io.memory_map is auto and no configured path is on network storage"
    return setting, f"io.memory_map is {str(setting).lower()}"
