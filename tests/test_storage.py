"""io.memory_map: the network filesystem check and the decision the
launcher makes before polars loads (gdeltforge.utils.storage)."""

import pytest

from gdeltforge.utils.storage import POLARS_FORCE_ASYNC, memory_map_decision, network_filesystem

MOUNTS = r"""/dev/sda1 / ext4 rw 0 0
fileserver:/export/gdelt /mnt/lab\040data nfs4 rw,relatime 0 0
//nas/share /srv/smb cifs rw 0 0
/dev/sdb1 /srv/smb/local ext4 rw 0 0
C:\134 /mnt/c 9p rw 0 0
tmpfs /tmp tmpfs rw 0 0
"""


class TestNetworkFilesystem:
    @pytest.mark.parametrize("path, expected", [
        ("/mnt/lab data/gdelt/events", "nfs4"),
        ("/mnt/lab data", "nfs4"),
        ("/srv/smb/events", "cifs"),
        # A local disk mounted inside a network share is local.
        ("/srv/smb/local/events", None),
        # A mount point is matched by whole path components only.
        ("/mnt/lab datax/events", None),
        # WSL's view of the Windows disk is the same machine's disk.
        ("/mnt/c/Users/data", None),
        ("/home/user/data", None),
        ("/tmp/x", None),
    ])
    def test_the_longest_mount_holding_the_path_decides(self, path, expected):
        assert network_filesystem(path, MOUNTS) == expected

    def test_this_machines_working_directory_is_local(self):
        # The test suite runs on local disks, in CI and locally.
        assert network_filesystem(".") is None


def _config(memory_map: object = "auto", **paths: str) -> dict:
    return {"io": {"memory_map": memory_map}, "paths": paths}


class TestMemoryMapDecision:
    def test_auto_maps_when_every_path_is_local(self):
        mapped, why = memory_map_decision(
            _config(parquet_data_directory="/home/user/data"), {}, MOUNTS
        )
        assert mapped is True
        assert "no configured path is on network storage" in why

    def test_auto_stops_mapping_when_a_path_is_on_network_storage(self):
        config = _config(
            parquet_data_directory="/home/user/data",
            gkg_v2_parquet_data_directory="/mnt/lab data/gkg",
        )
        mapped, why = memory_map_decision(config, {}, MOUNTS)
        assert mapped is False
        assert why == "io.memory_map is auto and paths.gkg_v2_parquet_data_directory is on nfs4"

    @pytest.mark.parametrize("setting, expected", [(True, True), (False, False)])
    def test_true_and_false_are_taken_as_given(self, setting, expected):
        config = _config(setting, parquet_data_directory="/mnt/lab data/events")
        assert memory_map_decision(config, {}, MOUNTS) == (
            expected, f"io.memory_map is {str(setting).lower()}"
        )

    @pytest.mark.parametrize("exported, expected", [("1", False), ("0", True)])
    def test_an_exported_polars_force_async_decides(self, exported, expected):
        config = _config(True)
        mapped, why = memory_map_decision(config, {POLARS_FORCE_ASYNC: exported}, MOUNTS)
        assert mapped is expected
        assert why == f"{POLARS_FORCE_ASYNC}={exported} is set"

    @pytest.mark.parametrize("value", ["sometimes", 1, None, ["auto"]])
    def test_an_invalid_setting_is_refused(self, value):
        with pytest.raises(ValueError, match="io.memory_map must be auto, true or false"):
            memory_map_decision(_config(value), {}, MOUNTS)

    def test_quoted_true_and_false_are_taken(self):
        assert memory_map_decision(_config("false"), {}, MOUNTS)[0] is False
        assert memory_map_decision(_config("True"), {}, MOUNTS)[0] is True
