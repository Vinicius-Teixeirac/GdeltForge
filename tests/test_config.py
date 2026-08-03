import pytest

from gdeltforge.utils.config import dataset_path_key


class TestDatasetPathKey:
    def test_events_keeps_unprefixed_key(self):
        # Events predates multi-dataset support; its paths.* keys must stay
        # unprefixed so existing settings.yaml files keep working unchanged.
        assert dataset_path_key("gdelt_event", "downloaded_data_directory") == (
            "downloaded_data_directory"
        )

    def test_other_datasets_get_a_prefixed_key(self):
        assert dataset_path_key("gdelt_gkg_v1", "downloaded_data_directory") == (
            "gkg_v1_downloaded_data_directory"
        )
        assert dataset_path_key("gdelt_gkg_v2", "parquet_data_directory") == (
            "gkg_v2_parquet_data_directory"
        )
        assert dataset_path_key("gdelt_mentions", "filtered_data_directory") == (
            "mentions_filtered_data_directory"
        )

    def test_unknown_dataset_raises(self):
        with pytest.raises(ValueError, match="Unknown dataset"):
            dataset_path_key("not_a_real_dataset", "downloaded_data_directory")
