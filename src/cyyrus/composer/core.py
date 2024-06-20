import os
import uuid
import shutil

import ray
from datasets import Dataset as HFDataset, concatenate_datasets

from cyyrus.pipeline.core import Pipeline


@ray.remote
class Composer:
    def __init__(
        self,
        pipeline: Pipeline,
        row_limit: int = 100,
    ):
        """
        Initializes the Workers class with the dataset name derived from the pipeline and the row limit.

        :param pipeline: Pipeline object containing the dataset name.
        :param row_limit: Maximum number of records each snapshot file should contain.
        """
        self.dataset_name = pipeline.name
        self.snapshot_dir = uuid.uuid4().hex
        os.makedirs(self.snapshot_dir, exist_ok=True)
        self.records = []
        self.row_limit = max(row_limit, 100)

    def add_record(self, record):
        """
        Adds a record to the internal buffer and triggers flush if the buffer size exceeds the row limit.

        :param record: A single data record to add to the buffer.
        """
        self.records.append(record)
        if len(self.records) > self.row_limit:
            self._flush_records()

    def _flush_records(self):
        """
        Flushes all currently buffered records to a snapshot file, creating a new file with a unique UUID.
        """
        if self.records:
            dataset_from_list = HFDataset.from_list(self.records)
            snapshot_uuid = uuid.uuid4().hex
            snapshot_file = os.path.join(self.snapshot_dir, f"snapshot_{snapshot_uuid}.parquet")
            dataset_from_list.to_parquet(snapshot_file)
            self.records = []

    def reset(self):
        """
        Performs final operations such as flushing remaining records and compacting all snapshots,
        then returns the final dataset.

        :return: The final compacted dataset.
        """
        self._flush_records()

    def compaction(self, clean_dir=False):
        """
        Compacts all existing snapshot files from a snapshot_dir into a single dataset.

        :return: A single combined dataset of all compacted data or None if no datasets exist.
        """
        snapshot_files = [
            os.path.join(self.snapshot_dir, f)
            for f in os.listdir(self.snapshot_dir)
            if f.endswith(".parquet")
        ]
        datasets = [
            HFDataset.from_parquet(snapshot)
            for snapshot in snapshot_files
            if os.path.isfile(snapshot)
        ]

        if clean_dir:
            shutil.rmtree(self.snapshot_dir)
            os.makedirs(self.snapshot_dir, exist_ok=True)

        if datasets:
            combined_dataset = concatenate_datasets(dsets=datasets)  # type: ignore
            return combined_dataset
        return None
