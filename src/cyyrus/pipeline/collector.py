import os
import uuid
import shutil

import ray
from datasets import Dataset as HFDataset, concatenate_datasets
from ray.util.actor_pool import ActorPool

from typeguard import typechecked

from cyyrus.pipeline.records import Record


@typechecked
class Collector:
    def __init__(
        self,
        pipeline_name: str,
        pool_size: int,
        row_limit: int,
    ):
        """
        Creates a pool of Workers actors.

        :param pipeline: The pipeline object defining the workflow for each worker.
        :param pool_size: The number of worker actors to create in the pool.
        :param row_limit: The maximum number of records each worker should handle before flushing to disk.
        :return: An ActorPool object containing all initialized worker actors.
        """
        pool_size = max(pool_size, 1)  # Ensure there is at least one actor in the pool
        actors = [
            Worker.remote(
                dataset_name=pipeline_name,  # type: ignore
                row_limit=row_limit,
            )
            for _ in range(pool_size)
        ]
        self.pool = ActorPool(actors)

    def submit(
        self,
        record,
    ):
        """
        Submits a record to a pool of composer workers.

        :param record: A single data record to add to the buffer.
        """
        self.pool.submit(lambda actor, value: actor.add_record.remote(value), record)

    def drain(
        self,
    ):
        """
        Drains all tasks from the pool of workers, ensuring that all pending operations are completed.
        """
        while self.pool.has_next():
            self.pool.get_next_unordered()

    def dump(
        self,
        reuse=True,
        clean_dir=True,
    ) -> HFDataset:
        """
        Prepares datasets by closing workers in the pool, compacts data, and optionally resets actors.

        :param reuse: Boolean flag indicating whether to reinitialize workers (True) or permanently close them (False) after compaction.
        :return: A combined dataset or an empty dataset if no datasets were generated.
        """
        self.drain()

        datasets = []
        idle_actors = []
        while self.pool.has_free():
            idle_actor = self.pool.pop_idle()

            ray.get(idle_actor.flush.remote())  # type: ignore

            dataset = ray.get(idle_actor.compaction.remote(clean_dir=clean_dir))  # type: ignore
            if dataset:
                datasets.append(dataset)

            if reuse:
                idle_actors.append(idle_actor)

        for actor in idle_actors:
            self.pool.push(actor)

        if datasets:
            combined_dataset = concatenate_datasets(dsets=datasets)  # type: ignore
            return combined_dataset

        return HFDataset.from_dict({})


@ray.remote
class Worker:

    def __init__(
        self,
        dataset_name: str,
        row_limit: int = 100,
    ):
        """
        Initializes the Workers class with the dataset name derived from the pipeline and the row limit.

        :param pipeline: Pipeline object containing the dataset name.
        :param row_limit: Maximum number of records each snapshot file should contain.
        """
        self.dataset_name = dataset_name
        self.snapshot_dir = uuid.uuid4().hex
        os.makedirs(self.snapshot_dir, exist_ok=True)
        self.records = []
        self.row_limit = max(row_limit, 1)  # ensure there's atleast 1 row to flush

    def add_record(
        self,
        record: Record,
    ):
        """
        Adds a record to the internal buffer and triggers flush if the buffer size exceeds the row limit.

        :param record: A single data record to add to the buffer.
        """
        self.records.append(record.to_dict())
        if len(self.records) >= self.row_limit:
            self.flush()

    def _flush_records(
        self,
    ):
        """
        Flushes all currently buffered records to a snapshot file, creating a new file with a unique UUID.
        """
        if self.records:
            dataset_from_list = HFDataset.from_list(self.records)
            snapshot_uuid = uuid.uuid4().hex
            snapshot_file = os.path.join(self.snapshot_dir, f"snapshot_{snapshot_uuid}.parquet")
            dataset_from_list.to_parquet(snapshot_file)
            self.records = []

    def flush(
        self,
    ):
        """
        Performs final operations such as flushing remaining records and compacting all snapshots,
        then returns the final dataset.

        :return: The final compacted dataset.
        """
        self._flush_records()

    def compaction(
        self,
        clean_dir=True,
    ):
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
