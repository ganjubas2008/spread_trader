from objects.action import Action
from objects.order_book import OrderBook

from __init__ import *

class ActionStream:
    """Handles streaming of actions from Parquet files in sorted order."""
    def __init__(self, filepath, batch_size=100_000):
        self.filepath = filepath
        self.batch_size = batch_size
        self.parquet_file = pq.ParquetFile(filepath)
        self.batch_iter = self.parquet_file.iter_batches(batch_size)
        self.current_batch = None
        self.current_index = 0
        self._load_next_batch()

    def _load_next_batch(self):
        """Loads the next batch if available."""
        try:
            self.current_batch = next(self.batch_iter).to_pandas()
            self.current_index = 0
        except StopIteration:
            self.current_batch = None  # No more data

    def next_action(self):
        """Retrieves the next action, or None if empty."""
        if self.current_batch is None:
            return None
        action = self.current_batch.iloc[self.current_index]
        self.current_index += 1
        if self.current_index >= len(self.current_batch):  # Load next batch
            self._load_next_batch()
        return action


def merge_sorted_actions(paths, batch_size=100_000):
    """Merge-sorts actions from multiple instruments using external sorting."""
    streams = {inst: ActionStream(path, batch_size) for inst, path in paths.items()}
    heap = []

    # Initialize heap with the first action from each stream
    for inst, stream in streams.items():
        action = stream.next_action()
        if action is not None:
            heapq.heappush(heap, (action.ts_dt, inst, action))

    while heap:
        ts_dt, inst, action = heapq.heappop(heap)
        yield action  # Process the action (or store it in another list)

        # Load the next action from the same instrument and push it to the heap
        next_action = streams[inst].next_action()
        if next_action is not None:
            heapq.heappush(heap, (next_action.ts_dt, inst, next_action))