"""Simple GPU scheduler with retry support."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Callable, Deque, Iterable, Iterator, List, Optional, Sequence, Tuple, TypeVar


class RetryableError(Exception):
    """Exception indicating a task should be retried."""


T = TypeVar("T")
R = TypeVar("R")


@dataclass
class Task:
    """Work unit scheduled onto a device."""

    name: str
    payload: T
    attempts: int = 0
    metadata: dict = field(default_factory=dict)


class GPUScheduler:
    """Round-robin scheduler with retry semantics."""

    def __init__(self, devices: Sequence[int], *, max_retries: int = 1):
        if not devices:
            raise ValueError("At least one device id is required.")
        self._devices = tuple(devices)
        self._max_retries = max_retries

    @property
    def devices(self) -> Tuple[int, ...]:
        return self._devices

    def dispatch(
        self,
        tasks: Iterable[Task],
        worker: Callable[[Task, int], R],
    ) -> List[Tuple[Task, int, R]]:
        """Run *tasks* using *worker*, distributing across devices.

        Returns list of (task, device, worker_result).
        """

        results: List[Tuple[Task, int, R]] = []
        queue: Deque[Task] = deque(tasks)
        device_iter = self._device_infinite_iterator()

        while queue:
            task = queue.popleft()
            device = next(device_iter)
            try:
                result = worker(task, device)
                results.append((task, device, result))
            except RetryableError:
                task.attempts += 1
                if task.attempts > self._max_retries:
                    raise
                queue.append(task)
        return results

    def _device_infinite_iterator(self) -> Iterator[int]:
        while True:
            for device in self._devices:
                yield device
