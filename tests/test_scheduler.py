import pytest

from hypervs1000.scheduler import GPUScheduler, RetryableError, Task


def test_scheduler_round_robin_assignment():
    scheduler = GPUScheduler([0, 1], max_retries=0)
    tasks = [Task(name=f"task_{idx}", payload=idx) for idx in range(4)]

    placements = []

    def worker(task, device):
        placements.append((task.name, device))
        return task.payload * 2

    scheduler.dispatch(tasks, worker)
    assert placements == [
        ("task_0", 0),
        ("task_1", 1),
        ("task_2", 0),
        ("task_3", 1),
    ]


def test_scheduler_retry_handling():
    scheduler = GPUScheduler([0], max_retries=1)
    tasks = [Task(name="flaky", payload=None)]
    attempts = []

    def worker(task, device):
        attempts.append(task.attempts)
        if len(attempts) == 1:
            raise RetryableError("transient failure")
        return "ok"

    results = scheduler.dispatch(tasks, worker)
    assert len(results) == 1
    task, device, value = results[0]
    assert value == "ok"
    assert attempts == [0, 1]
