import asyncio
import copy

import pytest

from daq_queuing_service.queue import TaskQueue, TaskWithPosition
from daq_queuing_service.task import ExperimentDefinition, Status, Task

pytest_plugins = ("pytest_asyncio",)


@pytest.fixture
def tasks() -> list[Task]:
    return [
        Task(experiment_definition=ExperimentDefinition(sample_id=str(i)), id=str(i))
        for i in range(5)
    ]


@pytest.fixture
async def task_queue(tasks: list[Task]):
    queue = TaskQueue()
    await queue.add_tasks(tasks)
    return queue


@pytest.fixture
async def task_queue_in_progress(task_queue: TaskQueue):
    first_task_id = task_queue.queue[0]
    first_task = task_queue._tasks[first_task_id]  # type: ignore  # noqa
    first_task.update_status(Status.IN_PROGRESS)
    return task_queue


@pytest.fixture
async def task_queue_with_history(task_queue: TaskQueue):
    for _ in range(2):
        task = await task_queue.claim_next_task_once_available()
        await task_queue.complete_task(task)
    # By this point should have 3 tasks in queue and 2 in history
    for i, task_id in enumerate(task_queue.history):
        # Real timestamps will break tests
        task_queue._tasks[task_id].time_started = 1 + i
        task_queue._tasks[task_id].time_completed = 1 + i + 0.9
    return task_queue


async def test_add_tasks_adds_to_end_when_no_position_given(task_queue: TaskQueue):
    new_task = Task(
        experiment_definition=ExperimentDefinition(sample_id="new"), id="new"
    )
    await task_queue.add_tasks([new_task])
    assert task_queue.queue == ["0", "1", "2", "3", "4", "new"]
    assert set(task_queue._tasks.keys()) == {"0", "1", "2", "3", "4", "new"}  # type: ignore  # noqa


async def test_add_tasks_with_position_works_as_expected(task_queue: TaskQueue):
    new_task = Task(
        experiment_definition=ExperimentDefinition(sample_id="new"), id="new"
    )
    await task_queue.add_tasks([new_task], 2)
    assert task_queue.queue == ["0", "1", "new", "2", "3", "4"]
    assert set(task_queue._tasks.keys()) == {"0", "1", "2", "3", "4", "new"}  # type: ignore  # noqa


async def test_add_tasks_adds_to_the_end_if_position_bigger_than_queue_length(
    task_queue: TaskQueue,
):
    new_task = Task(
        experiment_definition=ExperimentDefinition(sample_id="new"), id="new"
    )
    await task_queue.add_tasks([new_task], 20)
    assert task_queue.queue == ["0", "1", "2", "3", "4", "new"]
    assert set(task_queue._tasks.keys()) == {"0", "1", "2", "3", "4", "new"}  # type: ignore  # noqa


async def test_add_task_to_position_0_adds_to_position_1_if_first_task_in_progress(
    task_queue_in_progress: TaskQueue,
):
    new_tasks = [
        Task(experiment_definition=ExperimentDefinition(sample_id="new"), id="new"),
        Task(experiment_definition=ExperimentDefinition(sample_id="new_2"), id="new_2"),
    ]
    first_task = await task_queue_in_progress.get_task_by_position(0)
    assert first_task and first_task.status == Status.IN_PROGRESS

    await task_queue_in_progress.add_tasks(new_tasks, 0)

    assert task_queue_in_progress.queue == ["0", "new", "new_2", "1", "2", "3", "4"]
    assert set(task_queue_in_progress._tasks.keys()) == {  # pyright: ignore[reportPrivateUsage]
        "0",
        "1",
        "2",
        "3",
        "4",
        "new",
        "new_2",
    }


async def test_add_task_to_position_0_adds_to_position_0_if_first_task_waiting(
    task_queue: TaskQueue,
):
    new_tasks = [
        Task(experiment_definition=ExperimentDefinition(sample_id="new"), id="new"),
        Task(experiment_definition=ExperimentDefinition(sample_id="new_2"), id="new_2"),
    ]
    first_task = await task_queue.get_task_by_position(0)
    assert first_task and first_task.status == Status.WAITING

    await task_queue.add_tasks(new_tasks, 0)

    assert task_queue.queue == ["new", "new_2", "0", "1", "2", "3", "4"]
    assert set(task_queue._tasks.keys()) == {  # pyright: ignore[reportPrivateUsage]
        "0",
        "1",
        "2",
        "3",
        "4",
        "new",
        "new_2",
    }


async def test_add_task_to_negative_position_raises_error(
    task_queue_in_progress: TaskQueue,
):
    new_tasks = [
        Task(experiment_definition=ExperimentDefinition(sample_id="new"), id="new"),
        Task(experiment_definition=ExperimentDefinition(sample_id="new_2"), id="new_2"),
    ]
    with pytest.raises(ValueError):
        await task_queue_in_progress.add_tasks(new_tasks, -1)


@pytest.mark.parametrize(
    "task_to_move, new_position, expected_order",
    [
        [2, 2, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]],
        [5, 2, [0, 1, 5, 2, 3, 4, 6, 7, 8, 9]],
        [2, 3, [0, 1, 3, 2, 4, 5, 6, 7, 8, 9]],
        [9, 0, [9, 0, 1, 2, 3, 4, 5, 6, 7, 8]],
        [0, 9, [1, 2, 3, 4, 5, 6, 7, 8, 9, 0]],
    ],
)
async def test_move_task_works_as_expected(
    task_to_move: int, new_position: int, expected_order: list[int]
):
    queue = TaskQueue()
    tasks = [
        Task(experiment_definition=ExperimentDefinition(sample_id=str(i)), id=str(i))
        for i in range(10)
    ]
    await queue.add_tasks(tasks)
    task = str(task_to_move)
    await queue.move_task(task, new_position)
    result_order = [int(task_id) for task_id in queue.queue]
    assert result_order == expected_order


async def test_move_task_to_position_0_moves_to_position_1_if_first_task_in_progress(
    task_queue_in_progress: TaskQueue,
):
    task = await task_queue_in_progress.get_task_by_position(0)
    assert task and task.status == Status.IN_PROGRESS

    await task_queue_in_progress.move_task("4", 0)
    assert task_queue_in_progress.queue == ["0", "4", "1", "2", "3"]


async def test_move_task_to_position_0_moves_to_position_0_if_first_task_waiting(
    task_queue: TaskQueue,
):
    task = await task_queue.get_task_by_position(0)
    assert task and task.status == Status.WAITING

    await task_queue.move_task("4", 0)
    assert task_queue.queue == ["4", "0", "1", "2", "3"]


async def test_move_task_does_not_move_task_that_is_in_progress(
    task_queue_in_progress: TaskQueue,
):
    task = await task_queue_in_progress.get_task_by_position(0)
    assert task and task.status == Status.IN_PROGRESS

    await task_queue_in_progress.move_task("0", 3)
    assert task_queue_in_progress.queue == ["0", "1", "2", "3", "4"]
    assert set(task_queue_in_progress._tasks.keys()) == {"0", "1", "2", "3", "4"}


async def test_move_task_does_not_error_if_wrong_task_id_given(
    task_queue_in_progress: TaskQueue,
):
    task = await task_queue_in_progress.get_task_by_position(0)
    assert task and task.status == Status.IN_PROGRESS

    await task_queue_in_progress.move_task("10", 3)
    assert task_queue_in_progress.queue == ["0", "1", "2", "3", "4"]


async def test_remove_tasks_works_as_expected(task_queue: TaskQueue):
    await task_queue.remove_tasks(["4", "2"])
    assert task_queue.queue == ["0", "1", "3"]


async def test_remove_tasks_does_not_remove_task_that_is_in_progress(
    task_queue_in_progress: TaskQueue,
):
    task = await task_queue_in_progress.get_task_by_position(0)
    assert task and task.status == Status.IN_PROGRESS

    await task_queue_in_progress.remove_tasks(["0", "2"])
    assert task_queue_in_progress.queue == ["0", "1", "3", "4"]
    assert set(task_queue_in_progress._tasks.keys()) == {"0", "1", "3", "4"}


async def test_remove_tasks_does_not_error_if_wrong_task_id_used(task_queue: TaskQueue):
    await task_queue.remove_tasks(["4", "2", "10"])
    assert task_queue.queue == ["0", "1", "3"]


async def test_get_queue_only_returns_tasks_in_queue(
    task_queue_with_history: TaskQueue,
):
    assert task_queue_with_history.history == ["0", "1"]
    result = await task_queue_with_history.get_queue()
    assert result == [
        '{"experiment_definition":{"sample_id":"2"},"id":"2","status":"Waiting","time_started":null,"time_completed":null,"errors":[]}',
        '{"experiment_definition":{"sample_id":"3"},"id":"3","status":"Waiting","time_started":null,"time_completed":null,"errors":[]}',
        '{"experiment_definition":{"sample_id":"4"},"id":"4","status":"Waiting","time_started":null,"time_completed":null,"errors":[]}',
    ]


async def test_get_history_only_returns_tasks_in_history(
    task_queue_with_history: TaskQueue,
):
    assert task_queue_with_history.queue == ["2", "3", "4"]
    result = await task_queue_with_history.get_history()
    assert result == [
        '{"experiment_definition":{"sample_id":"0"},"id":"0","status":"Success","time_started":1.0,"time_completed":1.9,"errors":[]}',
        '{"experiment_definition":{"sample_id":"1"},"id":"1","status":"Success","time_started":2.0,"time_completed":2.9,"errors":[]}',
    ]


async def test_get_tasks_returns_tasks_in_queue_and_history(
    task_queue_with_history: TaskQueue,
):
    assert task_queue_with_history.queue == ["2", "3", "4"]
    assert task_queue_with_history.history == ["0", "1"]
    result = await task_queue_with_history.get_tasks()
    assert result == [
        '{"experiment_definition":{"sample_id":"0"},"id":"0","status":"Success","time_started":1.0,"time_completed":1.9,"errors":[]}',
        '{"experiment_definition":{"sample_id":"1"},"id":"1","status":"Success","time_started":2.0,"time_completed":2.9,"errors":[]}',
        '{"experiment_definition":{"sample_id":"2"},"id":"2","status":"Waiting","time_started":null,"time_completed":null,"errors":[]}',
        '{"experiment_definition":{"sample_id":"3"},"id":"3","status":"Waiting","time_started":null,"time_completed":null,"errors":[]}',
        '{"experiment_definition":{"sample_id":"4"},"id":"4","status":"Waiting","time_started":null,"time_completed":null,"errors":[]}',
    ]


async def test_get_task_by_id_returns_task_in_queue_or_history(
    task_queue_with_history: TaskQueue,
):
    assert (
        "0" not in task_queue_with_history.queue
        and "0" in task_queue_with_history.history
    )
    assert (
        "4" in task_queue_with_history.queue
        and "4" not in task_queue_with_history.history
    )
    assert isinstance(
        await task_queue_with_history.get_task_by_id("0"), TaskWithPosition
    )
    assert isinstance(
        await task_queue_with_history.get_task_by_id("4"), TaskWithPosition
    )


async def test_get_task_by_id_returns_none_if_task_id_does_not_exist(
    task_queue_with_history: TaskQueue,
):
    assert await task_queue_with_history.get_task_by_id("fake") is None


async def test_get_task_by_pos_returns_task_in_queue(
    task_queue_with_history: TaskQueue,
):
    task = await task_queue_with_history.get_task_by_position(2)
    assert isinstance(task, TaskWithPosition)
    assert task.position == 2


async def test_get_task_by_pos_returns_none_if_position_not_in_queue(
    task_queue: TaskQueue,
):
    assert await task_queue.get_task_by_position(5) is None
    assert await task_queue.get_task_by_position(-6) is None


async def test_get_task_by_pos_works_with_negative_indexing(task_queue: TaskQueue):
    last_task = await task_queue.get_task_by_position(-1)
    assert last_task and last_task.id == "4"
    last_task = await task_queue.get_task_by_position(-5)
    assert last_task and last_task.id == "0"


async def test_clear_history_removes_history_and_removes_completed_tasks_from_registry(
    task_queue_with_history: TaskQueue,
):
    assert task_queue_with_history.history == ["0", "1"]
    assert {"0", "1"}.issubset(task_queue_with_history._tasks.keys())

    await task_queue_with_history.clear_history()

    assert task_queue_with_history.history == []
    assert not {"0", "1"}.intersection(task_queue_with_history._tasks.keys())
    assert await task_queue_with_history.get_task_by_id("0") is None


async def test_pausing_queue_prevents_task_from_being_claimed(task_queue: TaskQueue):
    await task_queue.pause()
    assert task_queue.paused
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(
            task_queue.claim_next_task_once_available(), timeout=0.05
        )


async def test_unpausing_queue_allows_tasks_to_being_claimed(task_queue: TaskQueue):
    await task_queue.pause()
    await task_queue.unpause()
    assert not task_queue.paused
    await task_queue.claim_next_task_once_available()


async def test_claim_next_task_once_available_puts_task_in_progress_and_returns(
    task_queue: TaskQueue,
):
    next_task = task_queue._tasks[task_queue.queue[0]]
    assert next_task and next_task.status == Status.WAITING

    claimed_task = await task_queue.claim_next_task_once_available()
    assert claimed_task is next_task
    assert claimed_task.status == Status.IN_PROGRESS


async def test_claim_next_task_once_available_waits_if_next_task_is_already_in_progress(
    task_queue: TaskQueue,
):
    claimed_task = await task_queue.claim_next_task_once_available()
    assert claimed_task and claimed_task.status == Status.IN_PROGRESS
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(
            task_queue.claim_next_task_once_available(), timeout=0.05
        )


async def test_wait_until_task_available_waits_if_next_task_is_in_progress(
    task_queue: TaskQueue,
):
    claimed_task = await task_queue.claim_next_task_once_available()
    assert claimed_task and claimed_task.status == Status.IN_PROGRESS
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(task_queue.wait_until_task_available(), timeout=0.05)


async def test_wait_until_task_available_waits_if_queue_paused(
    task_queue: TaskQueue,
):
    await task_queue.pause()
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(task_queue.wait_until_task_available(), timeout=0.05)


async def test_wait_until_task_available_waits_if_queue_empty():
    task_queue = TaskQueue()
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(task_queue.wait_until_task_available(), timeout=0.05)


async def test_wait_until_task_available_does_not_wait_if_conditions_met(
    task_queue: TaskQueue,
):
    await asyncio.wait_for(task_queue.wait_until_task_available(), timeout=0.05)


async def test_complete_task_puts_task_in_history_and_updates_status_to_complete(
    task_queue: TaskQueue,
):
    task = await task_queue.claim_next_task_once_available()
    assert task.status == Status.IN_PROGRESS
    await task_queue.complete_task(task)
    assert task.id not in task_queue.queue
    assert task.id in task_queue.history
    assert task.status == Status.SUCCESS


async def test_complete_task_must_receive_exact_same_object_as_was_claimed(
    task_queue: TaskQueue,
):
    task = await task_queue.claim_next_task_once_available()
    similar_task = task.model_copy()
    another_similar_task = copy.copy(task)
    with pytest.raises(AssertionError):
        await task_queue.complete_task(similar_task)
    with pytest.raises(AssertionError):
        await task_queue.complete_task(another_similar_task)


async def test_complete_task_with_error_adds_error_to_task_and_changes_status_to_error(
    task_queue: TaskQueue,
):
    task = await task_queue.claim_next_task_once_available()
    error = ValueError("This task failed")
    await task_queue.complete_task(task, error=str(error))
    assert task.status == Status.ERROR
    assert task.errors == ["This task failed"]
    assert task.id in task_queue.history
    assert task.id not in task_queue.queue
