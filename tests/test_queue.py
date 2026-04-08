import pytest

from daq_queuing_service.queue import TaskQueue
from daq_queuing_service.task import ExperimentDefinition, Task

pytest_plugins = ("pytest_asyncio",)


@pytest.fixture
def tasks() -> list[Task]:
    return [
        Task(
            experiment_definition=ExperimentDefinition(sample_id=str(i)), task_id=str(i)
        )
        for i in range(3)
    ]


async def test_queue_can_have_tasks_added(tasks: list[Task]):
    queue = TaskQueue()
    await queue.add_tasks(tasks)
    assert queue.queue == tasks
    assert list(queue.tasks.keys()) == ["0", "1", "2"]


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
async def test_queue_can_move_multiple_tasks_at_once(
    task_to_move: int, new_position: int, expected_order: list[int]
):
    queue = TaskQueue()
    tasks = [
        Task(
            experiment_definition=ExperimentDefinition(sample_id=str(i)), task_id=str(i)
        )
        for i in range(10)
    ]
    await queue.add_tasks(tasks)
    task = str(task_to_move)
    await queue.move_task(task, new_position)
    result_order = [int(task.task_id) for task in queue.queue]
    print(result_order)
    assert result_order == expected_order
