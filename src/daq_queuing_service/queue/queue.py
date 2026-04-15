import asyncio
import logging
from collections.abc import Sequence

from pydantic import BaseModel

from daq_queuing_service.queue.queue_utils import (
    NegativePositionError,
    TaskAlreadyOwnedError,
    TaskIdInUseError,
    TaskInProgressError,
    TaskNotFoundError,
    TaskNotInQueueError,
)
from daq_queuing_service.task import Status, Task, TaskWithPosition

LOGGER = logging.getLogger(__name__)


class TaskRegistry(dict[str, Task]):
    def __missing__(self, task_id: str) -> Task:
        raise TaskNotFoundError(f"No task found matching id: {task_id}")


class QueueState(BaseModel):
    paused: bool


class TaskQueue:
    def __init__(self):
        self._tasks: TaskRegistry = TaskRegistry()
        self._queue: list[str] = []
        self._history: list[str] = []
        self._condition = asyncio.Condition()
        self._state: QueueState = QueueState(paused=True)

    async def claim_next_task_once_available(self) -> Task:
        async with self._condition:
            while not self._task_available():
                await self._condition.wait()
            task = self._tasks[self._queue[0]]
            task.claim()
            self._condition.notify_all()
        LOGGER.info(f"Task {task.id} has been claimed")
        return task

    async def wait_until_task_available(self) -> None:
        async with self._condition:
            while not self._task_available():
                await self._condition.wait()

    async def return_task_to_queue(self, task: Task) -> None:
        self._check_task_valid_to_be_returned(task)
        async with self._condition:
            match task.status:
                case Status.IN_PROGRESS:
                    assert task.id == self._queue[0]
                    task.wait()
                case _:
                    raise TaskAlreadyOwnedError(
                        f"Cannot return task {task.id}, "
                        + "it is already owned by the queue."
                    )
            self._condition.notify_all()
        LOGGER.info(f"Task {task.id} has been returned to the queue")

    async def complete_task(self, task: Task):
        async with self._condition:
            self._check_task_valid_to_be_returned(task)
            assert self._queue[0] == task.id, (
                f"This task is not at the front of the queue: {task}"
            )
            task.succeed()
            self._queue.pop(0)
            self._history.append(task.id)
            self._condition.notify_all()
        LOGGER.info(f"Task {task.id} has been completed successfully")

    async def fail_task(self, task: Task, errors: list[str] | None = None):
        async with self._condition:
            self._check_task_valid_to_be_returned(task)
            assert self._queue[0] == task.id, (
                f"This task is not at the front of the queue: {task}"
            )
            task.fail(errors)
            self._queue.pop(0)
            self._history.append(task.id)
            self._condition.notify_all()
        LOGGER.info(f"Task {task.id} has failed with the following errors: {errors}")

    async def get_task_by_id(self, task_id: str) -> TaskWithPosition:
        # Returns copy so don't have to be worried about caller modifying task.
        async with self._condition:
            return self._get_task_by_id(task_id)

    def _get_task_by_id(self, task_id: str) -> TaskWithPosition:
        task = self._tasks[task_id]
        position = self._queue.index(task.id) if task.id in self._queue else None
        return TaskWithPosition.from_task(task, position)

    async def get_task_by_position(self, position: int) -> TaskWithPosition | None:
        # Returns copy so don't have to be worried about caller modifying task.
        async with self._condition:
            if position < -self.length or position >= self.length:
                return None
            return self._get_task_by_id(self._queue[position])

    async def get_queue(self) -> list[TaskWithPosition]:
        # Returns copies so don't have to be worried about caller modifying tasks.
        async with self._condition:
            return self._get_queue()

    async def get_history(self) -> list[TaskWithPosition]:
        # Returns copies so don't have to be worried about caller modifying tasks.
        async with self._condition:
            return self._get_history()

    async def get_tasks(self) -> list[TaskWithPosition]:
        # Returns copies so don't have to be worried about caller modifying tasks.
        async with self._condition:
            return self._get_history() + self._get_queue()

    async def add_tasks(self, tasks: list[Task], position: int | None = None) -> None:
        async with self._condition:
            self._validate_new_tasks(tasks)
            if position is not None:
                position = self._get_valid_position(position)
            self._add_tasks(tasks, position)
            self._condition.notify_all()
        LOGGER.info(f"Successfully added tasks to queue: {[task.id for task in tasks]}")

    async def move_task(self, task_id: str, position: int) -> int:
        async with self._condition:
            self._validate_tasks_for_move_or_deletion([task_id])
            position = self._get_valid_position(position)
            self._remove_tasks_from_queue([task_id])
            self._queue[position:position] = [task_id]
            self._condition.notify_all()
            new_position = self._queue.index(task_id)
        LOGGER.info(f"Succesfully moved task {task_id} to position {new_position}")
        return new_position

    async def cancel_tasks(self, task_ids: Sequence[str]) -> list[Task]:
        async with self._condition:
            task_ids = list(task_ids)
            self._validate_tasks_for_move_or_deletion(task_ids)
            self._remove_tasks_from_queue(task_ids)
            tasks = self._remove_tasks_from_registry(task_ids)
            for task in tasks:
                task.cancel()
            self._condition.notify_all()
        LOGGER.info(f"Succesfully cancelled tasks: {task_ids}")
        return tasks

    async def clear_history(self):
        async with self._condition:
            for task_id in self._history:
                self._tasks.pop(task_id)
            self._history.clear()
            self._condition.notify_all()
        LOGGER.info("Succesfully cleared history")

    async def update_state(self, paused: bool | None = None):
        async with self._condition:
            self._state = QueueState(
                paused=self._state.paused if paused is None else paused
            )
            self._condition.notify_all()
        LOGGER.info(f"Succesfully updated queue state to {self._state}")
        return self._state

    @property
    def state(self):
        return self._state

    @property
    def length(self):
        return len(self._queue)

    def _task_available(self) -> bool:
        if self._state.paused or not self._queue:
            return False
        return self._tasks[self._queue[0]].status == Status.WAITING

    def _check_task_valid_to_be_returned(self, task: Task):
        # Check caller has actual task object not copy
        # This ensures the caller has claimed the task, reducing the chance a task is
        # returned that is actually still being run/modified by a different process.
        # However if the worker crashes we then lose the Task object and can't return
        # the task? Needs discussion with others.
        assert task is self._tasks[task.id]
        assert task.id in self._queue, f"This task is not in the queue: {task}"

    def _get_valid_position(self, position: int) -> int:
        if position < 0:
            raise NegativePositionError(f"Position must be >= 0, got {position}")
        if (  # if position 0 requested but a task is in progress, return position 1
            self.length
            and position == 0
            and self._tasks[self._queue[0]].status != Status.WAITING
        ):
            return 1
        return position

    def _validate_new_tasks(self, tasks: list[Task]):
        for task in tasks:
            if task.id in self._tasks:
                raise TaskIdInUseError(f"str '{task.id}' already in use!")

    def _add_tasks(self, tasks: list[Task], position: int | None) -> None:
        task_ids = [task.id for task in tasks]
        if position is None:
            self._queue.extend(task_ids)
        else:
            self._queue[position:position] = task_ids
        for task in tasks:
            self._tasks[task.id] = task

    def _remove_tasks_from_queue(self, task_ids: list[str]) -> list[str]:
        #  Only removes tasks in the queue (not history or registry)
        def should_be_removed(task_id: str):
            return (
                task_id in self._queue
                and self._tasks[task_id].status != Status.IN_PROGRESS
            )

        removed_ids = [task_id for task_id in task_ids if should_be_removed(task_id)]
        self._queue = [task_id for task_id in self._queue if task_id not in removed_ids]

        return removed_ids

    def _remove_tasks_from_registry(self, task_ids: list[str]) -> list[Task]:
        # Should remove tasks from queue/history before removing from registry
        def should_be_removed(task_id: str) -> bool:
            return (
                task_id in self._tasks
                and self._tasks[task_id].status != Status.IN_PROGRESS
                and task_id not in self._queue
                and task_id not in self._history
            )

        removed_ids = [task_id for task_id in task_ids if should_be_removed(task_id)]
        removed = [self._tasks[task_id] for task_id in removed_ids]
        self._tasks = TaskRegistry(
            {
                task_id: task
                for task_id, task in self._tasks.items()
                if task.id not in removed_ids
            }
        )
        return removed

    def _validate_tasks_for_move_or_deletion(self, task_ids: list[str]):
        for task_id in task_ids:
            task = self._tasks[task_id]
            if task_id not in self._queue:
                raise TaskNotInQueueError(f"Task {task_id} isn't present in queue")
            if task.status == Status.IN_PROGRESS:
                raise TaskInProgressError(
                    f"Cannot move task '{task_id}', it is currently in progress!"
                )

    def _get_queue(self) -> list[TaskWithPosition]:
        return [
            TaskWithPosition.from_task(self._tasks[task_id], i)
            for i, task_id in enumerate(self._queue)
        ]

    def _get_history(self) -> list[TaskWithPosition]:
        return [
            TaskWithPosition.from_task(self._tasks[task_id])
            for task_id in self._history
        ]
