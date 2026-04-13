import asyncio
from collections.abc import Sequence
from typing import Self

from daq_queuing_service.task import Status, Task, TaskID


class TaskWithPosition(Task):
    position: int | None

    @classmethod
    def from_task(cls, task: Task, position: int | None = None) -> Self:
        return cls.model_validate({**task.model_dump(), "position": position})


class TaskRegistry(dict[TaskID, Task]):
    def get_must_exist(self, task_id: TaskID) -> Task:
        task = self.get(task_id)
        if task is None:
            raise KeyError(f"No task found matching id: {task_id}")
        return task


class TaskQueue:
    def __init__(self):
        self._tasks: TaskRegistry = TaskRegistry()
        self._queue: list[TaskID] = []
        self._history: list[TaskID] = []
        self._condition = asyncio.Condition()
        self._paused: bool = False

    async def claim_next_task_once_available(self) -> Task:
        async with self._condition:
            while not self._task_available():
                await self._condition.wait()
            task = self._tasks[self._queue[0]]
            task.update_status(Status.IN_PROGRESS)
            self._condition.notify_all()
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
                    task.update_status(Status.WAITING)
                case _:
                    raise ValueError(
                        f"Cannot return task {task.id}, "
                        + "it is already owned by the queue."
                    )
            self._condition.notify_all()

    async def complete_task(self, task: Task, error: str | None = None):
        async with self._condition:
            self._check_task_valid_to_be_returned(task)
            assert self._queue[0] == task.id, (
                f"This task is not at the front of the queue: {task}"
            )
            assert task.status == Status.IN_PROGRESS, (
                f"This task is not currently in progress: {task}"
            )

            if error is not None:
                task.add_error(error)
                task.update_status(Status.ERROR)
            else:
                task.update_status(Status.SUCCESS)

            self._queue.pop(0)
            self._history.append(task.id)
            self._condition.notify_all()

    async def get_task_by_id(self, task_id: TaskID) -> TaskWithPosition | None:
        # Returns copy so don't have to be worried about caller modifying task.
        # Maybe should return json?
        async with self._condition:
            if task_id in self._tasks:
                return self._get_task_by_id(task_id)

    async def get_task_by_position(self, position: int) -> TaskWithPosition | None:
        # Returns copy so don't have to be worried about caller modifying task.
        # Maybe should return json?
        async with self._condition:
            if position < -self.length or position >= self.length:
                return None
            return TaskWithPosition.from_task(
                self._tasks[self._queue[position]], position
            )

    async def get_queue(self) -> list[TaskWithPosition]:
        # Returns copies so don't have to be worried about caller modifying tasks.
        # Maybe should return json?
        async with self._condition:
            return [self._get_task_by_id(task_id) for task_id in self._queue]

    async def get_history(self) -> list[TaskWithPosition]:
        # Returns copies so don't have to be worried about caller modifying tasks.
        # Maybe should return json?
        async with self._condition:
            return [self._get_task_by_id(task_id) for task_id in self._history]

    async def get_tasks(self) -> list[TaskWithPosition]:
        # Returns copies so don't have to be worried about caller modifying tasks.
        # Maybe should return json?
        async with self._condition:
            return [
                self._get_task_by_id(task_id) for task_id in self._history + self._queue
            ]

    async def add_tasks(self, tasks: list[Task], position: int | None = None) -> None:
        async with self._condition:
            self._verify_new_tasks(tasks, position)
            if position is not None:
                position = self._get_valid_position(position)
            self._add_tasks(tasks, position)
            self._condition.notify_all()

    async def move_task(self, task_id: TaskID, position: int) -> int:
        async with self._condition:
            position = self._get_valid_position(position)
            task_ids = self._remove_tasks_from_queue([task_id])
            self._queue[position:position] = task_ids
            self._condition.notify_all()
            return position

    async def remove_tasks(self, task_ids: Sequence[TaskID]) -> list[Task]:
        async with self._condition:
            task_ids = self._remove_tasks_from_queue(task_ids)
            tasks = self._remove_tasks_from_registry(task_ids)
            for task in tasks:
                task.update_status(Status.CANCELLED)
            self._condition.notify_all()
            return tasks

    async def clear_history(self):
        async with self._condition:
            for task_id in self._history:
                self._tasks.pop(task_id)
            self._history.clear()
            self._condition.notify_all()

    async def pause(self):
        async with self._condition:
            self._paused = True

    async def unpause(self):
        async with self._condition:
            self._paused = False
            self._condition.notify_all()

    @property
    def paused(self):
        return self._paused

    @property
    def length(self):
        return len(self._queue)

    def _task_available(self) -> bool:
        if self._paused or not self._queue:
            return False
        return self._tasks[self._queue[0]].status == Status.WAITING

    def _check_task_valid_to_be_returned(self, task: Task):
        # Check caller has actual task object not copy
        # This ensures the caller has claimed the task, reducing the chance a task is
        # returned that is actually still being run/modified by a different process.
        # However if the worker crashes we then lose the Task object and can't return
        # the task? Needs discussion with others.
        assert task is self._tasks.get_must_exist(task.id)
        assert task.id in self._queue, f"This task is not in the queue: {task}"

    def _get_valid_position(self, position: int) -> int:
        if position < 0:
            raise ValueError(f"Position must be >= 0, got {position}")
        if (  # if position 0 requested but a task is in progress, return position 1
            self.length
            and position == 0
            and self._tasks[self._queue[0]].status != Status.WAITING
        ):
            return 1
        return position

    def _get_task_by_id(self, task_id: TaskID) -> TaskWithPosition:
        task = self._tasks.get_must_exist(task_id)
        position = self._queue.index(task.id) if task.id in self._queue else None
        return TaskWithPosition.from_task(task, position)

    def _verify_new_tasks(self, tasks: list[Task], position: int | None):
        if position and position < 0:
            raise ValueError(f"Position: {position} cannot be less than 0.")
        for task in tasks:
            if task.id in self._tasks:
                raise ValueError(f"TaskID '{task.id}' already in use!")

    def _add_tasks(self, tasks: list[Task], position: int | None) -> None:
        task_ids = [task.id for task in tasks]
        if position is None:
            self._queue.extend(task_ids)
        else:
            self._queue[position:position] = task_ids
        for task in tasks:
            self._tasks[task.id] = task

    def _remove_tasks_from_queue(self, task_ids: Sequence[TaskID]) -> list[TaskID]:
        #  Only removes tasks in the queue (not history or registry)
        def should_be_removed(task_id: TaskID):
            return (
                task_id in self._queue
                and self._tasks[task_id].status != Status.IN_PROGRESS
            )

        removed_ids = [task_id for task_id in task_ids if should_be_removed(task_id)]
        self._queue = [task_id for task_id in self._queue if task_id not in removed_ids]

        return removed_ids

    def _remove_tasks_from_registry(self, task_ids: Sequence[TaskID]) -> list[Task]:
        # Should remove tasks from queue/history before removing from registry
        def should_be_removed(task_id: TaskID) -> bool:
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
