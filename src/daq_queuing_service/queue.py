import asyncio
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
        self.queue: list[TaskID] = []
        self.history: list[TaskID] = []
        self.condition = asyncio.Condition()
        self.paused: bool = False

    async def claim_next_task_once_available(self) -> Task:
        async with self.condition:
            while not self._task_available():
                await self.condition.wait()
            task = self._tasks[self.queue[0]]
            task.update_status(Status.IN_PROGRESS)
            self.condition.notify_all()
            return task

    async def wait_until_task_available(self) -> None:
        async with self.condition:
            while not self._task_available():
                await self.condition.wait()

    async def return_task_to_queue(self, task: Task) -> None:
        self._check_task_valid_to_be_returned(task)
        async with self.condition:
            match task.status:
                case Status.IN_PROGRESS:
                    assert task.id == self.queue[0]
                    task.update_status(Status.WAITING)
                case _:
                    raise ValueError(
                        f"Cannot return task {task.id}, "
                        + "it is already owned by the queue."
                    )
            self.condition.notify_all()

    async def complete_task(self, task: Task, error: str | None = None):
        async with self.condition:
            self._check_task_valid_to_be_returned(task)
            assert self.queue[0] == task.id, (
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

            self.queue.pop(0)
            self.history.append(task.id)
            self.condition.notify_all()

    async def get_task_by_id(self, task_id: TaskID) -> TaskWithPosition | None:
        # Returns copy so don't have to be worried about caller modifying task.
        # Maybe should return json?
        async with self.condition:
            if task_id in self._tasks:
                return self._get_task_by_id(task_id)

    def _get_task_by_id(self, task_id: TaskID) -> TaskWithPosition:
        task = self._tasks.get_must_exist(task_id)
        position = self.queue.index(task.id) if task.id in self.queue else None
        return TaskWithPosition.from_task(task, position)

    async def get_task_by_position(self, position: int) -> TaskWithPosition | None:
        # Returns copy so don't have to be worried about caller modifying task.
        # Maybe should return json?
        async with self.condition:
            if position < -self.length or position >= self.length:
                return None
            return TaskWithPosition.from_task(
                self._tasks[self.queue[position]], position
            )

    async def get_queue(self) -> list[TaskWithPosition]:
        # Returns copies so don't have to be worried about caller modifying tasks.
        # Maybe should return json?
        async with self.condition:
            return [self._get_task_by_id(task_id) for task_id in self.queue]

    async def get_history(self) -> list[TaskWithPosition]:
        # Returns copies so don't have to be worried about caller modifying tasks.
        # Maybe should return json?
        async with self.condition:
            return [self._get_task_by_id(task_id) for task_id in self.history]

    async def get_tasks(self) -> list[TaskWithPosition]:
        # Returns copies so don't have to be worried about caller modifying tasks.
        # Maybe should return json?
        async with self.condition:
            return [
                self._get_task_by_id(task_id) for task_id in self.history + self.queue
            ]

    async def add_tasks(self, tasks: list[Task], position: int | None = None) -> None:
        async with self.condition:
            self._verify_new_tasks(tasks, position)
            if position is not None:
                position = self._get_valid_position(position)
            self._add_tasks(tasks, position)
            self.condition.notify_all()

    async def move_task(self, task_id: TaskID, position: int):
        async with self.condition:
            position = self._get_valid_position(position)
            task_ids = self._remove_tasks_from_queue([task_id])
            self.queue[position:position] = task_ids
            self.condition.notify_all()

    async def remove_tasks(self, task_ids: list[TaskID]) -> list[Task]:
        async with self.condition:
            task_ids = self._remove_tasks_from_queue(task_ids)
            tasks = self._remove_tasks_from_registry(task_ids)
            self.condition.notify_all()
            return tasks

    async def clear_history(self):
        async with self.condition:
            for task_id in self.history:
                self._tasks.pop(task_id)
            self.history.clear()
            self.condition.notify_all()

    async def pause(self):
        async with self.condition:
            self.paused = True

    async def unpause(self):
        async with self.condition:
            self.paused = False
            self.condition.notify_all()

    def _task_available(self) -> bool:
        if self.paused or not self.queue:
            return False
        return self._tasks[self.queue[0]].status == Status.WAITING

    def _check_task_valid_to_be_returned(self, task: Task):
        # Check caller has actual task object not copy
        # This ensures the caller has claimed the task, reducing the chance a task is
        # returned that is actually still being run/modified by a different process.
        # However if the worker crashes we then lose the Task object and can't return
        # the task? Needs discussion with others.
        assert task is self._tasks.get_must_exist(task.id)
        assert task.id in self.queue, f"This task is not in the queue: {task}"

    def _get_valid_position(self, position: int) -> int:
        if position < 0:
            raise ValueError(f"Position must be >= 0, got {position}")
        if (  # if position 0 requested but a task is in progress, return position 1
            self.length
            and position == 0
            and self._tasks[self.queue[0]].status != Status.WAITING
        ):
            return 1
        return position

    def _add_tasks(self, tasks: list[Task], position: int | None) -> None:
        task_ids = [task.id for task in tasks]
        if position is None:
            self.queue.extend(task_ids)
        else:
            self.queue[position:position] = task_ids
        for task in tasks:
            self._tasks[task.id] = task

    def _remove_tasks_from_queue(self, task_ids: list[TaskID]) -> list[TaskID]:
        #  Only removes tasks in the queue (not history or registry)
        def should_be_removed(task_id: TaskID):
            return (
                task_id in self.queue
                and self._tasks[task_id].status != Status.IN_PROGRESS
            )

        removed_ids = [task_id for task_id in task_ids if should_be_removed(task_id)]
        self.queue = [task_id for task_id in self.queue if task_id not in removed_ids]

        return removed_ids

    def _remove_tasks_from_registry(self, task_ids: list[TaskID]) -> list[Task]:
        # Should remove tasks from queue/history before removing from registry
        def should_be_removed(task_id: TaskID) -> bool:
            return (
                task_id in self._tasks
                and self._tasks[task_id].status != Status.IN_PROGRESS
                and task_id not in self.queue
                and task_id not in self.history
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

    @property
    def length(self):
        return len(self.queue)

    def _verify_new_tasks(self, tasks: list[Task], position: int | None):
        if position and position < 0:
            raise ValueError(f"Position: {position} cannot be less than 0.")
        for task in tasks:
            if task.id in self._tasks:
                raise ValueError(f"TaskID '{task.id}' already in use!")
