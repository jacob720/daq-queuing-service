from daq_queuing_service.task import ExperimentDefinition, Status, Task
import asyncio


class TaskQueue:
    def __init__(self):
        self.queue: list[Task] = []
        self.tasks: dict[str, Task] = {}
        self.lock = asyncio.Lock()
        self.condition = asyncio.Condition(self.lock)
        self.paused: bool = False

    def _task_available(self) -> bool:
        if self.paused or not self.queue:
            return False
        return self.queue[0].status == Status.WAITING

    async def claim_next_task_once_available(self):
        async with self.condition:
            while not self._task_available():
                await self.condition.wait()
            return self.queue[0]

    async def get_task_by_id(self, task_id: str) -> Task | None:
        return self.tasks.get(task_id).model_dump_json()

    async def get_task_by_position(self, position: int) -> int | None:
        return self.queue[position] if position < self.length else None

    async def add_tasks(self, tasks: list[Task], position: int | None = None) -> None:
        async with self.lock:
            self._verify_new_tasks(tasks, position)
            self._add_tasks(tasks, position)
            self.condition.notify_all()

    async def move_tasks(self, task_ids: set[str], position: int):
        async with self.lock:
            tasks = self._remove_tasks(task_ids)
            self._add_tasks(tasks, position)

    async def remove_tasks(self, task_ids: set[str]) -> list[Task]:
        async with self.lock:
            tasks = self._remove_tasks(task_ids)
            self.condition.notify_all()
            return tasks

    def _add_tasks(self, tasks: list[Task], position: int | None) -> None:
        if position is None:
            self.queue.extend(tasks)
        else:
            self.queue[position:position] = tasks
        for task in tasks:
            self.tasks[task.task_id] = task

    def _remove_tasks(self, task_ids: set[str]) -> list[Task]:
        removed = [task for task in self.queue if task.task_id in task_ids]
        self.queue = [task for task in self.queue if task.task_id not in task_ids]
        self.tasks = {
            task_id: task
            for task_id, task in self.tasks.items()
            if task_id not in task_ids
        }
        return removed

    def pause(self):
        self.paused = True

    def unpause(self):
        self.paused = False
        self.condition.notify_all()

    def _verify_new_tasks(self, tasks, position): ...

    async def change_task_status(self, task_id: str, status: Status):
        if task := await self.get_task_by_id(task_id):
            task.update_status(status)

    def print_queue(self):
        for task in self.queue:
            print(task.model_dump_json())


async def main():
    queue = TaskQueue()
    some_tasks = [
        Task(
            experiment_definition=ExperimentDefinition(sample_id=str(i)), task_id=str(i)
        )
        for i in range(10)
    ]
    await queue.add_tasks(some_tasks)
    queue.print_queue()
    print("----------------------")
    await queue.remove_tasks({"4", "5", "6"})
    queue.print_queue()
    print("----------------------")
    await queue.move_tasks({"8"}, 0)
    queue.print_queue()


if __name__ == "__main__":
    asyncio.run(main())
