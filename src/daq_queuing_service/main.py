from fastapi import FastAPI, Request
from pydantic import BaseModel

from daq_queuing_service.queue import TaskQueue, TaskWithPosition
from daq_queuing_service.task import ExperimentDefinition, Status, Task, TaskID

app = FastAPI()
queue = TaskQueue()


def _filter_by_status(
    tasks: list[TaskWithPosition], status: Status | None
) -> list[TaskWithPosition]:
    if status is None:
        return tasks
    return [task for task in tasks if task.status == status]


@app.get("/")
def read_root(request: Request):
    base_url = str(request.base_url)
    return f"Welcome to the daq queuing service. Visit {base_url}docs for Uvicorn API."


@app.get("/queue")
async def get_queue(status: Status | None = None) -> list[TaskWithPosition]:
    return _filter_by_status(await queue.get_queue(), status)


@app.get("/queue/{position}")
async def get_task_by_position(position: int) -> TaskWithPosition | None:
    return await queue.get_task_by_position(position)


@app.get("/tasks")
async def get_tasks(status: Status | None = None) -> list[TaskWithPosition]:
    return _filter_by_status(await queue.get_tasks(), status)


@app.get("/tasks/{task_id}")
async def get_task(task_id: str) -> TaskWithPosition | None:
    return await queue.get_task_by_id(task_id)


@app.get("/history")
async def get_history(status: Status | None = None) -> list[TaskWithPosition]:
    return _filter_by_status(await queue.get_history(), status)


@app.post("/queue")
async def add_tasks(
    experiment_definitions: list[ExperimentDefinition], position: int | None = None
) -> list[TaskID]:
    tasks = [
        Task(experiment_definition=experiment_definition)
        for experiment_definition in experiment_definitions
    ]
    task_ids = [task.id for task in tasks]
    await queue.add_tasks(tasks, position)
    return task_ids


@app.post("/queue/move")
async def move_task(task_id: str, new_position: int) -> int:
    return await queue.move_task(task_id, new_position)


class TaskDeleteRequest(BaseModel):
    task_ids: list[str]


@app.delete("/tasks")
async def remove_tasks(payload: TaskDeleteRequest) -> list[Task]:
    return await queue.remove_tasks(payload.task_ids)


@app.delete("/history")
async def clear_history():
    return await queue.clear_history()


@app.post("/pause")
async def pause():
    return await queue.pause()


@app.post("/unpause")
async def unpause():
    return await queue.unpause()


@app.get("/paused")
def paused():
    return queue.paused
