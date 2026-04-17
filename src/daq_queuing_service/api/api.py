from fastapi import APIRouter, Request
from pydantic import BaseModel

from daq_queuing_service.blueapi_adapter import BlueapiClientAdapter
from daq_queuing_service.task import ExperimentDefinition, Status, Task
from daq_queuing_service.task_queue.queue import (
    QueueState,
    TaskQueue,
    TaskWithPosition,
)


# pyright: reportUnusedFunction=false
class QueueStateUpdate(BaseModel):
    paused: bool | None = None


class TaskCancelRequest(BaseModel):
    task_ids: list[str]


def _filter_by_status(
    tasks: list[TaskWithPosition], status: Status | None
) -> list[TaskWithPosition]:
    if status is None:
        return tasks
    return [task for task in tasks if task.status == status]


def create_api_router(
    queue: TaskQueue, blueapi_client: BlueapiClientAdapter
) -> APIRouter:
    router = APIRouter()

    @router.get("/")
    def read_root(request: Request):
        base_url = str(request.base_url)
        return (
            f"Welcome to the daq queuing service. Visit {base_url}docs for Uvicorn API."
        )

    @router.patch("/queue/state")
    async def update_queue_state(payload: QueueStateUpdate) -> QueueState:
        return await queue.update_state(**payload.model_dump(exclude_none=True))

    @router.get("/queue/state")
    def get_queue_state() -> QueueState:
        return queue.state

    @router.get("/queue")
    async def get_queued_tasks(status: Status | None = None) -> list[TaskWithPosition]:
        return _filter_by_status(await queue.get_queue(), status)

    @router.post("/queue")
    async def add_tasks_to_queue(
        experiment_definitions: list[ExperimentDefinition], position: int | None = None
    ) -> list[str]:
        tasks = [
            Task(experiment_definition=experiment_definition)
            for experiment_definition in experiment_definitions
        ]
        task_ids = [task.id for task in tasks]
        await queue.add_tasks(tasks, position)
        return task_ids

    @router.post("/queue/move")
    async def move_task(task_id: str, new_position: int) -> int:
        return await queue.move_task(task_id, new_position)

    @router.delete("/queue/tasks")
    async def cancel_tasks(payload: TaskCancelRequest) -> list[Task]:
        return await queue.cancel_tasks(payload.task_ids)

    @router.get("/queue/{position}")
    async def get_task_by_position(position: int) -> TaskWithPosition | None:
        return await queue.get_task_by_position(position)

    @router.get("/tasks")
    async def get_all_tasks(status: Status | None = None) -> list[TaskWithPosition]:
        return _filter_by_status(await queue.get_tasks(), status)

    @router.get("/tasks/{task_id}")
    async def get_task_by_id(task_id: str) -> TaskWithPosition:
        return await queue.get_task_by_id(task_id)

    @router.delete("/history")
    async def clear_history():
        return await queue.clear_history()

    @router.get("/history")
    async def get_historic_tasks(
        status: Status | None = None,
    ) -> list[TaskWithPosition]:
        return _filter_by_status(await queue.get_history(), status)

    return router
