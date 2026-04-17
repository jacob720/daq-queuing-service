import asyncio
import logging

from blueapi.client import BlueapiRestClient
from blueapi.client.rest import (
    BlueskyRemoteControlError,
    InvalidParametersError,
    ServiceUnavailableError,
    UnknownPlanError,
)
from blueapi.config import RestConfig
from blueapi.service.model import TrackableTask, WorkerTask
from blueapi.worker import WorkerState
from pydantic import HttpUrl

from daq_queuing_service.blueapi_adapter import (
    BlueapiClientAdapter,
    construct_blueapi_task_request,
)
from daq_queuing_service.task import Task
from daq_queuing_service.task_queue.queue import TaskQueue

LOGGER = logging.getLogger(__name__)


class QueueWorker:
    def __init__(
        self, queue: TaskQueue, blueapi_url: HttpUrl, poll_time_s: float = 1.0
    ):
        self.poll_time_s = poll_time_s
        self._queue = queue
        self._url = blueapi_url
        self._client = BlueapiClientAdapter(
            BlueapiRestClient(config=RestConfig(url=blueapi_url))
        )

    async def run_loop(self):
        while True:
            next_task = await self._wait_for_next_task()
            await self._process_task(next_task)

    async def _wait_for_next_task(self):
        while True:
            await self._queue.wait_until_task_available()
            result = self._client.get_state()
            if result.value == WorkerState.IDLE:
                break
            LOGGER.info(
                f"Waiting for BlueAPI worker to be IDLE, currently {result.value}"
            )
            await asyncio.sleep(self.poll_time_s)
        return await self._queue.claim_next_task_once_available()

    async def _process_task(self, task: Task):
        if await self._ensure_blueapi_task_exists(task) is not None:
            await self._run_and_complete_task(task)

    async def _ensure_blueapi_task_exists(self, task: Task) -> str | None:
        if task.blueapi_id:
            result = self._client.get_task(task.blueapi_id)
            if result.error:
                # If key error, create a new blueapi task and carry on
                if not await self._handle_get_value_errors(task, result.error):
                    # Otherwise return
                    return

            elif result.value and not result.value.is_pending:
                task.errors.append("This task has already been started.")
                LOGGER.error(
                    f"Cannot start task {task.id}, it is not pending."
                    + f"BlueAPI ID: {task.blueapi_id}"
                )
                # Monitor task until completion then return
                await self._complete_task(task)
                return

        if not task.blueapi_id:
            task_request = construct_blueapi_task_request(task)
            result = self._client.create_task(task_request)
            if result.value:
                task.blueapi_id = result.value.task_id
            else:
                assert result.error is not None
                await self._handle_create_task_error(task, result.error)
                return

        return task.blueapi_id

    async def _run_and_complete_task(self, task: Task):
        assert task.blueapi_id
        result = self._client.update_worker_task(WorkerTask(task_id=task.blueapi_id))

        if result.error:
            await self._handle_update_worker_task_error(task, result.error)
            return

        task.put_in_progress()
        LOGGER.info(f"Task {task.id} is in progress, blueapi ID: {task.blueapi_id}")
        await self._complete_task(task)

    async def _complete_task(self, task: Task):
        assert task.blueapi_id
        blueapi_task = await self._wait_for_task_to_finish(task.blueapi_id)

        if blueapi_task:
            if blueapi_task.errors:
                await self._queue.fail_task(task, blueapi_task.errors)
            else:
                await self._queue.complete_task(task)
        else:
            await self._queue.fail_task(task, errors=["Lost connection to blueapi"])

    async def _wait_for_task_to_finish(
        self, blueapi_task_id: str
    ) -> TrackableTask | None:
        complete = False
        blueapi_task = None

        while not complete:
            await asyncio.sleep(self.poll_time_s)
            result = self._client.get_task(blueapi_task_id)
            if result.value:
                blueapi_task = result.value
                complete = result.value.is_complete
            else:
                LOGGER.info("Lost connection to blueapi")
                return None

        return blueapi_task

    async def _handle_get_value_errors(
        self, task: Task, error: KeyError | ServiceUnavailableError
    ) -> bool:
        match error:
            case KeyError():
                task.errors.append(
                    f"Failed to find blueapi task with id {task.blueapi_id}"
                )
                task.blueapi_id = None
                return True
            case ServiceUnavailableError():
                await self._queue.return_task_to_queue(task)
                return False

    async def _handle_create_task_error(
        self,
        task: Task,
        error: InvalidParametersError | UnknownPlanError | ServiceUnavailableError,
    ):
        match error:
            case InvalidParametersError():
                await self._queue.fail_task(
                    task,
                    errors=["Invalid parameters"]
                    + [str(error) for error in error.errors],
                )
            case UnknownPlanError():
                await self._queue.fail_task(task, ["Unknown plan", str(error)])
            case ServiceUnavailableError():
                await self._queue.return_task_to_queue(task)

    async def _handle_update_worker_task_error(
        self,
        task: Task,
        error: BlueskyRemoteControlError | ServiceUnavailableError | KeyError,
    ):
        match error:
            case BlueskyRemoteControlError():
                # We get this error if the blueapi worker is busy
                await self._queue.return_task_to_queue(task)
            case KeyError():
                # We get this error if blueapi can't find a pending task with that ID
                task.blueapi_id = None
                await self._queue.return_task_to_queue(task)
            case ServiceUnavailableError():
                await self._queue.return_task_to_queue(task)
