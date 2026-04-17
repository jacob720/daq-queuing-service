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
from blueapi.service.model import TaskRequest, TrackableTask, WorkerTask
from blueapi.worker import WorkerState
from pydantic import HttpUrl

from daq_queuing_service.blueapi_adapter import BlueapiClientAdapter
from daq_queuing_service.task import Task
from daq_queuing_service.task_queue.queue import TaskQueue

LOGGER = logging.getLogger(__name__)


def construct_blueapi_task_request(task: Task) -> TaskRequest:
    return TaskRequest(
        name=task.experiment_definition.plan_name,
        params=task.experiment_definition.params,
        instrument_session=task.experiment_definition.instrument_session,
    )


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

    async def _process_task(self, task: Task, timeout_s: int = 600):
        if not await self._ensure_blueapi_task_exists(task):
            return

        await self._run_and_complete_task(task, timeout_s)

    async def _ensure_blueapi_task_exists(self, task: Task):
        if not task.blueapi_id:
            task_request = construct_blueapi_task_request(task)
            result = self._client.create_task(task_request)
            if result.value:
                task.blueapi_id = result.value.task_id
                return True
            else:
                assert result.error is not None
                await self._handle_create_task_error(task, result.error)
                return False

    async def _run_and_complete_task(self, task: Task, timeout_s: int = 600):
        assert task.blueapi_id
        result = self._client.update_worker_task(WorkerTask(task_id=task.blueapi_id))

        if not result.value:
            assert result.error
            await self._handle_update_worker_task_error(task, result.error)
            return

        task.put_in_progress()
        LOGGER.info(f"Task {task.id} is in progress, blueapi ID: {task.blueapi_id}")
        blueapi_task = await self._wait_for_task_to_finish(task.blueapi_id, timeout_s)

        if blueapi_task:
            if blueapi_task.errors:
                await self._queue.fail_task(task, blueapi_task.errors)
            else:
                await self._queue.complete_task(task)
        else:
            LOGGER.info("Lost connection to blueapi, terminating loop")

    async def _wait_for_task_to_finish(
        self, blueapi_task_id: str, timeout_s: int
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
                break

        return blueapi_task

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
