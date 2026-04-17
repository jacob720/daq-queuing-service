import asyncio
import logging

from blueapi.client import BlueapiRestClient
from blueapi.client.rest import (
    InvalidParametersError,
    ServiceUnavailableError,
)
from blueapi.config import RestConfig
from blueapi.service.model import TaskRequest, TrackableTask, WorkerTask
from blueapi.worker import WorkerState
from pydantic import HttpUrl

from daq_queuing_service.blueapi_adapter import BlueapiClientAdapter
from daq_queuing_service.queue.queue import TaskQueue
from daq_queuing_service.task import Task

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
            await self._wait_for_queue_and_blueapi_ready()
            next_task = await self._queue.claim_next_task_once_available()
            await self._send_task_to_blue_api_and_wait_for_completion(next_task)

    async def _wait_for_queue_and_blueapi_ready(self):
        while True:
            await self._queue.wait_until_task_available()
            result = self._client.get_state()
            if result.value == WorkerState.IDLE:
                break
            LOGGER.debug(
                f"Waiting for BlueAPI worker to be IDLE, currently {result.value}"
            )
            await asyncio.sleep(self.poll_time_s)

    async def _send_task_to_blue_api_and_wait_for_completion(
        self, task: Task, timeout_s: int = 600
    ):
        task_request = construct_blueapi_task_request(task)

        if not task.blueapi_id:
            result = self._client.create_task(task_request)
            if result.value:
                task.blueapi_id = result.value.task_id
            elif isinstance(result.error, InvalidParametersError):
                await self._queue.fail_task(
                    task, errors=[str(error) for error in result.error.errors]
                )
            elif isinstance(result.error, ServiceUnavailableError):
                await self._queue.return_task_to_queue(task)

        await self._start_blueapi_task_and_wait_for_completion(task, timeout_s)

    async def _start_blueapi_task_and_wait_for_completion(
        self, task: Task, timeout_s: int = 600
    ):
        assert task.blueapi_id
        result = self._client.update_worker_task(WorkerTask(task_id=task.blueapi_id))
        if not result.value:
            # Issue with blueapi worker state or connection - should retry task later
            await self._queue.return_task_to_queue(task)
            return

        else:
            task.put_in_progress()
            LOGGER.info(f"Task {task.id} is in progress, blueapi ID: {task.blueapi_id}")
            blueapi_task = await self._get_blueapi_task_once_complete(
                task.blueapi_id, timeout_s
            )

        if blueapi_task:
            if blueapi_task.errors:
                await self._queue.fail_task(task, blueapi_task.errors)
            else:
                await self._queue.complete_task(task)
        else:
            LOGGER.info("Lost connection to blueapi, terminating loop")

    async def _get_blueapi_task_once_complete(
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
