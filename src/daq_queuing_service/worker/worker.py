import asyncio
import logging

from blueapi.client import BlueapiRestClient
from blueapi.client.rest import ServiceUnavailableError
from blueapi.config import RestConfig
from blueapi.service.model import TaskRequest, TrackableTask, WorkerTask
from blueapi.worker import WorkerState
from pydantic import HttpUrl

from daq_queuing_service.queue.queue import TaskQueue
from daq_queuing_service.task import Task

LOGGER = logging.getLogger(__name__)


def construct_blueapi_task_request(task: Task) -> TaskRequest: ...


class QueueWorker:
    def __init__(
        self, queue: TaskQueue, blueapi_url: HttpUrl, poll_time_s: float = 1.0
    ):
        self.poll_time_s = poll_time_s
        self._queue = queue
        self._url = blueapi_url
        self._client = BlueapiRestClient(config=RestConfig(url=blueapi_url))

    async def run_loop(self):
        while True:
            await self._queue.wait_until_task_available()
            if not await self._is_blue_api_idle():
                await asyncio.sleep(self.poll_time_s)
                continue
            next_task = await self._queue.claim_next_task_once_available()
            await self._send_task_to_blue_api_and_wait_for_completion(next_task)

    async def _is_blue_api_idle(self) -> bool:
        return self._client.get_state() == WorkerState.IDLE

    async def _send_task_to_blue_api_and_wait_for_completion(
        self, task: Task, timeout_s: int = 600
    ):
        task_request = construct_blueapi_task_request(task)
        try:
            response = self._client.create_task(task_request)
            blueapi_task_id = response.task_id
            self._client.update_worker_task(WorkerTask(task_id=blueapi_task_id))
            task.put_in_progress(blueapi_task_id)
        except ServiceUnavailableError as e:
            # Issue with blueapi worker state or connection - should retry task later
            await self._queue.return_task_to_queue(task)
            LOGGER.error(e)
            return
        except Exception as e:
            # Validation issue - task will not work if retried so should cancel task
            await self._queue.fail_task(task, errors=[str(e)])
            LOGGER.error(e)
            return
        await self._get_blueapi_task_once_complete(blueapi_task_id, timeout_s)
        await self._queue.complete_task(task)

    async def _get_blueapi_task_once_complete(
        self, blueapi_task_id: str, timeout_s: int
    ) -> TrackableTask:
        blueapi_task: TrackableTask = self._client.get_task(blueapi_task_id)
        while not blueapi_task.is_complete:
            await asyncio.sleep(self.poll_time_s)
            blueapi_task = self._client.get_task(blueapi_task_id)
        return blueapi_task
