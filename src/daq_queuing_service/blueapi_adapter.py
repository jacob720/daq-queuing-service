import logging
from dataclasses import dataclass
from typing import TypeVar

from blueapi.client import BlueapiRestClient
from blueapi.client.rest import (
    BlueskyRemoteControlError,
    InvalidParametersError,
    ServiceUnavailableError,
    TaskResponse,
)
from blueapi.service.model import TaskRequest, TrackableTask, WorkerTask
from blueapi.worker import WorkerState

LOGGER = logging.getLogger(__name__)

T = TypeVar("T")
E = TypeVar("E", bound=Exception)


@dataclass(frozen=True)
class BlueapiResult[T, E: Exception]:
    value: T | None = None
    error: E | None = None

    def __post_init__(self):
        if (self.value is None) == (self.error is None):
            raise ValueError("Exactly one of value or error must be set")


class BlueapiClientAdapter:
    def __init__(self, client: BlueapiRestClient):
        self.client = client

    def get_state(self) -> BlueapiResult[WorkerState, ServiceUnavailableError]:
        try:
            return BlueapiResult(value=self.client.get_state())
        except ServiceUnavailableError as e:
            LOGGER.error(f"Lost connection to blueapi: {e}")
            return BlueapiResult(error=e)

    def create_task(
        self, task_request: TaskRequest
    ) -> BlueapiResult[TaskResponse, InvalidParametersError | ServiceUnavailableError]:
        try:
            return BlueapiResult(value=self.client.create_task(task_request))
        except InvalidParametersError as e:
            LOGGER.exception(e)
            return BlueapiResult(error=e)
        except ServiceUnavailableError as e:
            LOGGER.error(f"Lost connection to blueapi: {e}")
            return BlueapiResult(error=e)

    def update_worker_task(
        self, worker_task: WorkerTask
    ) -> BlueapiResult[WorkerTask, BlueskyRemoteControlError | ServiceUnavailableError]:
        try:
            return BlueapiResult(value=self.client.update_worker_task(worker_task))
        except BlueskyRemoteControlError as e:
            LOGGER.error(e)
            return BlueapiResult(error=e)
        except ServiceUnavailableError as e:
            LOGGER.error(f"Lost connection to blueapi: {e}")
            return BlueapiResult(error=e)

    def get_task(
        self, task_id: str
    ) -> BlueapiResult[TrackableTask, ServiceUnavailableError]:
        try:
            return BlueapiResult(value=self.client.get_task(task_id))
        except ServiceUnavailableError as e:
            LOGGER.error(f"Lost connection to blueapi: {e}")
            return BlueapiResult(error=e)
