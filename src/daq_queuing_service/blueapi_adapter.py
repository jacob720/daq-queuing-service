import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import TypeVar

from blueapi.client import BlueapiRestClient
from blueapi.client.rest import (
    BlueskyRemoteControlError,
    InvalidParametersError,
    ServiceUnavailableError,
    TaskResponse,
    UnknownPlanError,
)
from blueapi.service.model import TaskRequest, TrackableTask, WorkerTask
from blueapi.worker import WorkerState

from daq_queuing_service.task import ExperimentDefinition

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
    def __init__(
        self,
        client: BlueapiRestClient,
        blueapi_task_request_constructor: Callable[[ExperimentDefinition], TaskRequest],
    ):
        self.client = client
        self._task_request_constructor = blueapi_task_request_constructor

    def get_state(self) -> BlueapiResult[WorkerState, ServiceUnavailableError]:
        try:
            return BlueapiResult(value=self.client.get_state())
        except ServiceUnavailableError as e:
            LOGGER.error(f"Lost connection to blueapi: {e}")
            return BlueapiResult(error=e)

    def create_task(
        self, experiment_definition: ExperimentDefinition
    ) -> BlueapiResult[
        TaskResponse,
        InvalidParametersError | UnknownPlanError | ServiceUnavailableError,
    ]:
        task_request = self._task_request_constructor(experiment_definition)
        try:
            return BlueapiResult(value=self.client.create_task(task_request))
        except (InvalidParametersError, UnknownPlanError) as e:
            LOGGER.exception(e)
            return BlueapiResult(error=e)
        except ServiceUnavailableError as e:
            LOGGER.error("Lost connection to blueapi")
            return BlueapiResult(error=e)

    def update_worker_task(
        self, worker_task: WorkerTask
    ) -> BlueapiResult[
        WorkerTask, BlueskyRemoteControlError | KeyError | ServiceUnavailableError
    ]:
        try:
            return BlueapiResult(value=self.client.update_worker_task(worker_task))
        except BlueskyRemoteControlError as e:
            LOGGER.error(e)
            return BlueapiResult(error=e)
        except KeyError as e:
            LOGGER.error(e)
            return BlueapiResult(error=e)
        except ServiceUnavailableError as e:
            LOGGER.error(f"Lost connection to blueapi: {e}")
            return BlueapiResult(error=e)

    def get_task(
        self, task_id: str
    ) -> BlueapiResult[TrackableTask, KeyError | ServiceUnavailableError]:
        try:
            return BlueapiResult(value=self.client.get_task(task_id))
        except KeyError as e:
            LOGGER.error(f"No blueapi task found with ID {task_id}: {e}")
            return BlueapiResult(error=e)
        except ServiceUnavailableError as e:
            LOGGER.error(f"Lost connection to blueapi: {e}")
            return BlueapiResult(error=e)


def construct_blueapi_task_request(
    experiment_definition: ExperimentDefinition,
) -> TaskRequest:
    return TaskRequest(
        name=experiment_definition.plan_name,
        params=experiment_definition.params,
        instrument_session=experiment_definition.instrument_session,
    )
