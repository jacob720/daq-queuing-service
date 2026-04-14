import time
from enum import StrEnum
from typing import Self
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

type TaskID = str | UUID


class Status(StrEnum):
    WAITING = "Waiting"  # Waiting in the queue
    IN_PROGRESS = "In progress"  # Claimed by the worker
    SUCCESS = "Success"  # Completed successfully
    ERROR = "Error"  # Error while trying to run
    CANCELLED = "Cancelled"  # Cancelled before being run


class ExperimentDefinition(BaseModel):
    # match ulims
    sample_id: str
    # experiment_id: str
    # something_unique: str  # then we wouldn't need task_id
    # params: dict


class Task(BaseModel):
    experiment_definition: ExperimentDefinition
    id: TaskID = Field(default_factory=uuid4)
    status: Status = Status.WAITING
    time_started: float | None = None
    time_completed: float | None = None
    errors: list[str] = Field(default_factory=list[str])

    @property
    def _locked(self):
        return self.status in [Status.SUCCESS, Status.ERROR, Status.CANCELLED]

    def _check_lock(self):
        if self._locked:
            raise ValueError(
                f"Task is locked and read-only due to status: {self.status}"
            )

    def update_status(self, new_status: Status):
        self._check_lock()
        self.status = new_status
        if new_status == Status.IN_PROGRESS:
            self.time_started = time.time()
        elif new_status in [Status.SUCCESS, Status.ERROR]:
            self.time_completed = time.time()
        elif new_status == Status.WAITING:
            self.time_started = None

    def add_error(self, error: str):
        self._check_lock()
        self.errors.append(error)


class TaskWithPosition(Task):
    position: int | None

    @classmethod
    def from_task(cls, task: Task, position: int | None = None) -> Self:
        return cls.model_validate({**task.model_dump(), "position": position})
