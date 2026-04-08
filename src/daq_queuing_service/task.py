from enum import StrEnum
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class Status(StrEnum):
    WAITING = "Waiting"
    IN_PROGRESS = "In progress"
    COMPLETED = "Completed"
    FAILED = "Failed"


class ExperimentDefinition(BaseModel):
    # match ulims
    sample_id: str
    # experiment_id: str
    # something_unique: str  # then we wouldn't need task_id
    # params: dict


class Task(BaseModel):
    experiment_definition: ExperimentDefinition
    task_id: UUID | str = Field(default_factory=uuid4)
    status: Status = Status.WAITING

    def update_status(self, new_status: Status):
        self.status = new_status
