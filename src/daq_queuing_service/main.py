import asyncio
import logging
from contextlib import asynccontextmanager

from blueapi.client.rest import BlueapiRestClient, RestConfig
from fastapi import FastAPI
from pydantic import HttpUrl

from daq_queuing_service.api.api import create_api_router
from daq_queuing_service.api.errors import register_exception_handlers
from daq_queuing_service.blueapi_adapter import (
    BlueapiClientAdapter,
    construct_blueapi_task_request,
)
from daq_queuing_service.task_queue.queue import TaskQueue
from daq_queuing_service.worker.worker import QueueWorker

LOCAL_BLUEAPI_URL = "http://localhost:8000/"
I15_1_BLUEAPI_URL = "https://i15-15-blueapi.diamond.ac.uk/"


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
)


def create_app() -> FastAPI:
    queue = TaskQueue()
    blueapi_client = BlueapiClientAdapter(
        BlueapiRestClient(RestConfig(url=HttpUrl(LOCAL_BLUEAPI_URL))),
        construct_blueapi_task_request,
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        worker = QueueWorker(queue=queue, blueapi_client=blueapi_client)
        worker_task = asyncio.create_task(worker.run_loop())
        try:
            yield
        finally:
            worker_task.cancel()
            await asyncio.gather(worker_task, return_exceptions=True)

    app = FastAPI(lifespan=lifespan)
    register_exception_handlers(app)
    app.include_router(create_api_router(queue, blueapi_client))

    return app


app = create_app()
