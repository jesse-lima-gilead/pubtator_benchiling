from contextlib import asynccontextmanager

from fastapi import FastAPI

from src.routes import ingestion_pipeline_routes, query_routes
from src.pubtator_utils.logs_handler.logger import SingletonLogger

# Instantiate the singleton logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting up the application")
    yield
    logger.info("Shutting down the application")


# Initialising the FastAPI app
app = FastAPI(lifespan=lifespan)

# Include routers
# app.include_router(ingestion_pipeline_routes.router, prefix="")
app.include_router(query_routes.router, prefix="")

# Example usage of the logger in main.py
logger.debug("Debug message from main.py")
