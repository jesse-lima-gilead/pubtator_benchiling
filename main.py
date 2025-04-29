from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware

from src.routes import query_routes
from src.pubtator_utils.logs_handler.logger import SingletonLogger

logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting up the application")
    logger.debug("Debug message from main.py")
    yield
    logger.info("Shutting down the application")


app = FastAPI(
    title="GRAI Pubtator Retrieval API",
    description="GRAI Pubtator Retrieval API to retrieve semantically matched PubMed chunks using OpenSearch and PubMedBERT.",
    version="1.0.0",
    docs_url="/docs",  # Swagger UI
    redoc_url="/redoc",  # ReDoc UI
    openapi_url="/openapi.json",  # OpenAPI schema
    lifespan=lifespan,
)

# Optional: CORS if needed
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Optional: Health Check
@app.get("/")
def read_root():
    return {"message": "PubTator Retrieval API is up and running."}


# Exception Handlers
@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled error: {exc}")
    return JSONResponse(status_code=500, content={"detail": "Internal Server Error"})


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    logger.warning(f"Validation error: {exc}")
    return JSONResponse(status_code=422, content={"detail": exc.errors()})


# Route registration
app.include_router(query_routes.router, prefix="")
