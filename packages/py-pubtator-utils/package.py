from spack.package import *


class DataIngestionPipeline(PythonPackage):
    """Data Ingestion Pipeline for extracting and processing PMC articles."""

    homepage = "https://github.com/EDnA-IT-Research/GileadPubtator/"  # Replace with your repo URL
    url = "https://github.com/EDnA-IT-Research/GileadPubtator/archive/refs/tags/v0.1.tar.gz"  # Whole repo

    version("main", branch="main")  # Default branch for Spack to pull from

    # Required Python version
    depends_on("python@3.12:", type=("build", "run"))

    # External dependencies from pyproject.toml
    depends_on("py-python-dotenv", type=("build", "run"))
    depends_on("py-pyyaml", type=("build", "run"))
    depends_on("py-requests", type=("build", "run"))
    depends_on("py-pre-commit", type=("build", "run"))
    depends_on("py-alembic", type=("build", "run"))
    depends_on("py-sqlalchemy", type=("build", "run"))
    depends_on("py-bioc", type=("build", "run"))
    depends_on("py-bio", type=("build", "run"))
    depends_on("py-psycopg2-binary", type=("build", "run"))
    depends_on("py-qdrant-client", type=("build", "run"))
    depends_on("py-langchain-aws", type=("build", "run"))
    depends_on("py-jupyter", type=("build", "run"))
    depends_on("py-tiktoken", type=("build", "run"))
    depends_on("py-matplotlib", type=("build", "run"))
    depends_on("py-seaborn", type=("build", "run"))
    depends_on("py-plotly", type=("build", "run"))
    depends_on("py-pyarrow", type=("build", "run"))
    depends_on("py-fastapi", type=("build", "run"))
    depends_on("py-uvicorn", type=("build", "run"))
    depends_on("py-pydantic", type=("build", "run"))

    # Internal project dependencies (these should be available inside the repo)
    depends_on("py-filehandler", type=("build", "run"))
    depends_on("py-utils", type=("build", "run"))
    depends_on("py-llmhandler", type=("build", "run"))
    depends_on("py-prompts", type=("build", "run"))
    depends_on("py-vectordbhandler", type=("build", "run"))

    def install(self, spec, prefix):
        """Custom installation steps for the pipeline."""
        # Install Python package using setup.py
        pip_install = which("pip")
        pip_install("install", "--prefix=" + prefix, ".")

        # Ensure Spack copies configuration files
        install_tree("config", prefix.config)

        # Ensure Spack copies the source code
        install_tree("src", prefix.src)
