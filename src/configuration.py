import logging

from keboola.component.exceptions import UserException
from pydantic import BaseModel, Field, ValidationError, field_validator

from system_resources import detect_cpu_count, get_optimal_memory_mb
from versions import LATEST_ALIAS, SUPPORTED_VERSIONS

# Default constants for system resources
DEFAULT_CPU_COUNT = 4
DEFAULT_MEMORY_MB = 4096


class Code(BaseModel):
    name: str
    script: list[str]


class Block(BaseModel):
    name: str
    codes: list[Code] = Field(default_factory=list)


class Configuration(BaseModel):
    blocks: list[Block] = Field(default_factory=list)
    threads: int | None = Field(default=None, description="Number of threads (None for auto-detection)")
    max_memory_mb: int | None = Field(default=None, description="Memory limit in MB (None for auto-detection)")
    dtypes_infer: bool = False
    debug: bool = False
    syntax_check_on_startup: bool = Field(default=False)
    duckdb_version: str = LATEST_ALIAS

    @field_validator("duckdb_version")
    @classmethod
    def validate_duckdb_version(cls, v: str) -> str:
        if v != LATEST_ALIAS and v not in SUPPORTED_VERSIONS:
            raise ValueError(f"duckdb_version must be one of: {sorted(SUPPORTED_VERSIONS | {LATEST_ALIAS})}")
        return v

    def __init__(self, /, **data):
        try:
            super().__init__(**data)
            if self.debug:
                logging.debug("Component will run in Debug mode")
            # Apply resource detection
            self._apply_resource_detection()
        except ValidationError as e:
            error_messages = [f"{err['loc'][0]}: {err['msg']}" for err in e.errors()]
            raise UserException(f"Validation Error: {', '.join(error_messages)}")

    def _apply_resource_detection(self):
        """Apply resource detection logic."""
        # Get detected values for resources
        detected_threads = detect_cpu_count()
        optimal_memory = get_optimal_memory_mb()

        # Handle threads
        if self.threads is None:
            if detected_threads is not None:
                self.threads = detected_threads
            else:
                self.threads = DEFAULT_CPU_COUNT
                logging.info(f"Using default threads: {self.threads}")
        else:
            # Check if user value differs significantly from optimal
            if detected_threads is not None and self.threads != detected_threads:
                logging.info(f"User specified threads: {self.threads}, detected: {detected_threads}")

        # Handle memory
        if self.max_memory_mb is None:
            if optimal_memory is not None:
                self.max_memory_mb = optimal_memory
            else:
                self.max_memory_mb = DEFAULT_MEMORY_MB
                logging.info(f"Using default memory limit: {self.max_memory_mb}MB")
        else:
            # Check if user value differs significantly from optimal
            if optimal_memory is not None and self.max_memory_mb != optimal_memory:
                logging.info(f"User specified memory: {self.max_memory_mb}MB, optimal would be: {optimal_memory}MB")
