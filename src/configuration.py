import logging

from keboola.component.exceptions import UserException
from pydantic import BaseModel, Field, ValidationError


class Code(BaseModel):
    name: str
    script: list[str]


class Block(BaseModel):
    name: str
    codes: list[Code] = Field(default_factory=list)


class Configuration(BaseModel):
    blocks: list[Block] = Field(default_factory=list)
    threads: int = 1
    max_memory_mb: int = 768
    dtypes_infer: bool = False
    debug: bool = False

    def __init__(self, **data):
        try:
            super().__init__(**data)
        except ValidationError as e:
            error_messages = [f"{err['loc'][0]}: {err['msg']}" for err in e.errors()]
            raise UserException(f"Validation Error: {', '.join(error_messages)}")

        if self.debug:
            logging.debug("Component will run in Debug mode")
