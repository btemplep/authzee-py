"""This is the official python SDK for Authzee! It is a general usage SDK that is async, extensible, and scalable.

Authzee is a highly expressive grant-based authorization engine. Check out the [Authzee Repo](https://github.com/btemplep/authzee) for the core engine and specification.

See [](authzee.authzee.Authzee)
or [](authzee.authzee_async.AuthzeeAsync) for asyncio support!
"""

__version__ = "0.1.0a4"

__all__ = [
    "Authzee",
    "AuthzeeAsync",
    "authzee_specification_version",
    "context_def_schema",
    "exceptions",
    "grant_schema",
    "identity_def_schema",
    "paginator",
    "paginator_async",
    "reference",
    "resource_def_schema",
    "types"
]

from loguru import logger


logger.disable("authzee")

authzee_specification_version = "0.4.0"

from authzee import exceptions, reference, types
from authzee.authzee import Authzee
from authzee.authzee_async import AuthzeeAsync
from authzee.compute import *
from authzee.compute import __all__ as compute_all
from authzee.core import (
    context_def_schema,
    grant_schema,
    identity_def_schema,
    resource_def_schema
)
from authzee.paginators import paginator, paginator_async
from authzee.storage import *
from authzee.storage import __all__ as storage_all
from authzee.types import *
from authzee.types import __all__ as types_all

__all__ += compute_all + storage_all + types_all


try:
    from authzee.jmespath import *
    from authzee.jmespath import __all__ as jmespath_all
    __all__ += jmespath_all
except ModuleNotFoundError: # pragma: no cover
    pass
