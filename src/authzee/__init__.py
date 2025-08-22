

__version__ = "0.2.0"

__all__ = [
    "Authzee",
    "AuthzeeAsync"
]

from loguru import logger
logger.disable("authzee")

from authzee.authzee import Authzee
from authzee.authzee_async import AuthzeeAsync

from authzee.compute import *
from authzee.compute import __all__ as compute_all
__all__ += compute_all

from authzee.storage import *
from authzee.storage import __all__ as storage_all
__all__ += storage_all

