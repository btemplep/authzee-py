"""Compute and Storage Module Base Meta class and method handlers"""

__all__ = []

from abc import ABCMeta
import functools
from typing import Any, Callable

from authzee.types import GenericResult


def _generic_result_handler(func, error_type):
    @functools.wraps(func)
    async def wrapper(self, *args, **kwargs) -> GenericResult:
        try:
            return await func(self, *args, **kwargs)

        except Exception as exc:
            return {
                "error": {
                    "error_type": error_type,
                    "message": f"[{exc.__class__.__qualname__}] {exc}"
                }
            }

    return wrapper


def _make_result_handler(default_fields: dict[str, Any]) -> Callable:
    def handler(func, error_type):
        @functools.wraps(func)
        async def wrapper(self, *args, **kwargs):
            try:
                return await func(self, *args, **kwargs)

            except Exception as exc:
                result = dict(default_fields)
                result['error'] = {
                    "error_type": error_type,
                    "message": f"[{exc.__class__.__qualname__}] {exc}"
                }

                return result

        return wrapper

    return handler


class _ModuleMeta(ABCMeta):
    _error_type: str = "unknown"
    _handler_map: dict[str, Callable] = {}


    def __new__(mcls, name: str, bases, namespace: dict[str, Any]):
        for attr_name, attr_value in namespace.items():
            if (
                attr_name in mcls._handler_map
                and getattr(attr_value, "__isabstractmethod__", False) is False
            ):
                namespace[attr_name] = mcls._handler_map[attr_name](attr_value, mcls._error_type)

        return super().__new__(mcls, name, bases, namespace)
