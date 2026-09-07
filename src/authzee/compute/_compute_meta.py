"""TODO: Add module docstring."""

__all__ = []

from typing import Callable

from authzee._module_meta import (
    _generic_result_handler,
    _make_result_handler,
    _ModuleMeta
)


_validate_batch_request_result_handler = _make_result_handler(
    {
        "batch": []
    }
)
_audit_result_page_handler = _make_result_handler(
    {
        "results": [],
        "next_page_ref": None
    }
)
_authorize_result_handler = _make_result_handler(
    {
        "is_authorized": False,
        "grant": None,
        "message": "An error has occurred. Therefore, the request is not authorized."
    }
)
_batch_audit_result_page_handler = _make_result_handler(
    {
        "grants": [],
        "batch": [],
        "next_page_ref": None
    }
)
_batch_authorize_result_handler = _make_result_handler(
    {
        "batch": []
    }
)


class _ComputeMeta(_ModuleMeta):
    _error_type: str = "compute"
    _handler_map: dict[str, Callable] = {
        "start": _generic_result_handler,
        "shutdown": _generic_result_handler,
        "construct": _generic_result_handler,
        "destroy": _generic_result_handler,
        "validate_context_def": _generic_result_handler,
        "validate_identity_def": _generic_result_handler,
        "validate_resource_def": _generic_result_handler,
        "validate_grant": _generic_result_handler,
        "validate_request": _generic_result_handler,
        "validate_batch_request": _validate_batch_request_result_handler,
        "audit": _audit_result_page_handler,
        "authorize": _authorize_result_handler,
        "batch_audit": _batch_audit_result_page_handler,
        "batch_authorize": _batch_authorize_result_handler
    }
