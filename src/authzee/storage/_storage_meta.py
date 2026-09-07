"""Storage meta class"""

__all__ = []

from typing import Callable

from authzee._module_meta import (
    _generic_result_handler,
    _make_result_handler,
    _ModuleMeta
)


_context_def_result_handler = _make_result_handler(
    {
        "context_def": None
    }
)
_context_defs_page_handler = _make_result_handler(
    {
        "context_defs": [],
        "next_page_ref": None
    }
)
_identity_def_result_handler = _make_result_handler(
    {
        "identity_def": None
    }
)
_identity_defs_page_handler = _make_result_handler(
    {
        "identity_defs": [],
        "next_page_ref": None
    }
)
_resource_def_result_handler = _make_result_handler(
    {
        "resource_def": None
    }
)
_resource_defs_page_handler = _make_result_handler(
    {
        "resource_defs": [],
        "next_page_ref": None
    }
)
_grant_result_handler = _make_result_handler(
    {
        "grant": None
    }
)
_grants_page_handler = _make_result_handler(
    {
        "grants": [],
        "next_page_ref": None
    }
)
_page_refs_page_handler = _make_result_handler(
    {
        "page_refs": [],
        "next_page_ref": None
    }
)
_storage_latch_result_handler = _make_result_handler(
    {
        "storage_latch": None
    }
)


class _StorageMeta(_ModuleMeta):
    _error_type: str = "storage"
    _handler_map: dict[str, Callable] = {
        "start": _generic_result_handler,
        "shutdown": _generic_result_handler,
        "construct": _generic_result_handler,
        "destroy": _generic_result_handler,
        "list_context_defs": _context_defs_page_handler,
        "get_context_def": _context_def_result_handler,
        "put_context_def": _generic_result_handler,
        "delete_context_def": _generic_result_handler,
        "list_identity_defs": _identity_defs_page_handler,
        "get_identity_def": _identity_def_result_handler,
        "put_identity_def": _generic_result_handler,
        "delete_identity_def": _generic_result_handler,
        "list_resource_defs": _resource_defs_page_handler,
        "get_resource_def": _resource_def_result_handler,
        "put_resource_def": _generic_result_handler,
        "delete_resource_def": _generic_result_handler,
        "enact": _generic_result_handler,
        "repeal": _generic_result_handler,
        "get_grant": _grant_result_handler,
        "list_grants": _grants_page_handler,
        "list_grant_refs": _page_refs_page_handler,
        "create_latch": _storage_latch_result_handler,
        "get_latch": _storage_latch_result_handler,
        "set_latch": _storage_latch_result_handler,
        "delete_latch": _generic_result_handler,
        "cleanup_latches": _generic_result_handler
    }
