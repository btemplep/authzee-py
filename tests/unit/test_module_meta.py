"""Unit tests for the module metaclasses (`_StorageMeta` / `_ComputeMeta`).

Uses the full raising mock modules to verify that every wrapped method
translates a raised exception into the method's expected result body with the
correct ``error_type``.
"""

import asyncio
import datetime
import os
import sys

import jsonschema_rs
import pytest

from authzee.reference import (
    audit_result_schema,
    authorize_result_schema,
    batch_audit_result_schema,
    batch_authorize_result_schema,
    general_result_schema,
    validate_batch_request_result_schema,
    validate_request_result_schema
)


sys.path.insert(0, os.path.dirname(__file__))

from mock_modules import MockRaisingCompute, MockRaisingStorage


def _assert_error(result, error_type, message):
    assert result['error'] is not None
    assert result['error']['error_type'] == error_type
    assert "MockError" in result['error']['message']
    assert message in result['error']['message']


def _assert_fields(result, expected_non_error):
    for key, value in expected_non_error.items():
        assert result[key] == value, f"{key}: {result.get(key)!r} != {value!r}"

    assert set(result.keys()) == set(expected_non_error.keys()) | {"error"}


def _assert_matches_schema(result, schema):
    if schema is None:
        return

    jsonschema_rs.validator_for(schema).validate(result)


STORAGE_MESSAGE = "mock storage failure"

# Only GenericResult-shaped storage methods have a published result schema
# (`general_result_schema`). The page/def/latch result shapes have no dedicated
# schema in the reference, so they are validated structurally only (None).
STORAGE_SCHEMAS = {
    "start": general_result_schema,
    "shutdown": general_result_schema,
    "construct": general_result_schema,
    "destroy": general_result_schema,
    "list_context_defs": None,
    "get_context_def": None,
    "put_context_def": general_result_schema,
    "delete_context_def": general_result_schema,
    "list_identity_defs": None,
    "get_identity_def": None,
    "put_identity_def": general_result_schema,
    "delete_identity_def": general_result_schema,
    "list_resource_defs": None,
    "get_resource_def": None,
    "put_resource_def": general_result_schema,
    "delete_resource_def": general_result_schema,
    "enact": general_result_schema,
    "repeal": general_result_schema,
    "get_grant": None,
    "list_grants": None,
    "list_grant_refs": None,
    "create_latch": None,
    "get_latch": None,
    "set_latch": None,
    "delete_latch": general_result_schema,
    "cleanup_latches": general_result_schema
}

STORAGE_CASES = {
    "start": (
        {
            "config": {}
        },
        {}
    ),
    "shutdown": (
        {
            "config": {}
        },
        {}
    ),
    "construct": (
        {
            "config": {}
        },
        {}
    ),
    "destroy": (
        {
            "config": {}
        },
        {}
    ),
    "list_context_defs": (
        {
            "page_ref": None,
            "config": {}
        },
        {
            "context_defs": [],
            "next_page_ref": None
        }
    ),
    "get_context_def": (
        {
            "context_type": "x",
            "config": {}
        },
        {
            "context_def": None
        }
    ),
    "put_context_def": (
        {
            "context_def": {},
            "config": {}
        },
        {}
    ),
    "delete_context_def": (
        {
            "context_type": "x",
            "config": {}
        },
        {}
    ),
    "list_identity_defs": (
        {
            "page_ref": None,
            "config": {}
        },
        {
            "identity_defs": [],
            "next_page_ref": None
        }
    ),
    "get_identity_def": (
        {
            "identity_type": "x",
            "config": {}
        },
        {
            "identity_def": None
        }
    ),
    "put_identity_def": (
        {
            "identity_def": {},
            "config": {}
        },
        {}
    ),
    "delete_identity_def": (
        {
            "identity_type": "x",
            "config": {}
        },
        {}
    ),
    "list_resource_defs": (
        {
            "page_ref": None,
            "config": {}
        },
        {
            "resource_defs": [],
            "next_page_ref": None
        }
    ),
    "get_resource_def": (
        {
            "resource_type": "x",
            "config": {}
        },
        {
            "resource_def": None
        }
    ),
    "put_resource_def": (
        {
            "resource_def": {},
            "config": {}
        },
        {}
    ),
    "delete_resource_def": (
        {
            "resource_type": "x",
            "config": {}
        },
        {}
    ),
    "enact": (
        {
            "grant": {},
            "config": {}
        },
        {}
    ),
    "repeal": (
        {
            "grant_uuid": "x",
            "purge": False,
            "config": {}
        },
        {}
    ),
    "get_grant": (
        {
            "grant_uuid": "x",
            "config": {}
        },
        {
            "grant": None
        }
    ),
    "list_grants": (
        {
            "effect": None,
            "action": None,
            "page_ref": None,
            "config": {}
        },
        {
            "grants": [],
            "next_page_ref": None
        }
    ),
    "list_grant_refs": (
        {
            "effect": None,
            "action": None,
            "page_ref": None,
            "config": {}
        },
        {
            "page_refs": [],
            "next_page_ref": None
        }
    ),
    "create_latch": (
        {
            "config": {}
        },
        {
            "storage_latch": None
        }
    ),
    "get_latch": (
        {
            "storage_latch_uuid": "x",
            "config": {}
        },
        {
            "storage_latch": None
        }
    ),
    "set_latch": (
        {
            "storage_latch_uuid": "x",
            "config": {}
        },
        {
            "storage_latch": None
        }
    ),
    "delete_latch": (
        {
            "storage_latch_uuid": "x",
            "config": {}
        },
        {}
    ),
    "cleanup_latches": (
        {
            "before": datetime.datetime.now(tz=datetime.timezone.utc),
            "config": {}
        },
        {}
    )
}


@pytest.mark.parametrize(
    "method_name",
    list(STORAGE_CASES.keys())
)
def test_storage_meta_wraps_all_methods(method_name):
    storage = MockRaisingStorage()
    kwargs, expected_non_error = STORAGE_CASES[method_name]
    method = getattr(storage, method_name)
    result = asyncio.run(method(**kwargs))
    _assert_error(result, "storage", STORAGE_MESSAGE)
    _assert_fields(result, expected_non_error)
    _assert_matches_schema(result, STORAGE_SCHEMAS[method_name])


def test_storage_meta_covers_every_wrapped_method():
    from authzee.storage._storage_meta import _StorageMeta
    assert set(STORAGE_CASES.keys()) == set(_StorageMeta._handler_map.keys())
    assert set(STORAGE_SCHEMAS.keys()) == set(_StorageMeta._handler_map.keys())


COMPUTE_MESSAGE = "mock compute failure"

COMPUTE_SCHEMAS = {
    "start": general_result_schema,
    "shutdown": general_result_schema,
    "construct": general_result_schema,
    "destroy": general_result_schema,
    "validate_context_def": general_result_schema,
    "validate_identity_def": general_result_schema,
    "validate_resource_def": general_result_schema,
    "validate_grant": general_result_schema,
    "validate_request": validate_request_result_schema,
    "validate_batch_request": validate_batch_request_result_schema,
    "audit": audit_result_schema,
    "authorize": authorize_result_schema,
    "batch_audit": batch_audit_result_schema,
    "batch_authorize": batch_authorize_result_schema
}

COMPUTE_CASES = {
    "start": (
        {
            "execute": None,
            "storage_type": None,
            "storage_kwargs": {},
            "config": {}
        },
        {}
    ),
    "shutdown": (
        {
            "config": {}
        },
        {}
    ),
    "construct": (
        {
            "config": {}
        },
        {}
    ),
    "destroy": (
        {
            "config": {}
        },
        {}
    ),
    "validate_context_def": (
        {
            "context_def": {},
            "config": {}
        },
        {}
    ),
    "validate_identity_def": (
        {
            "identity_def": {},
            "config": {}
        },
        {}
    ),
    "validate_resource_def": (
        {
            "resource_def": {},
            "config": {}
        },
        {}
    ),
    "validate_grant": (
        {
            "grant": {},
            "config": {}
        },
        {}
    ),
    "validate_request": (
        {
            "request": {},
            "config": {}
        },
        {}
    ),
    "validate_batch_request": (
        {
            "batch_request": {},
            "config": {}
        },
        {
            "batch": []
        }
    ),
    "audit": (
        {
            "request": {},
            "page_ref": None,
            "config": {}
        },
        {
            "results": [],
            "next_page_ref": None
        }
    ),
    "authorize": (
        {
            "request": {},
            "config": {}
        },
        {
            "is_authorized": False,
            "grant": None,
            "message": "An error has occurred. Therefore, the request is not authorized."
        }
    ),
    "batch_audit": (
        {
            "batch_request": {},
            "page_ref": None,
            "config": {}
        },
        {
            "grants": [],
            "batch": [],
            "next_page_ref": None
        }
    ),
    "batch_authorize": (
        {
            "batch_request": {},
            "config": {}
        },
        {
            "batch": []
        }
    )
}


@pytest.mark.parametrize(
    "method_name",
    list(COMPUTE_CASES.keys())
)
def test_compute_meta_wraps_all_methods(method_name):
    compute = MockRaisingCompute()
    kwargs, expected_non_error = COMPUTE_CASES[method_name]
    method = getattr(compute, method_name)
    result = asyncio.run(method(**kwargs))
    _assert_error(result, "compute", COMPUTE_MESSAGE)
    _assert_fields(result, expected_non_error)
    _assert_matches_schema(result, COMPUTE_SCHEMAS[method_name])


def test_compute_meta_covers_every_wrapped_method():
    from authzee.compute._compute_meta import _ComputeMeta
    assert set(COMPUTE_CASES.keys()) == set(_ComputeMeta._handler_map.keys())
    assert set(COMPUTE_SCHEMAS.keys()) == set(_ComputeMeta._handler_map.keys())
