"""Unit tests for authzee.storage.sql_storage (SQLStorage).

Reuses the shared storage module test suite bound to an in-memory SQLite backed
`SQLStorage`. The shared `storage` fixture is defined here. A handful of base
tests are overridden below because they encode behavior specific to a dict
backed module (constructing an instance from a plain dict, `PROCESS` locality
after start, treating arbitrary non-UUID strings as "not found", and integer
page reference values). Everything else is exercised directly from the shared
suite.
"""

import asyncio
import datetime
import os
import sys
from uuid import uuid4

import pytest
from sqlalchemy.pool import StaticPool


sys.path.insert(0, os.path.dirname(__file__))

from storage_module_test_base import *
from storage_module_test_base import _grant

from authzee.module_locality import ModuleLocality
from authzee.storage.sql_storage import SQLStorage


def _new_sql_storage():
    """Build a fresh, unconstructed in-memory `SQLStorage` instance.

    Returns
    -------
    SQLStorage
        A new in-memory SQLite backed storage instance.
    """
    return SQLStorage(
        sqlalchemy_async_engine_kwargs={
            "url": "sqlite+aiosqlite:///:memory:",
            "connect_args": {
                "check_same_thread": False
            },
            "poolclass": StaticPool
        }
    )


@pytest.fixture
def storage():
    """A fully initialized in-memory SQLStorage instance."""
    s = _new_sql_storage()
    asyncio.run(s.start(config={}))
    asyncio.run(s.construct(config={}))

    yield s

    asyncio.run(s.shutdown(config={}))


def test_base_start():
    s = _new_sql_storage()
    result = asyncio.run(s.start(config={}))
    assert result['error'] is None
    assert s.locality == ModuleLocality.NETWORK


def test_base_construct():
    s = _new_sql_storage()
    asyncio.run(s.start(config={}))
    result = asyncio.run(s.construct(config={}))
    assert result['error'] is None
    context_def = {
        "context_type": "NONE",
        "schema": {
            "type": "object"
        }
    }
    asyncio.run(s.put_context_def(context_def, config={}))
    get_result = asyncio.run(s.get_context_def("NONE", config={}))
    assert get_result['error'] is None
    assert get_result['context_def'] == context_def


def test_base_get_grant_not_found(storage):
    result = asyncio.run(storage.get_grant(str(uuid4()), config={}))
    assert result['error'] is not None
    assert result['error']['error_type'] == "resource_not_found"
    assert result['grant'] is None


def test_base_get_latch_not_found(storage):
    result = asyncio.run(storage.get_latch(str(uuid4()), config={}))
    assert result['error'] is not None
    assert result['error']['error_type'] == "resource_not_found"


def test_base_set_latch_not_found(storage):
    result = asyncio.run(storage.set_latch(str(uuid4()), config={}))
    assert result['error'] is not None
    assert result['error']['error_type'] == "resource_not_found"


def test_base_list_grant_refs(storage):
    result = asyncio.run(
        storage.list_grant_refs(
            effect=None,
            action=None,
            page_ref=None,
            config={
                "page_size": 2
            }
        )
    )
    assert result['error'] is not None
    assert result['error']['error_type'] == "parallel_pagination_not_supported"
    assert result['page_refs'] == []
    assert result['next_page_ref'] is None


def test_base_list_grant_refs_filter_effect(storage):
    result = asyncio.run(
        storage.list_grant_refs(
            effect="deny",
            action=None,
            page_ref=None,
            config={
                "page_size": 2
            }
        )
    )
    assert result['error'] is not None
    assert result['error']['error_type'] == "parallel_pagination_not_supported"


def test_base_list_grant_refs_filter_action(storage):
    result = asyncio.run(
        storage.list_grant_refs(
            effect=None,
            action="write",
            page_ref=None,
            config={
                "page_size": 10
            }
        )
    )
    assert result['error'] is not None
    assert result['error']['error_type'] == "parallel_pagination_not_supported"


def test_sql_storage_start_no_parallel_paging():
    s = _new_sql_storage()
    result = asyncio.run(s.start(config={}))
    assert result['error'] is None
    assert s.has_parallel_paging is False


def test_sql_storage_shutdown(storage):
    result = asyncio.run(storage.shutdown(config={}))
    assert result['error'] is None


def test_sql_storage_destroy(storage):
    result = asyncio.run(storage.destroy(config={}))
    assert result['error'] is None
    put_result = asyncio.run(
        storage.put_context_def(
            {
                "context_type": "NONE",
                "schema": {
                    "type": "object"
                }
            },
            config={}
        )
    )
    assert put_result['error'] is not None


def test_sql_storage_get_grant_bad_uuid_is_storage_error(storage):
    result = asyncio.run(storage.get_grant("not-a-uuid", config={}))
    assert result['error'] is not None
    assert result['error']['error_type'] == "storage"
    assert result['grant'] is None


def test_sql_storage_put_context_def_updates_existing(storage):
    first = {
        "context_type": "NONE",
        "schema": {
            "type": "object",
            "additionalProperties": False
        }
    }
    second = {
        "context_type": "NONE",
        "schema": {
            "type": "object",
            "additionalProperties": True
        }
    }
    asyncio.run(storage.put_context_def(first, config={}))
    asyncio.run(storage.put_context_def(second, config={}))
    get_result = asyncio.run(storage.get_context_def("NONE", config={}))
    assert get_result['error'] is None
    assert get_result['context_def'] == second
    list_result = asyncio.run(
        storage.list_context_defs(
            page_ref=None,
            config={
                "page_size": 10
            }
        )
    )
    assert len(list_result['context_defs']) == 1


def test_sql_storage_put_resource_def_updates_existing(storage):
    first = {
        "resource_type": "balloon",
        "actions": [
            "balloon:read"
        ],
        "schema": {
            "type": "object"
        }
    }
    second = {
        "resource_type": "balloon",
        "actions": [
            "balloon:read",
            "balloon:inflate"
        ],
        "schema": {
            "type": "object"
        }
    }
    asyncio.run(storage.put_resource_def(first, config={}))
    asyncio.run(storage.put_resource_def(second, config={}))
    get_result = asyncio.run(storage.get_resource_def("balloon", config={}))
    assert get_result['error'] is None
    assert get_result['resource_def'] == second


def test_sql_storage_put_identity_def_updates_existing(storage):
    first = {
        "identity_type": "user",
        "schema": {
            "type": "object",
            "additionalProperties": False
        }
    }
    second = {
        "identity_type": "user",
        "schema": {
            "type": "object",
            "additionalProperties": True
        }
    }
    asyncio.run(storage.put_identity_def(first, config={}))
    asyncio.run(storage.put_identity_def(second, config={}))
    get_result = asyncio.run(storage.get_identity_def("user", config={}))
    assert get_result['error'] is None
    assert get_result['identity_def'] == second


def test_sql_storage_cleanup_latches_removes_old(storage):
    create_result = asyncio.run(storage.create_latch(config={}))
    latch_uuid = create_result['storage_latch']['storage_latch_uuid']
    future = (
        datetime.datetime.now(tz=datetime.timezone.utc)
        + datetime.timedelta(seconds=1)
    )
    result = asyncio.run(
        storage.cleanup_latches(before=future, config={})
    )
    assert result['error'] is None
    get_result = asyncio.run(storage.get_latch(latch_uuid, config={}))
    assert get_result['error'] is not None


def test_sql_storage_bare_memory_url_locality():
    s = SQLStorage(
        sqlalchemy_async_engine_kwargs={
            "url": "sqlite+aiosqlite://:memory:"
        }
    )
    assert s.locality == ModuleLocality.SYSTEM


def test_sql_storage_latch_from_db_normalizes_aware_datetime():
    from authzee.storage.sql_storage import StorageLatchDB

    s = _new_sql_storage()
    aware = datetime.datetime(
        2026,
        1,
        1,
        12,
        0,
        0,
        tzinfo=datetime.timezone(datetime.timedelta(hours=5))
    )
    db_latch = StorageLatchDB(
        storage_latch_uuid=uuid4(),
        is_set=False,
        created_at=aware
    )
    latch = s._latch_from_db(db_latch)
    assert latch['created_at'] == aware.astimezone(datetime.timezone.utc).isoformat()
