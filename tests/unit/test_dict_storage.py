"""Unit tests for authzee.storage modules (StorageModule and DictStorage).

Reuses the shared storage module test suite. Fixtures required by the shared
suite (``storage`` and ``storage_dict``) are defined here and bound to
DictStorage. Base-class ``TypeError`` tests for ``StorageModule`` stay here
since they test the abstract base rather than a concrete implementation, along
with DictStorage-specific tests that assert internal ``storage_dict`` structure
and ``has_parallel_paging``.
"""

import asyncio
import datetime
import os
import sys
from uuid import uuid4

import pytest


sys.path.insert(0, os.path.dirname(__file__))

from storage_module_test_base import *
from storage_module_test_base import register_storage_type

from authzee.module_locality import ModuleLocality
from authzee.storage.dict_storage import DictStorage
from authzee.storage.storage_module import StorageModule


register_storage_type(DictStorage)


@pytest.fixture
def storage_dict():
    return {}


@pytest.fixture
def storage(storage_dict):
    """A fully initialized DictStorage instance."""
    s = DictStorage(storage_dict=storage_dict)
    asyncio.run(s.construct(config={}))
    asyncio.run(s.start(config={}))

    return s


def test_storage_module_start():
    sm = StorageModule()
    result = asyncio.run(sm.start(config={}))
    assert sm.locality == ModuleLocality.PROCESS
    assert sm.has_parallel_paging is False


def test_storage_module_shutdown_raises():
    sm = StorageModule()
    with pytest.raises(TypeError):
        asyncio.run(sm.shutdown(config={}))


def test_storage_module_construct_raises():
    sm = StorageModule()
    with pytest.raises(TypeError):
        asyncio.run(sm.construct(config={}))


def test_storage_module_destroy_raises():
    sm = StorageModule()
    with pytest.raises(TypeError):
        asyncio.run(sm.destroy(config={}))


def test_storage_module_list_context_defs_raises():
    sm = StorageModule()
    with pytest.raises(TypeError):
        asyncio.run(sm.list_context_defs(page_ref=None, config={}))


def test_storage_module_get_context_def_raises():
    sm = StorageModule()
    with pytest.raises(TypeError):
        asyncio.run(sm.get_context_def(context_type="x", config={}))


def test_storage_module_put_context_def_raises():
    sm = StorageModule()
    with pytest.raises(TypeError):
        asyncio.run(sm.put_context_def(context_def={}, config={}))


def test_storage_module_delete_context_def_raises():
    sm = StorageModule()
    with pytest.raises(TypeError):
        asyncio.run(
            sm.delete_context_def(context_type="x", config={})
        )


def test_storage_module_list_identity_defs_raises():
    sm = StorageModule()
    with pytest.raises(TypeError):
        asyncio.run(sm.list_identity_defs(page_ref=None, config={}))


def test_storage_module_get_identity_def_raises():
    sm = StorageModule()
    with pytest.raises(TypeError):
        asyncio.run(
            sm.get_identity_def(identity_type="x", config={})
        )


def test_storage_module_put_identity_def_raises():
    sm = StorageModule()
    with pytest.raises(TypeError):
        asyncio.run(sm.put_identity_def(identity_def={}, config={}))


def test_storage_module_delete_identity_def_raises():
    sm = StorageModule()
    with pytest.raises(TypeError):
        asyncio.run(
            sm.delete_identity_def(identity_type="x", config={})
        )


def test_storage_module_list_resource_defs_raises():
    sm = StorageModule()
    with pytest.raises(TypeError):
        asyncio.run(sm.list_resource_defs(page_ref=None, config={}))


def test_storage_module_get_resource_def_raises():
    sm = StorageModule()
    with pytest.raises(TypeError):
        asyncio.run(
            sm.get_resource_def(resource_type="x", config={})
        )


def test_storage_module_put_resource_def_raises():
    sm = StorageModule()
    with pytest.raises(TypeError):
        asyncio.run(sm.put_resource_def(resource_def={}, config={}))


def test_storage_module_delete_resource_def_raises():
    sm = StorageModule()
    with pytest.raises(TypeError):
        asyncio.run(
            sm.delete_resource_def(resource_type="x", config={})
        )


def test_storage_module_enact_raises():
    sm = StorageModule()
    with pytest.raises(TypeError):
        asyncio.run(sm.enact(grant={}, config={}))


def test_storage_module_repeal_raises():
    sm = StorageModule()
    with pytest.raises(TypeError):
        asyncio.run(
            sm.repeal(
                grant_uuid="x",
                purge=False,
                config={}
            )
        )


def test_storage_module_get_grant_raises():
    sm = StorageModule()
    with pytest.raises(TypeError):
        asyncio.run(sm.get_grant(grant_uuid="x", config={}))


def test_storage_module_list_grants_raises():
    sm = StorageModule()
    with pytest.raises(TypeError):
        asyncio.run(
            sm.list_grants(
                effect=None,
                action=None,
                page_ref=None,
                config={}
            )
        )


def test_storage_module_list_grant_refs_raises():
    sm = StorageModule()
    with pytest.raises(TypeError):
        asyncio.run(
            sm.list_grant_refs(
                effect=None,
                action=None,
                page_ref=None,
                config={}
            )
        )


def test_storage_module_create_latch_raises():
    sm = StorageModule()
    with pytest.raises(TypeError):
        asyncio.run(sm.create_latch(config={}))


def test_storage_module_get_latch_raises():
    sm = StorageModule()
    with pytest.raises(TypeError):
        asyncio.run(sm.get_latch(storage_latch_uuid="x", config={}))


def test_storage_module_set_latch_raises():
    sm = StorageModule()
    with pytest.raises(TypeError):
        asyncio.run(sm.set_latch(storage_latch_uuid="x", config={}))


def test_storage_module_delete_latch_raises():
    sm = StorageModule()
    with pytest.raises(TypeError):
        asyncio.run(
            sm.delete_latch(storage_latch_uuid="x", config={})
        )


def test_storage_module_cleanup_latches_raises():
    sm = StorageModule()
    with pytest.raises(TypeError):
        asyncio.run(
            sm.cleanup_latches(
                before=datetime.datetime.now(),
                config={}
            )
        )


def test_dict_storage_start_parallel_paging(storage_dict):
    s = DictStorage(storage_dict=storage_dict)
    asyncio.run(s.construct(config={}))
    result = asyncio.run(s.start(config={}))
    assert result['error'] is None
    assert s.locality == ModuleLocality.PROCESS
    assert s.has_parallel_paging is True


def test_dict_storage_construct_creates_luts(storage_dict):
    s = DictStorage(storage_dict=storage_dict)
    result = asyncio.run(s.construct(config={}))
    assert result['error'] is None
    assert "context_defs_lut" in storage_dict
    assert "identity_defs_lut" in storage_dict
    assert "resource_defs_lut" in storage_dict
    assert "grants_lut" in storage_dict
    assert "latches_lut" in storage_dict


def test_dict_storage_destroy_removes_luts(storage, storage_dict):
    result = asyncio.run(storage.destroy(config={}))
    assert result['error'] is None
    assert "context_defs_lut" not in storage_dict


def test_dict_storage_cleanup_latches_removes_from_dict(storage):
    asyncio.run(storage.create_latch(config={}))
    asyncio.run(storage.create_latch(config={}))
    future = (
        datetime.datetime.now(tz=datetime.timezone.utc)
        + datetime.timedelta(seconds=1)
    )
    result = asyncio.run(
        storage.cleanup_latches(before=future, config={})
    )
    assert result['error'] is None
    assert len(storage._storage_dict['latches_lut']) == 0


def test_dict_storage_cleanup_latches_keeps_recent_in_dict(storage):
    asyncio.run(storage.create_latch(config={}))
    past = (
        datetime.datetime.now(tz=datetime.timezone.utc)
        - datetime.timedelta(seconds=10)
    )
    result = asyncio.run(storage.cleanup_latches(before=past, config={}))
    assert result['error'] is None
    assert len(storage._storage_dict['latches_lut']) == 1
