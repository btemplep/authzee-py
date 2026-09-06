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


def test_storage_module_cannot_instantiate_abstract():
    with pytest.raises(TypeError):
        StorageModule()


class _ConcreteStorageModule(StorageModule):
    """Minimal concrete StorageModule that defers to the abstract base bodies.

    Used to exercise the base-class method bodies for coverage. Every method
    calls ``super()`` so the base implementation runs.
    """


    async def start(self, config):
        return await super().start(config=config)


    async def shutdown(self, config):
        return await super().shutdown(config=config)


    async def construct(self, config):
        return await super().construct(config=config)


    async def destroy(self, config):
        return await super().destroy(config=config)


    async def list_context_defs(self, page_ref, config):
        return await super().list_context_defs(page_ref=page_ref, config=config)


    async def get_context_def(self, context_type, config):
        return await super().get_context_def(context_type=context_type, config=config)


    async def put_context_def(self, context_def, config):
        return await super().put_context_def(context_def=context_def, config=config)


    async def delete_context_def(self, context_type, config):
        return await super().delete_context_def(context_type=context_type, config=config)


    async def list_identity_defs(self, page_ref, config):
        return await super().list_identity_defs(page_ref=page_ref, config=config)


    async def get_identity_def(self, identity_type, config):
        return await super().get_identity_def(identity_type=identity_type, config=config)


    async def put_identity_def(self, identity_def, config):
        return await super().put_identity_def(identity_def=identity_def, config=config)


    async def delete_identity_def(self, identity_type, config):
        return await super().delete_identity_def(identity_type=identity_type, config=config)


    async def list_resource_defs(self, page_ref, config):
        return await super().list_resource_defs(page_ref=page_ref, config=config)


    async def get_resource_def(self, resource_type, config):
        return await super().get_resource_def(resource_type=resource_type, config=config)


    async def put_resource_def(self, resource_def, config):
        return await super().put_resource_def(resource_def=resource_def, config=config)


    async def delete_resource_def(self, resource_type, config):
        return await super().delete_resource_def(resource_type=resource_type, config=config)


    async def enact(self, grant, config):
        return await super().enact(grant=grant, config=config)


    async def repeal(self, grant_uuid, purge, config):
        return await super().repeal(grant_uuid=grant_uuid, purge=purge, config=config)


    async def get_grant(self, grant_uuid, config):
        return await super().get_grant(grant_uuid=grant_uuid, config=config)


    async def list_grants(
        self,
        effect,
        action,
        page_ref,
        config
    ):
        return await super().list_grants(
            effect=effect,
            action=action,
            page_ref=page_ref,
            config=config
        )


    async def list_grant_refs(
        self,
        effect,
        action,
        page_ref,
        config
    ):
        return await super().list_grant_refs(
            effect=effect,
            action=action,
            page_ref=page_ref,
            config=config
        )


    async def create_latch(self, config):
        return await super().create_latch(config=config)


    async def get_latch(self, storage_latch_uuid, config):
        return await super().get_latch(storage_latch_uuid=storage_latch_uuid, config=config)


    async def set_latch(self, storage_latch_uuid, config):
        return await super().set_latch(storage_latch_uuid=storage_latch_uuid, config=config)


    async def delete_latch(self, storage_latch_uuid, config):
        return await super().delete_latch(storage_latch_uuid=storage_latch_uuid, config=config)


    async def cleanup_latches(self, before, config):
        return await super().cleanup_latches(before=before, config=config)


def test_storage_module_base_start_sets_defaults():
    sm = _ConcreteStorageModule()
    result = asyncio.run(sm.start(config={}))
    assert result['error'] is None
    assert sm.locality == ModuleLocality.PROCESS
    assert sm.has_parallel_paging is False


def test_storage_module_base_methods_return_none():
    sm = _ConcreteStorageModule()

    async def run():
        return [
            await sm.shutdown(config={}),
            await sm.construct(config={}),
            await sm.destroy(config={}),
            await sm.list_context_defs(page_ref=None, config={}),
            await sm.get_context_def(context_type="x", config={}),
            await sm.put_context_def(context_def={}, config={}),
            await sm.delete_context_def(context_type="x", config={}),
            await sm.list_identity_defs(page_ref=None, config={}),
            await sm.get_identity_def(identity_type="x", config={}),
            await sm.put_identity_def(identity_def={}, config={}),
            await sm.delete_identity_def(identity_type="x", config={}),
            await sm.list_resource_defs(page_ref=None, config={}),
            await sm.get_resource_def(resource_type="x", config={}),
            await sm.put_resource_def(resource_def={}, config={}),
            await sm.delete_resource_def(resource_type="x", config={}),
            await sm.enact(grant={}, config={}),
            await sm.repeal(
                grant_uuid="x",
                purge=False,
                config={}
            ),
            await sm.get_grant(grant_uuid="x", config={}),
            await sm.list_grants(
                effect=None,
                action=None,
                page_ref=None,
                config={}
            ),
            await sm.list_grant_refs(
                effect=None,
                action=None,
                page_ref=None,
                config={}
            ),
            await sm.create_latch(config={}),
            await sm.get_latch(storage_latch_uuid="x", config={}),
            await sm.set_latch(storage_latch_uuid="x", config={}),
            await sm.delete_latch(storage_latch_uuid="x", config={}),
            await sm.cleanup_latches(
                before=datetime.datetime.now(),
                config={}
            )
        ]

    results = asyncio.run(run())
    assert all(r is None for r in results)


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
