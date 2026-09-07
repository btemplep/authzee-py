"""Unit tests for authzee.compute InProcessCompute.

Reuses the shared compute module test suite. Fixtures required by the shared
suite are defined here and bound to InProcessCompute. Base-class
NotImplementedError tests for ``ComputeModule`` stay here since they test the
abstract base rather than a concrete implementation.
"""

import asyncio
import os
import sys
from uuid import uuid4

import pytest


sys.path.insert(0, os.path.dirname(__file__))

from compute_module_test_base import *

from authzee.compute.compute_module import ComputeModule
from authzee.compute.in_process_compute import InProcessCompute
from authzee.jmespath import jmespath_execute
from authzee.module_locality import ModuleLocality
from authzee.storage.dict_storage import DictStorage


class FailingStorage(DictStorage):
    """A storage class that always returns an error for list_grants."""


    async def list_grants(
        self,
        effect,
        action,
        page_ref,
        config
    ):
        return {
            "grants": [],
            "next_page_ref": None,
            "error": {
                "error_type": "storage",
                "message": "forced failure"
            }
        }


class FailOnAllowStorage(DictStorage):
    """A storage class that fails only when listing allow grants."""


    async def list_grants(
        self,
        effect,
        action,
        page_ref,
        config
    ):
        if effect == "allow":
            return {
                "grants": [],
                "next_page_ref": None,
                "error": {
                    "error_type": "storage",
                    "message": "forced failure"
                }
            }

        return await super().list_grants(effect, action, page_ref, config)


async def _seed_storage(storage):
    await storage.put_context_def(
        {
            "context_type": "NONE",
            "schema": {
                "type": "object",
                "additionalProperties": False
            }
        },
        config={}
    )
    await storage.put_identity_def(
        {
            "identity_type": "user",
            "schema": {
                "type": "object",
                "required": [
                    "username",
                    "department"
                ],
                "additionalProperties": False,
                "properties": {
                    "username": {
                        "type": "string"
                    },
                    "department": {
                        "type": "string"
                    }
                }
            }
        },
        config={}
    )
    await storage.put_resource_def(
        {
            "resource_type": "balloon",
            "actions": [
                "balloon:read",
                "balloon:inflate",
                "balloon:pop"
            ],
            "schema": {
                "type": "object",
                "required": [
                    "color",
                    "is_inflated"
                ],
                "additionalProperties": False,
                "properties": {
                    "color": {
                        "type": "string"
                    },
                    "is_inflated": {
                        "type": "boolean"
                    }
                }
            }
        },
        config={}
    )
    await storage.enact(
        grant={
            "grant_uuid": str(uuid4()),
            "name": "Allow inflate",
            "description": "",
            "tags": {},
            "effect": "allow",
            "actions": [
                "balloon:read",
                "balloon:inflate"
            ],
            "query": "length(request.identities.user[?department == 'Balloon Dept']) > `0`",
            "equality": True,
            "applicable_on_failure": False,
            "data": {}
        },
        config={}
    )
    await storage.enact(
        grant={
            "grant_uuid": str(uuid4()),
            "name": "Deny pop for interns",
            "description": "",
            "tags": {},
            "effect": "deny",
            "actions": [
                "balloon:pop"
            ],
            "query": "length(request.identities.user[?department == 'Intern']) > `0`",
            "equality": True,
            "applicable_on_failure": False,
            "data": {}
        },
        config={}
    )


@pytest.fixture
def storage_dict():
    d = {}

    return d


@pytest.fixture
def compute(storage_dict):
    """Start an InProcessCompute instance with DictStorage."""
    c = InProcessCompute()

    async def setup():
        storage = DictStorage(storage_dict=storage_dict)
        await storage.construct(config={})
        await c.start(
            execute=jmespath_execute,
            storage_type=DictStorage,
            storage_kwargs={
                "storage_dict": storage_dict
            },
            config={
                "storage": {}
            }
        )

        return c

    asyncio.run(setup())

    return c


@pytest.fixture
def seeded_compute(compute, storage_dict):
    """InProcessCompute with definitions and grants already stored."""

    async def seed():
        storage = DictStorage(storage_dict=storage_dict)
        await storage.start(config={})
        await _seed_storage(storage)

    asyncio.run(seed())

    return compute


@pytest.fixture
def failing_compute(compute, storage_dict):
    """InProcessCompute whose storage always fails list_grants."""

    async def setup():
        storage = DictStorage(storage_dict=storage_dict)
        await storage.start(config={})
        await _seed_storage(storage)
        failing = FailingStorage(storage_dict=storage_dict)
        await failing.start(config={})
        compute._storage = failing

    asyncio.run(setup())

    return compute


@pytest.fixture
def fail_on_allow_compute(compute, storage_dict):
    """InProcessCompute whose storage fails list_grants only for effect allow."""

    async def setup():
        storage = DictStorage(storage_dict=storage_dict)
        await storage.start(config={})
        await _seed_storage(storage)
        failing = FailOnAllowStorage(storage_dict=storage_dict)
        await failing.start(config={})
        compute._storage = failing

    asyncio.run(setup())

    return compute


def test_compute_module_cannot_instantiate_abstract():
    with pytest.raises(TypeError):
        ComputeModule()


class _ConcreteComputeModule(ComputeModule):
    """Minimal concrete ComputeModule that defers to the abstract base bodies.

    Used to exercise the base-class method bodies for coverage. Every method
    calls ``super()`` so the base implementation runs.
    """


    async def start(
        self,
        execute,
        storage_type,
        storage_kwargs,
        config
    ):
        return await super().start(
            execute=execute,
            storage_type=storage_type,
            storage_kwargs=storage_kwargs,
            config=config
        )


    async def shutdown(self, config):
        return await super().shutdown(config=config)


    async def construct(self, config):
        return await super().construct(config=config)


    async def destroy(self, config):
        return await super().destroy(config=config)


    async def validate_context_def(self, context_def, config):
        return await super().validate_context_def(context_def=context_def, config=config)


    async def validate_identity_def(self, identity_def, config):
        return await super().validate_identity_def(identity_def=identity_def, config=config)


    async def validate_resource_def(self, resource_def, config):
        return await super().validate_resource_def(resource_def=resource_def, config=config)


    async def validate_grant(self, grant, config):
        return await super().validate_grant(grant=grant, config=config)


    async def validate_request(self, request, config):
        return await super().validate_request(request=request, config=config)


    async def validate_batch_request(self, batch_request, config):
        return await super().validate_batch_request(batch_request=batch_request, config=config)


    async def audit(self, request, page_ref, config):
        return await super().audit(request=request, page_ref=page_ref, config=config)


    async def authorize(self, request, config):
        return await super().authorize(request=request, config=config)


    async def batch_audit(self, batch_request, page_ref, config):
        return await super().batch_audit(
            batch_request=batch_request,
            page_ref=page_ref,
            config=config
        )


    async def batch_authorize(self, batch_request, config):
        return await super().batch_authorize(batch_request=batch_request, config=config)


def test_compute_module_base_start_sets_defaults():
    cm = _ConcreteComputeModule()

    async def run():
        await cm.start(
            execute=jmespath_execute,
            storage_type=DictStorage,
            storage_kwargs={},
            config={}
        )

    asyncio.run(run())
    assert cm.locality == ModuleLocality.PROCESS
    assert cm.has_parallel_paging is False


def test_compute_module_base_methods_return_none():
    cm = _ConcreteComputeModule()

    async def run():
        return [
            await cm.shutdown(config={}),
            await cm.construct(config={}),
            await cm.destroy(config={}),
            await cm.validate_context_def(context_def={}, config={}),
            await cm.validate_identity_def(identity_def={}, config={}),
            await cm.validate_resource_def(resource_def={}, config={}),
            await cm.validate_grant(grant={}, config={}),
            await cm.validate_request(request={}, config={}),
            await cm.validate_batch_request(batch_request={}, config={}),
            await cm.audit(
                request={},
                page_ref=None,
                config={}
            ),
            await cm.authorize(request={}, config={}),
            await cm.batch_audit(
                batch_request={},
                page_ref=None,
                config={}
            ),
            await cm.batch_authorize(batch_request={}, config={})
        ]

    results = asyncio.run(run())
    assert all(r is None for r in results)
