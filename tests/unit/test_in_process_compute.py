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


def test_compute_module_shutdown_raises():
    cm = ComputeModule()
    with pytest.raises(TypeError):
        asyncio.run(cm.shutdown(config={}))


def test_compute_module_construct_raises():
    cm = ComputeModule()
    with pytest.raises(TypeError):
        asyncio.run(cm.construct(config={}))


def test_compute_module_destroy_raises():
    cm = ComputeModule()
    with pytest.raises(TypeError):
        asyncio.run(cm.destroy(config={}))


def test_compute_module_validate_context_def_raises():
    cm = ComputeModule()
    with pytest.raises(TypeError):
        asyncio.run(
            cm.validate_context_def(context_def={}, config={})
        )


def test_compute_module_validate_identity_def_raises():
    cm = ComputeModule()
    with pytest.raises(TypeError):
        asyncio.run(
            cm.validate_identity_def(identity_def={}, config={})
        )


def test_compute_module_validate_resource_def_raises():
    cm = ComputeModule()
    with pytest.raises(TypeError):
        asyncio.run(
            cm.validate_resource_def(resource_def={}, config={})
        )


def test_compute_module_validate_grant_raises():
    cm = ComputeModule()
    with pytest.raises(TypeError):
        asyncio.run(cm.validate_grant(grant={}, config={}))


def test_compute_module_validate_request_raises():
    cm = ComputeModule()
    with pytest.raises(TypeError):
        asyncio.run(cm.validate_request(request={}, config={}))


def test_compute_module_validate_batch_request_raises():
    cm = ComputeModule()
    with pytest.raises(TypeError):
        asyncio.run(
            cm.validate_batch_request(batch_request={}, config={})
        )


def test_compute_module_audit_raises():
    cm = ComputeModule()
    with pytest.raises(TypeError):
        asyncio.run(
            cm.audit(
                request={},
                page_ref=None,
                config={}
            )
        )


def test_compute_module_authorize_raises():
    cm = ComputeModule()
    with pytest.raises(TypeError):
        asyncio.run(cm.authorize(request={}, config={}))


def test_compute_module_batch_audit_raises():
    cm = ComputeModule()
    with pytest.raises(TypeError):
        asyncio.run(
            cm.batch_audit(
                batch_request={},
                page_ref=None,
                config={}
            )
        )


def test_compute_module_batch_authorize_raises():
    cm = ComputeModule()
    with pytest.raises(TypeError):
        asyncio.run(cm.batch_authorize(batch_request={}, config={}))
