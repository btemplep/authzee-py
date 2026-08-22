"""Unit tests for authzee.compute modules (ComputeModule and InProcessCompute)."""

import asyncio
from uuid import uuid4

import pytest

from authzee.compute.compute_module import ComputeModule
from authzee.compute.in_process_compute import InProcessCompute
from authzee.exceptions import NotImplementedError as AuthzeeNotImplementedError
from authzee.jmespath import jmespath_execute
from authzee.module_locality import ModuleLocality
from authzee.storage.dict_storage import DictStorage


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

    asyncio.run(seed())

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


def test_in_process_compute_start(storage_dict):
    c = InProcessCompute()

    async def run():
        storage = DictStorage(storage_dict=storage_dict)
        await storage.construct(config={})
        result = await c.start(
            execute=jmespath_execute,
            storage_type=DictStorage,
            storage_kwargs={
                "storage_dict": storage_dict
            },
            config={
                "storage": {}
            }
        )

        return result

    result = asyncio.run(run())
    assert result['error'] is None
    assert c.locality == ModuleLocality.PROCESS
    assert c.has_parallel_paging is False


def test_in_process_compute_shutdown(compute):
    result = asyncio.run(
        compute.shutdown(
            config={
                "storage": {}
            }
        )
    )
    assert result['error'] is None


def test_in_process_compute_construct(storage_dict):
    c = InProcessCompute()
    result = asyncio.run(c.construct(config={}))
    assert result['error'] is None


def test_in_process_compute_destroy(storage_dict):
    c = InProcessCompute()
    result = asyncio.run(c.destroy(config={}))
    assert result['error'] is None


def test_in_process_validate_context_def_valid(compute):
    result = asyncio.run(
        compute.validate_context_def(
            context_def={
                "context_type": "NONE",
                "schema": {
                    "type": "object",
                    "additionalProperties": False
                }
            },
            config={}
        )
    )
    assert result['error'] is None


def test_in_process_validate_context_def_invalid(compute):
    result = asyncio.run(
        compute.validate_context_def(
            context_def={
                "bad": "data"
            },
            config={}
        )
    )
    assert result['error'] is not None


def test_in_process_validate_identity_def_valid(compute):
    result = asyncio.run(
        compute.validate_identity_def(
            identity_def={
                "identity_type": "user",
                "schema": {
                    "type": "object"
                }
            },
            config={}
        )
    )
    assert result['error'] is None


def test_in_process_validate_identity_def_invalid(compute):
    result = asyncio.run(
        compute.validate_identity_def(
            identity_def={
                "bad": "data"
            },
            config={}
        )
    )
    assert result['error'] is not None


def test_in_process_validate_resource_def_valid(compute):
    result = asyncio.run(
        compute.validate_resource_def(
            resource_def={
                "resource_type": "file",
                "actions": [
                    "read"
                ],
                "schema": {
                    "type": "object"
                }
            },
            config={}
        )
    )
    assert result['error'] is None


def test_in_process_validate_resource_def_invalid(compute):
    result = asyncio.run(
        compute.validate_resource_def(
            resource_def={
                "bad": "data"
            },
            config={}
        )
    )
    assert result['error'] is not None


def test_in_process_validate_grant_valid(compute):
    result = asyncio.run(
        compute.validate_grant(
            grant={
                "grant_uuid": str(uuid4()),
                "name": "Test",
                "description": "",
                "tags": {},
                "effect": "allow",
                "actions": [
                    "read"
                ],
                "query": "`true`",
                "equality": True,
                "applicable_on_failure": False,
                "data": {}
            },
            config={}
        )
    )
    assert result['error'] is None


def test_in_process_validate_grant_invalid(compute):
    result = asyncio.run(
        compute.validate_grant(
            grant={
                "bad": "data"
            },
            config={}
        )
    )
    assert result['error'] is not None


def test_in_process_validate_request_valid(seeded_compute):
    request = {
        "identities": {
            "user": [
                {
                    "username": "balloon_person",
                    "department": "Balloon Dept"
                }
            ]
        },
        "action": "balloon:inflate",
        "resource_type": "balloon",
        "resource": {
            "color": "blue",
            "is_inflated": False
        },
        "context_type": "NONE",
        "context": {}
    }
    config = {
        "get_context_def": {},
        "get_identity_def": {},
        "get_resource_def": {}
    }
    result = asyncio.run(
        seeded_compute.validate_request(
            request=request,
            config=config
        )
    )
    assert result['error'] is None


def test_in_process_validate_request_invalid_schema(seeded_compute):
    result = asyncio.run(
        seeded_compute.validate_request(
            request={
                "bad": "data"
            },
            config={}
        )
    )
    assert result['error'] is not None


def test_in_process_validate_request_unknown_context_type(seeded_compute):
    request = {
        "identities": {
            "user": [
                {
                    "username": "a",
                    "department": "b"
                }
            ]
        },
        "action": "balloon:inflate",
        "resource_type": "balloon",
        "resource": {
            "color": "blue",
            "is_inflated": False
        },
        "context_type": "UNKNOWN",
        "context": {}
    }
    config = {
        "get_context_def": {},
        "get_identity_def": {},
        "get_resource_def": {}
    }
    result = asyncio.run(
        seeded_compute.validate_request(
            request=request,
            config=config
        )
    )
    assert result['error'] is not None
    assert result['error'] is not None


def test_in_process_validate_request_invalid_context_data(seeded_compute):
    request = {
        "identities": {
            "user": [
                {
                    "username": "a",
                    "department": "b"
                }
            ]
        },
        "action": "balloon:inflate",
        "resource_type": "balloon",
        "resource": {
            "color": "blue",
            "is_inflated": False
        },
        "context_type": "NONE",
        "context": {
            "extra_field": "not allowed"
        }
    }
    config = {
        "get_context_def": {},
        "get_identity_def": {},
        "get_resource_def": {}
    }
    result = asyncio.run(
        seeded_compute.validate_request(
            request=request,
            config=config
        )
    )
    assert result['error'] is not None


def test_in_process_validate_request_unknown_resource_type(seeded_compute):
    request = {
        "identities": {
            "user": [
                {
                    "username": "a",
                    "department": "b"
                }
            ]
        },
        "action": "balloon:inflate",
        "resource_type": "UNKNOWN",
        "resource": {
            "color": "blue",
            "is_inflated": False
        },
        "context_type": "NONE",
        "context": {}
    }
    config = {
        "get_context_def": {},
        "get_identity_def": {},
        "get_resource_def": {}
    }
    result = asyncio.run(
        seeded_compute.validate_request(
            request=request,
            config=config
        )
    )
    assert result['error'] is not None


def test_in_process_validate_request_invalid_resource_data(seeded_compute):
    request = {
        "identities": {
            "user": [
                {
                    "username": "a",
                    "department": "b"
                }
            ]
        },
        "action": "balloon:inflate",
        "resource_type": "balloon",
        "resource": {
            "color": 123,
            "is_inflated": "not_bool"
        },
        "context_type": "NONE",
        "context": {}
    }
    config = {
        "get_context_def": {},
        "get_identity_def": {},
        "get_resource_def": {}
    }
    result = asyncio.run(
        seeded_compute.validate_request(
            request=request,
            config=config
        )
    )
    assert result['error'] is not None


def test_in_process_validate_request_invalid_action(seeded_compute):
    request = {
        "identities": {
            "user": [
                {
                    "username": "a",
                    "department": "b"
                }
            ]
        },
        "action": "balloon:NONEXISTENT",
        "resource_type": "balloon",
        "resource": {
            "color": "blue",
            "is_inflated": False
        },
        "context_type": "NONE",
        "context": {}
    }
    config = {
        "get_context_def": {},
        "get_identity_def": {},
        "get_resource_def": {}
    }
    result = asyncio.run(
        seeded_compute.validate_request(
            request=request,
            config=config
        )
    )
    assert result['error'] is not None


def test_in_process_validate_request_unknown_identity_type(seeded_compute):
    request = {
        "identities": {
            "unknown_id": [
                {
                    "username": "a",
                    "department": "b"
                }
            ]
        },
        "action": "balloon:inflate",
        "resource_type": "balloon",
        "resource": {
            "color": "blue",
            "is_inflated": False
        },
        "context_type": "NONE",
        "context": {}
    }
    config = {
        "get_context_def": {},
        "get_identity_def": {},
        "get_resource_def": {}
    }
    result = asyncio.run(
        seeded_compute.validate_request(
            request=request,
            config=config
        )
    )
    assert result['error'] is not None


def test_in_process_validate_request_invalid_identity_data(seeded_compute):
    request = {
        "identities": {
            "user": [
                {
                    "username": 123,
                    "department": 456
                }
            ]
        },
        "action": "balloon:inflate",
        "resource_type": "balloon",
        "resource": {
            "color": "blue",
            "is_inflated": False
        },
        "context_type": "NONE",
        "context": {}
    }
    config = {
        "get_context_def": {},
        "get_identity_def": {},
        "get_resource_def": {}
    }
    result = asyncio.run(
        seeded_compute.validate_request(
            request=request,
            config=config
        )
    )
    assert result['error'] is not None


def test_in_process_validate_batch_request_valid(seeded_compute):
    batch_request = {
        "identities": {
            "user": [
                {
                    "username": "balloon_person",
                    "department": "Balloon Dept"
                }
            ]
        },
        "action": "balloon:inflate",
        "resource_type": "balloon",
        "resource": {
            "color": "blue",
            "is_inflated": False
        },
        "context_type": "NONE",
        "context": {},
        "batch": [
            {
                "resource": {
                    "color": "red",
                    "is_inflated": True
                }
            }
        ]
    }
    config = {
        "get_context_def": {},
        "get_identity_def": {},
        "get_resource_def": {}
    }
    result = asyncio.run(
        seeded_compute.validate_batch_request(
            batch_request=batch_request,
            config=config
        )
    )
    assert result['error'] is None


def test_in_process_validate_batch_request_invalid_schema(seeded_compute):
    result = asyncio.run(
        seeded_compute.validate_batch_request(
            batch_request={
                "bad": "data"
            },
            config={}
        )
    )
    assert result['error'] is not None


def test_in_process_validate_batch_request_invalid_batch_item(seeded_compute):
    batch_request = {
        "identities": {
            "user": [
                {
                    "username": "balloon_person",
                    "department": "Balloon Dept"
                }
            ]
        },
        "action": "balloon:inflate",
        "resource_type": "balloon",
        "resource": {
            "color": "blue",
            "is_inflated": False
        },
        "context_type": "NONE",
        "context": {},
        "batch": [
            {
                "resource": {
                    "color": 123,
                    "is_inflated": "bad"
                }
            }
        ]
    }
    config = {
        "get_context_def": {},
        "get_identity_def": {},
        "get_resource_def": {}
    }
    result = asyncio.run(
        seeded_compute.validate_batch_request(
            batch_request=batch_request,
            config=config
        )
    )
    assert result['error'] is None
    assert result['batch'][0] is not None


def test_in_process_audit(seeded_compute):
    request = {
        "identities": {
            "user": [
                {
                    "username": "balloon_person",
                    "department": "Balloon Dept"
                }
            ]
        },
        "action": "balloon:inflate",
        "resource_type": "balloon",
        "resource": {
            "color": "blue",
            "is_inflated": False
        },
        "context_type": "NONE",
        "context": {}
    }
    config = {
        "validate_request": {
            "get_context_def": {},
            "get_identity_def": {},
            "get_resource_def": {}
        },
        "list_grants": {
            "page_size": 100,
            "use_cache": False
        }
    }
    result = asyncio.run(
        seeded_compute.audit(
            request=request,
            page_ref=None,
            config=config
        )
    )
    assert result['error'] is None
    assert len(result['results']) > 0
    assert result['results'][0]['grant'] is not None


def test_in_process_authorize_allowed(seeded_compute):
    request = {
        "identities": {
            "user": [
                {
                    "username": "balloon_person",
                    "department": "Balloon Dept"
                }
            ]
        },
        "action": "balloon:inflate",
        "resource_type": "balloon",
        "resource": {
            "color": "blue",
            "is_inflated": False
        },
        "context_type": "NONE",
        "context": {}
    }
    config = {
        "validate_request": {
            "get_context_def": {},
            "get_identity_def": {},
            "get_resource_def": {}
        },
        "list_grants": {
            "page_size": 100,
            "use_cache": False
        }
    }
    result = asyncio.run(
        seeded_compute.authorize(request=request, config=config)
    )
    assert result['is_authorized'] is True
    assert result['error'] is None


def test_in_process_authorize_denied(seeded_compute):
    request = {
        "identities": {
            "user": [
                {
                    "username": "intern_person",
                    "department": "Intern"
                }
            ]
        },
        "action": "balloon:pop",
        "resource_type": "balloon",
        "resource": {
            "color": "blue",
            "is_inflated": True
        },
        "context_type": "NONE",
        "context": {}
    }
    config = {
        "validate_request": {
            "get_context_def": {},
            "get_identity_def": {},
            "get_resource_def": {}
        },
        "list_grants": {
            "page_size": 100,
            "use_cache": False
        }
    }
    result = asyncio.run(
        seeded_compute.authorize(request=request, config=config)
    )
    assert result['is_authorized'] is False
    assert result['error'] is None


def test_in_process_authorize_implicit_deny(seeded_compute):
    """No matching grants -> implicit deny."""
    request = {
        "identities": {
            "user": [
                {
                    "username": "nobody",
                    "department": "None"
                }
            ]
        },
        "action": "balloon:inflate",
        "resource_type": "balloon",
        "resource": {
            "color": "blue",
            "is_inflated": False
        },
        "context_type": "NONE",
        "context": {}
    }
    config = {
        "validate_request": {
            "get_context_def": {},
            "get_identity_def": {},
            "get_resource_def": {}
        },
        "list_grants": {
            "page_size": 100,
            "use_cache": False
        }
    }
    result = asyncio.run(
        seeded_compute.authorize(request=request, config=config)
    )
    assert result['is_authorized'] is False
    assert result['error'] is None
    assert "implicitly denied" in result['message']


def test_in_process_batch_audit(seeded_compute):
    batch_request = {
        "identities": {
            "user": [
                {
                    "username": "balloon_person",
                    "department": "Balloon Dept"
                }
            ]
        },
        "action": "balloon:inflate",
        "resource_type": "balloon",
        "resource": {
            "color": "blue",
            "is_inflated": False
        },
        "context_type": "NONE",
        "context": {},
        "batch": [
            {
                "resource": {
                    "color": "red",
                    "is_inflated": True
                }
            },
            {
                "resource": {
                    "color": "green",
                    "is_inflated": False
                }
            }
        ]
    }
    config = {
        "validate_batch_request": {
            "get_context_def": {},
            "get_identity_def": {},
            "get_resource_def": {}
        },
        "list_grants": {
            "page_size": 100,
            "use_cache": False
        }
    }
    result = asyncio.run(
        seeded_compute.batch_audit(
            batch_request=batch_request,
            page_ref=None,
            config=config
        )
    )
    assert result['error'] is None
    assert len(result['batch']) == 2


def test_in_process_batch_authorize_mixed(seeded_compute):
    batch_request = {
        "identities": {
            "user": [
                {
                    "username": "balloon_person",
                    "department": "Balloon Dept"
                }
            ]
        },
        "action": "balloon:inflate",
        "resource_type": "balloon",
        "resource": {
            "color": "blue",
            "is_inflated": False
        },
        "context_type": "NONE",
        "context": {},
        "batch": [
            {
                "resource": {
                    "color": "red",
                    "is_inflated": True
                }
            },
            {
                "resource": {
                    "color": "green",
                    "is_inflated": False
                }
            }
        ]
    }
    config = {
        "validate_batch_request": {
            "get_context_def": {},
            "get_identity_def": {},
            "get_resource_def": {}
        },
        "list_grants": {
            "page_size": 100,
            "use_cache": False
        }
    }
    result = asyncio.run(
        seeded_compute.batch_authorize(
            batch_request=batch_request,
            config=config
        )
    )
    assert result['error'] is None
    assert len(result['batch']) == 2
    for br in result['batch']:
        assert br['is_authorized'] is True


def test_in_process_batch_authorize_deny(seeded_compute):
    """Batch authorize where a deny grant applies."""
    batch_request = {
        "identities": {
            "user": [
                {
                    "username": "intern_person",
                    "department": "Intern"
                }
            ]
        },
        "action": "balloon:pop",
        "resource_type": "balloon",
        "resource": {
            "color": "blue",
            "is_inflated": True
        },
        "context_type": "NONE",
        "context": {},
        "batch": [
            {
                "resource": {
                    "color": "red",
                    "is_inflated": True
                }
            }
        ]
    }
    config = {
        "validate_batch_request": {
            "get_context_def": {},
            "get_identity_def": {},
            "get_resource_def": {}
        },
        "list_grants": {
            "page_size": 100,
            "use_cache": False
        }
    }
    result = asyncio.run(
        seeded_compute.batch_authorize(
            batch_request=batch_request,
            config=config
        )
    )
    assert result['error'] is None
    for br in result['batch']:
        assert br['is_authorized'] is False


def test_in_process_batch_authorize_implicit_deny(seeded_compute):
    """Batch authorize where no grants match -> implicit deny."""
    batch_request = {
        "identities": {
            "user": [
                {
                    "username": "nobody",
                    "department": "None"
                }
            ]
        },
        "action": "balloon:inflate",
        "resource_type": "balloon",
        "resource": {
            "color": "blue",
            "is_inflated": False
        },
        "context_type": "NONE",
        "context": {},
        "batch": [
            {
                "resource": {
                    "color": "red",
                    "is_inflated": True
                }
            }
        ]
    }
    config = {
        "validate_batch_request": {
            "get_context_def": {},
            "get_identity_def": {},
            "get_resource_def": {}
        },
        "list_grants": {
            "page_size": 100,
            "use_cache": False
        }
    }
    result = asyncio.run(
        seeded_compute.batch_authorize(
            batch_request=batch_request,
            config=config
        )
    )
    assert result['error'] is None
    for br in result['batch']:
        assert br['is_authorized'] is False
        assert "implicitly denied" in br['message']


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


@pytest.fixture
def failing_compute(storage_dict):
    """InProcessCompute with a failing storage module."""
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
        failing = FailingStorage(storage_dict=storage_dict)
        await failing.start(config={})
        c._storage = failing

        return c

    asyncio.run(setup())

    return c


@pytest.fixture
def seeded_failing_compute(failing_compute, storage_dict):
    """Failing compute with definitions stored."""

    async def seed():
        storage = DictStorage(storage_dict=storage_dict)
        await storage.start(config={})
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
                        "username"
                    ],
                    "additionalProperties": False,
                    "properties": {
                        "username": {
                            "type": "string"
                        }
                    }
                }
            },
            config={}
        )
        await storage.put_resource_def(
            {
                "resource_type": "file",
                "actions": [
                    "read"
                ],
                "schema": {
                    "type": "object",
                    "required": [
                        "path"
                    ],
                    "additionalProperties": False,
                    "properties": {
                        "path": {
                            "type": "string"
                        }
                    }
                }
            },
            config={}
        )

    asyncio.run(seed())

    return failing_compute


def test_in_process_audit_storage_failure(seeded_failing_compute):
    """Audit when storage.list_grants fails."""
    request = {
        "identities": {
            "user": [
                {
                    "username": "test"
                }
            ]
        },
        "action": "read",
        "resource_type": "file",
        "resource": {
            "path": "/tmp"
        },
        "context_type": "NONE",
        "context": {}
    }
    config = {
        "validate_request": {
            "get_context_def": {},
            "get_identity_def": {},
            "get_resource_def": {}
        },
        "list_grants": {
            "page_size": 100,
            "use_cache": False
        }
    }
    result = asyncio.run(
        seeded_failing_compute.audit(
            request=request,
            page_ref=None,
            config=config
        )
    )
    assert result['error'] is not None


def test_in_process_authorize_storage_failure(seeded_failing_compute):
    """Authorize when storage.list_grants fails."""
    request = {
        "identities": {
            "user": [
                {
                    "username": "test"
                }
            ]
        },
        "action": "read",
        "resource_type": "file",
        "resource": {
            "path": "/tmp"
        },
        "context_type": "NONE",
        "context": {}
    }
    config = {
        "validate_request": {
            "get_context_def": {},
            "get_identity_def": {},
            "get_resource_def": {}
        },
        "list_grants": {
            "page_size": 100,
            "use_cache": False
        }
    }
    result = asyncio.run(
        seeded_failing_compute.authorize(
            request=request,
            config=config
        )
    )
    assert result['error'] is not None


def test_in_process_batch_audit_storage_failure(seeded_failing_compute):
    """Batch audit when storage.list_grants fails."""
    batch_request = {
        "identities": {
            "user": [
                {
                    "username": "test"
                }
            ]
        },
        "action": "read",
        "resource_type": "file",
        "resource": {
            "path": "/tmp"
        },
        "context_type": "NONE",
        "context": {},
        "batch": [
            {
                "resource": {
                    "path": "/other"
                }
            }
        ]
    }
    config = {
        "validate_batch_request": {
            "get_context_def": {},
            "get_identity_def": {},
            "get_resource_def": {}
        },
        "list_grants": {
            "page_size": 100,
            "use_cache": False
        }
    }
    result = asyncio.run(
        seeded_failing_compute.batch_audit(
            batch_request=batch_request,
            page_ref=None,
            config=config
        )
    )
    assert result['error'] is not None


def test_in_process_batch_authorize_storage_failure(seeded_failing_compute):
    """Batch authorize when storage.list_grants fails in deny phase."""
    batch_request = {
        "identities": {
            "user": [
                {
                    "username": "test"
                }
            ]
        },
        "action": "read",
        "resource_type": "file",
        "resource": {
            "path": "/tmp"
        },
        "context_type": "NONE",
        "context": {},
        "batch": [
            {
                "resource": {
                    "path": "/other"
                }
            }
        ]
    }
    config = {
        "validate_batch_request": {
            "get_context_def": {},
            "get_identity_def": {},
            "get_resource_def": {}
        },
        "list_grants": {
            "page_size": 100,
            "use_cache": False
        }
    }
    result = asyncio.run(
        seeded_failing_compute.batch_authorize(
            batch_request=batch_request,
            config=config
        )
    )
    assert result['error'] is not None
    assert result['error'] is not None


def test_in_process_batch_authorize_allow_phase_storage_failure(storage_dict):
    """Batch authorize storage failure in allow phase."""
    c = InProcessCompute()

    async def setup_and_run():
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
        await storage.start(config={})
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
                        "username"
                    ],
                    "additionalProperties": False,
                    "properties": {
                        "username": {
                            "type": "string"
                        }
                    }
                }
            },
            config={}
        )
        await storage.put_resource_def(
            {
                "resource_type": "file",
                "actions": [
                    "read"
                ],
                "schema": {
                    "type": "object",
                    "required": [
                        "path"
                    ],
                    "additionalProperties": False,
                    "properties": {
                        "path": {
                            "type": "string"
                        }
                    }
                }
            },
            config={}
        )
        failing = FailOnAllowStorage(storage_dict=storage_dict)
        await failing.start(config={})
        c._storage = failing

        batch_request = {
            "identities": {
                "user": [
                    {
                        "username": "test"
                    }
                ]
            },
            "action": "read",
            "resource_type": "file",
            "resource": {
                "path": "/tmp"
            },
            "context_type": "NONE",
            "context": {},
            "batch": [
                {
                    "resource": {
                        "path": "/other"
                    }
                }
            ]
        }
        config_val = {
            "validate_batch_request": {
                "get_context_def": {},
                "get_identity_def": {},
                "get_resource_def": {}
            },
            "list_grants": {
                "page_size": 100,
                "use_cache": False
            }
        }

        return await c.batch_authorize(
            batch_request=batch_request,
            config=config_val
        )

    result = asyncio.run(setup_and_run())
    assert result['error'] is not None
    assert result['error'] is not None


def test_in_process_authorize_allow_phase_storage_failure(storage_dict):
    """Authorize storage failure in allow grants phase."""
    c = InProcessCompute()

    async def setup_and_run():
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
        await storage.start(config={})
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
                        "username"
                    ],
                    "additionalProperties": False,
                    "properties": {
                        "username": {
                            "type": "string"
                        }
                    }
                }
            },
            config={}
        )
        await storage.put_resource_def(
            {
                "resource_type": "file",
                "actions": [
                    "read"
                ],
                "schema": {
                    "type": "object",
                    "required": [
                        "path"
                    ],
                    "additionalProperties": False,
                    "properties": {
                        "path": {
                            "type": "string"
                        }
                    }
                }
            },
            config={}
        )
        failing = FailOnAllowStorage(storage_dict=storage_dict)
        await failing.start(config={})
        c._storage = failing

        request = {
            "identities": {
                "user": [
                    {
                        "username": "test"
                    }
                ]
            },
            "action": "read",
            "resource_type": "file",
            "resource": {
                "path": "/tmp"
            },
            "context_type": "NONE",
            "context": {}
        }
        config_val = {
            "validate_request": {
                "get_context_def": {},
                "get_identity_def": {},
                "get_resource_def": {}
            },
            "list_grants": {
                "page_size": 100,
                "use_cache": False
            }
        }

        return await c.authorize(request=request, config=config_val)

    result = asyncio.run(setup_and_run())
    assert result['error'] is not None


def test_in_process_validate_batch_request_base_request_invalid(seeded_compute):
    """validate_batch_request where the batch schema passes but base request is invalid."""
    batch_request = {
        "identities": {
            "user": [
                {
                    "username": "balloon_person",
                    "department": "Balloon Dept"
                }
            ]
        },
        "action": "balloon:inflate",
        "resource_type": "balloon",
        "resource": {
            "color": "blue",
            "is_inflated": False
        },
        "context_type": "NONEXISTENT",
        "context": {},
        "batch": [
            {
                "resource": {
                    "color": "red",
                    "is_inflated": True
                }
            }
        ]
    }
    config = {
        "get_context_def": {},
        "get_identity_def": {},
        "get_resource_def": {}
    }
    result = asyncio.run(
        seeded_compute.validate_batch_request(
            batch_request=batch_request,
            config=config
        )
    )
    assert result['error'] is not None


def test_in_process_batch_authorize_deny_applicable_continue(storage_dict):
    """Test batch_authorize where deny grant is applicable."""
    c = InProcessCompute()

    async def setup_and_run():
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
        await storage.start(config={})
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
                        "username"
                    ],
                    "additionalProperties": False,
                    "properties": {
                        "username": {
                            "type": "string"
                        }
                    }
                }
            },
            config={}
        )
        await storage.put_resource_def(
            {
                "resource_type": "file",
                "actions": [
                    "read"
                ],
                "schema": {
                    "type": "object",
                    "required": [
                        "path"
                    ],
                    "additionalProperties": False,
                    "properties": {
                        "path": {
                            "type": "string"
                        }
                    }
                }
            },
            config={}
        )
        await storage.enact(
            grant={
                "grant_uuid": str(uuid4()),
                "name": "Deny All",
                "description": "",
                "tags": {},
                "effect": "deny",
                "actions": [
                    "read"
                ],
                "query": "`true`",
                "equality": True,
                "applicable_on_failure": False,
                "data": {}
            },
            config={}
        )
        await storage.enact(
            grant={
                "grant_uuid": str(uuid4()),
                "name": "Allow All",
                "description": "",
                "tags": {},
                "effect": "allow",
                "actions": [
                    "read"
                ],
                "query": "`true`",
                "equality": True,
                "applicable_on_failure": False,
                "data": {}
            },
            config={}
        )

        batch_request = {
            "identities": {
                "user": [
                    {
                        "username": "test"
                    }
                ]
            },
            "action": "read",
            "resource_type": "file",
            "resource": {
                "path": "/tmp"
            },
            "context_type": "NONE",
            "context": {},
            "batch": [
                {
                    "resource": {
                        "path": "/other"
                    }
                }
            ]
        }
        config_val = {
            "validate_batch_request": {
                "get_context_def": {},
                "get_identity_def": {},
                "get_resource_def": {}
            },
            "list_grants": {
                "page_size": 100,
                "use_cache": False
            }
        }

        return await c.batch_authorize(
            batch_request=batch_request,
            config=config_val
        )

    result = asyncio.run(setup_and_run())
    assert result['batch'][0]['is_authorized'] is False
    assert "deny grant" in result['batch'][0]['message']


def test_in_process_batch_authorize_deny_phase_skip_complete(storage_dict):
    """Test that once an item is marked complete by a deny grant,
    subsequent deny grants skip it."""
    c = InProcessCompute()

    async def setup_and_run():
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
        await storage.start(config={})
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
                        "username"
                    ],
                    "additionalProperties": False,
                    "properties": {
                        "username": {
                            "type": "string"
                        }
                    }
                }
            },
            config={}
        )
        await storage.put_resource_def(
            {
                "resource_type": "file",
                "actions": [
                    "read"
                ],
                "schema": {
                    "type": "object",
                    "required": [
                        "path"
                    ],
                    "additionalProperties": False,
                    "properties": {
                        "path": {
                            "type": "string"
                        }
                    }
                }
            },
            config={}
        )
        await storage.enact(
            grant={
                "grant_uuid": str(uuid4()),
                "name": "Deny All 1",
                "description": "",
                "tags": {},
                "effect": "deny",
                "actions": [
                    "read"
                ],
                "query": "`true`",
                "equality": True,
                "applicable_on_failure": False,
                "data": {}
            },
            config={}
        )
        await storage.enact(
            grant={
                "grant_uuid": str(uuid4()),
                "name": "Deny All 2",
                "description": "",
                "tags": {},
                "effect": "deny",
                "actions": [
                    "read"
                ],
                "query": "`true`",
                "equality": True,
                "applicable_on_failure": False,
                "data": {}
            },
            config={}
        )

        batch_request = {
            "identities": {
                "user": [
                    {
                        "username": "test"
                    }
                ]
            },
            "action": "read",
            "resource_type": "file",
            "resource": {
                "path": "/tmp"
            },
            "context_type": "NONE",
            "context": {},
            "batch": [
                {
                    "resource": {
                        "path": "/other"
                    }
                }
            ]
        }
        config_val = {
            "validate_batch_request": {
                "get_context_def": {},
                "get_identity_def": {},
                "get_resource_def": {}
            },
            "list_grants": {
                "page_size": 100,
                "use_cache": False
            }
        }

        return await c.batch_authorize(
            batch_request=batch_request,
            config=config_val
        )

    result = asyncio.run(setup_and_run())
    assert result['batch'][0]['is_authorized'] is False
