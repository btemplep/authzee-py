"""Unit tests for authzee.storage modules (StorageModule and DictStorage)."""

import asyncio
import datetime
from uuid import uuid4

import pytest

from authzee.exceptions import NotImplementedError as AuthzeeNotImplementedError
from authzee.module_locality import ModuleLocality
from authzee.storage.dict_storage import DictStorage
from authzee.storage.storage_module import StorageModule


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


def test_dict_storage_start(storage_dict):
    s = DictStorage(storage_dict=storage_dict)
    asyncio.run(s.construct(config={}))
    result = asyncio.run(s.start(config={}))
    assert result['has_failed'] is False
    assert s.locality == ModuleLocality.PROCESS
    assert s.has_parallel_paging is True


def test_dict_storage_shutdown(storage):
    result = asyncio.run(storage.shutdown(config={}))
    assert result['has_failed'] is False


def test_dict_storage_construct(storage_dict):
    s = DictStorage(storage_dict=storage_dict)
    result = asyncio.run(s.construct(config={}))
    assert result['has_failed'] is False
    assert "context_defs_lut" in storage_dict
    assert "identity_defs_lut" in storage_dict
    assert "resource_defs_lut" in storage_dict
    assert "grants_lut" in storage_dict
    assert "latches_lut" in storage_dict


def test_dict_storage_destroy(storage, storage_dict):
    result = asyncio.run(storage.destroy(config={}))
    assert result['has_failed'] is False
    assert "context_defs_lut" not in storage_dict


def test_dict_storage_put_and_get_context_def(storage):
    context_def = {
        "context_type": "NONE",
        "schema": {
            "type": "object"
        }
    }
    asyncio.run(storage.put_context_def(context_def, config={}))
    result = asyncio.run(storage.get_context_def("NONE", config={}))
    assert result['has_failed'] is False
    assert result['context_def'] == context_def


def test_dict_storage_get_context_def_not_found(storage):
    result = asyncio.run(storage.get_context_def("MISSING", config={}))
    assert result['has_failed'] is True
    assert result['context_def'] is None


def test_dict_storage_list_context_defs(storage):
    asyncio.run(
        storage.put_context_def(
            {
                "context_type": "A",
                "schema": {
                    "type": "object"
                }
            },
            config={}
        )
    )
    asyncio.run(
        storage.put_context_def(
            {
                "context_type": "B",
                "schema": {
                    "type": "object"
                }
            },
            config={}
        )
    )
    result = asyncio.run(
        storage.list_context_defs(
            page_ref=None,
            config={
                "page_size": 10
            }
        )
    )
    assert result['has_failed'] is False
    assert len(result['context_defs']) == 2
    assert result['next_page_ref'] is None


def test_dict_storage_list_context_defs_pagination(storage):
    for i in range(5):
        asyncio.run(
            storage.put_context_def(
                {
                    "context_type": f"T{i}",
                    "schema": {
                        "type": "object"
                    }
                },
                config={}
            )
        )

    result = asyncio.run(
        storage.list_context_defs(
            page_ref=None,
            config={
                "page_size": 2
            }
        )
    )
    assert len(result['context_defs']) == 2
    assert result['next_page_ref'] is not None
    result2 = asyncio.run(
        storage.list_context_defs(
            page_ref=result['next_page_ref'],
            config={
                "page_size": 2
            }
        )
    )
    assert len(result2['context_defs']) == 2


def test_dict_storage_delete_context_def(storage):
    asyncio.run(
        storage.put_context_def(
            {
                "context_type": "DEL",
                "schema": {
                    "type": "object"
                }
            },
            config={}
        )
    )
    result = asyncio.run(storage.delete_context_def("DEL", config={}))
    assert result['has_failed'] is False
    get_result = asyncio.run(storage.get_context_def("DEL", config={}))
    assert get_result['context_def'] is None


def test_dict_storage_put_and_get_identity_def(storage):
    identity_def = {
        "identity_type": "user",
        "schema": {
            "type": "object"
        }
    }
    asyncio.run(
        storage.put_identity_def(identity_def, config={})
    )
    result = asyncio.run(storage.get_identity_def("user", config={}))
    assert result['has_failed'] is False
    assert result['identity_def'] == identity_def


def test_dict_storage_get_identity_def_not_found(storage):
    result = asyncio.run(storage.get_identity_def("MISSING", config={}))
    assert result['has_failed'] is True
    assert result['identity_def'] is None


def test_dict_storage_list_identity_defs(storage):
    asyncio.run(
        storage.put_identity_def(
            {
                "identity_type": "A",
                "schema": {
                    "type": "object"
                }
            },
            config={}
        )
    )
    result = asyncio.run(
        storage.list_identity_defs(
            page_ref=None,
            config={
                "page_size": 10
            }
        )
    )
    assert len(result['identity_defs']) == 1


def test_dict_storage_list_identity_defs_pagination(storage):
    for i in range(5):
        asyncio.run(
            storage.put_identity_def(
                {
                    "identity_type": f"T{i}",
                    "schema": {
                        "type": "object"
                    }
                },
                config={}
            )
        )

    result = asyncio.run(
        storage.list_identity_defs(
            page_ref=None,
            config={
                "page_size": 2
            }
        )
    )
    assert len(result['identity_defs']) == 2
    assert result['next_page_ref'] is not None
    result2 = asyncio.run(
        storage.list_identity_defs(
            page_ref=result['next_page_ref'],
            config={
                "page_size": 2
            }
        )
    )
    assert len(result2['identity_defs']) == 2


def test_dict_storage_delete_identity_def(storage):
    asyncio.run(
        storage.put_identity_def(
            {
                "identity_type": "DEL",
                "schema": {
                    "type": "object"
                }
            },
            config={}
        )
    )
    asyncio.run(storage.delete_identity_def("DEL", config={}))
    result = asyncio.run(storage.get_identity_def("DEL", config={}))
    assert result['identity_def'] is None


def test_dict_storage_put_and_get_resource_def(storage):
    resource_def = {
        "resource_type": "file",
        "actions": [
            "read"
        ],
        "schema": {
            "type": "object"
        }
    }
    asyncio.run(
        storage.put_resource_def(resource_def, config={})
    )
    result = asyncio.run(storage.get_resource_def("file", config={}))
    assert result['has_failed'] is False
    assert result['resource_def'] == resource_def


def test_dict_storage_get_resource_def_not_found(storage):
    result = asyncio.run(storage.get_resource_def("MISSING", config={}))
    assert result['has_failed'] is True
    assert result['resource_def'] is None


def test_dict_storage_list_resource_defs(storage):
    asyncio.run(
        storage.put_resource_def(
            {
                "resource_type": "A",
                "actions": [
                    "x"
                ],
                "schema": {
                    "type": "object"
                }
            },
            config={}
        )
    )
    result = asyncio.run(
        storage.list_resource_defs(
            page_ref=None,
            config={
                "page_size": 10
            }
        )
    )
    assert len(result['resource_defs']) == 1


def test_dict_storage_list_resource_defs_pagination(storage):
    for i in range(5):
        asyncio.run(
            storage.put_resource_def(
                {
                    "resource_type": f"T{i}",
                    "actions": [
                        "x"
                    ],
                    "schema": {
                        "type": "object"
                    }
                },
                config={}
            )
        )

    result = asyncio.run(
        storage.list_resource_defs(
            page_ref=None,
            config={
                "page_size": 2
            }
        )
    )
    assert len(result['resource_defs']) == 2
    assert result['next_page_ref'] is not None
    result2 = asyncio.run(
        storage.list_resource_defs(
            page_ref=result['next_page_ref'],
            config={
                "page_size": 2
            }
        )
    )
    assert len(result2['resource_defs']) == 2


def test_dict_storage_delete_resource_def(storage):
    asyncio.run(
        storage.put_resource_def(
            {
                "resource_type": "DEL",
                "actions": [
                    "x"
                ],
                "schema": {
                    "type": "object"
                }
            },
            config={}
        )
    )
    asyncio.run(storage.delete_resource_def("DEL", config={}))
    result = asyncio.run(storage.get_resource_def("DEL", config={}))
    assert result['resource_def'] is None


@pytest.fixture
def sample_grant():
    return {
        "grant_uuid": str(uuid4()),
        "name": "Test",
        "description": "",
        "tags": {},
        "effect": "allow",
        "actions": [
            "read"
        ],
        "query": "`true`",
        "evaluation_handler": "evaluate",
        "equality": True,
        "data": {}
    }


def test_dict_storage_enact_and_get_grant(storage, sample_grant):
    asyncio.run(storage.enact(sample_grant, config={}))
    result = asyncio.run(
        storage.get_grant(sample_grant['grant_uuid'], config={})
    )
    assert result['has_failed'] is False
    assert result['grant'] == sample_grant


def test_dict_storage_get_grant_not_found(storage):
    result = asyncio.run(
        storage.get_grant("nonexistent-uuid", config={})
    )
    assert result['has_failed'] is True
    assert result['grant'] is None


def test_dict_storage_repeal(storage, sample_grant):
    asyncio.run(storage.enact(sample_grant, config={}))
    result = asyncio.run(
        storage.repeal(
            sample_grant['grant_uuid'],
            purge=True,
            config={}
        )
    )
    assert result['has_failed'] is False
    get_result = asyncio.run(
        storage.get_grant(sample_grant['grant_uuid'], config={})
    )
    assert get_result['grant'] is None


def test_dict_storage_list_grants(storage, sample_grant):
    asyncio.run(storage.enact(sample_grant, config={}))
    result = asyncio.run(
        storage.list_grants(
            effect=None,
            action=None,
            page_ref=None,
            config={
                "page_size": 10
            }
        )
    )
    assert len(result['grants']) == 1


def test_dict_storage_list_grants_filter_effect(storage):
    allow_grant = {
        "grant_uuid": str(uuid4()),
        "name": "A",
        "description": "",
        "tags": {},
        "effect": "allow",
        "actions": [
            "read"
        ],
        "query": "`true`",
        "evaluation_handler": "evaluate",
        "equality": True,
        "data": {}
    }
    deny_grant = {
        "grant_uuid": str(uuid4()),
        "name": "D",
        "description": "",
        "tags": {},
        "effect": "deny",
        "actions": [
            "read"
        ],
        "query": "`true`",
        "evaluation_handler": "evaluate",
        "equality": True,
        "data": {}
    }
    asyncio.run(storage.enact(allow_grant, config={}))
    asyncio.run(storage.enact(deny_grant, config={}))
    result = asyncio.run(
        storage.list_grants(
            effect="allow",
            action=None,
            page_ref=None,
            config={
                "page_size": 10
            }
        )
    )
    assert len(result['grants']) == 1
    assert result['grants'][0]['effect'] == "allow"


def test_dict_storage_list_grants_filter_action(storage):
    grant1 = {
        "grant_uuid": str(uuid4()),
        "name": "G1",
        "description": "",
        "tags": {},
        "effect": "allow",
        "actions": [
            "read",
            "write"
        ],
        "query": "`true`",
        "evaluation_handler": "evaluate",
        "equality": True,
        "data": {}
    }
    grant2 = {
        "grant_uuid": str(uuid4()),
        "name": "G2",
        "description": "",
        "tags": {},
        "effect": "allow",
        "actions": [
            "delete"
        ],
        "query": "`true`",
        "evaluation_handler": "evaluate",
        "equality": True,
        "data": {}
    }
    asyncio.run(storage.enact(grant1, config={}))
    asyncio.run(storage.enact(grant2, config={}))
    result = asyncio.run(
        storage.list_grants(
            effect=None,
            action="write",
            page_ref=None,
            config={
                "page_size": 10
            }
        )
    )
    assert len(result['grants']) == 1
    assert result['grants'][0]['name'] == "G1"


def test_dict_storage_list_grants_pagination(storage):
    for i in range(5):
        g = {
            "grant_uuid": str(uuid4()),
            "name": f"G{i}",
            "description": "",
            "tags": {},
            "effect": "allow",
            "actions": [
                "read"
            ],
            "query": "`true`",
            "evaluation_handler": "evaluate",
            "equality": True,
            "data": {}
        }
        asyncio.run(storage.enact(g, config={}))

    result = asyncio.run(
        storage.list_grants(
            effect=None,
            action=None,
            page_ref=None,
            config={
                "page_size": 2
            }
        )
    )
    assert len(result['grants']) == 2
    assert result['next_page_ref'] is not None
    result2 = asyncio.run(
        storage.list_grants(
            effect=None,
            action=None,
            page_ref=result['next_page_ref'],
            config={
                "page_size": 2
            }
        )
    )
    assert len(result2['grants']) == 2


def test_dict_storage_list_grant_refs(storage):
    for i in range(5):
        g = {
            "grant_uuid": str(uuid4()),
            "name": f"G{i}",
            "description": "",
            "tags": {},
            "effect": "allow",
            "actions": [
                "read"
            ],
            "query": "`true`",
            "evaluation_handler": "evaluate",
            "equality": True,
            "data": {}
        }
        asyncio.run(storage.enact(g, config={}))

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
    assert result['has_failed'] is False
    assert len(result['page_refs']) > 0
    if result['next_page_ref'] is not None:
        result2 = asyncio.run(
            storage.list_grant_refs(
                effect=None,
                action=None,
                page_ref=str(result['next_page_ref']),
                config={
                    "page_size": 2
                }
            )
        )
        assert result2['has_failed'] is False


def test_dict_storage_list_grant_refs_filter_effect(storage):
    allow_grant = {
        "grant_uuid": str(uuid4()),
        "name": "A",
        "description": "",
        "tags": {},
        "effect": "allow",
        "actions": [
            "read"
        ],
        "query": "`true`",
        "evaluation_handler": "evaluate",
        "equality": True,
        "data": {}
    }
    asyncio.run(storage.enact(allow_grant, config={}))
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
    assert result['page_refs'] == [0]


def test_dict_storage_list_grant_refs_filter_action(storage):
    g = {
        "grant_uuid": str(uuid4()),
        "name": "G",
        "description": "",
        "tags": {},
        "effect": "allow",
        "actions": [
            "write"
        ],
        "query": "`true`",
        "evaluation_handler": "evaluate",
        "equality": True,
        "data": {}
    }
    asyncio.run(storage.enact(g, config={}))
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
    assert result['has_failed'] is False


def test_dict_storage_create_and_get_latch(storage):
    create_result = asyncio.run(storage.create_latch(config={}))
    assert create_result['has_failed'] is False
    latch = create_result['storage_latch']
    assert latch['is_set'] is False

    get_result = asyncio.run(
        storage.get_latch(latch['storage_latch_uuid'], config={})
    )
    assert get_result['has_failed'] is False
    assert get_result['storage_latch'] == latch


def test_dict_storage_get_latch_not_found(storage):
    result = asyncio.run(storage.get_latch("nonexistent", config={}))
    assert result['has_failed'] is True


def test_dict_storage_set_latch(storage):
    create_result = asyncio.run(storage.create_latch(config={}))
    latch_uuid = create_result['storage_latch']['storage_latch_uuid']
    set_result = asyncio.run(storage.set_latch(latch_uuid, config={}))
    assert set_result['has_failed'] is False
    assert set_result['storage_latch']['is_set'] is True


def test_dict_storage_set_latch_not_found(storage):
    result = asyncio.run(storage.set_latch("nonexistent", config={}))
    assert result['has_failed'] is True


def test_dict_storage_delete_latch(storage):
    create_result = asyncio.run(storage.create_latch(config={}))
    latch_uuid = create_result['storage_latch']['storage_latch_uuid']
    del_result = asyncio.run(storage.delete_latch(latch_uuid, config={}))
    assert del_result['has_failed'] is False
    get_result = asyncio.run(storage.get_latch(latch_uuid, config={}))
    assert get_result['has_failed'] is True


def test_dict_storage_cleanup_latches(storage):
    asyncio.run(storage.create_latch(config={}))
    asyncio.run(storage.create_latch(config={}))
    future = (
        datetime.datetime.now(tz=datetime.timezone.utc)
        + datetime.timedelta(seconds=1)
    )
    result = asyncio.run(
        storage.cleanup_latches(before=future, config={})
    )
    assert result['has_failed'] is False
    assert len(storage._storage_dict['latches_lut']) == 0


def test_dict_storage_cleanup_latches_keeps_recent(storage):
    asyncio.run(storage.create_latch(config={}))
    past = (
        datetime.datetime.now(tz=datetime.timezone.utc)
        - datetime.timedelta(seconds=10)
    )
    result = asyncio.run(storage.cleanup_latches(before=past, config={}))
    assert result['has_failed'] is False
    assert len(storage._storage_dict['latches_lut']) == 1
