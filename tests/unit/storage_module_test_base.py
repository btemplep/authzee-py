"""Reusable base test suite for Authzee storage modules.

Any concrete storage module test file can reuse this suite by importing all of
its test functions via ``from storage_module_test_base import *`` and supplying
the required pytest fixtures:

- ``storage_dict``
- ``storage``

The shared test functions reference these fixtures by name so pytest resolves
them against whatever the concrete test module defines. Only strictly generic
``StorageModule`` behavior is asserted here; implementation-specific internals
(e.g. backing-dict layout, ``has_parallel_paging`` values) belong in the
concrete test module.
"""

import asyncio
import datetime
from uuid import uuid4

from authzee.module_locality import ModuleLocality


def _grant(
    effect="allow",
    actions=None,
    name="Test"
):
    """Build a valid grant dict.

    Arguments
    ---------
    effect : str, default="allow"
        The grant effect, ``"allow"`` or ``"deny"``.
    actions : list | None, default=["read"]
        The actions the grant matches. By default ``["read"]``.
    name : str, default="Test"
        The grant name.

    Returns
    -------
    dict
        A valid grant object with a fresh ``grant_uuid``.
    """
    if actions is None:
        actions = ["read"]

    return {
        "grant_uuid": str(uuid4()),
        "name": name,
        "description": "",
        "tags": {},
        "effect": effect,
        "actions": actions,
        "query": "`true`",
        "equality": True,
        "applicable_on_failure": False,
        "data": {}
    }


def test_base_start(storage_dict):
    s = _new_storage(storage_dict)
    asyncio.run(s.construct(config={}))
    result = asyncio.run(s.start(config={}))
    assert result['error'] is None
    assert s.locality == ModuleLocality.PROCESS


def test_base_shutdown(storage):
    result = asyncio.run(storage.shutdown(config={}))
    assert result['error'] is None


def test_base_construct(storage_dict):
    s = _new_storage(storage_dict)
    result = asyncio.run(s.construct(config={}))
    assert result['error'] is None
    asyncio.run(s.start(config={}))
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


def test_base_destroy(storage):
    result = asyncio.run(storage.destroy(config={}))
    assert result['error'] is None


def test_base_put_and_get_context_def(storage):
    context_def = {
        "context_type": "NONE",
        "schema": {
            "type": "object"
        }
    }
    asyncio.run(storage.put_context_def(context_def, config={}))
    result = asyncio.run(storage.get_context_def("NONE", config={}))
    assert result['error'] is None
    assert result['context_def'] == context_def


def test_base_get_context_def_not_found(storage):
    result = asyncio.run(storage.get_context_def("MISSING", config={}))
    assert result['error'] is not None
    assert result['error']['error_type'] == "resource_not_found"
    assert result['context_def'] is None


def test_base_list_context_defs(storage):
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
    assert result['error'] is None
    assert len(result['context_defs']) == 2
    assert result['next_page_ref'] is None


def test_base_list_context_defs_pagination(storage):
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


def test_base_delete_context_def(storage):
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
    assert result['error'] is None
    get_result = asyncio.run(storage.get_context_def("DEL", config={}))
    assert get_result['context_def'] is None


def test_base_put_and_get_identity_def(storage):
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
    assert result['error'] is None
    assert result['identity_def'] == identity_def


def test_base_get_identity_def_not_found(storage):
    result = asyncio.run(storage.get_identity_def("MISSING", config={}))
    assert result['error'] is not None
    assert result['error']['error_type'] == "resource_not_found"
    assert result['identity_def'] is None


def test_base_list_identity_defs(storage):
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


def test_base_list_identity_defs_pagination(storage):
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


def test_base_delete_identity_def(storage):
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


def test_base_put_and_get_resource_def(storage):
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
    assert result['error'] is None
    assert result['resource_def'] == resource_def


def test_base_get_resource_def_not_found(storage):
    result = asyncio.run(storage.get_resource_def("MISSING", config={}))
    assert result['error'] is not None
    assert result['error']['error_type'] == "resource_not_found"
    assert result['resource_def'] is None


def test_base_list_resource_defs(storage):
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


def test_base_list_resource_defs_pagination(storage):
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


def test_base_delete_resource_def(storage):
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


def test_base_enact_and_get_grant(storage):
    grant = _grant()
    asyncio.run(storage.enact(grant, config={}))
    result = asyncio.run(
        storage.get_grant(grant['grant_uuid'], config={})
    )
    assert result['error'] is None
    assert result['grant'] == grant


def test_base_get_grant_not_found(storage):
    result = asyncio.run(
        storage.get_grant("nonexistent-uuid", config={})
    )
    assert result['error'] is not None
    assert result['error']['error_type'] == "resource_not_found"
    assert result['grant'] is None


def test_base_repeal(storage):
    grant = _grant()
    asyncio.run(storage.enact(grant, config={}))
    result = asyncio.run(
        storage.repeal(
            grant['grant_uuid'],
            purge=True,
            config={}
        )
    )
    assert result['error'] is None
    get_result = asyncio.run(
        storage.get_grant(grant['grant_uuid'], config={})
    )
    assert get_result['grant'] is None


def test_base_list_grants(storage):
    grant = _grant()
    asyncio.run(storage.enact(grant, config={}))
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


def test_base_list_grants_filter_effect(storage):
    asyncio.run(
        storage.enact(_grant(effect="allow", name="A"), config={})
    )
    asyncio.run(
        storage.enact(_grant(effect="deny", name="D"), config={})
    )
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


def test_base_list_grants_filter_action(storage):
    grant1 = _grant(actions=["read", "write"], name="G1")
    grant2 = _grant(actions=["delete"], name="G2")
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


def test_base_list_grants_pagination(storage):
    for i in range(5):
        asyncio.run(storage.enact(_grant(name=f"G{i}"), config={}))

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


def test_base_list_grant_refs(storage):
    for i in range(5):
        asyncio.run(storage.enact(_grant(name=f"G{i}"), config={}))

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
    assert result['error'] is None
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
        assert result2['error'] is None


def test_base_list_grant_refs_filter_effect(storage):
    asyncio.run(
        storage.enact(_grant(effect="allow", name="A"), config={})
    )
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
    assert result['error'] is None
    assert result['page_refs'] == [0]


def test_base_list_grant_refs_filter_action(storage):
    asyncio.run(
        storage.enact(
            _grant(actions=["write"], name="G"),
            config={}
        )
    )
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
    assert result['error'] is None


def test_base_create_and_get_latch(storage):
    create_result = asyncio.run(storage.create_latch(config={}))
    assert create_result['error'] is None
    latch = create_result['storage_latch']
    assert latch['is_set'] is False

    get_result = asyncio.run(
        storage.get_latch(latch['storage_latch_uuid'], config={})
    )
    assert get_result['error'] is None
    assert get_result['storage_latch'] == latch


def test_base_get_latch_not_found(storage):
    result = asyncio.run(storage.get_latch("nonexistent", config={}))
    assert result['error'] is not None
    assert result['error']['error_type'] == "resource_not_found"


def test_base_set_latch(storage):
    create_result = asyncio.run(storage.create_latch(config={}))
    latch_uuid = create_result['storage_latch']['storage_latch_uuid']
    set_result = asyncio.run(storage.set_latch(latch_uuid, config={}))
    assert set_result['error'] is None
    assert set_result['storage_latch']['is_set'] is True


def test_base_set_latch_not_found(storage):
    result = asyncio.run(storage.set_latch("nonexistent", config={}))
    assert result['error'] is not None
    assert result['error']['error_type'] == "resource_not_found"


def test_base_delete_latch(storage):
    create_result = asyncio.run(storage.create_latch(config={}))
    latch_uuid = create_result['storage_latch']['storage_latch_uuid']
    del_result = asyncio.run(storage.delete_latch(latch_uuid, config={}))
    assert del_result['error'] is None
    get_result = asyncio.run(storage.get_latch(latch_uuid, config={}))
    assert get_result['error'] is not None


def test_base_cleanup_latches(storage):
    create_a = asyncio.run(storage.create_latch(config={}))
    create_b = asyncio.run(storage.create_latch(config={}))
    uuid_a = create_a['storage_latch']['storage_latch_uuid']
    uuid_b = create_b['storage_latch']['storage_latch_uuid']
    future = (
        datetime.datetime.now(tz=datetime.timezone.utc)
        + datetime.timedelta(seconds=1)
    )
    result = asyncio.run(
        storage.cleanup_latches(before=future, config={})
    )
    assert result['error'] is None
    get_a = asyncio.run(storage.get_latch(uuid_a, config={}))
    get_b = asyncio.run(storage.get_latch(uuid_b, config={}))
    assert get_a['error'] is not None
    assert get_b['error'] is not None


def test_base_cleanup_latches_keeps_recent(storage):
    create_result = asyncio.run(storage.create_latch(config={}))
    latch_uuid = create_result['storage_latch']['storage_latch_uuid']
    past = (
        datetime.datetime.now(tz=datetime.timezone.utc)
        - datetime.timedelta(seconds=10)
    )
    result = asyncio.run(storage.cleanup_latches(before=past, config={}))
    assert result['error'] is None
    get_result = asyncio.run(storage.get_latch(latch_uuid, config={}))
    assert get_result['error'] is None


def _new_storage(storage_dict):
    """Construct a fresh storage instance of the same concrete type used by the
    ``storage`` fixture, backed by ``storage_dict``.

    The concrete test module registers its storage type via
    ``register_storage_type`` so the base tests can build additional instances
    for construct/start lifecycle checks without knowing the class.

    Arguments
    ---------
    storage_dict : dict
        The backing storage dict to pass to the new instance.

    Returns
    -------
    StorageModule
        A new, unconstructed storage instance.
    """
    return _STORAGE_TYPE_HOLDER['storage_type'](
        storage_dict=storage_dict
    )


_STORAGE_TYPE_HOLDER = {
    "storage_type": None
}


def register_storage_type(storage_type):
    """Register the concrete storage type used by the reusable base tests.

    Concrete test modules must call this at import time so lifecycle tests can
    build fresh instances.

    Arguments
    ---------
    storage_type : type
        The concrete ``StorageModule`` subclass to instantiate.
    """
    _STORAGE_TYPE_HOLDER['storage_type'] = storage_type
