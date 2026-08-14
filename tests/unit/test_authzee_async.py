"""Comprehensive unit tests for the AuthzeeAsync class.

Tests use DictStorage and InProcessCompute as the storage/compute modules.
Black box testing - only uses public API methods of AuthzeeAsync.
Uses asyncio.run() pattern since pytest-asyncio is not installed.
"""

import asyncio
import datetime
from uuid import uuid4

import pytest

from authzee import (
    AuthzeeAsync,
    DictStorage,
    InProcessCompute,
    authzee_specification_version,
    exceptions,
    jmespath_execute,
    paginator_async
)


@pytest.fixture
def storage_dict():
    return {}


@pytest.fixture
def authz(storage_dict):
    """Create a fully initialized AuthzeeAsync instance with raise_crits=False."""
    a = AuthzeeAsync(
        execute=jmespath_execute,
        compute_type=InProcessCompute,
        compute_kwargs={},
        storage_type=DictStorage,
        storage_kwargs={
            "storage_dict": storage_dict
        },
        config={
            "authzee": {
                "raise_crits": False
            }
        }
    )
    asyncio.run(a.construct())
    asyncio.run(a.start())

    return a


@pytest.fixture
def context_def():
    return {
        "context_type": "NONE",
        "schema": {
            "type": "object",
            "additionalProperties": False
        }
    }


@pytest.fixture
def identity_def():
    return {
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
    }


@pytest.fixture
def resource_def():
    return {
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
    }


@pytest.fixture
def grant():
    return {
        "grant_uuid": str(uuid4()),
        "name": "Allow inflate for balloon department",
        "description": "Balloon dept can read and inflate balloons.",
        "tags": {
            "team": "balloon"
        },
        "effect": "allow",
        "actions": [
            "balloon:read",
            "balloon:inflate"
        ],
        "query": "length(request.identities.user[?department == 'Balloon Dept']) > `0`",
        "evaluation_handler": "evaluate",
        "equality": True,
        "data": {}
    }


@pytest.fixture
def deny_grant():
    return {
        "grant_uuid": str(uuid4()),
        "name": "Deny pop for interns",
        "description": "Interns cannot pop balloons.",
        "tags": {},
        "effect": "deny",
        "actions": [
            "balloon:pop"
        ],
        "query": "length(request.identities.user[?department == 'Intern']) > `0`",
        "evaluation_handler": "evaluate",
        "equality": True,
        "data": {}
    }


@pytest.fixture
def auth_request():
    return {
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
        "evaluation_handler": "grant",
        "context_type": "NONE",
        "context": {}
    }


@pytest.fixture
def batch_request():
    return {
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
        "evaluation_handler": "grant",
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


@pytest.fixture
def seeded_authz(
    authz,
    context_def,
    identity_def,
    resource_def,
    grant
):
    """An AuthzeeAsync instance with definitions and a grant already stored."""
    asyncio.run(authz.put_context_def(context_def))
    asyncio.run(authz.put_identity_def(identity_def))
    asyncio.run(authz.put_resource_def(resource_def))
    asyncio.run(authz.enact(grant))

    return authz


def test_authzee_specification_version_is_string():
    assert isinstance(authzee_specification_version, str)
    assert len(authzee_specification_version) > 0


def test_construct(storage_dict):
    authz = AuthzeeAsync(
        execute=jmespath_execute,
        compute_type=InProcessCompute,
        compute_kwargs={},
        storage_type=DictStorage,
        storage_kwargs={
            "storage_dict": storage_dict
        }
    )
    result = asyncio.run(authz.construct())
    assert result['has_failed'] is False


def test_start(storage_dict):
    authz = AuthzeeAsync(
        execute=jmespath_execute,
        compute_type=InProcessCompute,
        compute_kwargs={},
        storage_type=DictStorage,
        storage_kwargs={
            "storage_dict": storage_dict
        }
    )
    asyncio.run(authz.construct())
    result = asyncio.run(authz.start())
    assert result['has_failed'] is False


def test_shutdown(authz):
    result = asyncio.run(authz.shutdown())
    assert result['has_failed'] is False


def test_destroy(storage_dict):
    authz = AuthzeeAsync(
        execute=jmespath_execute,
        compute_type=InProcessCompute,
        compute_kwargs={},
        storage_type=DictStorage,
        storage_kwargs={
            "storage_dict": storage_dict
        }
    )
    asyncio.run(authz.construct())
    asyncio.run(authz.start())
    result = asyncio.run(authz.destroy())
    assert result['has_failed'] is False


def test_construct_with_config(storage_dict):
    authz = AuthzeeAsync(
        execute=jmespath_execute,
        compute_type=InProcessCompute,
        compute_kwargs={},
        storage_type=DictStorage,
        storage_kwargs={
            "storage_dict": storage_dict
        }
    )
    result = asyncio.run(
        authz.construct(
            config={
                "authzee": {
                    "raise_crits": True
                }
            }
        )
    )
    assert result['has_failed'] is False


def test_start_with_config(storage_dict):
    authz = AuthzeeAsync(
        execute=jmespath_execute,
        compute_type=InProcessCompute,
        compute_kwargs={},
        storage_type=DictStorage,
        storage_kwargs={
            "storage_dict": storage_dict
        }
    )
    asyncio.run(authz.construct())
    result = asyncio.run(
        authz.start(
            config={
                "authzee": {
                    "raise_crits": True
                }
            }
        )
    )
    assert result['has_failed'] is False


def test_shutdown_with_config(authz):
    result = asyncio.run(
        authz.shutdown(
            config={
                "authzee": {
                    "raise_crits": True
                }
            }
        )
    )
    assert result['has_failed'] is False


def test_destroy_with_config(storage_dict):
    authz = AuthzeeAsync(
        execute=jmespath_execute,
        compute_type=InProcessCompute,
        compute_kwargs={},
        storage_type=DictStorage,
        storage_kwargs={
            "storage_dict": storage_dict
        }
    )
    asyncio.run(authz.construct())
    asyncio.run(authz.start())
    result = asyncio.run(
        authz.destroy(
            config={
                "authzee": {
                    "raise_crits": True
                }
            }
        )
    )
    assert result['has_failed'] is False


def test_validate_context_def_valid(authz, context_def):
    result = asyncio.run(authz.validate_context_def(context_def))
    assert result['has_failed'] is False


def test_validate_context_def_invalid(authz):
    result = asyncio.run(
        authz.validate_context_def(
            {
                "context_type": "BAD",
                "schema": "not_a_dict"
            }
        )
    )
    assert result['has_failed'] is True


def test_validate_context_def_non_object_schema(authz):
    result = asyncio.run(
        authz.validate_context_def(
            {
                "context_type": "BAD",
                "schema": {
                    "type": "array"
                }
            }
        )
    )
    assert result['has_failed'] is True


def test_put_context_def(authz, context_def):
    result = asyncio.run(authz.put_context_def(context_def))
    assert result['has_failed'] is False


def test_put_context_def_invalid(authz):
    result = asyncio.run(
        authz.put_context_def(
            {
                "context_type": "BAD",
                "schema": "nope"
            }
        )
    )
    assert result['has_failed'] is True


def test_get_context_def(authz, context_def):
    asyncio.run(authz.put_context_def(context_def))
    result = asyncio.run(authz.get_context_def(context_type="NONE"))
    assert result['has_failed'] is False
    assert result['context_def']['context_type'] == "NONE"


def test_get_context_def_not_found(authz):
    result = asyncio.run(
        authz.get_context_def(context_type="DOES_NOT_EXIST")
    )
    assert result['context_def'] is None
    assert result['has_failed'] is True


def test_list_context_defs_empty(authz):
    result = asyncio.run(authz.list_context_defs())
    assert result['has_failed'] is False
    assert result['context_defs'] == []
    assert result['next_page_ref'] is None


def test_list_context_defs_with_data(authz, context_def):
    asyncio.run(authz.put_context_def(context_def))
    result = asyncio.run(authz.list_context_defs())
    assert len(result['context_defs']) == 1
    assert result['context_defs'][0]['context_type'] == "NONE"


def test_list_context_defs_paginator_async(authz, context_def):
    asyncio.run(authz.put_context_def(context_def))

    async def _collect():
        all_defs = []
        async for page in paginator_async(authz.list_context_defs):
            all_defs.extend(page['context_defs'])

        return all_defs

    all_defs = asyncio.run(_collect())
    assert len(all_defs) == 1


def test_delete_context_def(authz, context_def):
    asyncio.run(authz.put_context_def(context_def))
    result = asyncio.run(authz.delete_context_def(context_type="NONE"))
    assert result['has_failed'] is False
    get_result = asyncio.run(authz.get_context_def(context_type="NONE"))
    assert get_result['context_def'] is None
    assert get_result['has_failed'] is True


def test_delete_context_def_not_found(authz):
    result = asyncio.run(
        authz.delete_context_def(context_type="DOES_NOT_EXIST")
    )
    assert result['has_failed'] is False


def test_validate_context_def_with_config(authz, context_def):
    result = asyncio.run(
        authz.validate_context_def(
            context_def, config={
                "authzee": {
                    "raise_crits": True
                }
            }
        )
    )
    assert result['has_failed'] is False


def test_put_context_def_with_config(authz, context_def):
    result = asyncio.run(
        authz.put_context_def(
            context_def,
            config={
                "authzee": {
                    "raise_crits": False
                }
            }
        )
    )
    assert result['has_failed'] is False


def test_validate_identity_def_valid(authz, identity_def):
    result = asyncio.run(authz.validate_identity_def(identity_def))
    assert result['has_failed'] is False


def test_validate_identity_def_invalid(authz):
    result = asyncio.run(
        authz.validate_identity_def(
            {
                "identity_type": "BAD",
                "schema": "not_a_dict"
            }
        )
    )
    assert result['has_failed'] is True


def test_validate_identity_def_non_object_schema(authz):
    result = asyncio.run(
        authz.validate_identity_def(
            {
                "identity_type": "BAD",
                "schema": {
                    "type": "string"
                }
            }
        )
    )
    assert result['has_failed'] is True


def test_put_identity_def(authz, identity_def):
    result = asyncio.run(authz.put_identity_def(identity_def))
    assert result['has_failed'] is False


def test_put_identity_def_invalid(authz):
    result = asyncio.run(
        authz.put_identity_def(
            {
                "identity_type": "X",
                "schema": 123
            }
        )
    )
    assert result['has_failed'] is True


def test_get_identity_def(authz, identity_def):
    asyncio.run(authz.put_identity_def(identity_def))
    result = asyncio.run(authz.get_identity_def(identity_type="user"))
    assert result['has_failed'] is False
    assert result['identity_def']['identity_type'] == "user"


def test_get_identity_def_not_found(authz):
    result = asyncio.run(
        authz.get_identity_def(identity_type="DOES_NOT_EXIST")
    )
    assert result['identity_def'] is None
    assert result['has_failed'] is True


def test_list_identity_defs_empty(authz):
    result = asyncio.run(authz.list_identity_defs())
    assert result['has_failed'] is False
    assert result['identity_defs'] == []


def test_list_identity_defs_with_data(authz, identity_def):
    asyncio.run(authz.put_identity_def(identity_def))
    result = asyncio.run(authz.list_identity_defs())
    assert len(result['identity_defs']) == 1


def test_list_identity_defs_paginator_async(authz, identity_def):
    asyncio.run(authz.put_identity_def(identity_def))

    async def _collect():
        all_defs = []
        async for page in paginator_async(authz.list_identity_defs):
            all_defs.extend(page['identity_defs'])

        return all_defs

    all_defs = asyncio.run(_collect())
    assert len(all_defs) == 1


def test_delete_identity_def(authz, identity_def):
    asyncio.run(authz.put_identity_def(identity_def))
    result = asyncio.run(authz.delete_identity_def(identity_type="user"))
    assert result['has_failed'] is False
    get_result = asyncio.run(authz.get_identity_def(identity_type="user"))
    assert get_result['identity_def'] is None
    assert get_result['has_failed'] is True


def test_delete_identity_def_not_found(authz):
    result = asyncio.run(
        authz.delete_identity_def(identity_type="DOES_NOT_EXIST")
    )
    assert result['has_failed'] is False


def test_validate_identity_def_with_config(authz, identity_def):
    result = asyncio.run(
        authz.validate_identity_def(
            identity_def, config={
                "authzee": {
                    "raise_crits": True
                }
            }
        )
    )
    assert result['has_failed'] is False


def test_validate_resource_def_valid(authz, resource_def):
    result = asyncio.run(authz.validate_resource_def(resource_def))
    assert result['has_failed'] is False


def test_validate_resource_def_invalid(authz):
    result = asyncio.run(
        authz.validate_resource_def(
            {
                "resource_type": "X",
                "actions": [],
                "schema": "bad"
            }
        )
    )
    assert result['has_failed'] is True


def test_validate_resource_def_non_object_schema(authz):
    result = asyncio.run(
        authz.validate_resource_def(
            {
                "resource_type": "X",
                "actions": [],
                "schema": {
                    "type": "array"
                }
            }
        )
    )
    assert result['has_failed'] is True


def test_put_resource_def(authz, resource_def):
    result = asyncio.run(authz.put_resource_def(resource_def))
    assert result['has_failed'] is False


def test_put_resource_def_invalid(authz):
    result = asyncio.run(
        authz.put_resource_def(
            {
                "resource_type": "X",
                "actions": [],
                "schema": "bad"
            }
        )
    )
    assert result['has_failed'] is True


def test_get_resource_def(authz, resource_def):
    asyncio.run(authz.put_resource_def(resource_def))
    result = asyncio.run(authz.get_resource_def(resource_type="balloon"))
    assert result['has_failed'] is False
    assert result['resource_def']['resource_type'] == "balloon"


def test_get_resource_def_not_found(authz):
    result = asyncio.run(
        authz.get_resource_def(resource_type="DOES_NOT_EXIST")
    )
    assert result['resource_def'] is None
    assert result['has_failed'] is True


def test_list_resource_defs_empty(authz):
    result = asyncio.run(authz.list_resource_defs())
    assert result['has_failed'] is False
    assert result['resource_defs'] == []


def test_list_resource_defs_with_data(authz, resource_def):
    asyncio.run(authz.put_resource_def(resource_def))
    result = asyncio.run(authz.list_resource_defs())
    assert len(result['resource_defs']) == 1


def test_list_resource_defs_paginator_async(authz, resource_def):
    asyncio.run(authz.put_resource_def(resource_def))

    async def _collect():
        all_defs = []
        async for page in paginator_async(authz.list_resource_defs):
            all_defs.extend(page['resource_defs'])

        return all_defs

    all_defs = asyncio.run(_collect())
    assert len(all_defs) == 1


def test_delete_resource_def(authz, resource_def):
    asyncio.run(authz.put_resource_def(resource_def))
    result = asyncio.run(
        authz.delete_resource_def(resource_type="balloon")
    )
    assert result['has_failed'] is False
    get_result = asyncio.run(authz.get_resource_def(resource_type="balloon"))
    assert get_result['resource_def'] is None
    assert get_result['has_failed'] is True


def test_delete_resource_def_not_found(authz):
    result = asyncio.run(
        authz.delete_resource_def(resource_type="DOES_NOT_EXIST")
    )
    assert result['has_failed'] is False


def test_validate_resource_def_with_config(authz, resource_def):
    result = asyncio.run(
        authz.validate_resource_def(
            resource_def, config={
                "authzee": {
                    "raise_crits": True
                }
            }
        )
    )
    assert result['has_failed'] is False


def test_validate_grant_valid(authz, grant):
    result = asyncio.run(authz.validate_grant(grant))
    assert result['has_failed'] is False


def test_validate_grant_invalid(authz):
    result = asyncio.run(authz.validate_grant({"effect": "bad"}))
    assert result['has_failed'] is True


def test_enact_grant(authz, grant):
    result = asyncio.run(authz.enact(grant))
    assert result['has_failed'] is False


def test_enact_invalid_grant(authz):
    result = asyncio.run(authz.enact({"effect": "bad"}))
    assert result['has_failed'] is True


def test_get_grant(authz, grant):
    asyncio.run(authz.enact(grant))
    result = asyncio.run(authz.get_grant(grant_uuid=grant['grant_uuid']))
    assert result['has_failed'] is False
    assert result['grant']['grant_uuid'] == grant['grant_uuid']


def test_get_grant_not_found(authz):
    result = asyncio.run(authz.get_grant(grant_uuid="nonexistent-uuid"))
    assert result['grant'] is None
    assert result['has_failed'] is True


def test_list_grants_empty(authz):
    result = asyncio.run(authz.list_grants())
    assert result['has_failed'] is False
    assert result['grants'] == []


def test_list_grants_with_data(authz, grant):
    asyncio.run(authz.enact(grant))
    result = asyncio.run(authz.list_grants())
    assert len(result['grants']) == 1


def test_list_grants_filter_by_effect(authz, grant, deny_grant):
    asyncio.run(authz.enact(grant))
    asyncio.run(authz.enact(deny_grant))
    allow_result = asyncio.run(authz.list_grants(effect="allow"))
    deny_result = asyncio.run(authz.list_grants(effect="deny"))
    assert all(g['effect'] == "allow" for g in allow_result['grants'])
    assert all(g['effect'] == "deny" for g in deny_result['grants'])


def test_list_grants_filter_by_action(authz, grant, deny_grant):
    asyncio.run(authz.enact(grant))
    asyncio.run(authz.enact(deny_grant))
    result = asyncio.run(authz.list_grants(action="balloon:pop"))
    assert all("balloon:pop" in g['actions'] for g in result['grants'])


def test_list_grants_paginator_async(authz, grant):
    asyncio.run(authz.enact(grant))

    async def _collect():
        all_grants = []
        async for page in paginator_async(authz.list_grants):
            all_grants.extend(page['grants'])

        return all_grants

    all_grants = asyncio.run(_collect())
    assert len(all_grants) == 1


def test_list_grant_refs(authz, grant):
    asyncio.run(authz.enact(grant))
    result = asyncio.run(authz.list_grant_refs())
    assert result['has_failed'] is False
    assert "page_refs" in result


def test_list_grant_refs_filter_by_effect(authz, grant, deny_grant):
    asyncio.run(authz.enact(grant))
    asyncio.run(authz.enact(deny_grant))
    result = asyncio.run(authz.list_grant_refs(effect="allow"))
    assert result['has_failed'] is False


def test_list_grant_refs_paginator_async(authz, grant):
    asyncio.run(authz.enact(grant))

    async def _collect():
        all_refs = []
        async for page in paginator_async(authz.list_grant_refs):
            all_refs.extend(page['page_refs'])

        return all_refs

    all_refs = asyncio.run(_collect())
    assert isinstance(all_refs, list)


def test_repeal_grant(authz, grant):
    asyncio.run(authz.enact(grant))
    result = asyncio.run(
        authz.repeal(grant_uuid=grant['grant_uuid'], purge=False)
    )
    assert result['has_failed'] is False
    get_result = asyncio.run(authz.get_grant(grant_uuid=grant['grant_uuid']))
    assert get_result['grant'] is None
    assert get_result['has_failed'] is True


def test_repeal_grant_purge(authz, grant):
    asyncio.run(authz.enact(grant))
    result = asyncio.run(
        authz.repeal(grant_uuid=grant['grant_uuid'], purge=True)
    )
    assert result['has_failed'] is False


def test_repeal_grant_not_found(authz):
    # DictStorage repeal returns has_failed=False even when not found
    result = asyncio.run(
        authz.repeal(grant_uuid="nonexistent-uuid", purge=False)
    )
    assert result['has_failed'] is False


def test_validate_grant_with_config(authz, grant):
    result = asyncio.run(
        authz.validate_grant(
            grant,
            config={
                "authzee": {
                    "raise_crits": True
                }
            }
        )
    )
    assert result['has_failed'] is False


def test_enact_with_config(authz, grant):
    result = asyncio.run(
        authz.enact(
            grant,
            config={
                "authzee": {
                    "raise_crits": False
                }
            }
        )
    )
    assert result['has_failed'] is False


def test_cleanup_latches(authz):
    result = asyncio.run(
        authz.cleanup_latches(before=datetime.datetime(2030, 1, 1))
    )
    assert result['has_failed'] is False


def test_cleanup_latches_with_config(authz):
    result = asyncio.run(
        authz.cleanup_latches(
            before=datetime.datetime(2030, 1, 1),
            config={
                "authzee": {
                    "raise_crits": True
                }
            }
        )
    )
    assert result['has_failed'] is False


def test_authorize_allowed(seeded_authz, auth_request):
    result = asyncio.run(seeded_authz.authorize(request=auth_request))
    assert result['is_authorized'] is True
    assert result['has_failed'] is False
    assert result['grant'] is not None
    assert isinstance(result['message'], str)


def test_authorize_denied_no_matching_grant(seeded_authz, auth_request):
    request = {
        **auth_request,
        "action": "balloon:pop"
    }
    result = asyncio.run(seeded_authz.authorize(request=request))
    assert result['is_authorized'] is False


def test_authorize_denied_by_deny_grant(seeded_authz, deny_grant):
    asyncio.run(seeded_authz.enact(deny_grant))
    request = {
        "identities": {
            "user": [
                {
                    "username": "intern_1",
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
        "evaluation_handler": "grant",
        "context_type": "NONE",
        "context": {}
    }
    result = asyncio.run(seeded_authz.authorize(request=request))
    assert result['is_authorized'] is False


def test_authorize_with_config(seeded_authz, auth_request):
    result = asyncio.run(
        seeded_authz.authorize(
            request=auth_request, config={
                "authzee": {
                    "raise_crits": True
                }
            }
        )
    )
    assert result['is_authorized'] is True


def test_audit(seeded_authz, auth_request):
    result = asyncio.run(seeded_authz.audit(request=auth_request))
    assert result['has_failed'] is False
    assert "grants" in result
    assert "results" in result
    assert len(result['grants']) == len(result['results'])


def test_audit_with_applicable_grant(seeded_authz, auth_request):
    result = asyncio.run(seeded_authz.audit(request=auth_request))
    assert any(r['is_applicable'] for r in result['results'])


def test_audit_paginator_async(seeded_authz, auth_request):
    async def _collect():
        all_grants = []
        all_results = []
        async for page in paginator_async(seeded_authz.audit, request=auth_request):
            all_grants.extend(page['grants'])
            all_results.extend(page['results'])

        return all_grants, all_results

    all_grants, all_results = asyncio.run(_collect())
    assert len(all_grants) >= 1


def test_audit_with_config(seeded_authz, auth_request):
    result = asyncio.run(
        seeded_authz.audit(
            request=auth_request, config={
                "authzee": {
                    "raise_crits": True
                }
            }
        )
    )
    assert result['has_failed'] is False


def test_batch_authorize(seeded_authz, batch_request):
    result = asyncio.run(
        seeded_authz.batch_authorize(batch_request=batch_request)
    )
    assert result['has_failed'] is False
    assert "batch_results" in result
    assert len(result['batch_results']) == 2


def test_batch_authorize_all_authorized(seeded_authz, batch_request):
    result = asyncio.run(
        seeded_authz.batch_authorize(batch_request=batch_request)
    )
    for item in result['batch_results']:
        assert item['is_authorized'] is True


def test_batch_authorize_with_config(seeded_authz, batch_request):
    result = asyncio.run(
        seeded_authz.batch_authorize(
            batch_request=batch_request, config={
                "authzee": {
                    "raise_crits": True
                }
            }
        )
    )
    assert result['has_failed'] is False


def test_batch_audit(seeded_authz, batch_request):
    result = asyncio.run(
        seeded_authz.batch_audit(batch_request=batch_request)
    )
    assert result['has_failed'] is False
    assert "grants" in result
    assert "batch_results" in result
    assert len(result['batch_results']) == 2


def test_batch_audit_paginator_async(seeded_authz, batch_request):
    async def _collect():
        all_grants = []
        all_batch = []
        async for page in paginator_async(
            seeded_authz.batch_audit,
            batch_request=batch_request
        ):
            all_grants.extend(page['grants'])
            all_batch.extend(page['batch_results'])

        return all_grants, all_batch

    all_grants, all_batch = asyncio.run(_collect())
    assert len(all_grants) >= 1


def test_batch_audit_with_config(seeded_authz, batch_request):
    result = asyncio.run(
        seeded_authz.batch_audit(
            batch_request=batch_request, config={
                "authzee": {
                    "raise_crits": True
                }
            }
        )
    )
    assert result['has_failed'] is False


def test_instance_level_config():
    """Test that config can be set at the instance level."""
    storage_dict = {}
    authz = AuthzeeAsync(
        execute=jmespath_execute,
        compute_type=InProcessCompute,
        compute_kwargs={},
        storage_type=DictStorage,
        storage_kwargs={
            "storage_dict": storage_dict
        },
        config={
            "authzee": {
                "raise_crits": False
            }
        }
    )
    result = asyncio.run(authz.construct())
    assert result['has_failed'] is False
    result = asyncio.run(authz.start())
    assert result['has_failed'] is False


def test_raise_crits_config_raises_on_invalid_def():
    """Test that raise_crits=True raises DefinitionError on invalid validation."""
    storage_dict = {}
    authz = AuthzeeAsync(
        execute=jmespath_execute,
        compute_type=InProcessCompute,
        compute_kwargs={},
        storage_type=DictStorage,
        storage_kwargs={
            "storage_dict": storage_dict
        },
        config={
            "authzee": {
                "raise_crits": True
            }
        }
    )
    asyncio.run(authz.construct())
    asyncio.run(authz.start())
    with pytest.raises(exceptions.DefinitionError):
        asyncio.run(
            authz.validate_context_def(
                {
                    "context_type": "BAD",
                    "schema": "not_a_dict"
                }
            )
        )


def test_raise_crits_override_at_call_level(authz):
    """Test that config override at method level takes precedence."""
    with pytest.raises(exceptions.DefinitionError):
        asyncio.run(
            authz.validate_context_def(
                {
                    "context_type": "BAD",
                    "schema": "not_a_dict"
                },
                config={
                    "authzee": {
                        "raise_crits": True
                    }
                }
            )
        )


def test_raise_crits_false_does_not_raise(authz):
    """Test that raise_crits=False returns error result without raising."""
    result = asyncio.run(
        authz.validate_context_def(
            {
                "context_type": "BAD",
                "schema": "not_a_dict"
            },
            config={
                "authzee": {
                    "raise_crits": False
                }
            }
        )
    )
    assert result['has_failed'] is True


def test_raise_crits_grant_error():
    """Test that raise_crits raises GrantError on invalid grant validation."""
    storage_dict = {}
    authz = AuthzeeAsync(
        execute=jmespath_execute,
        compute_type=InProcessCompute,
        compute_kwargs={},
        storage_type=DictStorage,
        storage_kwargs={
            "storage_dict": storage_dict
        },
        config={
            "authzee": {
                "raise_crits": True
            }
        }
    )
    asyncio.run(authz.construct())
    asyncio.run(authz.start())
    with pytest.raises(exceptions.GrantError):
        asyncio.run(authz.validate_grant({"effect": "bad"}))


def test_compute_storage_kwargs_override():
    """Test that compute_storage_kwargs is accepted."""
    storage_dict = {}
    authz = AuthzeeAsync(
        execute=jmespath_execute,
        compute_type=InProcessCompute,
        compute_kwargs={},
        storage_type=DictStorage,
        storage_kwargs={
            "storage_dict": storage_dict
        },
        compute_storage_kwargs={
            "storage_dict": storage_dict
        }
    )
    asyncio.run(authz.construct())
    result = asyncio.run(authz.start())
    assert result['has_failed'] is False


def test_put_context_def_overwrite(authz, context_def):
    """Putting the same context_type twice should succeed (upsert)."""
    asyncio.run(authz.put_context_def(context_def))
    updated_def = {
        **context_def,
        "schema": {
            "type": "object"
        }
    }
    result = asyncio.run(authz.put_context_def(updated_def))
    assert result['has_failed'] is False


def test_put_identity_def_overwrite(authz, identity_def):
    asyncio.run(authz.put_identity_def(identity_def))
    updated_def = {
        **identity_def,
        "schema": {
            "type": "object",
            "properties": {
                "username": {
                    "type": "string"
                }
            }
        }
    }
    result = asyncio.run(authz.put_identity_def(updated_def))
    assert result['has_failed'] is False


def test_put_resource_def_overwrite(authz, resource_def):
    asyncio.run(authz.put_resource_def(resource_def))
    updated_def = {
        **resource_def,
        "actions": [
            "balloon:read",
            "balloon:inflate",
            "balloon:pop",
            "balloon:tie"
        ]
    }
    result = asyncio.run(authz.put_resource_def(updated_def))
    assert result['has_failed'] is False


def test_multiple_grants(authz, grant, deny_grant):
    asyncio.run(authz.enact(grant))
    asyncio.run(authz.enact(deny_grant))
    result = asyncio.run(authz.list_grants())
    assert len(result['grants']) == 2


def test_multiple_context_defs(authz):
    asyncio.run(
        authz.put_context_def(
            {
                "context_type": "A",
                "schema": {
                    "type": "object"
                }
            }
        )
    )
    asyncio.run(
        authz.put_context_def(
            {
                "context_type": "B",
                "schema": {
                    "type": "object"
                }
            }
        )
    )
    result = asyncio.run(authz.list_context_defs())
    assert len(result['context_defs']) == 2


def test_multiple_identity_defs(authz):
    asyncio.run(
        authz.put_identity_def(
            {
                "identity_type": "A",
                "schema": {
                    "type": "object"
                }
            }
        )
    )
    asyncio.run(
        authz.put_identity_def(
            {
                "identity_type": "B",
                "schema": {
                    "type": "object"
                }
            }
        )
    )
    result = asyncio.run(authz.list_identity_defs())
    assert len(result['identity_defs']) == 2


def test_multiple_resource_defs(authz):
    asyncio.run(
        authz.put_resource_def(
            {
                "resource_type": "A",
                "actions": [
                    "A:read"
                ],
                "schema": {
                    "type": "object"
                }
            }
        )
    )
    asyncio.run(
        authz.put_resource_def(
            {
                "resource_type": "B",
                "actions": [
                    "B:read"
                ],
                "schema": {
                    "type": "object"
                }
            }
        )
    )
    result = asyncio.run(authz.list_resource_defs())
    assert len(result['resource_defs']) == 2


def test_raise_result_raises_on_critical_definition_error(storage_dict):
    """Test that _raise_result raises DefinitionError when raise_crits=True."""
    a = AuthzeeAsync(
        execute=jmespath_execute,
        compute_type=InProcessCompute,
        compute_kwargs={},
        storage_type=DictStorage,
        storage_kwargs={
            "storage_dict": storage_dict
        },
        config={
            "authzee": {
                "raise_crits": True
            }
        }
    )
    asyncio.run(a.construct())
    asyncio.run(a.start())
    # Try to put an invalid context def - should raise
    with pytest.raises(exceptions.DefinitionError):
        asyncio.run(a.put_context_def({"bad": "data"}))


def test_raise_result_raises_on_critical_resource_not_found(storage_dict):
    """Test that getting a non-existent resource raises ResourceNotFoundError."""
    a = AuthzeeAsync(
        execute=jmespath_execute,
        compute_type=InProcessCompute,
        compute_kwargs={},
        storage_type=DictStorage,
        storage_kwargs={
            "storage_dict": storage_dict
        },
        config={
            "authzee": {
                "raise_crits": True
            }
        }
    )
    asyncio.run(a.construct())
    asyncio.run(a.start())
    with pytest.raises(exceptions.ResourceNotFoundError):
        asyncio.run(a.get_context_def("NONEXISTENT"))


def test_raise_result_with_critical_errors_key(storage_dict):
    """Test _raise_result when result has 'critical_errors' key (authorize path)."""
    a = AuthzeeAsync(
        execute=jmespath_execute,
        compute_type=InProcessCompute,
        compute_kwargs={},
        storage_type=DictStorage,
        storage_kwargs={
            "storage_dict": storage_dict
        },
        config={
            "authzee": {
                "raise_crits": True
            }
        }
    )
    asyncio.run(a.construct())
    asyncio.run(a.start())
    asyncio.run(
        a.put_context_def(
            {
                "context_type": "NONE",
                "schema": {
                    "type": "object",
                    "additionalProperties": False
                }
            }
        )
    )
    asyncio.run(
        a.put_identity_def(
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
            }
        )
    )
    asyncio.run(
        a.put_resource_def(
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
            }
        )
    )
    # Enact a grant with bad query + critical handler
    asyncio.run(
        a.enact(
            {
                "grant_uuid": str(uuid4()),
                "name": "Bad Grant",
                "description": "",
                "tags": {},
                "effect": "allow",
                "actions": [
                    "read"
                ],
                "query": "bad.[invalid",
                "evaluation_handler": "critical",
                "equality": True,
                "data": {}
            }
        )
    )
    with pytest.raises(exceptions.EvaluationError):
        asyncio.run(
            a.authorize(
                {
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
                    "evaluation_handler": "grant",
                    "context_type": "NONE",
                    "context": {}
                }
            )
        )


def test_combine_errors_called_during_start(storage_dict):
    """Start calls _combine_errors internally with compute and storage results."""
    a = AuthzeeAsync(
        execute=jmespath_execute,
        compute_type=InProcessCompute,
        compute_kwargs={},
        storage_type=DictStorage,
        storage_kwargs={
            "storage_dict": storage_dict
        }
    )
    asyncio.run(a.construct())
    result = asyncio.run(a.start())
    assert result['has_failed'] is False


def test_authorize_validation_failure(seeded_authz):
    """authorize with an invalid request returns failure without raising."""
    result = asyncio.run(seeded_authz.authorize({"bad": "request"}))
    assert result['has_failed'] is True
    assert result['is_authorized'] is False
    assert "critical_errors" in result


def test_authorize_validation_failure_raises(storage_dict):
    """authorize with invalid request and raise_crits=True raises an exception."""
    a = AuthzeeAsync(
        execute=jmespath_execute,
        compute_type=InProcessCompute,
        compute_kwargs={},
        storage_type=DictStorage,
        storage_kwargs={
            "storage_dict": storage_dict
        },
        config={
            "authzee": {
                "raise_crits": True
            }
        }
    )
    asyncio.run(a.construct())
    asyncio.run(a.start())
    with pytest.raises(Exception):
        asyncio.run(a.authorize({"bad": "request"}))


def test_audit_validation_failure(seeded_authz):
    """audit with an invalid request returns failure."""
    result = asyncio.run(seeded_authz.audit({"bad": "request"}))
    assert result['has_failed'] is True
    assert result['grants'] == []
    assert result['results'] == []


def test_audit_validation_failure_raises(storage_dict):
    """audit with invalid request and raise_crits=True raises an exception."""
    a = AuthzeeAsync(
        execute=jmespath_execute,
        compute_type=InProcessCompute,
        compute_kwargs={},
        storage_type=DictStorage,
        storage_kwargs={
            "storage_dict": storage_dict
        },
        config={
            "authzee": {
                "raise_crits": True
            }
        }
    )
    asyncio.run(a.construct())
    asyncio.run(a.start())
    with pytest.raises(Exception):
        asyncio.run(a.audit({"bad": "request"}))


def test_batch_audit_validation_failure(seeded_authz):
    """batch_audit with an invalid request returns failure."""
    result = asyncio.run(seeded_authz.batch_audit({"bad": "request"}))
    assert result['has_failed'] is True
    assert result['grants'] == []
    assert result['batch_results'] == []


def test_batch_audit_validation_failure_raises(storage_dict):
    """batch_audit with invalid request and raise_crits=True raises an exception."""
    a = AuthzeeAsync(
        execute=jmespath_execute,
        compute_type=InProcessCompute,
        compute_kwargs={},
        storage_type=DictStorage,
        storage_kwargs={
            "storage_dict": storage_dict
        },
        config={
            "authzee": {
                "raise_crits": True
            }
        }
    )
    asyncio.run(a.construct())
    asyncio.run(a.start())
    with pytest.raises(Exception):
        asyncio.run(a.batch_audit({"bad": "request"}))


def test_batch_authorize_validation_failure(seeded_authz):
    """batch_authorize with an invalid request returns failure."""
    result = asyncio.run(
        seeded_authz.batch_authorize(
            {
                "bad": "request"
            }
        )
    )
    assert result['has_failed'] is True
    assert result['batch_results'] == []


def test_batch_authorize_validation_failure_raises(storage_dict):
    """batch_authorize with invalid request and raise_crits=True raises an exception."""
    a = AuthzeeAsync(
        execute=jmespath_execute,
        compute_type=InProcessCompute,
        compute_kwargs={},
        storage_type=DictStorage,
        storage_kwargs={
            "storage_dict": storage_dict
        },
        config={
            "authzee": {
                "raise_crits": True
            }
        }
    )
    asyncio.run(a.construct())
    asyncio.run(a.start())
    with pytest.raises(Exception):
        asyncio.run(a.batch_authorize({"bad": "request"}))


def test_compute_storage_kwargs_override(storage_dict):
    """Test that compute_storage_kwargs overrides storage_kwargs for compute."""
    a = AuthzeeAsync(
        execute=jmespath_execute,
        compute_type=InProcessCompute,
        compute_kwargs={},
        storage_type=DictStorage,
        storage_kwargs={
            "storage_dict": storage_dict
        },
        compute_storage_kwargs={
            "storage_dict": storage_dict
        }
    )
    asyncio.run(a.construct())
    result = asyncio.run(a.start())
    assert result['has_failed'] is False


def test_validate_batch_request_valid(seeded_authz):
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
        "evaluation_handler": "grant",
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
    result = asyncio.run(
        seeded_authz.validate_batch_request(batch_request)
    )
    assert result['has_failed'] is False


def test_validate_batch_request_invalid(seeded_authz):
    result = asyncio.run(
        seeded_authz.validate_batch_request(
            {
                "bad": "data"
            }
        )
    )
    assert result['has_failed'] is True


def test_validate_request_valid(seeded_authz):
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
        "evaluation_handler": "grant",
        "context_type": "NONE",
        "context": {}
    }
    result = asyncio.run(seeded_authz.validate_request(request))
    assert result['has_failed'] is False


def test_validate_request_invalid(seeded_authz):
    result = asyncio.run(seeded_authz.validate_request({"bad": "data"}))
    assert result['has_failed'] is True


def test_combine_errors_method_via_shutdown(storage_dict):
    """The _combine_errors method is called during shutdown (combine compute + storage results)."""
    a = AuthzeeAsync(
        execute=jmespath_execute,
        compute_type=InProcessCompute,
        compute_kwargs={},
        storage_type=DictStorage,
        storage_kwargs={
            "storage_dict": storage_dict
        }
    )
    asyncio.run(a.construct())
    asyncio.run(a.start())
    # Calling shutdown exercises _combine_errors internally via core.combine_errors
    result = asyncio.run(a.shutdown())
    assert result['has_failed'] is False


def test_combine_errors_instance_method_directly(storage_dict):
    """Directly test the _combine_errors private method for coverage."""
    a = AuthzeeAsync(
        execute=jmespath_execute,
        compute_type=InProcessCompute,
        compute_kwargs={},
        storage_type=DictStorage,
        storage_kwargs={
            "storage_dict": storage_dict
        }
    )
    result = {
        "has_failed": False,
        "errors": {
            "a": [
                {
                    "is_critical": False,
                    "message": "x"
                }
            ]
        }
    }
    new_result = {
        "has_failed": True,
        "errors": {
            "b": [
                {
                    "is_critical": True,
                    "message": "y"
                }
            ]
        }
    }
    a._combine_errors(result, new_result)
    assert result['has_failed'] is True
    assert "a" in result['errors']
    assert "b" in result['errors']


def test_combine_errors_instance_method_merges_existing_keys(storage_dict):
    """Test _combine_errors merging into existing error keys."""
    a = AuthzeeAsync(
        execute=jmespath_execute,
        compute_type=InProcessCompute,
        compute_kwargs={},
        storage_type=DictStorage,
        storage_kwargs={
            "storage_dict": storage_dict
        }
    )
    result = {
        "has_failed": False,
        "errors": {
            "a": [
                {
                    "is_critical": False,
                    "message": "1"
                }
            ]
        }
    }
    new_result = {
        "has_failed": False,
        "errors": {
            "a": [
                {
                    "is_critical": False,
                    "message": "2"
                }
            ]
        }
    }
    a._combine_errors(result, new_result)
    assert len(result['errors']['a']) == 2


def test_locality_incompatibility_warning(storage_dict):
    """Test that an incompatible locality produces an error in the start result."""
    from authzee.compute.in_process_compute import InProcessCompute as _IPC
    from authzee.module_locality import ModuleLocality

    class NetworkCompute(_IPC):
        """A compute module that reports NETWORK locality."""


        async def start(
            self,
            execute,
            storage_type,
            storage_kwargs,
            config
        ):
            result = await super().start(execute, storage_type, storage_kwargs, config)
            self.locality = ModuleLocality.NETWORK

            return result

    a = AuthzeeAsync(
        execute=jmespath_execute,
        compute_type=NetworkCompute,
        compute_kwargs={},
        storage_type=DictStorage,
        storage_kwargs={
            "storage_dict": storage_dict
        }
    )
    asyncio.run(a.construct())
    result = asyncio.run(a.start())
    # DictStorage has PROCESS locality, NetworkCompute has NETWORK locality
    # PROCESS storage is not compatible with NETWORK compute (only NETWORK storage is)
    assert "locality_incompatibility" in result.get("errors", {})
