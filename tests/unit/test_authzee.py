"""Comprehensive unit tests for the Authzee (sync) class.

Tests use DictStorage and InProcessCompute as the storage/compute modules.
Black box testing - only uses public API methods of Authzee.
"""

import datetime
from uuid import uuid4

import pytest

from authzee import (
    Authzee,
    DictStorage,
    InProcessCompute,
    authzee_specification_version,
    exceptions,
    jmespath_execute,
    paginator
)


@pytest.fixture
def storage_dict():
    return {}


@pytest.fixture
def authz(storage_dict):
    """Create a fully initialized Authzee instance with raise_crits=False for black-box testing."""
    a = Authzee(
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
    a.construct()
    a.start()

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
    """An Authzee instance with definitions and a grant already stored."""
    authz.put_context_def(context_def)
    authz.put_identity_def(identity_def)
    authz.put_resource_def(resource_def)
    authz.enact(grant)

    return authz


def test_authzee_specification_version_is_string():
    assert isinstance(authzee_specification_version, str)
    assert len(authzee_specification_version) > 0


def test_construct(storage_dict):
    authz = Authzee(
        execute=jmespath_execute,
        compute_type=InProcessCompute,
        compute_kwargs={},
        storage_type=DictStorage,
        storage_kwargs={
            "storage_dict": storage_dict
        }
    )
    result = authz.construct()
    assert result['has_failed'] is False


def test_start(storage_dict):
    authz = Authzee(
        execute=jmespath_execute,
        compute_type=InProcessCompute,
        compute_kwargs={},
        storage_type=DictStorage,
        storage_kwargs={
            "storage_dict": storage_dict
        }
    )
    authz.construct()
    result = authz.start()
    assert result['has_failed'] is False


def test_shutdown(authz):
    result = authz.shutdown()
    assert result['has_failed'] is False


def test_destroy(storage_dict):
    authz = Authzee(
        execute=jmespath_execute,
        compute_type=InProcessCompute,
        compute_kwargs={},
        storage_type=DictStorage,
        storage_kwargs={
            "storage_dict": storage_dict
        }
    )
    authz.construct()
    authz.start()
    result = authz.destroy()
    assert result['has_failed'] is False


def test_construct_with_config(storage_dict):
    authz = Authzee(
        execute=jmespath_execute,
        compute_type=InProcessCompute,
        compute_kwargs={},
        storage_type=DictStorage,
        storage_kwargs={
            "storage_dict": storage_dict
        }
    )
    result = authz.construct(
        config={
            "authzee": {
                "raise_crits": True
            }
        }
    )
    assert result['has_failed'] is False


def test_start_with_config(storage_dict):
    authz = Authzee(
        execute=jmespath_execute,
        compute_type=InProcessCompute,
        compute_kwargs={},
        storage_type=DictStorage,
        storage_kwargs={
            "storage_dict": storage_dict
        }
    )
    authz.construct()
    result = authz.start(
        config={
            "authzee": {
                "raise_crits": True
            }
        }
    )
    assert result['has_failed'] is False


def test_shutdown_with_config(authz):
    result = authz.shutdown(
        config={
            "authzee": {
                "raise_crits": True
            }
        }
    )
    assert result['has_failed'] is False


def test_destroy_with_config(storage_dict):
    authz = Authzee(
        execute=jmespath_execute,
        compute_type=InProcessCompute,
        compute_kwargs={},
        storage_type=DictStorage,
        storage_kwargs={
            "storage_dict": storage_dict
        }
    )
    authz.construct()
    authz.start()
    result = authz.destroy(
        config={
            "authzee": {
                "raise_crits": True
            }
        }
    )
    assert result['has_failed'] is False


def test_validate_context_def_valid(authz, context_def):
    result = authz.validate_context_def(context_def)
    assert result['has_failed'] is False


def test_validate_context_def_invalid(authz):
    result = authz.validate_context_def(
        {
            "context_type": "BAD",
            "schema": "not_a_dict"
        }
    )
    assert result['has_failed'] is True


def test_validate_context_def_non_object_schema(authz):
    result = authz.validate_context_def(
        {
            "context_type": "BAD",
            "schema": {
                "type": "array"
            }
        }
    )
    assert result['has_failed'] is True


def test_put_context_def(authz, context_def):
    result = authz.put_context_def(context_def)
    assert result['has_failed'] is False


def test_put_context_def_invalid(authz):
    result = authz.put_context_def(
        {
            "context_type": "BAD",
            "schema": "nope"
        }
    )
    assert result['has_failed'] is True


def test_get_context_def(authz, context_def):
    authz.put_context_def(context_def)
    result = authz.get_context_def(context_type="NONE")
    assert result['has_failed'] is False
    assert result['context_def']['context_type'] == "NONE"


def test_get_context_def_not_found(authz):
    result = authz.get_context_def(context_type="DOES_NOT_EXIST")
    assert result['context_def'] is None
    assert result['has_failed'] is True


def test_list_context_defs_empty(authz):
    result = authz.list_context_defs()
    assert result['has_failed'] is False
    assert result['context_defs'] == []
    assert result['next_page_ref'] is None


def test_list_context_defs_with_data(authz, context_def):
    authz.put_context_def(context_def)
    result = authz.list_context_defs()
    assert len(result['context_defs']) == 1
    assert result['context_defs'][0]['context_type'] == "NONE"


def test_list_context_defs_paginator(authz, context_def):
    authz.put_context_def(context_def)
    all_defs = []
    for page in paginator(authz.list_context_defs):
        all_defs.extend(page['context_defs'])

    assert len(all_defs) == 1


def test_delete_context_def(authz, context_def):
    authz.put_context_def(context_def)
    result = authz.delete_context_def(context_type="NONE")
    assert result['has_failed'] is False
    # Verify it was deleted - get returns has_failed=True with resource_not_found
    get_result = authz.get_context_def(context_type="NONE")
    assert get_result['context_def'] is None
    assert get_result['has_failed'] is True


def test_delete_context_def_not_found(authz):
    result = authz.delete_context_def(context_type="DOES_NOT_EXIST")
    assert result['has_failed'] is False


def test_validate_context_def_with_config(authz, context_def):
    result = authz.validate_context_def(
        context_def, config={
            "authzee": {
                "raise_crits": True
            }
        }
    )
    assert result['has_failed'] is False


def test_put_context_def_with_config(authz, context_def):
    result = authz.put_context_def(
        context_def, config={
            "authzee": {
                "raise_crits": False
            }
        }
    )
    assert result['has_failed'] is False


def test_validate_identity_def_valid(authz, identity_def):
    result = authz.validate_identity_def(identity_def)
    assert result['has_failed'] is False


def test_validate_identity_def_invalid(authz):
    result = authz.validate_identity_def(
        {
            "identity_type": "BAD",
            "schema": "not_a_dict"
        }
    )
    assert result['has_failed'] is True


def test_validate_identity_def_non_object_schema(authz):
    result = authz.validate_identity_def(
        {
            "identity_type": "BAD",
            "schema": {
                "type": "string"
            }
        }
    )
    assert result['has_failed'] is True


def test_put_identity_def(authz, identity_def):
    result = authz.put_identity_def(identity_def)
    assert result['has_failed'] is False


def test_put_identity_def_invalid(authz):
    result = authz.put_identity_def(
        {
            "identity_type": "X",
            "schema": 123
        }
    )
    assert result['has_failed'] is True


def test_get_identity_def(authz, identity_def):
    authz.put_identity_def(identity_def)
    result = authz.get_identity_def(identity_type="user")
    assert result['has_failed'] is False
    assert result['identity_def']['identity_type'] == "user"


def test_get_identity_def_not_found(authz):
    result = authz.get_identity_def(identity_type="DOES_NOT_EXIST")
    assert result['identity_def'] is None
    assert result['has_failed'] is True


def test_list_identity_defs_empty(authz):
    result = authz.list_identity_defs()
    assert result['has_failed'] is False
    assert result['identity_defs'] == []


def test_list_identity_defs_with_data(authz, identity_def):
    authz.put_identity_def(identity_def)
    result = authz.list_identity_defs()
    assert len(result['identity_defs']) == 1


def test_list_identity_defs_paginator(authz, identity_def):
    authz.put_identity_def(identity_def)
    all_defs = []
    for page in paginator(authz.list_identity_defs):
        all_defs.extend(page['identity_defs'])

    assert len(all_defs) == 1


def test_delete_identity_def(authz, identity_def):
    authz.put_identity_def(identity_def)
    result = authz.delete_identity_def(identity_type="user")
    assert result['has_failed'] is False
    get_result = authz.get_identity_def(identity_type="user")
    assert get_result['identity_def'] is None
    assert get_result['has_failed'] is True


def test_delete_identity_def_not_found(authz):
    result = authz.delete_identity_def(identity_type="DOES_NOT_EXIST")
    assert result['has_failed'] is False


def test_validate_identity_def_with_config(authz, identity_def):
    result = authz.validate_identity_def(
        identity_def, config={
            "authzee": {
                "raise_crits": True
            }
        }
    )
    assert result['has_failed'] is False


def test_validate_resource_def_valid(authz, resource_def):
    result = authz.validate_resource_def(resource_def)
    assert result['has_failed'] is False


def test_validate_resource_def_invalid(authz):
    result = authz.validate_resource_def(
        {
            "resource_type": "X",
            "actions": [],
            "schema": "bad"
        }
    )
    assert result['has_failed'] is True


def test_validate_resource_def_non_object_schema(authz):
    result = authz.validate_resource_def(
        {
            "resource_type": "X",
            "actions": [],
            "schema": {
                "type": "array"
            }
        }
    )
    assert result['has_failed'] is True


def test_put_resource_def(authz, resource_def):
    result = authz.put_resource_def(resource_def)
    assert result['has_failed'] is False


def test_put_resource_def_invalid(authz):
    result = authz.put_resource_def(
        {
            "resource_type": "X",
            "actions": [],
            "schema": "bad"
        }
    )
    assert result['has_failed'] is True


def test_get_resource_def(authz, resource_def):
    authz.put_resource_def(resource_def)
    result = authz.get_resource_def(resource_type="balloon")
    assert result['has_failed'] is False
    assert result['resource_def']['resource_type'] == "balloon"


def test_get_resource_def_not_found(authz):
    result = authz.get_resource_def(resource_type="DOES_NOT_EXIST")
    assert result['resource_def'] is None
    assert result['has_failed'] is True


def test_list_resource_defs_empty(authz):
    result = authz.list_resource_defs()
    assert result['has_failed'] is False
    assert result['resource_defs'] == []


def test_list_resource_defs_with_data(authz, resource_def):
    authz.put_resource_def(resource_def)
    result = authz.list_resource_defs()
    assert len(result['resource_defs']) == 1


def test_list_resource_defs_paginator(authz, resource_def):
    authz.put_resource_def(resource_def)
    all_defs = []
    for page in paginator(authz.list_resource_defs):
        all_defs.extend(page['resource_defs'])

    assert len(all_defs) == 1


def test_delete_resource_def(authz, resource_def):
    authz.put_resource_def(resource_def)
    result = authz.delete_resource_def(resource_type="balloon")
    assert result['has_failed'] is False
    get_result = authz.get_resource_def(resource_type="balloon")
    assert get_result['resource_def'] is None
    assert get_result['has_failed'] is True


def test_delete_resource_def_not_found(authz):
    result = authz.delete_resource_def(resource_type="DOES_NOT_EXIST")
    assert result['has_failed'] is False


def test_validate_resource_def_with_config(authz, resource_def):
    result = authz.validate_resource_def(
        resource_def, config={
            "authzee": {
                "raise_crits": True
            }
        }
    )
    assert result['has_failed'] is False


def test_validate_grant_valid(authz, grant):
    result = authz.validate_grant(grant)
    assert result['has_failed'] is False


def test_validate_grant_invalid(authz):
    result = authz.validate_grant(
        {
            "effect": "bad"
        }
    )
    assert result['has_failed'] is True


def test_enact_grant(authz, grant):
    result = authz.enact(grant)
    assert result['has_failed'] is False


def test_enact_invalid_grant(authz):
    result = authz.enact(
        {
            "effect": "bad"
        }
    )
    assert result['has_failed'] is True


def test_get_grant(authz, grant):
    authz.enact(grant)
    result = authz.get_grant(grant_uuid=grant['grant_uuid'])
    assert result['has_failed'] is False
    assert result['grant']['grant_uuid'] == grant['grant_uuid']


def test_get_grant_not_found(authz):
    result = authz.get_grant(grant_uuid="nonexistent-uuid")
    assert result['grant'] is None
    assert result['has_failed'] is True


def test_list_grants_empty(authz):
    result = authz.list_grants()
    assert result['has_failed'] is False
    assert result['grants'] == []


def test_list_grants_with_data(authz, grant):
    authz.enact(grant)
    result = authz.list_grants()
    assert len(result['grants']) == 1


def test_list_grants_filter_by_effect(authz, grant, deny_grant):
    authz.enact(grant)
    authz.enact(deny_grant)
    allow_result = authz.list_grants(effect="allow")
    deny_result = authz.list_grants(effect="deny")
    assert all(g['effect'] == "allow" for g in allow_result['grants'])
    assert all(g['effect'] == "deny" for g in deny_result['grants'])


def test_list_grants_filter_by_action(authz, grant, deny_grant):
    authz.enact(grant)
    authz.enact(deny_grant)
    result = authz.list_grants(action="balloon:pop")
    assert all("balloon:pop" in g['actions'] for g in result['grants'])


def test_list_grants_paginator(authz, grant):
    authz.enact(grant)
    all_grants = []
    for page in paginator(authz.list_grants):
        all_grants.extend(page['grants'])

    assert len(all_grants) == 1


def test_list_grant_refs(authz, grant):
    authz.enact(grant)
    result = authz.list_grant_refs()
    assert result['has_failed'] is False
    assert "page_refs" in result


def test_list_grant_refs_filter_by_effect(authz, grant, deny_grant):
    authz.enact(grant)
    authz.enact(deny_grant)
    result = authz.list_grant_refs(effect="allow")
    assert result['has_failed'] is False


def test_list_grant_refs_paginator(authz, grant):
    authz.enact(grant)
    all_refs = []
    for page in paginator(authz.list_grant_refs):
        all_refs.extend(page['page_refs'])

    assert isinstance(all_refs, list)


def test_repeal_grant(authz, grant):
    authz.enact(grant)
    result = authz.repeal(grant_uuid=grant['grant_uuid'], purge=False)
    assert result['has_failed'] is False
    # Verify it was repealed - get returns has_failed=True with not found
    get_result = authz.get_grant(grant_uuid=grant['grant_uuid'])
    assert get_result['grant'] is None
    assert get_result['has_failed'] is True


def test_repeal_grant_purge(authz, grant):
    authz.enact(grant)
    result = authz.repeal(grant_uuid=grant['grant_uuid'], purge=True)
    assert result['has_failed'] is False


def test_repeal_grant_not_found(authz):
    # DictStorage repeal returns has_failed=False even when not found
    result = authz.repeal(grant_uuid="nonexistent-uuid", purge=False)
    assert result['has_failed'] is False


def test_validate_grant_with_config(authz, grant):
    result = authz.validate_grant(
        grant,
        config={
            "authzee": {
                "raise_crits": True
            }
        }
    )
    assert result['has_failed'] is False


def test_enact_with_config(authz, grant):
    result = authz.enact(
        grant,
        config={
            "authzee": {
                "raise_crits": False
            }
        }
    )
    assert result['has_failed'] is False


def test_cleanup_latches(authz):
    result = authz.cleanup_latches(before=datetime.datetime(2030, 1, 1))
    assert result['has_failed'] is False


def test_cleanup_latches_with_config(authz):
    result = authz.cleanup_latches(
        before=datetime.datetime(2030, 1, 1),
        config={
            "authzee": {
                "raise_crits": True
            }
        }
    )
    assert result['has_failed'] is False


def test_authorize_allowed(seeded_authz, auth_request):
    result = seeded_authz.authorize(request=auth_request)
    assert result['is_authorized'] is True
    assert result['has_failed'] is False
    assert result['grant'] is not None
    assert isinstance(result['message'], str)


def test_authorize_denied_no_matching_grant(seeded_authz, auth_request):
    # Change to an action that has no allow grant
    request = {
        **auth_request,
        "action": "balloon:pop"
    }
    result = seeded_authz.authorize(request=request)
    assert result['is_authorized'] is False


def test_authorize_denied_by_deny_grant(seeded_authz, deny_grant):
    # Enact a deny grant that matches the request
    seeded_authz.enact(deny_grant)
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
    result = seeded_authz.authorize(request=request)
    assert result['is_authorized'] is False


def test_authorize_with_config(seeded_authz, auth_request):
    result = seeded_authz.authorize(
        request=auth_request, config={
            "authzee": {
                "raise_crits": True
            }
        }
    )
    assert result['is_authorized'] is True


def test_audit(seeded_authz, auth_request):
    result = seeded_authz.audit(request=auth_request)
    assert result['has_failed'] is False
    assert "grants" in result
    assert "results" in result
    assert len(result['grants']) == len(result['results'])


def test_audit_with_applicable_grant(seeded_authz, auth_request):
    result = seeded_authz.audit(request=auth_request)
    assert any(r['is_applicable'] for r in result['results'])


def test_audit_paginator(seeded_authz, auth_request):
    all_grants = []
    all_results = []
    for page in paginator(seeded_authz.audit, request=auth_request):
        all_grants.extend(page['grants'])
        all_results.extend(page['results'])

    assert len(all_grants) >= 1


def test_audit_with_config(seeded_authz, auth_request):
    result = seeded_authz.audit(
        request=auth_request, config={
            "authzee": {
                "raise_crits": True
            }
        }
    )
    assert result['has_failed'] is False


def test_batch_authorize(seeded_authz, batch_request):
    result = seeded_authz.batch_authorize(batch_request=batch_request)
    assert result['has_failed'] is False
    assert "batch_results" in result
    assert len(result['batch_results']) == 2


def test_batch_authorize_all_authorized(seeded_authz, batch_request):
    result = seeded_authz.batch_authorize(batch_request=batch_request)
    for item in result['batch_results']:
        assert item['is_authorized'] is True


def test_batch_authorize_with_config(seeded_authz, batch_request):
    result = seeded_authz.batch_authorize(
        batch_request=batch_request, config={
            "authzee": {
                "raise_crits": True
            }
        }
    )
    assert result['has_failed'] is False


def test_batch_audit(seeded_authz, batch_request):
    result = seeded_authz.batch_audit(batch_request=batch_request)
    assert result['has_failed'] is False
    assert "grants" in result
    assert "batch_results" in result
    assert len(result['batch_results']) == 2


def test_batch_audit_paginator(seeded_authz, batch_request):
    all_grants = []
    all_batch = []
    for page in paginator(
        seeded_authz.batch_audit,
        batch_request=batch_request
    ):
        all_grants.extend(page['grants'])
        all_batch.extend(page['batch_results'])

    assert len(all_grants) >= 1


def test_batch_audit_with_config(seeded_authz, batch_request):
    result = seeded_authz.batch_audit(
        batch_request=batch_request, config={
            "authzee": {
                "raise_crits": True
            }
        }
    )
    assert result['has_failed'] is False


def test_instance_level_config():
    """Test that config can be set at the instance level."""
    storage_dict = {}
    authz = Authzee(
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
    result = authz.construct()
    assert result['has_failed'] is False
    result = authz.start()
    assert result['has_failed'] is False


def test_raise_crits_config_raises_on_invalid_def():
    """Test that raise_crits=True raises DefinitionError on invalid put."""
    storage_dict = {}
    authz = Authzee(
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
    authz.construct()
    authz.start()
    with pytest.raises(exceptions.DefinitionError):
        authz.validate_context_def(
            {
                "context_type": "BAD",
                "schema": "not_a_dict"
            }
        )


def test_raise_crits_override_at_call_level(authz):
    """Test that config override at method level takes precedence."""
    with pytest.raises(exceptions.DefinitionError):
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


def test_raise_crits_false_does_not_raise(authz):
    """Test that raise_crits=False returns error result without raising."""
    result = authz.validate_context_def(
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
    assert result['has_failed'] is True


def test_raise_crits_grant_error():
    """Test that raise_crits raises GrantError on invalid grant validation."""
    storage_dict = {}
    authz = Authzee(
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
    authz.construct()
    authz.start()
    with pytest.raises(exceptions.GrantError):
        authz.validate_grant(
            {
                "effect": "bad"
            }
        )


def test_compute_storage_kwargs_override():
    """Test that compute_storage_kwargs is accepted."""
    storage_dict = {}
    authz = Authzee(
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
    authz.construct()
    result = authz.start()
    assert result['has_failed'] is False


def test_put_context_def_overwrite(authz, context_def):
    """Putting the same context_type twice should succeed (upsert)."""
    authz.put_context_def(context_def)
    updated_def = {
        **context_def,
        "schema": {
            "type": "object"
        }
    }
    result = authz.put_context_def(updated_def)
    assert result['has_failed'] is False


def test_put_identity_def_overwrite(authz, identity_def):
    authz.put_identity_def(identity_def)
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
    result = authz.put_identity_def(updated_def)
    assert result['has_failed'] is False


def test_put_resource_def_overwrite(authz, resource_def):
    authz.put_resource_def(resource_def)
    updated_def = {
        **resource_def,
        "actions": [
            "balloon:read",
            "balloon:inflate",
            "balloon:pop",
            "balloon:tie"
        ]
    }
    result = authz.put_resource_def(updated_def)
    assert result['has_failed'] is False


def test_multiple_grants(authz, grant, deny_grant):
    authz.enact(grant)
    authz.enact(deny_grant)
    result = authz.list_grants()
    assert len(result['grants']) == 2


def test_multiple_context_defs(authz):
    authz.put_context_def(
        {
            "context_type": "A",
            "schema": {
                "type": "object"
            }
        }
    )
    authz.put_context_def(
        {
            "context_type": "B",
            "schema": {
                "type": "object"
            }
        }
    )
    result = authz.list_context_defs()
    assert len(result['context_defs']) == 2


def test_multiple_identity_defs(authz):
    authz.put_identity_def(
        {
            "identity_type": "A",
            "schema": {
                "type": "object"
            }
        }
    )
    authz.put_identity_def(
        {
            "identity_type": "B",
            "schema": {
                "type": "object"
            }
        }
    )
    result = authz.list_identity_defs()
    assert len(result['identity_defs']) == 2


def test_multiple_resource_defs(authz):
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
    result = authz.list_resource_defs()
    assert len(result['resource_defs']) == 2
