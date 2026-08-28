"""Reusable base test suite for Authzee compute modules.

Any concrete compute module test file can reuse this suite by importing all of
its test functions via ``from compute_module_test_base import *`` and supplying
the required pytest fixtures:

- ``storage_dict``
- ``compute``
- ``seeded_compute``
- ``failing_compute``
- ``fail_on_allow_compute``

The shared test functions reference these fixtures by name so pytest resolves
them against whatever the concrete test module defines.
"""

import asyncio
from uuid import uuid4


def vr_config(use_list=False, page_size=1000):
    """Build a full validate_request / validate_batch_request config.

    Arguments
    ---------
    use_list : bool, default=False
        If ``True``, toggle all ``use_list_*_defs`` flags to ``True`` so the
        list-based def lookup paths are exercised.
    page_size : int, default=1000
        Page size for all ``list_*`` config blocks. Use a small value (e.g. 1)
        to force multi-page pagination.

    Returns
    -------
    dict
        The full config object.
    """
    return {
        "get_context_def": {},
        "use_list_context_defs": use_list,
        "list_context_defs": {
            "page_size": page_size,
            "use_cache": True
        },
        "get_identity_def": {},
        "use_list_identity_defs": use_list,
        "list_identity_defs": {
            "page_size": page_size,
            "use_cache": True
        },
        "get_resource_def": {},
        "use_list_resource_defs": use_list,
        "list_resource_defs": {
            "page_size": page_size,
            "use_cache": True
        }
    }


def op_config(
    use_list=False,
    page_size=100,
    grants_page_size=100
):
    """Build a full audit / authorize / batch_* config.

    Arguments
    ---------
    use_list : bool, default=False
        Toggle for the nested validate config def lookup paths.
    page_size : int, default=100
        Page size for the nested validate config ``list_*`` blocks.
    grants_page_size : int, default=100
        Page size for the ``list_grants`` block.

    Returns
    -------
    dict
        The full config object with nested ``validate_request`` /
        ``validate_batch_request`` and a ``list_grants`` block.
    """
    validate = vr_config(use_list=use_list, page_size=page_size)

    return {
        "validate_request": validate,
        "validate_batch_request": validate,
        "list_grants": {
            "page_size": grants_page_size,
            "use_cache": False
        }
    }


def _base_request(
    context_type="NONE",
    resource_type="balloon",
    action="balloon:inflate",
    department="Balloon Dept",
    color="blue",
    is_inflated=False
):
    return {
        "identities": {
            "user": [
                {
                    "username": "balloon_person",
                    "department": department
                }
            ]
        },
        "action": action,
        "resource_type": resource_type,
        "resource": {
            "color": color,
            "is_inflated": is_inflated
        },
        "context_type": context_type,
        "context": {}
    }


def _base_batch_request(
    batch,
    department="Balloon Dept",
    action="balloon:inflate"
):
    request = _base_request(department=department, action=action)
    request['batch'] = batch

    return request


def _install_error_method(compute_instance, method_name):
    """Patch a storage method to return a non-resource_not_found storage error.

    Arguments
    ---------
    compute_instance : ComputeModule
        The compute instance whose ``_storage`` will be patched.
    method_name : str
        The storage method to patch. One of the ``get_*`` or ``list_*`` def
        lookup methods.
    """
    error = {
        "error_type": "storage",
        "message": "forced storage failure"
    }
    payloads = {
        "get_context_def": {
            "context_def": None,
            "error": error
        },
        "list_context_defs": {
            "context_defs": [],
            "next_page_ref": None,
            "error": error
        },
        "get_identity_def": {
            "identity_def": None,
            "error": error
        },
        "list_identity_defs": {
            "identity_defs": [],
            "next_page_ref": None,
            "error": error
        },
        "get_resource_def": {
            "resource_def": None,
            "error": error
        },
        "list_resource_defs": {
            "resource_defs": [],
            "next_page_ref": None,
            "error": error
        }
    }
    payload = payloads[method_name]

    async def _method(*args, **kwargs):
        return payload

    setattr(compute_instance._storage, method_name, _method)


def test_base_start_locality_and_paging(compute):
    from authzee.module_locality import ModuleLocality
    assert compute.locality == ModuleLocality.PROCESS
    assert compute.has_parallel_paging is False


def test_base_shutdown_returns_no_error(compute):
    result = asyncio.run(
        compute.shutdown(
            config={
                "storage": {}
            }
        )
    )
    assert result['error'] is None


def test_base_construct_returns_no_error(compute):
    result = asyncio.run(compute.construct(config={}))
    assert result['error'] is None


def test_base_destroy_returns_no_error(compute):
    result = asyncio.run(compute.destroy(config={}))
    assert result['error'] is None


def test_base_validate_context_def_valid(compute):
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


def test_base_validate_context_def_invalid(compute):
    result = asyncio.run(
        compute.validate_context_def(
            context_def={
                "bad": "data"
            },
            config={}
        )
    )
    assert result['error'] is not None


def test_base_validate_identity_def_valid(compute):
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


def test_base_validate_identity_def_invalid(compute):
    result = asyncio.run(
        compute.validate_identity_def(
            identity_def={
                "bad": "data"
            },
            config={}
        )
    )
    assert result['error'] is not None


def test_base_validate_resource_def_valid(compute):
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


def test_base_validate_resource_def_invalid(compute):
    result = asyncio.run(
        compute.validate_resource_def(
            resource_def={
                "bad": "data"
            },
            config={}
        )
    )
    assert result['error'] is not None


def test_base_validate_grant_valid(compute):
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


def test_base_validate_grant_invalid(compute):
    result = asyncio.run(
        compute.validate_grant(
            grant={
                "bad": "data"
            },
            config={}
        )
    )
    assert result['error'] is not None


def test_base_validate_request_valid_get_path(seeded_compute):
    result = asyncio.run(
        seeded_compute.validate_request(
            request=_base_request(),
            config=vr_config(use_list=False)
        )
    )
    assert result['error'] is None


def test_base_validate_request_valid_list_path(seeded_compute):
    result = asyncio.run(
        seeded_compute.validate_request(
            request=_base_request(),
            config=vr_config(use_list=True)
        )
    )
    assert result['error'] is None


def test_base_validate_request_valid_list_path_paginated(seeded_compute):
    result = asyncio.run(
        seeded_compute.validate_request(
            request=_base_request(),
            config=vr_config(use_list=True, page_size=1)
        )
    )
    assert result['error'] is None


def test_base_validate_request_invalid_schema(seeded_compute):
    result = asyncio.run(
        seeded_compute.validate_request(
            request={
                "bad": "data"
            },
            config=vr_config()
        )
    )
    assert result['error'] is not None


def test_base_validate_request_unknown_context_type_get(seeded_compute):
    request = _base_request(context_type="UNKNOWN")
    result = asyncio.run(
        seeded_compute.validate_request(
            request=request,
            config=vr_config(use_list=False)
        )
    )
    assert result['error'] is not None
    assert result['error']['error_type'] == "request"


def test_base_validate_request_unknown_context_type_list(seeded_compute):
    request = _base_request(context_type="UNKNOWN")
    result = asyncio.run(
        seeded_compute.validate_request(
            request=request,
            config=vr_config(use_list=True)
        )
    )
    assert result['error'] is not None
    assert result['error']['error_type'] == "request"


def test_base_validate_request_unknown_resource_type_get(seeded_compute):
    request = _base_request(resource_type="UNKNOWN")
    result = asyncio.run(
        seeded_compute.validate_request(
            request=request,
            config=vr_config(use_list=False)
        )
    )
    assert result['error'] is not None
    assert result['error']['error_type'] == "request"


def test_base_validate_request_unknown_resource_type_list(seeded_compute):
    request = _base_request(resource_type="UNKNOWN")
    result = asyncio.run(
        seeded_compute.validate_request(
            request=request,
            config=vr_config(use_list=True)
        )
    )
    assert result['error'] is not None
    assert result['error']['error_type'] == "request"


def test_base_validate_request_unknown_identity_type_get(seeded_compute):
    request = _base_request()
    request['identities'] = {
        "unknown_id": [
            {
                "username": "a",
                "department": "b"
            }
        ]
    }
    result = asyncio.run(
        seeded_compute.validate_request(
            request=request,
            config=vr_config(use_list=False)
        )
    )
    assert result['error'] is not None
    assert result['error']['error_type'] == "request"


def test_base_validate_request_unknown_identity_type_list(seeded_compute):
    request = _base_request()
    request['identities'] = {
        "unknown_id": [
            {
                "username": "a",
                "department": "b"
            }
        ]
    }
    result = asyncio.run(
        seeded_compute.validate_request(
            request=request,
            config=vr_config(use_list=True)
        )
    )
    assert result['error'] is not None
    assert result['error']['error_type'] == "request"


def test_base_validate_request_invalid_context_data(seeded_compute):
    request = _base_request()
    request['context'] = {
        "extra_field": "not allowed"
    }
    result = asyncio.run(
        seeded_compute.validate_request(
            request=request,
            config=vr_config()
        )
    )
    assert result['error'] is not None
    assert result['error']['error_type'] == "request"


def test_base_validate_request_invalid_resource_data(seeded_compute):
    request = _base_request()
    request['resource'] = {
        "color": 123,
        "is_inflated": "not_bool"
    }
    result = asyncio.run(
        seeded_compute.validate_request(
            request=request,
            config=vr_config()
        )
    )
    assert result['error'] is not None
    assert result['error']['error_type'] == "request"


def test_base_validate_request_invalid_action(seeded_compute):
    request = _base_request(action="balloon:NONEXISTENT")
    result = asyncio.run(
        seeded_compute.validate_request(
            request=request,
            config=vr_config()
        )
    )
    assert result['error'] is not None
    assert result['error']['error_type'] == "request"


def test_base_validate_request_invalid_identity_data(seeded_compute):
    request = _base_request()
    request['identities'] = {
        "user": [
            {
                "username": 123,
                "department": 456
            }
        ]
    }
    result = asyncio.run(
        seeded_compute.validate_request(
            request=request,
            config=vr_config()
        )
    )
    assert result['error'] is not None
    assert result['error']['error_type'] == "request"


def test_base_validate_request_storage_error_get_context(seeded_compute):
    _install_error_method(seeded_compute, "get_context_def")
    result = asyncio.run(
        seeded_compute.validate_request(
            request=_base_request(),
            config=vr_config(use_list=False)
        )
    )
    assert result['error'] is not None
    assert result['error']['error_type'] == "storage"


def test_base_validate_request_storage_error_list_context(seeded_compute):
    _install_error_method(seeded_compute, "list_context_defs")
    result = asyncio.run(
        seeded_compute.validate_request(
            request=_base_request(),
            config=vr_config(use_list=True)
        )
    )
    assert result['error'] is not None
    assert result['error']['error_type'] == "storage"


def test_base_validate_request_storage_error_get_identity(seeded_compute):
    _install_error_method(seeded_compute, "get_identity_def")
    result = asyncio.run(
        seeded_compute.validate_request(
            request=_base_request(),
            config=vr_config(use_list=False)
        )
    )
    assert result['error'] is not None
    assert result['error']['error_type'] == "storage"


def test_base_validate_request_storage_error_list_identity(seeded_compute):
    _install_error_method(seeded_compute, "list_identity_defs")
    result = asyncio.run(
        seeded_compute.validate_request(
            request=_base_request(),
            config=vr_config(use_list=True)
        )
    )
    assert result['error'] is not None
    assert result['error']['error_type'] == "storage"


def test_base_validate_request_storage_error_get_resource(seeded_compute):
    _install_error_method(seeded_compute, "get_resource_def")
    result = asyncio.run(
        seeded_compute.validate_request(
            request=_base_request(),
            config=vr_config(use_list=False)
        )
    )
    assert result['error'] is not None
    assert result['error']['error_type'] == "storage"


def test_base_validate_request_storage_error_list_resource(seeded_compute):
    _install_error_method(seeded_compute, "list_resource_defs")
    result = asyncio.run(
        seeded_compute.validate_request(
            request=_base_request(),
            config=vr_config(use_list=True)
        )
    )
    assert result['error'] is not None
    assert result['error']['error_type'] == "storage"


def test_base_validate_batch_request_valid_get_path(seeded_compute):
    batch_request = _base_batch_request(
        batch=[
            {
                "resource": {
                    "color": "red",
                    "is_inflated": True
                }
            }
        ]
    )
    result = asyncio.run(
        seeded_compute.validate_batch_request(
            batch_request=batch_request,
            config=vr_config(use_list=False)
        )
    )
    assert result['error'] is None
    assert result['batch'][0]['error'] is None


def test_base_validate_batch_request_valid_list_path(seeded_compute):
    batch_request = _base_batch_request(
        batch=[
            {
                "resource": {
                    "color": "red",
                    "is_inflated": True
                }
            }
        ]
    )
    result = asyncio.run(
        seeded_compute.validate_batch_request(
            batch_request=batch_request,
            config=vr_config(use_list=True)
        )
    )
    assert result['error'] is None
    assert result['batch'][0]['error'] is None


def test_base_validate_batch_request_valid_list_path_paginated(seeded_compute):
    batch_request = _base_batch_request(
        batch=[
            {
                "resource": {
                    "color": "red",
                    "is_inflated": True
                }
            }
        ]
    )
    result = asyncio.run(
        seeded_compute.validate_batch_request(
            batch_request=batch_request,
            config=vr_config(use_list=True, page_size=1)
        )
    )
    assert result['error'] is None
    assert result['batch'][0]['error'] is None


def test_base_validate_batch_request_invalid_schema(seeded_compute):
    result = asyncio.run(
        seeded_compute.validate_batch_request(
            batch_request={
                "bad": "data"
            },
            config=vr_config()
        )
    )
    assert result['error'] is not None
    assert result['batch'] == []


def test_base_validate_batch_request_item_context_override(seeded_compute):
    batch_request = _base_batch_request(
        batch=[
            {
                "context_type": "NONE",
                "context": {}
            }
        ]
    )
    result = asyncio.run(
        seeded_compute.validate_batch_request(
            batch_request=batch_request,
            config=vr_config()
        )
    )
    assert result['error'] is None
    assert result['batch'][0]['error'] is None


def test_base_validate_batch_request_item_identities_override(seeded_compute):
    batch_request = _base_batch_request(
        batch=[
            {
                "identities": {
                    "user": [
                        {
                            "username": "other",
                            "department": "Other Dept"
                        }
                    ]
                }
            }
        ]
    )
    result = asyncio.run(
        seeded_compute.validate_batch_request(
            batch_request=batch_request,
            config=vr_config()
        )
    )
    assert result['error'] is None
    assert result['batch'][0]['error'] is None


def test_base_validate_batch_request_item_invalid_data(seeded_compute):
    batch_request = _base_batch_request(
        batch=[
            {
                "resource": {
                    "color": 123,
                    "is_inflated": "bad"
                }
            }
        ]
    )
    result = asyncio.run(
        seeded_compute.validate_batch_request(
            batch_request=batch_request,
            config=vr_config()
        )
    )
    assert result['error'] is None
    assert result['batch'][0]['error'] is not None


def test_base_validate_batch_request_root_unknown_context_get(seeded_compute):
    batch_request = _base_batch_request(
        batch=[
            {
                "resource": {
                    "color": "red",
                    "is_inflated": True
                }
            }
        ]
    )
    batch_request['context_type'] = "NONEXISTENT"
    result = asyncio.run(
        seeded_compute.validate_batch_request(
            batch_request=batch_request,
            config=vr_config(use_list=False)
        )
    )
    assert result['error'] is not None
    assert result['batch'] == []


def test_base_validate_batch_request_root_unknown_identity_get(seeded_compute):
    batch_request = _base_batch_request(
        batch=[
            {
                "resource": {
                    "color": "red",
                    "is_inflated": True
                }
            }
        ]
    )
    batch_request['identities'] = {
        "unknown_id": [
            {
                "username": "a",
                "department": "b"
            }
        ]
    }
    result = asyncio.run(
        seeded_compute.validate_batch_request(
            batch_request=batch_request,
            config=vr_config(use_list=False)
        )
    )
    assert result['error'] is not None
    assert result['batch'] == []


def test_base_validate_batch_request_root_unknown_resource_get(seeded_compute):
    batch_request = _base_batch_request(
        batch=[
            {
                "resource": {
                    "color": "red",
                    "is_inflated": True
                }
            }
        ]
    )
    batch_request['resource_type'] = "NONEXISTENT"
    result = asyncio.run(
        seeded_compute.validate_batch_request(
            batch_request=batch_request,
            config=vr_config(use_list=False)
        )
    )
    assert result['error'] is not None
    assert result['batch'] == []


def test_base_validate_batch_request_storage_error_get_context(seeded_compute):
    _install_error_method(seeded_compute, "get_context_def")
    batch_request = _base_batch_request(
        batch=[
            {
                "resource": {
                    "color": "red",
                    "is_inflated": True
                }
            }
        ]
    )
    result = asyncio.run(
        seeded_compute.validate_batch_request(
            batch_request=batch_request,
            config=vr_config(use_list=False)
        )
    )
    assert result['error'] is not None
    assert result['error']['error_type'] == "storage"
    assert result['batch'] == []


def test_base_validate_batch_request_storage_error_list_context(seeded_compute):
    _install_error_method(seeded_compute, "list_context_defs")
    batch_request = _base_batch_request(
        batch=[
            {
                "resource": {
                    "color": "red",
                    "is_inflated": True
                }
            }
        ]
    )
    result = asyncio.run(
        seeded_compute.validate_batch_request(
            batch_request=batch_request,
            config=vr_config(use_list=True)
        )
    )
    assert result['error'] is not None
    assert result['error']['error_type'] == "storage"
    assert result['batch'] == []


def test_base_validate_batch_request_storage_error_get_identity(seeded_compute):
    _install_error_method(seeded_compute, "get_identity_def")
    batch_request = _base_batch_request(
        batch=[
            {
                "resource": {
                    "color": "red",
                    "is_inflated": True
                }
            }
        ]
    )
    result = asyncio.run(
        seeded_compute.validate_batch_request(
            batch_request=batch_request,
            config=vr_config(use_list=False)
        )
    )
    assert result['error'] is not None
    assert result['error']['error_type'] == "storage"
    assert result['batch'] == []


def test_base_validate_batch_request_storage_error_list_identity(
    seeded_compute
):
    _install_error_method(seeded_compute, "list_identity_defs")
    batch_request = _base_batch_request(
        batch=[
            {
                "resource": {
                    "color": "red",
                    "is_inflated": True
                }
            }
        ]
    )
    result = asyncio.run(
        seeded_compute.validate_batch_request(
            batch_request=batch_request,
            config=vr_config(use_list=True)
        )
    )
    assert result['error'] is not None
    assert result['error']['error_type'] == "storage"
    assert result['batch'] == []


def test_base_validate_batch_request_storage_error_get_resource(seeded_compute):
    _install_error_method(seeded_compute, "get_resource_def")
    batch_request = _base_batch_request(
        batch=[
            {
                "resource": {
                    "color": "red",
                    "is_inflated": True
                }
            }
        ]
    )
    result = asyncio.run(
        seeded_compute.validate_batch_request(
            batch_request=batch_request,
            config=vr_config(use_list=False)
        )
    )
    assert result['error'] is not None
    assert result['error']['error_type'] == "storage"
    assert result['batch'] == []


def test_base_validate_batch_request_storage_error_list_resource(
    seeded_compute
):
    _install_error_method(seeded_compute, "list_resource_defs")
    batch_request = _base_batch_request(
        batch=[
            {
                "resource": {
                    "color": "red",
                    "is_inflated": True
                }
            }
        ]
    )
    result = asyncio.run(
        seeded_compute.validate_batch_request(
            batch_request=batch_request,
            config=vr_config(use_list=True)
        )
    )
    assert result['error'] is not None
    assert result['error']['error_type'] == "storage"
    assert result['batch'] == []


def test_base_audit_valid(seeded_compute):
    result = asyncio.run(
        seeded_compute.audit(
            request=_base_request(),
            page_ref=None,
            config=op_config()
        )
    )
    assert result['error'] is None
    assert len(result['results']) > 0
    assert result['results'][0]['grant'] is not None
    assert "is_applicable" in result['results'][0]
    assert "query_result" in result['results'][0]
    assert "failure" in result['results'][0]


def test_base_audit_pagination(seeded_compute):
    first = asyncio.run(
        seeded_compute.audit(
            request=_base_request(action="balloon:read"),
            page_ref=None,
            config=op_config(grants_page_size=1)
        )
    )
    assert first['error'] is None
    assert len(first['results']) == 1
    if first['next_page_ref'] is not None:
        second = asyncio.run(
            seeded_compute.audit(
                request=_base_request(action="balloon:read"),
                page_ref=first['next_page_ref'],
                config=op_config(grants_page_size=1)
            )
        )
        assert second['error'] is None


def test_base_audit_storage_error(failing_compute):
    result = asyncio.run(
        failing_compute.audit(
            request=_base_request(),
            page_ref=None,
            config=op_config()
        )
    )
    assert result['error'] is not None


def test_base_authorize_allow(seeded_compute):
    result = asyncio.run(
        seeded_compute.authorize(
            request=_base_request(),
            config=op_config()
        )
    )
    assert result['is_authorized'] is True
    assert result['error'] is None
    assert result['grant'] is not None


def test_base_authorize_deny(seeded_compute):
    request = _base_request(
        action="balloon:pop",
        department="Intern",
        is_inflated=True
    )
    result = asyncio.run(
        seeded_compute.authorize(
            request=request,
            config=op_config()
        )
    )
    assert result['is_authorized'] is False
    assert result['error'] is None
    assert "deny grant" in result['message']


def test_base_authorize_implicit_deny(seeded_compute):
    request = _base_request(department="None")
    result = asyncio.run(
        seeded_compute.authorize(
            request=request,
            config=op_config()
        )
    )
    assert result['is_authorized'] is False
    assert result['error'] is None
    assert "implicitly denied" in result['message']


def test_base_authorize_deny_phase_storage_error(failing_compute):
    result = asyncio.run(
        failing_compute.authorize(
            request=_base_request(),
            config=op_config()
        )
    )
    assert result['is_authorized'] is False
    assert result['error'] is not None


def test_base_authorize_allow_phase_storage_error(fail_on_allow_compute):
    result = asyncio.run(
        fail_on_allow_compute.authorize(
            request=_base_request(),
            config=op_config()
        )
    )
    assert result['is_authorized'] is False
    assert result['error'] is not None


def test_base_batch_audit_valid(seeded_compute):
    batch_request = _base_batch_request(
        batch=[
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
    )
    result = asyncio.run(
        seeded_compute.batch_audit(
            batch_request=batch_request,
            page_ref=None,
            config=op_config()
        )
    )
    assert result['error'] is None
    assert len(result['batch']) == 2


def test_base_batch_audit_pagination(seeded_compute):
    batch_request = _base_batch_request(
        batch=[
            {
                "resource": {
                    "color": "red",
                    "is_inflated": True
                }
            }
        ],
        action="balloon:read"
    )
    first = asyncio.run(
        seeded_compute.batch_audit(
            batch_request=batch_request,
            page_ref=None,
            config=op_config(grants_page_size=1)
        )
    )
    assert first['error'] is None
    if first['next_page_ref'] is not None:
        second = asyncio.run(
            seeded_compute.batch_audit(
                batch_request=batch_request,
                page_ref=first['next_page_ref'],
                config=op_config(grants_page_size=1)
            )
        )
        assert second['error'] is None


def test_base_batch_audit_storage_error(failing_compute):
    batch_request = _base_batch_request(
        batch=[
            {
                "resource": {
                    "color": "red",
                    "is_inflated": True
                }
            }
        ]
    )
    result = asyncio.run(
        failing_compute.batch_audit(
            batch_request=batch_request,
            page_ref=None,
            config=op_config()
        )
    )
    assert result['error'] is not None


def test_base_batch_authorize_allow(seeded_compute):
    batch_request = _base_batch_request(
        batch=[
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
    )
    result = asyncio.run(
        seeded_compute.batch_authorize(
            batch_request=batch_request,
            config=op_config()
        )
    )
    assert result['error'] is None
    assert len(result['batch']) == 2
    for br in result['batch']:
        assert br['is_authorized'] is True


def test_base_batch_authorize_deny(seeded_compute):
    batch_request = _base_batch_request(
        batch=[
            {
                "resource": {
                    "color": "red",
                    "is_inflated": True
                }
            }
        ],
        department="Intern",
        action="balloon:pop"
    )
    result = asyncio.run(
        seeded_compute.batch_authorize(
            batch_request=batch_request,
            config=op_config()
        )
    )
    assert result['error'] is None
    for br in result['batch']:
        assert br['is_authorized'] is False
        assert "deny grant" in br['message']


def test_base_batch_authorize_implicit_deny(seeded_compute):
    batch_request = _base_batch_request(
        batch=[
            {
                "resource": {
                    "color": "red",
                    "is_inflated": True
                }
            }
        ],
        department="None"
    )
    result = asyncio.run(
        seeded_compute.batch_authorize(
            batch_request=batch_request,
            config=op_config()
        )
    )
    assert result['error'] is None
    for br in result['batch']:
        assert br['is_authorized'] is False
        assert "implicitly denied" in br['message']


def test_base_batch_authorize_deny_phase_storage_error(failing_compute):
    batch_request = _base_batch_request(
        batch=[
            {
                "resource": {
                    "color": "red",
                    "is_inflated": True
                }
            }
        ]
    )
    result = asyncio.run(
        failing_compute.batch_authorize(
            batch_request=batch_request,
            config=op_config()
        )
    )
    assert result['error'] is not None


def test_base_batch_authorize_allow_phase_storage_error(fail_on_allow_compute):
    batch_request = _base_batch_request(
        batch=[
            {
                "resource": {
                    "color": "red",
                    "is_inflated": True
                }
            }
        ]
    )
    result = asyncio.run(
        fail_on_allow_compute.batch_authorize(
            batch_request=batch_request,
            config=op_config()
        )
    )
    assert result['error'] is not None


def _seed_extra_defs(storage_dict):
    """Insert extra context/identity/resource defs so the target defs land on
    a later page when paginating with ``page_size=1``.

    The target types (``LATE_CTX``, ``late_user``, ``late_balloon``) are
    inserted last so a ``page_size=1`` list must loop past earlier pages,
    exercising the create-next-page-task branches.

    Arguments
    ---------
    storage_dict : dict
        The backing storage dict shared with the compute's storage module.
    """
    storage_dict['context_defs_lut']['EXTRA_CTX'] = {
        "context_type": "EXTRA_CTX",
        "schema": {
            "type": "object",
            "additionalProperties": False
        }
    }
    storage_dict['context_defs_lut']['LATE_CTX'] = {
        "context_type": "LATE_CTX",
        "schema": {
            "type": "object",
            "additionalProperties": False
        }
    }
    storage_dict['identity_defs_lut']['extra_user'] = {
        "identity_type": "extra_user",
        "schema": {
            "type": "object",
            "additionalProperties": True
        }
    }
    storage_dict['identity_defs_lut']['late_user'] = {
        "identity_type": "late_user",
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
    storage_dict['resource_defs_lut']['extra_balloon'] = {
        "resource_type": "extra_balloon",
        "actions": [
            "extra:read"
        ],
        "schema": {
            "type": "object",
            "additionalProperties": True
        }
    }
    storage_dict['resource_defs_lut']['late_balloon'] = {
        "resource_type": "late_balloon",
        "actions": [
            "late:read"
        ],
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "path": {
                    "type": "string"
                }
            }
        }
    }


def _late_request():
    return {
        "identities": {
            "late_user": [
                {
                    "username": "someone"
                }
            ]
        },
        "action": "late:read",
        "resource_type": "late_balloon",
        "resource": {
            "path": "/tmp"
        },
        "context_type": "LATE_CTX",
        "context": {}
    }


def test_base_validate_request_list_pagination_multi_page(
    seeded_compute,
    storage_dict
):
    _seed_extra_defs(storage_dict)
    result = asyncio.run(
        seeded_compute.validate_request(
            request=_late_request(),
            config=vr_config(use_list=True, page_size=1)
        )
    )
    assert result['error'] is None


def test_base_validate_batch_request_list_pagination_multi_page(
    seeded_compute,
    storage_dict
):
    _seed_extra_defs(storage_dict)
    batch_request = _late_request()
    batch_request['batch'] = [
        {
            "resource": {
                "path": "/other"
            }
        }
    ]
    result = asyncio.run(
        seeded_compute.validate_batch_request(
            batch_request=batch_request,
            config=vr_config(use_list=True, page_size=1)
        )
    )
    assert result['error'] is None
    assert result['batch'][0]['error'] is None


def test_base_validate_batch_request_item_context_type_only(seeded_compute):
    batch_request = _base_batch_request(
        batch=[
            {
                "context_type": "NONE"
            }
        ]
    )
    result = asyncio.run(
        seeded_compute.validate_batch_request(
            batch_request=batch_request,
            config=vr_config()
        )
    )
    assert result['error'] is None
    assert result['batch'][0]['error'] is None


def test_base_validate_batch_request_item_context_only(seeded_compute):
    batch_request = _base_batch_request(
        batch=[
            {
                "context": {}
            }
        ]
    )
    result = asyncio.run(
        seeded_compute.validate_batch_request(
            batch_request=batch_request,
            config=vr_config()
        )
    )
    assert result['error'] is None
    assert result['batch'][0]['error'] is None


def test_base_validate_batch_request_item_resource_type_only(seeded_compute):
    batch_request = _base_batch_request(
        batch=[
            {
                "resource_type": "balloon"
            }
        ]
    )
    result = asyncio.run(
        seeded_compute.validate_batch_request(
            batch_request=batch_request,
            config=vr_config()
        )
    )
    assert result['error'] is None
    assert result['batch'][0]['error'] is None


def test_base_batch_authorize_multi_deny_skip_complete(
    seeded_compute,
    storage_dict
):
    """Two matching deny grants so an already-complete item is skipped by the
    second deny grant, then a matching allow grant is also skipped."""
    from uuid import uuid4 as _uuid4
    storage_dict['grants_lut'][str(_uuid4())] = {
        "grant_uuid": str(_uuid4()),
        "name": "Deny pop 2",
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
    }
    storage_dict['grants_lut'][str(_uuid4())] = {
        "grant_uuid": str(_uuid4()),
        "name": "Allow pop",
        "description": "",
        "tags": {},
        "effect": "allow",
        "actions": [
            "balloon:pop"
        ],
        "query": "`true`",
        "equality": True,
        "applicable_on_failure": False,
        "data": {}
    }
    batch_request = _base_batch_request(
        batch=[
            {
                "resource": {
                    "color": "red",
                    "is_inflated": True
                }
            }
        ],
        department="Intern",
        action="balloon:pop"
    )
    result = asyncio.run(
        seeded_compute.batch_authorize(
            batch_request=batch_request,
            config=op_config(grants_page_size=1)
        )
    )
    assert result['error'] is None
    assert result['batch'][0]['is_authorized'] is False
    assert "deny grant" in result['batch'][0]['message']


def test_base_batch_authorize_multi_allow_skip_complete(
    seeded_compute,
    storage_dict
):
    """Two matching allow grants so an already-complete (allowed) item is
    skipped by the second allow grant."""
    from uuid import uuid4 as _uuid4
    storage_dict['grants_lut'][str(_uuid4())] = {
        "grant_uuid": str(_uuid4()),
        "name": "Allow inflate 2",
        "description": "",
        "tags": {},
        "effect": "allow",
        "actions": [
            "balloon:inflate"
        ],
        "query": "`true`",
        "equality": True,
        "applicable_on_failure": False,
        "data": {}
    }
    batch_request = _base_batch_request(
        batch=[
            {
                "resource": {
                    "color": "red",
                    "is_inflated": True
                }
            }
        ]
    )
    result = asyncio.run(
        seeded_compute.batch_authorize(
            batch_request=batch_request,
            config=op_config(grants_page_size=1)
        )
    )
    assert result['error'] is None
    assert result['batch'][0]['is_authorized'] is True


def test_base_validate_request_list_multi_identity_types(
    seeded_compute,
    storage_dict
):
    """validate_request list path with two identity types so the inner
    'still-missing' loop after finding one identity def runs."""
    _seed_extra_defs(storage_dict)
    request = _base_request()
    request['identities'] = {
        "user": [
            {
                "username": "balloon_person",
                "department": "Balloon Dept"
            }
        ],
        "late_user": [
            {
                "username": "someone"
            }
        ]
    }
    result = asyncio.run(
        seeded_compute.validate_request(
            request=request,
            config=vr_config(use_list=True, page_size=1)
        )
    )
    assert result['error'] is None


def test_base_validate_batch_request_item_adds_unregistered_identity(
    seeded_compute
):
    """A batch item introduces a new identity type absent from the root, so it
    is added to the identity lookup. Because the type is unregistered, the base
    request validation returns a graceful 'not registered' request error rather
    than crashing."""
    batch_request = _base_batch_request(
        batch=[
            {
                "identities": {
                    "ghost_id": [
                        {
                            "anything": True
                        }
                    ]
                }
            }
        ]
    )
    result = asyncio.run(
        seeded_compute.validate_batch_request(
            batch_request=batch_request,
            config=vr_config(use_list=False)
        )
    )
    assert result['error'] is not None
    assert result['error']['error_type'] == "request"
    assert result['batch'] == []


def _multi_type_batch_request():
    """Batch request whose root has two identity types and whose item adds a
    new (registered) context type and resource type.

    The root carries both identity types so the base-request validation has
    them available, while the item introduces additional context/resource
    types to exercise the lookup-collection and multi-type list-path loops.
    """
    batch_request = _base_batch_request(
        batch=[
            {
                "context_type": "EXTRA_CTX",
                "context": {},
                "resource_type": "extra_balloon",
                "resource": {
                    "anything": True
                }
            }
        ]
    )
    batch_request['identities'] = {
        "user": [
            {
                "username": "balloon_person",
                "department": "Balloon Dept"
            }
        ],
        "late_user": [
            {
                "username": "someone"
            }
        ]
    }

    return batch_request


def test_base_validate_batch_request_item_added_types_list(
    seeded_compute,
    storage_dict
):
    """Batch root has two identity types and an item references extra
    registered context/resource types not present at the root, exercising
    lookup collection and the multi-type list-path 'still-missing' loops."""
    _seed_extra_defs(storage_dict)
    result = asyncio.run(
        seeded_compute.validate_batch_request(
            batch_request=_multi_type_batch_request(),
            config=vr_config(use_list=True, page_size=1)
        )
    )
    assert result['error'] is None
    assert len(result['batch']) == 1


def test_base_validate_batch_request_item_added_types_get(
    seeded_compute,
    storage_dict
):
    """Same multi-type batch request but via the get_* def lookup path."""
    _seed_extra_defs(storage_dict)
    result = asyncio.run(
        seeded_compute.validate_batch_request(
            batch_request=_multi_type_batch_request(),
            config=vr_config(use_list=False)
        )
    )
    assert result['error'] is None
    assert len(result['batch']) == 1


def test_base_validate_batch_request_base_from_cache_invalid(seeded_compute):
    """Batch base request passes schema and def lookup but fails validation
    from cache (registered resource type with invalid resource data) so the
    early-return with empty batch fires."""
    batch_request = _base_batch_request(
        batch=[
            {
                "resource": {
                    "color": "red",
                    "is_inflated": True
                }
            }
        ]
    )
    batch_request['resource'] = {
        "color": 123,
        "is_inflated": "bad"
    }
    result = asyncio.run(
        seeded_compute.validate_batch_request(
            batch_request=batch_request,
            config=vr_config()
        )
    )
    assert result['error'] is not None
    assert result['error']['error_type'] == "request"
    assert result['batch'] == []
