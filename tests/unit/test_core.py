"""Unit tests for authzee.core module."""

from uuid import uuid4

import pytest

from authzee.core import (
    combine_errors,
    evaluate,
    validate_batch_request_schema,
    validate_context_def,
    validate_grant,
    validate_identity_def,
    validate_request_schema,
    validate_resource_def
)
from authzee.jmespath import jmespath_execute


def test_validate_context_def_valid():
    context_def = {
        "context_type": "NONE",
        "schema": {
            "type": "object",
            "additionalProperties": False
        }
    }
    result = validate_context_def(context_def)
    assert result['has_failed'] is False
    assert result['errors'] == {}


def test_validate_context_def_invalid_schema():
    context_def = {
        "bad_key": "nope"
    }
    result = validate_context_def(context_def)
    assert result['has_failed'] is True
    assert "definition" in result['errors']


def test_validate_context_def_schema_not_object_type():
    context_def = {
        "context_type": "NONE",
        "schema": {
            "type": "string"
        }
    }
    result = validate_context_def(context_def)
    assert result['has_failed'] is True
    assert "definition" in result['errors']
    assert "root type of object" in result['errors']['definition'][0]['message']


def test_validate_identity_def_valid():
    identity_def = {
        "identity_type": "user",
        "schema": {
            "type": "object",
            "properties": {
                "name": {
                    "type": "string"
                }
            }
        }
    }
    result = validate_identity_def(identity_def)
    assert result['has_failed'] is False


def test_validate_identity_def_invalid_schema():
    identity_def = {
        "bad": "data"
    }
    result = validate_identity_def(identity_def)
    assert result['has_failed'] is True
    assert "definition" in result['errors']


def test_validate_identity_def_schema_not_object_type():
    identity_def = {
        "identity_type": "user",
        "schema": {
            "type": "array"
        }
    }
    result = validate_identity_def(identity_def)
    assert result['has_failed'] is True
    assert "root type of object" in result['errors']['definition'][0]['message']


def test_validate_resource_def_valid():
    resource_def = {
        "resource_type": "file",
        "actions": [
            "read",
            "write"
        ],
        "schema": {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string"
                }
            }
        }
    }
    result = validate_resource_def(resource_def)
    assert result['has_failed'] is False


def test_validate_resource_def_invalid_schema():
    resource_def = {
        "bad": "data"
    }
    result = validate_resource_def(resource_def)
    assert result['has_failed'] is True
    assert "definition" in result['errors']


def test_validate_resource_def_schema_not_object_type():
    resource_def = {
        "resource_type": "file",
        "actions": [
            "read"
        ],
        "schema": {
            "type": "number"
        }
    }
    result = validate_resource_def(resource_def)
    assert result['has_failed'] is True
    assert "root type of object" in result['errors']['definition'][0]['message']


def test_validate_grant_valid():
    grant = {
        "grant_uuid": str(uuid4()),
        "name": "Test Grant",
        "description": "A test grant.",
        "tags": {},
        "effect": "allow",
        "actions": [
            "read"
        ],
        "query": "`true`",
        "evaluation_handler": "evaluate",
        "equality": True,
        "applicable_on_failure": False,
        "data": {}
    }
    result = validate_grant(grant)
    assert result['has_failed'] is False


def test_validate_grant_invalid():
    grant = {
        "bad": "data"
    }
    result = validate_grant(grant)
    assert result['has_failed'] is True
    assert "grant" in result['errors']


def test_validate_request_schema_valid():
    request = {
        "identities": {
            "user": [
                {
                    "name": "test"
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
    result = validate_request_schema(request)
    assert result['has_failed'] is False


def test_validate_request_schema_invalid():
    request = {
        "bad": "data"
    }
    result = validate_request_schema(request)
    assert result['has_failed'] is True
    assert "definition" in result['errors']


def test_validate_batch_request_schema_valid():
    batch_request = {
        "identities": {
            "user": [
                {
                    "name": "test"
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
        "context": {},
        "batch": [
            {
                "resource": {
                    "path": "/other"
                }
            }
        ]
    }
    result = validate_batch_request_schema(batch_request)
    assert result['has_failed'] is False


def test_validate_batch_request_schema_invalid():
    batch_request = {
        "bad": "data"
    }
    result = validate_batch_request_schema(batch_request)
    assert result['has_failed'] is True
    assert "definition" in result['errors']


def test_evaluate_applicable():
    request = {
        "identities": {
            "user": [
                {
                    "name": "test"
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
    grant = {
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
        "applicable_on_failure": False,
        "data": {}
    }
    result = evaluate(
        request,
        grant,
        jmespath_execute,
        only_crits=False
    )
    assert result['is_applicable'] is True
    assert result['has_failed'] is False


def test_evaluate_not_applicable():
    request = {
        "identities": {
            "user": [
                {
                    "name": "test"
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
    grant = {
        "grant_uuid": str(uuid4()),
        "name": "Test",
        "description": "",
        "tags": {},
        "effect": "allow",
        "actions": [
            "read"
        ],
        "query": "`false`",
        "evaluation_handler": "evaluate",
        "equality": True,
        "applicable_on_failure": False,
        "data": {}
    }
    result = evaluate(
        request,
        grant,
        jmespath_execute,
        only_crits=False
    )
    assert result['is_applicable'] is False
    assert result['has_failed'] is False


def test_evaluate_query_error_with_error_handler():
    """When evaluation_handler is 'error' on the grant and request uses 'grant',
    a query error should produce an error result but not fail critically."""
    request = {
        "identities": {
            "user": [
                {
                    "name": "test"
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
    grant = {
        "grant_uuid": str(uuid4()),
        "name": "Test",
        "description": "",
        "tags": {},
        "effect": "allow",
        "actions": [
            "read"
        ],
        "query": "bad_query.[invalid",
        "evaluation_handler": "error",
        "equality": True,
        "applicable_on_failure": False,
        "data": {}
    }
    result = evaluate(
        request,
        grant,
        jmespath_execute,
        only_crits=False
    )
    assert result['is_applicable'] is False
    assert result['has_failed'] is False
    assert "evaluation" in result['errors']


def test_evaluate_query_error_with_critical_handler():
    """When evaluation_handler is 'critical', a query error should fail critically."""
    request = {
        "identities": {
            "user": [
                {
                    "name": "test"
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
    grant = {
        "grant_uuid": str(uuid4()),
        "name": "Test",
        "description": "",
        "tags": {},
        "effect": "allow",
        "actions": [
            "read"
        ],
        "query": "bad_query.[invalid",
        "evaluation_handler": "critical",
        "equality": True,
        "applicable_on_failure": False,
        "data": {}
    }
    result = evaluate(
        request,
        grant,
        jmespath_execute,
        only_crits=False
    )
    assert result['is_applicable'] is False
    assert result['has_failed'] is True
    assert "evaluation" in result['errors']


def test_evaluate_query_error_with_error_handler_only_crits():
    """When only_crits is True and handler is 'error', errors should be suppressed."""
    request = {
        "identities": {
            "user": [
                {
                    "name": "test"
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
    grant = {
        "grant_uuid": str(uuid4()),
        "name": "Test",
        "description": "",
        "tags": {},
        "effect": "allow",
        "actions": [
            "read"
        ],
        "query": "bad_query.[invalid",
        "evaluation_handler": "error",
        "equality": True,
        "applicable_on_failure": False,
        "data": {}
    }
    result = evaluate(
        request,
        grant,
        jmespath_execute,
        only_crits=True
    )
    assert result['is_applicable'] is False
    assert result['has_failed'] is False
    assert result['errors'] == {}


def test_evaluate_request_evaluation_handler_overrides_grant():
    """When request evaluation_handler is not 'grant', it overrides the grant's handler."""
    request = {
        "identities": {
            "user": [
                {
                    "name": "test"
                }
            ]
        },
        "action": "read",
        "resource_type": "file",
        "resource": {
            "path": "/tmp"
        },
        "evaluation_handler": "critical",
        "context_type": "NONE",
        "context": {}
    }
    grant = {
        "grant_uuid": str(uuid4()),
        "name": "Test",
        "description": "",
        "tags": {},
        "effect": "allow",
        "actions": [
            "read"
        ],
        "query": "bad_query.[invalid",
        "evaluation_handler": "error",
        "equality": True,
        "applicable_on_failure": False,
        "data": {}
    }
    result = evaluate(
        request,
        grant,
        jmespath_execute,
        only_crits=False
    )
    assert result['has_failed'] is True
    assert "evaluation" in result['errors']


def test_combine_errors_empty():
    result = {
        "has_failed": False,
        "errors": {}
    }
    combine_errors(result)
    assert result['has_failed'] is False
    assert result['errors'] == {}


def test_combine_errors_merges_new_keys():
    result = {
        "has_failed": False,
        "errors": {
            "definition": [
                {
                    "is_critical": False,
                    "message": "a"
                }
            ]
        }
    }
    new_result = {
        "has_failed": False,
        "errors": {
            "grant": [
                {
                    "is_critical": False,
                    "message": "b"
                }
            ]
        }
    }
    combine_errors(result, new_result)
    assert "definition" in result['errors']
    assert "grant" in result['errors']


def test_combine_errors_merges_existing_keys():
    result = {
        "has_failed": False,
        "errors": {
            "definition": [
                {
                    "is_critical": False,
                    "message": "a"
                }
            ]
        }
    }
    new_result = {
        "has_failed": False,
        "errors": {
            "definition": [
                {
                    "is_critical": False,
                    "message": "b"
                }
            ]
        }
    }
    combine_errors(result, new_result)
    assert len(result['errors']['definition']) == 2


def test_combine_errors_propagates_failure():
    result = {
        "has_failed": False,
        "errors": {}
    }
    new_result = {
        "has_failed": True,
        "errors": {
            "grant": [
                {
                    "is_critical": True,
                    "message": "fail"
                }
            ]
        }
    }
    combine_errors(result, new_result)
    assert result['has_failed'] is True


def test_combine_errors_multiple_args():
    result = {
        "has_failed": False,
        "errors": {}
    }
    r1 = {
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
    r2 = {
        "has_failed": True,
        "errors": {
            "b": [
                {
                    "is_critical": True,
                    "message": "2"
                }
            ]
        }
    }
    combine_errors(result, r1, r2)
    assert result['has_failed'] is True
    assert "a" in result['errors']
    assert "b" in result['errors']
