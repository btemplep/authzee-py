"""Unit tests for authzee.core module."""

from uuid import uuid4

import pytest

from authzee.core import (
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
    assert result['error'] is None


def test_validate_context_def_invalid_schema():
    context_def = {
        "bad_key": "nope"
    }
    result = validate_context_def(context_def)
    assert result['error'] is not None


def test_validate_context_def_schema_not_object_type():
    context_def = {
        "context_type": "NONE",
        "schema": {
            "type": "string"
        }
    }
    result = validate_context_def(context_def)
    assert "root type of object" in result['error']['message']


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


def test_validate_identity_def_invalid_schema():
    identity_def = {
        "bad": "data"
    }
    result = validate_identity_def(identity_def)


def test_validate_identity_def_schema_not_object_type():
    identity_def = {
        "identity_type": "user",
        "schema": {
            "type": "array"
        }
    }
    result = validate_identity_def(identity_def)


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


def test_validate_resource_def_invalid_schema():
    resource_def = {
        "bad": "data"
    }
    result = validate_resource_def(resource_def)


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
        "equality": True,
        "applicable_on_failure": False,
        "data": {}
    }
    result = validate_grant(grant)


def test_validate_grant_invalid():
    grant = {
        "bad": "data"
    }
    result = validate_grant(grant)


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
        "context_type": "NONE",
        "context": {}
    }
    result = validate_request_schema(request)


def test_validate_request_schema_invalid():
    request = {
        "bad": "data"
    }
    result = validate_request_schema(request)


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


def test_validate_batch_request_schema_invalid():
    batch_request = {
        "bad": "data"
    }
    result = validate_batch_request_schema(batch_request)


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
        "equality": True,
        "applicable_on_failure": False,
        "data": {}
    }
    result = evaluate(request, grant, jmespath_execute)
    assert result['is_applicable'] is True
    assert result['failure'] is None


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
        "equality": True,
        "applicable_on_failure": False,
        "data": {}
    }
    result = evaluate(request, grant, jmespath_execute)
    assert result['is_applicable'] is False


def test_evaluate_query_failure_not_applicable():
    """When a query fails and applicable_on_failure is False, grant is not applicable."""
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
        "equality": True,
        "applicable_on_failure": False,
        "data": {}
    }
    result = evaluate(request, grant, jmespath_execute)
    assert result['failure'] is not None
    assert "JMESPath Query error" in result['failure']


def test_evaluate_query_failure_applicable_on_failure():
    """When a query fails and applicable_on_failure is True, grant is still applicable."""
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
        "equality": True,
        "applicable_on_failure": True,
        "data": {}
    }
    result = evaluate(request, grant, jmespath_execute)
