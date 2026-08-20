"""Unit tests for authzee.exceptions module."""

import pytest

from authzee.exceptions import (
    AuthzeeError,
    AuthzeeSDKError,
    AuthzeeSpecError,
    ComputeError,
    DefinitionError,
    GrantError,
    LocalityIncompatibilityError,
    NotImplementedError as AuthzeeNotImplementedError,
    ParallelPaginationNotSupported,
    RequestError,
    ResourceNotFoundError,
    StorageError,
    _exception_map
)


def test_authzee_error_is_exception():
    with pytest.raises(AuthzeeError):
        raise AuthzeeError("test")


def test_authzee_spec_error():
    result = {
        "error": {
            "error_type": "definition",
            "message": "test message"
        }
    }
    exc = AuthzeeSpecError("test message", result)
    assert exc.message == "test message"
    assert exc.result is result
    assert str(exc) == "test message"


def test_definition_error():
    result = {
        "error": {
            "error_type": "definition",
            "message": "def error"
        }
    }
    exc = DefinitionError("def error", result)
    assert isinstance(exc, AuthzeeSpecError)
    assert exc.message == "def error"
    assert exc.result is result


def test_grant_error():
    result = {
        "error": {
            "error_type": "grant",
            "message": "grant error"
        }
    }
    exc = GrantError("grant error", result)
    assert isinstance(exc, AuthzeeSpecError)


def test_request_error():
    result = {
        "error": {
            "error_type": "request",
            "message": "request error"
        }
    }
    exc = RequestError("request error", result)
    assert isinstance(exc, AuthzeeSpecError)


def test_authzee_sdk_error():
    result = {
        "error": {
            "error_type": "compute",
            "message": "sdk error"
        }
    }
    exc = AuthzeeSDKError("sdk error", result)
    assert exc.message == "sdk error"
    assert exc.result is result


def test_locality_incompatibility_error():
    result = {
        "error": {
            "error_type": "locality_incompatibility",
            "message": "locality error"
        }
    }
    exc = LocalityIncompatibilityError("locality error", result)
    assert isinstance(exc, AuthzeeSDKError)


def test_not_implemented_error_default_message():
    result = {
        "error": {
            "error_type": "not_implemented",
            "message": "This method is not implemented."
        }
    }
    exc = AuthzeeNotImplementedError(result=result)
    assert "not implemented" in exc.message.lower()


def test_not_implemented_error_custom_message():
    result = {
        "error": {
            "error_type": "not_implemented",
            "message": "Custom msg"
        }
    }
    exc = AuthzeeNotImplementedError("Custom msg", result=result)
    assert exc.message == "Custom msg"


def test_parallel_pagination_not_supported():
    result = {
        "error": {
            "error_type": "parallel_pagination_not_supported",
            "message": "no parallel"
        }
    }
    exc = ParallelPaginationNotSupported("no parallel", result)
    assert isinstance(exc, AuthzeeSDKError)


def test_compute_error():
    result = {
        "error": {
            "error_type": "compute",
            "message": "compute failed"
        }
    }
    exc = ComputeError("compute failed", result)
    assert isinstance(exc, AuthzeeSDKError)


def test_storage_error():
    result = {
        "error": {
            "error_type": "storage",
            "message": "storage failed"
        }
    }
    exc = StorageError("storage failed", result)
    assert isinstance(exc, AuthzeeSDKError)


def test_resource_not_found_error():
    result = {
        "error": {
            "error_type": "resource_not_found",
            "message": "not found"
        }
    }
    exc = ResourceNotFoundError("not found", result)
    assert isinstance(exc, StorageError)


def test_exception_map_contains_expected_keys():
    expected_keys = [
        "definition",
        "grant",
        "request",
        "locality_incompatibility",
        "not_implemented",
        "parallel_pagination_not_supported",
        "compute",
        "storage",
        "resource_not_found"
    ]
    for key in expected_keys:
        assert key in _exception_map


def test_exception_map_values_are_exception_classes():
    for key, cls in _exception_map.items():
        assert issubclass(cls, AuthzeeError)
