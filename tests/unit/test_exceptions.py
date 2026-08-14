"""Unit tests for authzee.exceptions module."""

import pytest

from authzee.exceptions import (
    AuthzeeError,
    AuthzeeSDKError,
    AuthzeeSpecError,
    DefinitionError,
    EvaluationError,
    GrantError,
    LocalityIncompatibilityError,
    NotImplementedError as AuthzeeNotImplementedError,
    PageReferenceError,
    ParallelPaginationNotSupported,
    RequestError,
    ResourceNotFoundError,
    StartError,
    _exception_map
)


def test_authzee_error_is_exception():
    with pytest.raises(AuthzeeError):
        raise AuthzeeError("test")


def test_authzee_spec_error():
    result = {
        "has_failed": True,
        "errors": {}
    }
    exc = AuthzeeSpecError("test message", result)
    assert exc.message == "test message"
    assert exc.result is result
    assert str(exc) == "test message"


def test_definition_error():
    result = {
        "has_failed": True,
        "errors": {
            "definition": []
        }
    }
    exc = DefinitionError("def error", result)
    assert isinstance(exc, AuthzeeSpecError)
    assert exc.message == "def error"
    assert exc.result is result


def test_evaluation_error():
    result = {
        "has_failed": True,
        "errors": {
            "evaluation": []
        }
    }
    exc = EvaluationError("eval error", result)
    assert isinstance(exc, AuthzeeSpecError)


def test_grant_error():
    result = {
        "has_failed": True,
        "errors": {
            "grant": []
        }
    }
    exc = GrantError("grant error", result)
    assert isinstance(exc, AuthzeeSpecError)


def test_request_error():
    result = {
        "has_failed": True,
        "errors": {
            "request": []
        }
    }
    exc = RequestError("request error", result)
    assert isinstance(exc, AuthzeeSpecError)


def test_authzee_sdk_error():
    result = {
        "has_failed": True,
        "errors": {}
    }
    exc = AuthzeeSDKError("sdk error", result)
    assert exc.message == "sdk error"
    assert exc.result is result


def test_locality_incompatibility_error():
    result = {
        "has_failed": True,
        "errors": {}
    }
    exc = LocalityIncompatibilityError("locality error", result)
    assert isinstance(exc, AuthzeeSDKError)


def test_not_implemented_error_default_message():
    result = {
        "has_failed": True,
        "errors": {}
    }
    exc = AuthzeeNotImplementedError(result=result)
    assert "not implemented" in exc.message.lower()


def test_not_implemented_error_custom_message():
    result = {
        "has_failed": True,
        "errors": {}
    }
    exc = AuthzeeNotImplementedError("Custom msg", result=result)
    assert exc.message == "Custom msg"


def test_parallel_pagination_not_supported():
    result = {
        "has_failed": True,
        "errors": {}
    }
    exc = ParallelPaginationNotSupported("no parallel", result)
    assert isinstance(exc, AuthzeeSDKError)


def test_page_reference_error():
    result = {
        "has_failed": True,
        "errors": {}
    }
    exc = PageReferenceError("bad page ref", result)
    assert isinstance(exc, AuthzeeSDKError)


def test_resource_not_found_error():
    result = {
        "has_failed": True,
        "errors": {}
    }
    exc = ResourceNotFoundError("not found", result)
    assert isinstance(exc, AuthzeeSDKError)


def test_start_error():
    result = {
        "has_failed": True,
        "errors": {}
    }
    exc = StartError("start failed", result)
    assert isinstance(exc, AuthzeeSDKError)


def test_exception_map_contains_expected_keys():
    expected_keys = [
        "definition",
        "evaluation",
        "grant",
        "request",
        "locality_incompatibility",
        "not_implemented",
        "parallel_pagination_not_supported",
        "page_reference",
        "resource_not_found",
        "start"
    ]
    for key in expected_keys:
        assert key in _exception_map


def test_exception_map_values_are_exception_classes():
    for key, cls in _exception_map.items():
        assert issubclass(cls, AuthzeeError)
