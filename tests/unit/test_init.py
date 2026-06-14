"""Unit tests for the authzee package __init__.py module."""

import pytest


def test_authzee_exports():
    import authzee

    assert hasattr(authzee, "Authzee")
    assert hasattr(authzee, "AuthzeeAsync")
    assert hasattr(authzee, "context_def_schema")
    assert hasattr(authzee, "identity_def_schema")
    assert hasattr(authzee, "resource_def_schema")
    assert hasattr(authzee, "grant_schema")
    assert hasattr(authzee, "paginator")
    assert hasattr(authzee, "paginator_async")
    assert hasattr(authzee, "exceptions")
    assert hasattr(authzee, "reference")
    assert hasattr(authzee, "types")
    assert hasattr(authzee, "authzee_specification_version")


def test_authzee_version():
    import authzee

    assert isinstance(authzee.__version__, str)
    assert len(authzee.__version__) > 0


def test_jmespath_exports():
    import authzee

    assert hasattr(authzee, "jmespath_execute")
    assert hasattr(authzee, "jmespath_custom_execute")
    assert hasattr(authzee, "CustomJMESPathFunctions")


def test_compute_exports():
    import authzee

    assert hasattr(authzee, "InProcessCompute")
    assert hasattr(authzee, "ComputeModule")


def test_storage_exports():
    import authzee

    assert hasattr(authzee, "DictStorage")
    assert hasattr(authzee, "StorageModule")


def test_all_list():
    import authzee

    assert "Authzee" in authzee.__all__
    assert "AuthzeeAsync" in authzee.__all__
