"""Mock storage and compute modules whose every method raises an exception.

These exist to exercise the ``_StorageMeta`` / ``_ComputeMeta`` metaclass
handlers, which wrap every concrete (non-abstract) module method in a
try/except that translates any raised exception into the method's expected
result body with the correct ``error_type``.

Each mock is a full concrete implementation of its base module where every
method raises ``MockError`` so the wrapper's exception path is taken. The
metaclass then produces the shaped result body, which the tests assert against.
"""

from authzee.compute.compute_module import ComputeModule
from authzee.storage.storage_module import StorageModule


class MockError(Exception):
    """Distinct exception type raised by the mock modules."""
    pass


class MockRaisingStorage(StorageModule):
    """A `StorageModule` where every method raises `MockError`.

    Used to verify that `_StorageMeta` catches exceptions from each method and
    returns the correctly shaped result body with ``error_type == "storage"``.
    """


    def __init__(self, message: str="mock storage failure"):
        self._message = message


    async def start(self, config):
        raise MockError(self._message)


    async def shutdown(self, config):
        raise MockError(self._message)


    async def construct(self, config):
        raise MockError(self._message)


    async def destroy(self, config):
        raise MockError(self._message)


    async def list_context_defs(self, page_ref, config):
        raise MockError(self._message)


    async def get_context_def(self, context_type, config):
        raise MockError(self._message)


    async def put_context_def(self, context_def, config):
        raise MockError(self._message)


    async def delete_context_def(self, context_type, config):
        raise MockError(self._message)


    async def list_identity_defs(self, page_ref, config):
        raise MockError(self._message)


    async def get_identity_def(self, identity_type, config):
        raise MockError(self._message)


    async def put_identity_def(self, identity_def, config):
        raise MockError(self._message)


    async def delete_identity_def(self, identity_type, config):
        raise MockError(self._message)


    async def list_resource_defs(self, page_ref, config):
        raise MockError(self._message)


    async def get_resource_def(self, resource_type, config):
        raise MockError(self._message)


    async def put_resource_def(self, resource_def, config):
        raise MockError(self._message)


    async def delete_resource_def(self, resource_type, config):
        raise MockError(self._message)


    async def enact(self, grant, config):
        raise MockError(self._message)


    async def repeal(self, grant_uuid, purge, config):
        raise MockError(self._message)


    async def get_grant(self, grant_uuid, config):
        raise MockError(self._message)


    async def list_grants(
        self,
        effect,
        action,
        page_ref,
        config
    ):
        raise MockError(self._message)


    async def list_grant_refs(
        self,
        effect,
        action,
        page_ref,
        config
    ):
        raise MockError(self._message)


    async def create_latch(self, config):
        raise MockError(self._message)


    async def get_latch(self, storage_latch_uuid, config):
        raise MockError(self._message)


    async def set_latch(self, storage_latch_uuid, config):
        raise MockError(self._message)


    async def delete_latch(self, storage_latch_uuid, config):
        raise MockError(self._message)


    async def cleanup_latches(self, before, config):
        raise MockError(self._message)


class MockRaisingCompute(ComputeModule):
    """A `ComputeModule` where every method raises `MockError`.

    Used to verify that `_ComputeMeta` catches exceptions from each method and
    returns the correctly shaped result body with ``error_type == "compute"``.
    """


    def __init__(self, message: str="mock compute failure"):
        self._message = message


    async def start(
        self,
        execute,
        storage_type,
        storage_kwargs,
        config
    ):
        raise MockError(self._message)


    async def shutdown(self, config):
        raise MockError(self._message)


    async def construct(self, config):
        raise MockError(self._message)


    async def destroy(self, config):
        raise MockError(self._message)


    async def validate_context_def(self, context_def, config):
        raise MockError(self._message)


    async def validate_identity_def(self, identity_def, config):
        raise MockError(self._message)


    async def validate_resource_def(self, resource_def, config):
        raise MockError(self._message)


    async def validate_grant(self, grant, config):
        raise MockError(self._message)


    async def validate_request(self, request, config):
        raise MockError(self._message)


    async def validate_batch_request(self, batch_request, config):
        raise MockError(self._message)


    async def audit(self, request, page_ref, config):
        raise MockError(self._message)


    async def authorize(self, request, config):
        raise MockError(self._message)


    async def batch_audit(self, batch_request, page_ref, config):
        raise MockError(self._message)


    async def batch_authorize(self, batch_request, config):
        raise MockError(self._message)
