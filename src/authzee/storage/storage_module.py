"""Base storage module for Authzee.

See [](authzee.storage.storage_module.StorageModule)
"""

__all__ = [
    "StorageModule"
]

from abc import ABCMeta, abstractmethod
import datetime
import functools
from typing import Any, Callable

from authzee.module_locality import ModuleLocality
from authzee.types.authzee import *
from authzee.types.config import (
    CleanupLatchesConfig,
    CreateLatchConfig,
    DeleteContextDefConfig,
    DeleteIdentityDefConfig,
    DeleteLatchConfig,
    DeleteResourceDefConfig,
    EnactConfig,
    GetContextDefConfig,
    GetGrantConfig,
    GetIdentityDefConfig,
    GetLatchConfig,
    GetResourceDefConfig,
    ListContextDefsConfig,
    ListGrantRefsConfig,
    ListGrantsConfig,
    ListIdentityDefsConfig,
    ListResourceDefsConfig,
    PutContextDefConfig,
    PutIdentityDefConfig,
    PutResourceDefConfig,
    RepealConfig,
    SetLatchConfig,
    StorageConstructConfig,
    StorageDestroyConfig,
    StorageShutdownConfig,
    StorageStartConfig
)


def _generic_result_handler(func, error_type):
    @functools.wraps(func)
    async def wrapper(self, *args, **kwargs) -> GenericResult:
        try:
            return await func(self, *args, **kwargs)

        except Exception as exc:
            return {
                "error": {
                    "error_type": error_type,
                    "message": f"[{exc.__class__.__qualname__}] {exc}"
                }
            }

    return wrapper


class _ModuleMeta(ABCMeta):
    _error_type: str = "unknown"
    _handler_map: dict[str, Callable] = {}


    def __new__(mcls, name: str, bases, namespace: dict[str, Any]):
        for attr_name, attr_value in namespace.items():
            if (
                attr_name in mcls._handler_map
                and getattr(attr_value, "__isabstractmethod__", False) is False
            ):
                namespace[attr_name] = mcls._handler_map[attr_name](attr_value, mcls._error_type)

        return super().__new__(mcls, name, bases, namespace)


class _StorageMeta(_ModuleMeta):
    _error_type: str = "storage"
    _handler_map: dict[str, Callable] = {
        "list_context_defs": _generic_result_handler
    }


class StorageModule(metaclass=_StorageMeta):


    @abstractmethod
    async def start(self, config: StorageStartConfig) -> GenericResult:
        """Start up storage module.

        - run before use
        - After this method is complete these public instance vars or getters must be available:
            - locality - Storage [Module Locality](#module-locality)
            - has_parallel_paging - if the storage module supports parallel paging (returning a page of grant page references).
        """
        self.locality = ModuleLocality.PROCESS
        self.has_parallel_paging = False

        return {
            "error": None
        }


    @abstractmethod
    async def shutdown(self, config: StorageShutdownConfig) -> GenericResult:
        """Shutdown storage module.

        - clean up runtime resources
        """
        ...


    @abstractmethod
    async def construct(self, config: StorageConstructConfig) -> GenericResult:
        """Construct backend resources for storage.

        - one time setup
        """
        ...


    @abstractmethod
    async def destroy(self, config: StorageDestroyConfig) -> GenericResult:
        """Tear down backend resources.

        - destructive - may lose all long lasting storage resources
        """
        ...


    @abstractmethod
    async def list_context_defs(
        self,
        page_ref: str | None,
        config: ListContextDefsConfig
    ) -> ContextDefsPage:
        """Get a page of context definitions.

        Pass the returned page reference to get the next page until a null page reference is returned.
        """
        ...


    @abstractmethod
    async def get_context_def(
        self,
        context_type: str,
        config: GetContextDefConfig
    ) -> ContextDefResult:
        """Get a context definition by type.
        """
        ...


    @abstractmethod
    async def put_context_def(
        self,
        context_def: ContextDef,
        config: PutContextDefConfig
    ) -> GenericResult:
        """Add a new Context Definition or update an existing one.
        """
        ...


    @abstractmethod
    async def delete_context_def(
        self,
        context_type: str,
        config: DeleteContextDefConfig
    ) -> GenericResult:
        """Delete a context definition by type.
        """
        ...


    @abstractmethod
    async def list_identity_defs(
        self,
        page_ref: str | None,
        config: ListIdentityDefsConfig
    ) -> IdentityDefsPage:
        """Get a page of identity definitions.

        Pass the returned page reference to get the next page until a null page reference is returned.
        """
        ...


    @abstractmethod
    async def get_identity_def(
        self,
        identity_type: str,
        config: GetIdentityDefConfig
    ) -> IdentityDefResult:
        """Get an identity definition by type.
        """
        ...


    @abstractmethod
    async def put_identity_def(
        self,
        identity_def: IdentityDef,
        config: PutIdentityDefConfig
    ) -> GenericResult:
        """Add a new Identity Definition or update an existing one.
        """
        ...


    @abstractmethod
    async def delete_identity_def(
        self,
        identity_type: str,
        config: DeleteIdentityDefConfig
    ) -> GenericResult:
        """Delete an identity definition by type.
        """
        ...


    @abstractmethod
    async def list_resource_defs(
        self,
        page_ref: str | None,
        config: ListResourceDefsConfig
    ) -> ResourceDefsPage:
        """Get a page of resource definitions.

        Pass the returned page reference to get the next page until a null page reference is returned.
        """
        ...


    @abstractmethod
    async def get_resource_def(
        self,
        resource_type: str,
        config: GetResourceDefConfig
    ) -> ResourceDefResult:
        """Get a resource definition by type.
        """
        ...


    @abstractmethod
    async def put_resource_def(
        self,
        resource_def: ResourceDef,
        config: PutResourceDefConfig
    ) -> GenericResult:
        """Add a new Resource Definition or update an existing one.
        """
        ...


    @abstractmethod
    async def delete_resource_def(
        self,
        resource_type: str,
        config: DeleteResourceDefConfig
    ) -> GenericResult:
        """Delete a resource definition by type.
        """
        ...


    @abstractmethod
    async def enact(self, grant: Grant, config: EnactConfig) -> GenericResult:
        """Add a new grant.
        """
        ...


    @abstractmethod
    async def repeal(
        self,
        grant_uuid: str,
        purge: bool,
        config: RepealConfig
    ) -> GenericResult:
        """Delete a grant.
        """
        ...


    @abstractmethod
    async def get_grant(
        self,
        grant_uuid: str,
        config: GetGrantConfig
    ) -> GrantResult:
        """Get a grant by UUID.
        """
        ...


    @abstractmethod
    async def list_grants(
        self,
        effect: str | None,
        action: str | None,
        page_ref: str | None,
        config: ListGrantsConfig
    ) -> GrantsPage:
        """Retrieve a page of grants.

        Pass the returned page reference to get the next page until a null page reference is returned.
        """
        ...


    @abstractmethod
    async def list_grant_refs(
        self,
        effect: str | None,
        action: str | None,
        page_ref: str | None,
        config: ListGrantRefsConfig
    ) -> PageRefsPage:
        """Retrieve a page of grant page references for parallel pagination.

        Pass the returned page reference to get the next page until a null page reference is returned.

        For some storage modules this may not be possible.
        Check the `parallel_paging` attribute on the storage module after `start()` is complete.
        """
        ...


    @abstractmethod
    async def create_latch(self, config: CreateLatchConfig) -> StorageLatchResult:
        """Create a new [storage latch](#storage-latches).
        """
        ...


    @abstractmethod
    async def get_latch(
        self,
        storage_latch_uuid: str,
        config: GetLatchConfig
    ) -> StorageLatchResult:
        """Get a [storage latch](#storage-latches) by UUID.
        """
        ...


    @abstractmethod
    async def set_latch(
        self,
        storage_latch_uuid: str,
        config: SetLatchConfig
    ) -> StorageLatchResult:
        """Set a [storage latch](#storage-latches) by UUID.
        """
        ...


    @abstractmethod
    async def delete_latch(
        self,
        storage_latch_uuid: str,
        config: DeleteLatchConfig
    ) -> GenericResult:
        """Delete a [storage latch](#storage-latches) by UUID.
        """
        ...


    @abstractmethod
    async def cleanup_latches(
        self,
        before: datetime.datetime,
        config: CleanupLatchesConfig
    ) -> GenericResult:
        """Delete all latches before the specified datetime.

        - operations should clean up their own latches, but in case of a failure this can be used to clean up zombie latches.
        """
        ...
