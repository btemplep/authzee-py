

import datetime
from typing import AsyncIterable
from uuid import UUID

from authzee.dcs import *
from authzee.exceptions import NotImplementedError
from authzee.module_locality import ModuleLocality


class StorageModule:

    def __init__(self): 
        pass


    async def start(self) -> GenericResult:
        """Start up storage module.

        - run before use
        - After this method is complete these public instance vars or getters must be available:
            - locality - Storage [Module Locality](#module-locality)
            - has_parallel_paging - if the storage module supports parallel paging (returning a page of grant page references).
        """
        self.locality = ModuleLocality.PROCESS
        self.has_parallel_paging = False
   
        return GenericResult(has_failed=False)


    async def shutdown(self) -> GenericResult:
        """Shutdown storage module.

        - clean up runtime resources
        """
        raise NotImplementedError()


    async def construct(self) -> GenericResult:
        """Construct backend resources for storage.

        - one time setup
        """
        raise NotImplementedError()


    async def destroy(self) -> GenericResult:
        """Tear down backend resources.

        - destructive - may lose all long lasting storage resources
        """
        raise NotImplementedError()


    async def get_context_defs_page(
        self,
        page_ref: str | None,
        page_size: int
    ) -> ContextDefsPage:
        """Get a page of context definitions.

        Pass the returned page reference to get the next page until a null page reference is returned.
        """
        raise NotImplementedError()


    async def get_context_def(self, context_type: str) -> ContextDefResult:
        """Get a context definition by type.
        """
        raise NotImplementedError()


    async def put_context_def(self, context_def: ContextDef) -> GenericResult:
        """Add a new Context Definition or update an existing one.
        """
        raise NotImplementedError()


    async def delete_context_def(self, context_type: str) -> GenericResult:
        """Delete a context definition by type.
        """
        raise NotImplementedError()


    async def get_identity_defs_page(
        self,
        page_ref: str | None,
        page_size: int
    ) -> IdentityDefsPage:
        """Get a page of identity definitions.

        Pass the returned page reference to get the next page until a null page reference is returned.
        """
        raise NotImplementedError()


    async def get_identity_def(self, identity_type: str) -> IdentityDefResult:
        """Get an identity definition by type.
        """
        raise NotImplementedError()


    async def put_identity_def(self, identity_def: IdentityDef) -> GenericResult:
        """Add a new Identity Definition or update an existing one.
        """
        raise NotImplementedError()


    async def delete_identity_def(self, identity_type: str) -> GenericResult:
        """Delete an identity definition by type.
        """
        raise NotImplementedError()


    async def get_resource_defs_page(
        self,
        page_ref: str | None,
        page_size: int
    ) -> ResourceDefsPage:
        """Get a page of resource definitions.

        Pass the returned page reference to get the next page until a null page reference is returned.
        """
        raise NotImplementedError()


    async def get_resource_def(self, resource_type: str) -> ResourceDefResult:
        """Get a resource definition by type.
        """
        raise NotImplementedError()


    async def list_resource_defs(self, page_size: int) -> AsyncIterable[ResourceDef]:
        """Auto-paginate resource definitions - only included if the language supports it.
        """
        raise NotImplementedError()


    async def put_resource_def(self, resource_def: ResourceDef) -> ResourceDef:
        """Add a new Resource Definition or update an existing one.
        """
        raise NotImplementedError()


    async def delete_resource_def(self, resource_type: str) -> None:
        """Delete a resource definition by type.
        """
        raise NotImplementedError()


    async def enact(self, grant: Grant) -> GenericResult:
        """Add a new grant.
        """
        raise NotImplementedError()


    async def repeal(self, grant_uuid: UUID, purge: bool) -> GenericResult:
        """Delete a grant.
        """
        raise NotImplementedError()


    async def get_grant(self, grant_uuid: UUID) -> GrantResult:
        """Get a grant by UUID.
        """
        raise NotImplementedError()


    async def get_grants_page(
        self,
        effect: str | None,
        action: str | None,
        page_ref: str | None,
        page_size: int
    ) -> GrantsPage:
        """Retrieve a page of grants.

        Pass the returned page reference to get the next page until a null page reference is returned.
        """
        raise NotImplementedError()


    async def get_grant_refs_page(
        self,
        effect: str | None,
        action: str | None,
        page_ref: str | None,
        refs_page_size: int,
        grants_page_size: int
    ) -> PageRefsPage:
        """Retrieve a page of grant page references for parallel pagination.

        Pass the returned page reference to get the next page until a null page reference is returned.

        For some storage modules this may not be possible.
        Check the `parallel_paging` attribute on the storage module after `start()` is complete.
        """
        raise NotImplementedError()


    async def create_latch(self) -> StorageLatchResult:
        """Create a new [storage latch](#storage-latches).
        """
        raise NotImplementedError()


    async def get_latch(self, storage_latch_uuid: UUID) -> StorageLatchResult:
        """Get a [storage latch](#storage-latches) by UUID.
        """
        raise NotImplementedError()


    async def set_latch(self, storage_latch_uuid: UUID) -> StorageLatchResult:
        """Set a [storage latch](#storage-latches) by UUID.
        """
        raise NotImplementedError()


    async def delete_latch(self, storage_latch_uuid: UUID) -> GenericResult:
        """Delete a [storage latch](#storage-latches) by UUID.
        """
        raise NotImplementedError()


    async def cleanup_latches(self, before: datetime.datetime) -> GenericResult:
        """Delete all latches before the specified datetime.

        - operations should clean up their own latches, but in case of a failure this can be used to clean up zombie latches.
        """
        raise NotImplementedError()
