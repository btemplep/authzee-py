

import datetime
from typing import List
from uuid import UUID

from authzee.dcs import *
from authzee.exceptions import NotImplementedError
from authzee.module_locality import ModuleLocality


class InProcessStorage:

    def __init__(self, storage_ptr: dict): 
        self._storage_prt = storage_ptr


    async def start(self, authzee_config: AuthzeeConfig) -> GenericResult:
        """Start up storage module.

        - run before use
        - After this method is complete these public instance vars or getters must be available:
            - locality - Storage [Module Locality](#module-locality)
            - has_parallel_paging - if the storage module supports parallel paging (returning a page of grant page references).
        """
        self.locality = ModuleLocality.PROCESS
        self.has_parallel_paging = True
        self._storage_prt['context_defs'] = []
        self._storage_prt['identity_defs'] = []
        self._storage_prt['resource_defs'] = []
        self._storage_prt['grants'] = []
        self._storage_prt['latches'] = []
        self._storage_prt['context_defs_lut'] = {}
        self._storage_prt['identity_defs_lut'] = {}
        self._storage_prt['resource_defs_lut'] = {}
        self._storage_prt['grants_lut'] = {}
        self._storage_prt['latches_lut'] = {}
   
        return GenericResult(has_failed=False)


    async def shutdown(self, authzee_config: AuthzeeConfig) -> GenericResult:
        """Shutdown storage module.

        - clean up runtime resources
        """
        pass


    async def construct(self, authzee_config: AuthzeeConfig) -> GenericResult:
        """Construct backend resources for storage.

        - one time setup
        """
        pass


    async def destroy(self, authzee_config: AuthzeeConfig) -> GenericResult:
        """Tear down backend resources.

        - destructive - may lose all long lasting storage resources
        """
        pass


    async def get_context_defs_page(
        self,
        page_ref: str | None,
        authzee_config: AuthzeeConfig
    ) -> ContextDefsPage:
        """Get a page of context definitions.

        Pass the returned page reference to get the next page until a null page reference is returned.
        """
        if page_ref is None:
            start_index = 0
        else:
            start_index = int(page_ref)

        end_index = start_index + authzee_config.context_defs_page_size  
        next_page_ref = end_index      
        if end_index >= len(self._storage_prt['context_defs']):
            next_page_ref = None
        
        return ContextDefsPage(
            context_defs=self._storage_prt['context_defs'][start_index:end_index],
            next_page_ref=next_page_ref,
            has_failed=False
        )


    async def get_context_def(
        self, 
        context_type: str,
        authzee_config: AuthzeeConfig
    ) -> ContextDefResult:
        """Get a context definition by type.
        """
        result = ContextDefResult(
            context_def=self._storage_prt['context_defs_lut'].get(context_type, None),
            has_failed=False
        )
        if result.context_def is None:
            result.has_failed = True
            result.errors.sdk = [
                SDKError(
                    error_type="ResourceNotFoundError",
                    is_critical=True,
                    message=f"Context type '{context_type}' was not found."
                )
            ]
        
        return result


    async def put_context_def(
        self, 
        context_def: ContextDef,
        authzee_config: AuthzeeConfig
    ) -> GenericResult:
        """Add a new Context Definition or update an existing one.
        """
        raise NotImplementedError()


    async def delete_context_def(
        self, 
        context_type: str,
        authzee_config: AuthzeeConfig
    ) -> GenericResult:
        """Delete a context definition by type.
        """
        raise NotImplementedError()


    async def get_identity_defs_page(
        self,
        page_ref: str | None,
        authzee_config: AuthzeeConfig
    ) -> IdentityDefsPage:
        """Get a page of identity definitions.

        Pass the returned page reference to get the next page until a null page reference is returned.
        """
        raise NotImplementedError()


    async def get_identity_def(
        self, 
        identity_type: str,
        authzee_config: AuthzeeConfig
    ) -> IdentityDefResult:
        """Get an identity definition by type.
        """
        raise NotImplementedError()


    async def put_identity_def(
        self, 
        identity_def: IdentityDef,
        authzee_config: AuthzeeConfig
    ) -> GenericResult:
        """Add a new Identity Definition or update an existing one.
        """
        raise NotImplementedError()


    async def delete_identity_def(
        self, 
        identity_type: str,
        authzee_config: AuthzeeConfig
    ) -> GenericResult:
        """Delete an identity definition by type.
        """
        raise NotImplementedError()


    async def get_resource_defs_page(
        self,
        page_ref: str | None,
        authzee_config: AuthzeeConfig
    ) -> ResourceDefsPage:
        """Get a page of resource definitions.

        Pass the returned page reference to get the next page until a null page reference is returned.
        """
        raise NotImplementedError()


    async def get_resource_def(
        self, 
        resource_type: str,
        authzee_config: AuthzeeConfig
    ) -> ResourceDefResult:
        """Get a resource definition by type.
        """
        raise NotImplementedError()


    async def put_resource_def(
        self, 
        resource_def: ResourceDef,
        authzee_config: AuthzeeConfig
    ) -> GenericResult:
        """Add a new Resource Definition or update an existing one.
        """
        raise NotImplementedError()


    async def delete_resource_def(
        self, 
        resource_type: str,
        authzee_config: AuthzeeConfig
    ) -> GenericResult:
        """Delete a resource definition by type.
        """
        raise NotImplementedError()


    async def enact(
        self, 
        grant: Grant,
        authzee_config: AuthzeeConfig
    ) -> GenericResult:
        """Add a new grant.
        """
        raise NotImplementedError()


    async def repeal(
        self, 
        grant_uuid: UUID, 
        purge: bool,
        authzee_config: AuthzeeConfig
    ) -> GenericResult:
        """Delete a grant.
        """
        raise NotImplementedError()


    async def get_grant(
        self, 
        grant_uuid: UUID,
        authzee_config: AuthzeeConfig
    ) -> GrantResult:
        """Get a grant by UUID.
        """
        raise NotImplementedError()


    async def get_grants_page(
        self,
        effect: str | None,
        action: str | None,
        page_ref: str | None,
        authzee_config: AuthzeeConfig
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
        authzee_config: AuthzeeConfig
    ) -> PageRefsPage:
        """Retrieve a page of grant page references for parallel pagination.

        Pass the returned page reference to get the next page until a null page reference is returned.

        For some storage modules this may not be possible.
        Check the `parallel_paging` attribute on the storage module after `start()` is complete.
        """
        raise NotImplementedError()


    async def create_latch(self, authzee_config: AuthzeeConfig) -> StorageLatchResult:
        """Create a new [storage latch](#storage-latches).
        """
        raise NotImplementedError()


    async def get_latch(
        self, 
        storage_latch_uuid: UUID,
        authzee_config: AuthzeeConfig
    ) -> StorageLatchResult:
        """Get a [storage latch](#storage-latches) by UUID.
        """
        raise NotImplementedError()


    async def set_latch(
        self, 
        storage_latch_uuid: UUID,
        authzee_config: AuthzeeConfig
    ) -> StorageLatchResult:
        """Set a [storage latch](#storage-latches) by UUID.
        """
        raise NotImplementedError()


    async def delete_latch(
        self, 
        storage_latch_uuid: UUID,
        authzee_config: AuthzeeConfig
    ) -> GenericResult:
        """Delete a [storage latch](#storage-latches) by UUID.
        """
        raise NotImplementedError()


    async def cleanup_latches(
        self, 
        before: datetime.datetime,
        authzee_config: AuthzeeConfig
    ) -> GenericResult:
        """Delete all latches before the specified datetime.

        - operations should clean up their own latches, but in case of a failure this can be used to clean up zombie latches.
        """
        raise NotImplementedError()
