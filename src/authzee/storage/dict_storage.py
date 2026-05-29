

__all__ = [
    "DictStorage",
]

import datetime
from typing import List
from uuid import uuid4

from authzee.storage.storage_module import StorageModule
from authzee.types import *
from authzee.module_locality import ModuleLocality


class DictStorage(StorageModule):

    def __init__(self, storage_dict: dict): 
        super().__init__()
        self._storage_dict = storage_dict


    async def start(self, config: AuthzeeConfig) -> GenericResult:
        """Start up storage module.

        - run before use
        - After this method is complete these public instance vars or getters must be available:
            - locality - Storage [Module Locality](#module-locality)
            - has_parallel_paging - if the storage module supports parallel paging (returning a page of grant page references).
        """
        self.locality = ModuleLocality.PROCESS
        self.has_parallel_paging = True
   
        return {
            "has_failed": False, 
            "errors": {}
        }


    async def shutdown(self, config: AuthzeeConfig) -> GenericResult:
        """Shutdown storage module.

        - clean up runtime resources
        """
        return {
            "has_failed": False, 
            "errors": {}
        }


    async def construct(self, config: AuthzeeConfig) -> GenericResult:
        """Construct backend resources for storage.

        - one time setup
        """
        self._storage_dict['context_defs_lut'] = {}
        self._storage_dict['identity_defs_lut'] = {}
        self._storage_dict['resource_defs_lut'] = {}
        self._storage_dict['grants_lut'] = {}
        self._storage_dict['latches_lut'] = {}

        return {
            "has_failed": False, 
            "errors": {}
        }


    async def destroy(self, config: AuthzeeConfig) -> GenericResult:
        """Tear down backend resources.

        - destructive - may lose all long lasting storage resources
        """
        self._storage_dict.pop("context_defs_lut", None)
        self._storage_dict.pop("identity_defs_lut", None)
        self._storage_dict.pop("resource_defs_lut", None)
        self._storage_dict.pop("grants_lut", None)
        self._storage_dict.pop("latches_lut", None)

        return {
            "has_failed": False, 
            "errors": {}
        }


    async def list_context_defs(
        self,
        page_ref: str | None,
        config: AuthzeeConfig
    ) -> ContextDefsPage:
        """Get a page of context definitions.

        Pass the returned page reference to get the next page until a null page reference is returned.
        """
        if page_ref is None:
            start_index = 0
        else:
            start_index = int(page_ref)

        context_defs = list(self._storage_dict['context_defs_lut'].values())
        end_index = start_index + config['context_defs_page_size']  
        
        return {
            "context_defs": context_defs[start_index:end_index],
            "next_page_ref": str(end_index) if end_index < len(context_defs) else None,
            "has_failed": False,
            "errors": {}
        }


    async def get_context_def(
        self, 
        context_type: str,
        config: AuthzeeConfig
    ) -> ContextDefResult:
        """Get a context definition by type.
        """
        context_def = self._storage_dict['context_defs_lut'].get(context_type, None)
        if context_def is None:
            return {
                "context_def": None,
                "has_failed": True,
                "errors": {
                    "resource_not_found": [
                        {
                            "is_critical": True,
                            "message": f"Context type '{context_type}' was not found."
                        }
                    ]
                }
            }
        
        return {
            "context_def": context_def,
            "has_failed": False,
            "errors": {}
        }


    async def put_context_def(
        self, 
        context_def: ContextDef,
        config: AuthzeeConfig
    ) -> GenericResult:
        """Add a new Context Definition or update an existing one.

        Validated by authzee class
        """
        self._storage_dict['context_defs_lut'][context_def['context_type']] = context_def

        return {
            "has_failed": False, 
            "errors": {}
        }
        

    async def delete_context_def(
        self, 
        context_type: str,
        config: AuthzeeConfig
    ) -> GenericResult:
        """Delete a context definition by type.
        """
        self._storage_dict['context_defs_lut'].pop(context_type, None)
        
        return {
            "has_failed": False, 
            "errors": {}
        }


    async def list_identity_defs(
        self,
        page_ref: str | None,
        config: AuthzeeConfig
    ) -> IdentityDefsPage:
        """Get a page of identity definitions.

        Pass the returned page reference to get the next page until a null page reference is returned.
        """
        if page_ref is None:
            start_index = 0
        else:
            start_index = int(page_ref)

        identity_defs = list(self._storage_dict['identity_defs_lut'].values())
        end_index = start_index + config['identity_defs_page_size']       
        
        return {
            "identity_defs": identity_defs[start_index:end_index],
            "next_page_ref": str(end_index) if end_index < len(identity_defs) else None,
            "has_failed": False,
            "errors": {}
        }


    async def get_identity_def(
        self, 
        identity_type: str,
        config: AuthzeeConfig
    ) -> IdentityDefResult:
        """Get an identity definition by type.
        """
        identity_def = self._storage_dict['identity_defs_lut'].get(identity_type, None)
        if identity_def is None:
            return {
                "identity_def": None,
                "has_failed": True,
                "errors": {
                    "resource_not_found": [
                        {
                            "is_critical": True,
                            "message": f"identity type '{identity_type}' was not found."
                        }
                    ]
                }
            }
        
        return {
            "identity_def": identity_def,
            "has_failed": False,
            "errors": {}
        }


    async def put_identity_def(
        self, 
        identity_def: IdentityDef,
        config: AuthzeeConfig
    ) -> GenericResult:
        """Add a new Identity Definition or update an existing one.
        """
        self._storage_dict['identity_defs_lut'][identity_def['identity_type']] = identity_def

        return {
            "has_failed": False, 
            "errors": {}
        }


    async def delete_identity_def(
        self, 
        identity_type: str,
        config: AuthzeeConfig
    ) -> GenericResult:
        """Delete an identity definition by type.
        """
        self._storage_dict['identity_defs_lut'].pop(identity_type, None)
        
        return {
            "has_failed": False, 
            "errors": {}
        }


    async def list_resource_defs(
        self,
        page_ref: str | None,
        config: AuthzeeConfig
    ) -> ResourceDefsPage:
        """Get a page of resource definitions.

        Pass the returned page reference to get the next page until a null page reference is returned.
        """
        if page_ref is None:
            start_index = 0
        else:
            start_index = int(page_ref)

        resource_defs = list(self._storage_dict['resource_defs_lut'].values())
        end_index = start_index + config['resource_defs_page_size'] 
        
        return {
            "resource_defs": resource_defs[start_index:end_index],
            "next_page_ref": str(end_index) if end_index < len(resource_defs) else None,
            "has_failed": False,
            "errors": {}
        }


    async def get_resource_def(
        self, 
        resource_type: str,
        config: AuthzeeConfig
    ) -> ResourceDefResult:
        """Get a resource definition by type.
        """
        resource_def = self._storage_dict['resource_defs_lut'].get(resource_type, None)
        if resource_def is None:
            return {
                "resource_def": None,
                "has_failed": True,
                "errors": {
                    "resource_not_found": [
                        {
                            "is_critical": True,
                            "message": f"resource type '{resource_type}' was not found."
                        }
                    ]
                }
            }
        
        return {
            "resource_def": resource_def,
            "has_failed": False,
            "errors": {}
        }


    async def put_resource_def(
        self, 
        resource_def: ResourceDef,
        config: AuthzeeConfig
    ) -> GenericResult:
        """Add a new Resource Definition or update an existing one.
        """
        self._storage_dict['resource_defs_lut'][resource_def['resource_type']] = resource_def

        return {
            "has_failed": False, 
            "errors": {}
        }


    async def delete_resource_def(
        self, 
        resource_type: str,
        config: AuthzeeConfig
    ) -> GenericResult:
        """Delete a resource definition by type.
        """
        self._storage_dict['resource_defs_lut'].pop(resource_type, None)
        
        return {
            "has_failed": False, 
            "errors": {}
        }


    async def enact(
        self, 
        grant: Grant,
        config: AuthzeeConfig
    ) -> GenericResult:
        """Add a new grant.
        """
        self._storage_dict['grants_lut'][grant['grant_uuid']] = grant

        return {
            "has_failed": False, 
            "errors": {}
        }


    async def repeal(
        self, 
        grant_uuid: str, 
        purge: bool,
        config: AuthzeeConfig
    ) -> GenericResult:
        """Delete a grant.
        """
        self._storage_dict['grants_lut'].pop(grant_uuid, None)

        return {
            "has_failed": False, 
            "errors": {}
        }


    async def get_grant(
        self, 
        grant_uuid: str,
        config: AuthzeeConfig
    ) -> GrantResult:
        """Get a grant by UUID.
        """
        grant = self._storage_dict['grants_lut'].get(grant_uuid, None)
        if grant is None:
            return {
                "grant": None,
                "has_failed": True,
                "errors": {
                    "resource_not_found": [
                        {
                            "is_critical": True,
                            "message": f"Grant with UUID '{grant_uuid}' was not found."
                        }
                    ]
                }
            }
        
        return {
            "grant": grant,
            "has_failed": False,
            "errors": {}
        }


    async def list_grants(
        self,
        effect: str | None,
        action: str | None,
        page_ref: str | None,
        config: AuthzeeConfig
    ) -> GrantsPage:
        """Retrieve a page of grants.

        Pass the returned page reference to get the next page until a null page reference is returned.
        """
        if page_ref is None:
            start_index = 0
        else:
            start_index = int(page_ref)

        grants: List[Grant] = list(self._storage_dict['grants_lut'].values())
        if effect is not None:
            grants = [g for g in grants if g['effect'] == effect]
        
        if action is not None:
            grants = [g for g in grants if action in g['actions']]

        end_index = start_index + config['grants_page_size']  
        
        return {
            "grants": grants[start_index:end_index],
            "next_page_ref": str(end_index) if end_index < len(grants) else None,
            "has_failed": False,
            "errors": {}
        }


    async def list_grant_refs(
        self,
        effect: str | None,
        action: str | None,
        page_ref: str | None,
        config: AuthzeeConfig
    ) -> PageRefsPage:
        """Retrieve a page of grant page references for parallel pagination.

        Pass the returned page reference to get the next page until a null page reference is returned.

        For some storage modules this may not be possible.
        Check the `parallel_paging` attribute on the storage module after `start()` is complete.
        """
        if page_ref is None:
            start_index = 0
        else:
            start_index = int(page_ref)

        grants: List[Grant] = list(self._storage_dict['grants_lut'].values())
        if effect is not None:
            grants = [g for g in grants if g['effect'] == effect]
        
        if action is not None:
            grants = [g for g in grants if action in g['actions']]
        
        num_grants = len(grants)
        refs = []
        for _ in range(config['grant_refs_page_size']):
            end_index = start_index + config['grants_page_size']  
            next_page_ref = end_index          
            refs.append(start_index)
            start_index = end_index
            if end_index >= num_grants:
                next_page_ref = None
                break
        
        return {
            "page_refs": refs,
            "next_page_ref": next_page_ref,
            "has_failed": False,
            "errors": {}
        }


    async def create_latch(self, config: AuthzeeConfig) -> StorageLatchResult:
        """Create a new [storage latch](#storage-latches).
        """
        latch_uuid = str(uuid4())
        latch = {
            "storage_latch_uuid": latch_uuid,
            "is_set": False,
            "created_at": datetime.datetime.now(tz=datetime.timezone.utc)
        }
        self._storage_dict['latches_lut'][latch_uuid] = latch

        return {
            "storage_latch": latch,
            "has_failed": False,
            "errors": {}
        }


    async def get_latch(
        self, 
        storage_latch_uuid: str,
        config: AuthzeeConfig
    ) -> StorageLatchResult:
        """Get a [storage latch](#storage-latches) by UUID.
        """
        latch = self._storage_dict['latches_lut'].get(storage_latch_uuid, None)
        if latch is None:
            return {
                "storage_latch": None,
                "has_failed": True,
                "errors": {
                    "resource_not_found": [
                        {
                            "is_critical": True,
                            "message": f"Storage latch with UUID '{storage_latch_uuid}' was not found."
                        }
                    ]
                }
            }
        
        return {
            "storage_latch": latch,
            "has_failed": False,
            "errors": {}
        }


    async def set_latch(
        self, 
        storage_latch_uuid: str,
        config: AuthzeeConfig
    ) -> StorageLatchResult:
        """Set a [storage latch](#storage-latches) by UUID.
        """
        result = await self.get_latch(
            storage_latch_uuid=storage_latch_uuid,
            config=config
        )
        if result['has_failed'] is True:
            return result
    
        result['storage_latch']['is_set'] = True
        
        return result


    async def delete_latch(
        self, 
        storage_latch_uuid: str,
        config: AuthzeeConfig
    ) -> GenericResult:
        """Delete a [storage latch](#storage-latches) by UUID.
        """
        self._storage_dict['latches_lut'].pop(storage_latch_uuid, None)

        return {
            "has_failed": False, 
            "errors": {}
        }



    async def cleanup_latches(
        self, 
        before: datetime.datetime,
        config: AuthzeeConfig
    ) -> GenericResult:
        """Delete all latches before the specified datetime.

        - operations should clean up their own latches, but in case of a failure this can be used to clean up zombie latches.
        """
        new_lut = {}
        for lu, l in self._storage_dict['latches_lut'].items():
            if l['created_at'] > before:
                new_lut[lu] = l
        
        self._storage_dict['latches_lut'] = new_lut
        
        return {
            "has_failed": False, 
            "errors": {}
        }