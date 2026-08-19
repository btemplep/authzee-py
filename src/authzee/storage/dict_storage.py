"""Dict-based in-memory storage module for Authzee.

See [](authzee.storage.dict_storage.DictStorage)
"""

__all__ = [
    "DictStorage"
]

import datetime
from typing import List
from uuid import uuid4

from authzee.module_locality import ModuleLocality
from authzee.storage.storage_module import StorageModule
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


class DictStorage(StorageModule):


    def __init__(self, storage_dict: dict):
        super().__init__()
        self._storage_dict = storage_dict


    async def start(self, config: StorageStartConfig) -> GenericResult:
        self.locality = ModuleLocality.PROCESS
        self.has_parallel_paging = True

        return {
            "error": None
        }


    async def shutdown(self, config: StorageShutdownConfig) -> GenericResult:
        return {
            "error": None
        }


    async def construct(self, config: StorageConstructConfig) -> GenericResult:
        self._storage_dict['context_defs_lut'] = {}
        self._storage_dict['identity_defs_lut'] = {}
        self._storage_dict['resource_defs_lut'] = {}
        self._storage_dict['grants_lut'] = {}
        self._storage_dict['latches_lut'] = {}

        return {
            "error": None
        }


    async def destroy(self, config: StorageDestroyConfig) -> GenericResult:
        self._storage_dict.pop("context_defs_lut", None)
        self._storage_dict.pop("identity_defs_lut", None)
        self._storage_dict.pop("resource_defs_lut", None)
        self._storage_dict.pop("grants_lut", None)
        self._storage_dict.pop("latches_lut", None)

        return {
            "error": None
        }


    async def list_context_defs(
        self,
        page_ref: str | None,
        config: ListContextDefsConfig
    ) -> ContextDefsPage:
        if page_ref is None:
            start_index = 0
        else:
            start_index = int(page_ref)

        context_defs = list(self._storage_dict['context_defs_lut'].values())
        end_index = start_index + config['page_size']

        return {
            "context_defs": context_defs[start_index:end_index],
            "next_page_ref": str(end_index) if end_index < len(context_defs) else None,
            "error": None
        }


    async def get_context_def(
        self,
        context_type: str,
        config: GetContextDefConfig
    ) -> ContextDefResult:
        context_def = self._storage_dict['context_defs_lut'].get(
            context_type,
            None
        )
        if context_def is None:
            return {
                "context_def": None,
                "error": {
                    "error_type": "resource_not_found",
                    "message": f"Context type '{context_type}' was not found."
                }
            }

        return {
            "context_def": context_def,
            "error": None
        }


    async def put_context_def(
        self,
        context_def: ContextDef,
        config: PutContextDefConfig
    ) -> GenericResult:
        self._storage_dict['context_defs_lut'][context_def['context_type']] = context_def

        return {
            "error": None
        }


    async def delete_context_def(
        self,
        context_type: str,
        config: DeleteContextDefConfig
    ) -> GenericResult:
        self._storage_dict['context_defs_lut'].pop(
            context_type,
            None
        )

        return {
            "error": None
        }


    async def list_identity_defs(
        self,
        page_ref: str | None,
        config: ListIdentityDefsConfig
    ) -> IdentityDefsPage:
        if page_ref is None:
            start_index = 0
        else:
            start_index = int(page_ref)

        identity_defs = list(self._storage_dict['identity_defs_lut'].values())
        end_index = start_index + config['page_size']

        return {
            "identity_defs": identity_defs[start_index:end_index],
            "next_page_ref": str(end_index) if end_index < len(identity_defs) else None,
            "error": None
        }


    async def get_identity_def(
        self,
        identity_type: str,
        config: GetIdentityDefConfig
    ) -> IdentityDefResult:
        identity_def = self._storage_dict['identity_defs_lut'].get(
            identity_type,
            None
        )
        if identity_def is None:
            return {
                "identity_def": None,
                "error": {
                    "error_type": "resource_not_found",
                    "message": f"identity type '{identity_type}' was not found."
                }
            }

        return {
            "identity_def": identity_def,
            "error": None
        }


    async def put_identity_def(
        self,
        identity_def: IdentityDef,
        config: PutIdentityDefConfig
    ) -> GenericResult:
        self._storage_dict['identity_defs_lut'][identity_def['identity_type']] = identity_def

        return {
            "error": None
        }


    async def delete_identity_def(
        self,
        identity_type: str,
        config: DeleteIdentityDefConfig
    ) -> GenericResult:
        self._storage_dict['identity_defs_lut'].pop(
            identity_type,
            None
        )

        return {
            "error": None
        }


    async def list_resource_defs(
        self,
        page_ref: str | None,
        config: ListResourceDefsConfig
    ) -> ResourceDefsPage:
        if page_ref is None:
            start_index = 0
        else:
            start_index = int(page_ref)

        resource_defs = list(self._storage_dict['resource_defs_lut'].values())
        end_index = start_index + config['page_size']

        return {
            "resource_defs": resource_defs[start_index:end_index],
            "next_page_ref": str(end_index) if end_index < len(resource_defs) else None,
            "error": None
        }


    async def get_resource_def(
        self,
        resource_type: str,
        config: GetResourceDefConfig
    ) -> ResourceDefResult:
        resource_def = self._storage_dict['resource_defs_lut'].get(
            resource_type,
            None
        )
        if resource_def is None:
            return {
                "resource_def": None,
                "error": {
                    "error_type": "resource_not_found",
                    "message": f"resource type '{resource_type}' was not found."
                }
            }

        return {
            "resource_def": resource_def,
            "error": None
        }


    async def put_resource_def(
        self,
        resource_def: ResourceDef,
        config: PutResourceDefConfig
    ) -> GenericResult:
        self._storage_dict['resource_defs_lut'][resource_def['resource_type']] = resource_def

        return {
            "error": None
        }


    async def delete_resource_def(
        self,
        resource_type: str,
        config: DeleteResourceDefConfig
    ) -> GenericResult:
        self._storage_dict['resource_defs_lut'].pop(
            resource_type,
            None
        )

        return {
            "error": None
        }


    async def enact(self, grant: Grant, config: EnactConfig) -> GenericResult:
        self._storage_dict['grants_lut'][grant['grant_uuid']] = grant

        return {
            "error": None
        }


    async def repeal(
        self,
        grant_uuid: str,
        purge: bool,
        config: RepealConfig
    ) -> GenericResult:
        self._storage_dict['grants_lut'].pop(grant_uuid, None)

        return {
            "error": None
        }


    async def get_grant(
        self,
        grant_uuid: str,
        config: GetGrantConfig
    ) -> GrantResult:
        grant = self._storage_dict['grants_lut'].get(grant_uuid, None)
        if grant is None:
            return {
                "grant": None,
                "error": {
                    "error_type": "resource_not_found",
                    "message": f"Grant with UUID '{grant_uuid}' was not found."
                }
            }

        return {
            "grant": grant,
            "error": None
        }


    async def list_grants(
        self,
        effect: str | None,
        action: str | None,
        page_ref: str | None,
        config: ListGrantsConfig
    ) -> GrantsPage:
        if page_ref is None:
            start_index = 0
        else:
            start_index = int(page_ref)

        grants: List[Grant] = list(self._storage_dict['grants_lut'].values())
        if effect is not None:
            grants = [g for g in grants if g['effect'] == effect]

        if action is not None:
            grants = [g for g in grants if action in g['actions']]

        end_index = start_index + config['page_size']

        return {
            "grants": grants[start_index:end_index],
            "next_page_ref": str(end_index) if end_index < len(grants) else None,
            "error": None
        }


    async def list_grant_refs(
        self,
        effect: str | None,
        action: str | None,
        page_ref: str | None,
        config: ListGrantRefsConfig
    ) -> PageRefsPage:
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
        for _ in range(config['page_size']):
            end_index = start_index + config['page_size']
            next_page_ref = end_index
            refs.append(start_index)
            start_index = end_index
            if end_index >= num_grants:
                next_page_ref = None
                break

        return {
            "page_refs": refs,
            "next_page_ref": next_page_ref,
            "error": None
        }


    async def create_latch(self, config: CreateLatchConfig) -> StorageLatchResult:
        latch_uuid = str(uuid4())
        latch = {
            "storage_latch_uuid": latch_uuid,
            "is_set": False,
            "created_at": datetime.datetime.now(tz=datetime.timezone.utc)
        }
        self._storage_dict['latches_lut'][latch_uuid] = latch

        return {
            "storage_latch": latch,
            "error": None
        }


    async def get_latch(
        self,
        storage_latch_uuid: str,
        config: GetLatchConfig
    ) -> StorageLatchResult:
        latch = self._storage_dict['latches_lut'].get(
            storage_latch_uuid,
            None
        )
        if latch is None:
            return {
                "storage_latch": None,
                "error": {
                    "error_type": "resource_not_found",
                    "message": f"Storage latch with UUID '{storage_latch_uuid}' was not found."
                }
            }

        return {
            "storage_latch": latch,
            "error": None
        }


    async def set_latch(
        self,
        storage_latch_uuid: str,
        config: SetLatchConfig
    ) -> StorageLatchResult:
        result = await self.get_latch(
            storage_latch_uuid=storage_latch_uuid,
            config=config
        )
        if result['error'] is not None:
            return result

        result['storage_latch']['is_set'] = True

        return result


    async def delete_latch(
        self,
        storage_latch_uuid: str,
        config: DeleteLatchConfig
    ) -> GenericResult:
        self._storage_dict['latches_lut'].pop(
            storage_latch_uuid,
            None
        )

        return {
            "error": None
        }


    async def cleanup_latches(
        self,
        before: datetime.datetime,
        config: CleanupLatchesConfig
    ) -> GenericResult:
        new_lut = {}
        for lu, l in self._storage_dict['latches_lut'].items():
            if l['created_at'] > before:
                new_lut[lu] = l

        self._storage_dict['latches_lut'] = new_lut

        return {
            "error": None
        }
