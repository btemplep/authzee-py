
import asyncio
import datetime
from typing import Any, AsyncIterable, Callable, Coroutine, Dict, List, Type
from uuid import UUID

from authzee.types import *
from authzee.exceptions import *
from authzee import core
from authzee.compute.compute_module import ComputeModule
from authzee.storage.storage_module import StorageModule

from authzee import exceptions
from authzee.module_locality import locality_compatibility


_default_config = {
    "context_defs_page_size": 100,
    "identity_defs_page_size": 100,
    "resource_defs_page_size": 100,
    "grants_page_size": 100,
    "grant_refs_page_size": 10,
    "authorize_parallel_paging": True,
    "batch_authorize_parallel_paging": True,
    "raise_crits": True
}
_exception_map = {
    "definition": DefinitionError,
    
}


class AuthzeeAsync:
    """Authzee application with asyncio.

    Parameters
    ----------
    execute : Callable[[str, Any], Any]
        JSON query function.
    compute_type : Type[ComputeModule]
        Compute Module Type.
    compute_kwargs : Dict[str, Any]
        Compute module KWArgs used to create instances.
    storage_type : Type[StorageModule]
        Storage Module Type. 
    storage_kwargs : Dict[str, Any]
        Storage module KWArgs used to create instances.
    config : AuthzeeConfig
        Authzee configuration.
    compute_config: AuthzeeConfig
        Override the default config for calls that are passed to the compute backend.
        - audit
        - authorize
        - batch_audit
        - batch_authorize
    
    Examples
    --------
    Example here
    """

    def __init__(
        self, 
        execute: Callable[[str, Any], Any],
        compute_type: Type[ComputeModule],
        compute_kwargs: Dict[str, Any],
        storage_type: Type[StorageModule],
        storage_kwargs: Dict[str, Any],
        config: AuthzeeConfig,
        compute_config: AuthzeeConfig
    ):
        self.execute = execute
        self.compute_type = compute_type
        self.compute_kwargs = compute_kwargs
        self.storage_type = storage_type
        self.storage_kwargs= storage_kwargs
        self.config = _default_config | config
        self.compute_config = config | compute_config
        self._compute: ComputeModule = None
        self._storage: StorageModule = None
    

    async def _raise_result(
        self,
        coro: Coroutine[Any, Any, GenericResult],
        config: AuthzeeConfig
    ) -> Any:
        result = await coro
        if config['raise_crits'] is True and result['has_failed'] is True:
            for et in result['errors']:
                for err in result['errors'][et]:
                    err: GenericError
                    if err.is_critical:
                        if et == "sdk": 
                            err: SDKError
                            raise _exception_map[err.error_type](
                                is_critical=True,
                                message=err.message,
                                result=result
                            )
                        else:
                            raise _exception_map[err['error_type']](
                                is_critical=True,
                                message=err['message']
                            )
        
        return result
    

    async def _get_results(
        self,
        coros: List[Coroutine],
        error_messages: List[str | None],
        config: AuthzeeConfig
    ) -> GenericResult:
        """Check results for errors and raise upon failure if selected. 
        """
        results = await asyncio.gather(*coros)
        for result, message in zip(results, error_messages):
            if result['has_failed'] is True:
                if config['raise_crits'] is True:
                    raise exceptions.AuthzeeSDKError(
                        message=message if message is not None else "",
                        is_critical=True,
                        result=result
                    )
                else:
                    return result
            
        return results

    
    async def start(self, config: AuthzeeConfig | None = None) -> GenericResult:
        config = config if config is not None else self.config | config
        self._compute = self.compute_type(**self.compute_kwargs)
        self._storage = self.storage_type(**self.storage_kwargs)

        return await self._get_results(
            coros=[
                self._compute.start(
                    execute=self.execute,
                    storage_type=self.storage_type,
                    storage_kwargs=self.storage_kwargs,
                    config=config
                ),
                self._storage.start(config)
            ],
            error_messages=[
                "Error when starting the compute module.",
                "Error when starting the storage module."
            ],
            config=config
        )


    async def shutdown(
        self, 
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        return await self._get_results(
            coros=[
                self._compute.shutdown(
                    execute=self.execute,
                    storage_type=self.storage_type,
                    storage_kwargs=self.storage_kwargs,
                    config=config
                ),
                self._storage.shutdown(config)
            ],
            error_messages=[
                "Error when shutting down the compute module.",
                "Error when shutting down the storage module."
            ],
            config=config
        )


    async def construct(
        self, 
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        config = config if config is not None else self.config | config
        
        return await self._get_results(
            coros=[
                self._compute.construct(
                    execute=self.execute,
                    storage_type=self.storage_type,
                    storage_kwargs=self.storage_kwargs,
                    config=config
                ),
                self._storage.construct(config)
            ],
            error_messages=[
                "Error when running construct in the compute module.",
                "Error when running construct in the storage module."
            ],
            config=config
        )


    async def destroy(
        self, 
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        config = config if config is not None else self.config | config
        
        return await self._get_results(
            coros=[
                self._compute.destroy(
                    execute=self.execute,
                    storage_type=self.storage_type,
                    storage_kwargs=self.storage_kwargs,
                    config=config
                ),
                self._storage.destroy(config)
            ],
            error_messages=[
                "Error when running destroy in the compute module.",
                "Error when running destroy in the storage module."
            ],
            config=config
        )


    async def validate_context_def(
        self,
        context_def: ContextDef, 
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        """Validate a context definition.
        """
        config = config if config is not None else self.config | config
        
        return core.validate_context_def(context_def=context_def)


    async def get_context_defs_page(
        self, 
        page_ref: str | None,
        config: AuthzeeConfig | None = None
    ) -> ContextDefsPage:
        config = config if config is not None else self.config | config

        return await self._storage.get_context_defs_page(
            page_ref=page_ref,
            config=config
        )


    async def get_context_def(
        self, 
        context_type: str, 
        config: AuthzeeConfig | None = None
    ) -> ContextDefResult:
        config = config if config is not None else self.config | config

        return await self._storage.get_context_def(
            context_type=context_type,
            config=config
        )


    async def list_context_defs(
        self,
        config: AuthzeeConfig | None = None
    ) -> AsyncIterable[ContextDef]:
        config = config if config is not None else self.config | config

        return await self._storage


    async def put_context_def(
        self, 
        context_def: ContextDef, 
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        config = config if config is not None else self.config | config

        return await self._storage.put_context_def(
            context_def=context_def,
            config=config
        )


    async def delete_context_def(
        self, 
        context_type: str, 
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        config = config if config is not None else self.config | config

        return await self._storage.delete_context_def(
            context_type=context_type,
            config=config
        )


    async def validate_identity_def(
        self,
        identity_def: IdentityDef, 
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        """Validate an identity definition.
        """
        config = config if config is not None else self.config | config

        return core.validate_identity_def(identity_def)


    async def get_identity_defs_page(
        self, 
        page_ref: str | None,
        config: AuthzeeConfig | None = None
    ) -> IdentityDefsPage:
        config = config if config is not None else self.config | config

        return await self._storage.get_identity_defs_page(
            page_ref=page_ref,
            config=config
        )


    async def get_identity_def(
        self, 
        identity_type: str,
        config: AuthzeeConfig | None = None
    ) -> IdentityDefResult:
        config = config if config is not None else self.config | config

        return await self._storage.get_identity_def(
            identity_type=identity_type,
            config=config
        )


    async def list_identity_defs(
        self, 
        config: AuthzeeConfig | None = None
    ) -> AsyncIterable[IdentityDef]:
        config = config if config is not None else self.config | config

        return


    async def put_identity_def(
        self, 
        identity_def: IdentityDef, 
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        config = config if config is not None else self.config | config

        return await self._storage.put_identity_def(
            identity_def=identity_def,
            config=config
        )


    async def delete_identity_def(
        self, 
        identity_type: str,
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        config = config if config is not None else self.config | config

        return await self._storage.delete_identity_def(
            identity_type=identity_type,
            config=config
        )


    async def validate_resource_def(
        self,
        resource_def: ResourceDef, 
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        """Validate a resource definition.
        """
        config = config if config is not None else self.config | config

        return core.validate_resource_def(resource_def)


    async def get_resource_defs_page(
        self, 
        page_ref: str | None,
        config: AuthzeeConfig | None = None
    ) -> ResourceDefsPage:
        config = config if config is not None else self.config | config

        return await self._storage.get_resource_defs_page(
            page_ref=page_ref,
            config=config
        )


    async def get_resource_def(
        self, 
        resource_type: str,
        config: AuthzeeConfig | None = None
    ) -> ResourceDefResult:
        config = config if config is not None else self.config | config

        return await self._storage.get_resource_def(
            resource_type=resource_type,
            config=config
        )


    async def list_resource_defs(
        self, 
        config: AuthzeeConfig | None = None
    ) -> AsyncIterable[ResourceDef]:
        config = config if config is not None else self.config | config

        return await self._storage
    
    
    async def put_resource_def(
        self, 
        resource_def: ResourceDef,
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        config = config if config is not None else self.config | config

        return await self._storage.put_resource_def(
            resource_def=resource_def,
            config=config
        )


    async def delete_resource_def(
        self, 
        resource_type: str,
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        config = config if config is not None else self.config | config

        return await self._storage.delete_resource_def(
            resource_type=resource_type,
            config=config
        )


    async def enact(
        self, 
        grant: Grant,
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        config = config if config is not None else self.config | config

        return await self._storage.enact(
            grant=grant,
            config=config
        )

        
    async def repeal(
        self, 
        grant_uuid: UUID, 
        purge: bool,
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        config = config if config is not None else self.config | config

        return await self._storage.repeal(
            grant_uuid=grant_uuid,
            purge=purge,
            config=config
        )


    async def get_grant(
        self, 
        grant_uuid: UUID,
        config: AuthzeeConfig | None = None
    ) -> GrantResult:
        config = config if config is not None else self.config | config

        return await self._storage.get_grant(
            grant_uuid=grant_uuid,
            config=config
        )


    async def get_grants_page(
        self,
        effect: str | None, 
        action: str | None, 
        page_ref: str | None, 
        config: AuthzeeConfig | None = None
    ) -> GrantsPage:
        config = config if config is not None else self.config | config

        return await self._storage.get_grants_page(
            effect=effect,
            action=action,
            page_ref=page_ref,
            config=config
        )
    

    async def list_grants(
        self,
        effect: str | None, 
        action: str | None, 
        page_ref: str | None, 
        config: AuthzeeConfig | None = None
    ) -> AsyncIterable[Grant]:
        config = config if config is not None else self.config | config

        return await self._storage


    async def get_grant_refs_page(
        self,
        effect: str | None, 
        action: str | None, 
        page_ref: str | None, 
        config: AuthzeeConfig | None = None
    ) -> PageRefsPage:
        config = config if config is not None else self.config | config

        return await self._storage.get_grant_refs_page(
            effect=effect,
            action=action,
            page_ref=page_ref,
            config=config
        )


    async def cleanup_latches(
        self, 
        before: datetime.datetime, 
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        """Delete all latches before the specified datetime.

        - operations should clean up their own latches, but in case of a failure this can be used to clean up zombie latches.
        """
        config = config if config is not None else self.config | config

        return await self._storage
        

    async def audit_page(
        self,
        request: AuthzeeRequest, 
        page_ref: str | None, 
        config: AuthzeeConfig | None = None
    ) -> AuditResultPage:
        config = config if config is not None else self.compute_config | config

        return await self._compute.audit_page(
            request=request,
            page_ref=page_ref,
            config=config
        )


    async def authorize(
        self, 
        request: AuthzeeRequest,
        config: AuthzeeConfig | None = None
    ) -> AuthorizeResult:
        config = config if config is not None else self.compute_config | config

        return await self._compute.authorize(
            request=request,
            config=config
        )


    async def batch_audit_page(
        self,
        batch_request: AuthzeeBatchRequest, 
        page_ref: str | None, 
        config: AuthzeeConfig | None = None
    ) -> BatchAuditResultPage:
        config = config if config is not None else self.compute_config | config

        return await self._compute.batch_audit_page(
            batch_request=batch_request,
            page_ref=page_ref,
            config=config
        )


    async def batch_authorize(
        self, 
        batch_request: AuthzeeBatchRequest,
        config: AuthzeeConfig | None = None
    ) -> BatchAuthorizeResult:
        config = config if config is not None else self.compute_config | config

        return await self._compute.batch_authorize(
            batch_request=batch_request,
            config=config
        )
        