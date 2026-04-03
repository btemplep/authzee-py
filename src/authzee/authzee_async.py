
import asyncio
import datetime
from typing import Any, AsyncIterable, Callable, Coroutine, Dict, List, Type
from uuid import UUID, uuid4

from authzee.dcs import *
from authzee import core
from authzee.compute.compute_module import ComputeModule
from authzee.storage.storage_module import StorageModule

from authzee import exceptions
from authzee.module_locality import locality_compatibility


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
    authzee_config : AuthzeeConfig
        Authzee configuration.
    
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
        authzee_config: AuthzeeConfig
    ):
        self.execute = execute
        self.compute_type = compute_type
        self.compute_kwargs = compute_kwargs
        self.storage_type = storage_type
        self.storage_kwargs= storage_kwargs
        self.authzee_config = authzee_config
        self._compute: ComputeModule = None
        self._storage: StorageModule = None
    

    async def _get_results(
        self,
        coros: List[Coroutine[Any, Any, GenericResult]],
        error_messages: List[str | None],
        authzee_config: AuthzeeConfig
    ) -> GenericResult:
        """Check results for errors and raise upon failure if selected. 
        """
        results = await asyncio.gather(*coros)
        for result, message in zip(results, error_messages):
            if result.has_failed is True:
                if authzee_config.raise_crits is True:
                    raise exceptions.AuthzeeSDKError(
                        message=message if message is not None else "",
                        is_critical=True,
                        result=result
                    )
                else:
                    return result
            
        return results

    
    async def start(
        self, 
        authzee_config: AuthzeeConfig | None = None
    ) -> GenericResult:
        authzee_config = authzee_config if authzee_config is not None else self.authzee_config
        self._compute = self.compute_type(**self.compute_kwargs)
        self._storage = self.storage_type(**self.storage_kwargs)

        return await self._get_results(
            coros=[
                self._compute.start(authzee_config),
                self._storage.start(authzee_config)
            ],
            error_messages=[
                "Error when starting compute module.",
                "Error when starting storage module."
            ],
            authzee_config=authzee_config
        )


    async def shutdown(
        self, 
        authzee_config: AuthzeeConfig | None = None
    ) -> GenericResult:
        authzee_config = authzee_config if authzee_config is not None else self.authzee_config
        results = await asyncio.gather(
            self._compute.stop(authzee_config),
            self._storage.stop(authzee_config)
        )
        compute_result = self._exception_handler(results[0], authzee_config)
        if compute_result.has_failed is True:
            if authzee_config.raise_crits:
                raise exceptions.AuthzeeSDKError(
                    message="Error when starting compute module",
                    is_critical=True,
                    result=compute_result
                )
            else:
                return compute_result
    
        storage_result = self._exception_handler(results[0], authzee_config)
        if storage_result.has_failed is True:
            if authzee_config.raise_crits:
                raise exceptions.AuthzeeSDKError(
                    message="Error when starting storage module",
                    is_critical=True,
                    result=storage_result
                )
            else:
                return storage_result



    async def construct(
        self, 
        authzee_config: AuthzeeConfig | None = None
    ) -> GenericResult:
        authzee_config = authzee_config if authzee_config is not None else self.authzee_config


    async def destroy(
        self, 
        authzee_config: AuthzeeConfig | None = None
    ) -> GenericResult:
        authzee_config = authzee_config if authzee_config is not None else self.authzee_config


    async def validate_context_def(
        self,
        context_def: ContextDef, 
        authzee_config: AuthzeeConfig | None = None
    ) -> GenericResult:
        """Validate a context definition.
        """
        authzee_config = authzee_config if authzee_config is not None else self.authzee_config


    async def get_context_defs_page(
        self, 
        authzee_config: AuthzeeConfig | None = None
    ) -> ContextDefsPage:
        authzee_config = authzee_config if authzee_config is not None else self.authzee_config


    async def get_context_def(
        self, 
        context_type: str, 
        authzee_config: AuthzeeConfig | None = None
    ) -> ContextDefResult:
        authzee_config = authzee_config if authzee_config is not None else self.authzee_config


    async def list_context_defs(
        self,
        authzee_config: AuthzeeConfig | None = None
    ) -> AsyncIterable[ContextDef]:
        authzee_config = authzee_config if authzee_config is not None else self.authzee_config


    async def put_context_def(
        self, 
        context_def: ContextDef, 
        authzee_config: AuthzeeConfig | None = None
    ) -> GenericResult:
        authzee_config = authzee_config if authzee_config is not None else self.authzee_config


    async def delete_context_def(
        self, 
        context_type: str, 
        authzee_config: AuthzeeConfig | None = None
    ) -> GenericResult:
        authzee_config = authzee_config if authzee_config is not None else self.authzee_config


    async def validate_identity_def(
        self,
        identity_def: IdentityDef, 
        authzee_config: AuthzeeConfig | None = None
    ) -> GenericResult:
        """Validate an identity definition.
        """
        authzee_config = authzee_config if authzee_config is not None else self.authzee_config


    async def get_identity_defs_page(
        self, 
        authzee_config: AuthzeeConfig | None = None
    ) -> IdentityDefsPage:
        authzee_config = authzee_config if authzee_config is not None else self.authzee_config


    async def get_identity_def(
        self, 
        identity_type: str,
        authzee_config: AuthzeeConfig | None = None
    ) -> IdentityDefResult:
        authzee_config = authzee_config if authzee_config is not None else self.authzee_config


    async def list_identity_defs(
        self, 
        authzee_config: AuthzeeConfig | None = None
    ) -> AsyncIterable[IdentityDef]:
        authzee_config = authzee_config if authzee_config is not None else self.authzee_config


    async def put_identity_def(
        self, 
        identity_def: IdentityDef, 
        authzee_config: AuthzeeConfig | None = None
    ) -> GenericResult:
        authzee_config = authzee_config if authzee_config is not None else self.authzee_config


    async def delete_identity_def(
        self, 
        identity_type: str,
        authzee_config: AuthzeeConfig | None = None
    ) -> GenericResult:
        authzee_config = authzee_config if authzee_config is not None else self.authzee_config


    async def validate_resource_def(
        self,
        resource_def: ResourceDef, 
        authzee_config: AuthzeeConfig | None = None
    ) -> GenericResult:
        """Validate a resource definition.
        """
        authzee_config = authzee_config if authzee_config is not None else self.authzee_config


    async def get_resource_defs_page(
        self, 
        authzee_config: AuthzeeConfig | None = None
    ) -> ResourceDefsPage:
        authzee_config = authzee_config if authzee_config is not None else self.authzee_config


    async def get_resource_def(
        self, 
        resource_type: str,
        authzee_config: AuthzeeConfig | None = None
    ) -> ResourceDefResult:
        authzee_config = authzee_config if authzee_config is not None else self.authzee_config


    async def list_resource_defs(
        self, 
        authzee_config: AuthzeeConfig | None = None
    ) -> AsyncIterable[ResourceDef]:
        authzee_config = authzee_config if authzee_config is not None else self.authzee_config
    
    
    async def put_resource_def(
        self, 
        resource_def: ResourceDef,
        authzee_config: AuthzeeConfig | None = None
    ) -> GenericResult:
        authzee_config = authzee_config if authzee_config is not None else self.authzee_config


    async def delete_resource_def(
        self, 
        resource_type: str,
        authzee_config: AuthzeeConfig | None = None
    ) -> GenericResult:
        authzee_config = authzee_config if authzee_config is not None else self.authzee_config


    async def enact(
        self, 
        grant: Grant,
        authzee_config: AuthzeeConfig | None = None
    ) -> GenericResult:
        authzee_config = authzee_config if authzee_config is not None else self.authzee_config

        
    async def repeal(
        self, 
        grant_uuid: UUID, 
        run_scan: bool,
        authzee_config: AuthzeeConfig | None = None
    ) -> GenericResult:
        authzee_config = authzee_config if authzee_config is not None else self.authzee_config


    async def get_grant(
        self, 
        grant_uuid: UUID,
        authzee_config: AuthzeeConfig | None = None
    ) -> GrantResult:
        authzee_config = authzee_config if authzee_config is not None else self.authzee_config


    async def get_grants_page(
        self,
        effect: str | None, 
        action: str | None, 
        page_ref: str | None, 
        authzee_config: AuthzeeConfig | None = None
    ) -> GrantsPage:
        authzee_config = authzee_config if authzee_config is not None else self.authzee_config
    

    async def list_grants(
        self,
        effect: str | None, 
        action: str | None, 
        page_ref: str | None, 
        authzee_config: AuthzeeConfig | None = None
    ) -> AsyncIterable[Grant]:
        authzee_config = authzee_config if authzee_config is not None else self.authzee_config


    async def get_grant_refs_page(
        self,
        effect: str | None, 
        action: str | None, 
        page_ref: str | None, 
        authzee_config: AuthzeeConfig | None = None
    ) -> PageRefsPage:
        authzee_config = authzee_config if authzee_config is not None else self.authzee_config


    async def cleanup_latches(
        self, 
        before: datetime.datetime, 
        authzee_config: AuthzeeConfig | None = None
    ) -> GenericResult:
        """Delete all latches before the specified datetime.

        - operations should clean up their own latches, but in case of a failure this can be used to clean up zombie latches.
        """
        authzee_config = authzee_config if authzee_config is not None else self.authzee_config
        

    async def audit_page(
        self,
        request: AuthzeeRequest, 
        page_ref: str | None, 
        authzee_config: AuthzeeConfig | None = None
    ) -> AuditResultPage:
        authzee_config = authzee_config if authzee_config is not None else self.authzee_config


    async def authorize(
        self, 
        request: AuthzeeRequest,
        authzee_config: AuthzeeConfig | None = None
    ) -> AuthorizeResult:
        authzee_config = authzee_config if authzee_config is not None else self.authzee_config


    async def batch_audit_page(
        self,
        batch_request: AuthzeeBatchRequest, 
        page_ref: str | None, 
        authzee_config: AuthzeeConfig | None = None
    ) -> BatchAuditResultPage:
        authzee_config = authzee_config if authzee_config is not None else self.authzee_config


    async def batch_authorize(
        self, 
        batch_request: AuthzeeBatchRequest,
        authzee_config: AuthzeeConfig | None = None
    ) -> BatchAuthorizeResult:
        authzee_config = authzee_config if authzee_config is not None else self.authzee_config
        