
from asyncio import gather
import datetime
from typing import Any, AsyncIterable, Callable, Coroutine, Dict, List, Type
from uuid import UUID

from authzee.types import *
from authzee.exceptions import *
from authzee import core
from authzee.compute.compute_module import ComputeModule
from authzee.storage.storage_module import StorageModule

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
    "evaluation": EvaluationError,
    "grant": GrantError,
    "request": RequestError, 
    "locality_incompatibility": LocalityIncompatibilityError,
    "not_implemented": NotImplementedError,
    "parallel_pagination_not_supported": ParallelPaginationNotSupported,
    "page_reference": PageReferenceError,
    "resource_not_found": ResourceNotFoundError,
    "start": StartError
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
    compute_storage_kwargs : Dict[str, Any]
        Override storage module KWArgs that the compute module will use.  Only include KWArgs you with to override.
    config : AuthzeeConfig
        Authzee configuration.
    compute_config: AuthzeeConfig
        Override default config values for calls that are passed to the compute backend. Only include KWArgs you with to override.
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
        compute_storage_kwargs: Dict[str, Any],
        config: AuthzeeConfig,
        compute_config: AuthzeeConfig
    ):
        self.execute = execute
        self.compute_type = compute_type
        self.compute_kwargs = compute_kwargs
        self.storage_type = storage_type
        self.storage_kwargs= storage_kwargs
        self.compute_storage_kwargs = storage_kwargs | compute_storage_kwargs
        self.config = _default_config | config
        self.compute_config = config | compute_config
        self._compute: ComputeModule = None
        self._storage: StorageModule = None
    

    def _raise_result(self, result: GenericResult, config: AuthzeeConfig) -> None:
        if config['raise_crits'] is True and result['has_failed'] is True:
            for error_type in result['errors']:
                for err in result['errors'][error_type]:
                    if err['is_critical']:
                        if error_type == "sdk": 
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


    def _combine_errors(result: GenericResult, *args: dict) ->  None:
        errors = result['errors']
        for new_result in args:
            if new_result['has_failed'] is True:
                result['has_failed'] = True

            new_errors = new_result['errors']
            for k in errors:
                if k in new_errors:
                    errors[k] += new_errors[k]
                
            for k in new_errors:
                if k not in errors:
                    errors[k] = new_errors[k]

    
    async def start(self, config: AuthzeeConfig | None = None) -> GenericResult:
        config = self.config if config is None else self.config | config
        self._compute = self.compute_type(**self.compute_kwargs)
        self._storage = self.storage_type(**self.storage_kwargs)
        result = {
            "has_failed": False,
            "errors": {}
        }
        compute_results, storage_result = await gather(
            self._compute.start(
                execute=self.execute,
                storage_type=self.storage_type,
                storage_kwargs=self.storage_kwargs,
                config=config
            ),
            self._storage.start(config)
        )
        self._combine_errors(result, compute_results, storage_result)
        self._raise_result(result, config)

        return result


    async def shutdown(
        self, 
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        config = self.config if config is None else self.config | config
        result = {
            "has_failed": False,
            "errors": {}
        }
        compute_result, storage_result = await gather(
            self._compute.shutdown(config),
            self._storage.shutdown(config)
        )
        self._combine_errors(result, compute_result, storage_result)
        self._raise_result(result)

        return result
        

    async def construct(
        self, 
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        config = self.config if config is None else self.config | config
        result = {
            "has_failed": False,
            "errors": {}
        }
        compute_result, storage_result = await gather(
            self._compute.construct(config),
            self._storage.construct(config)
        )
        self._combine_errors(result, compute_result, storage_result)
        self._raise_result(result)

        return result


    async def destroy(
        self, 
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        config = self.config if config is None else self.config | config
        result = {
            "has_failed": False,
            "errors": {}
        }
        compute_result, storage_result = await gather(
            self._compute.destroy(config),
            self._storage.destroy(config)
        )
        self._combine_errors(result, compute_result, storage_result)
        self._raise_result(result)

        return result


    async def validate_context_def(
        self,
        context_def: ContextDef, 
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        """Validate a context definition.
        """
        config = self.config if config is None else self.config | config
        result = core.validate_context_def(context_def=context_def)
        self._raise_result(result)
        
        return result


    async def get_context_defs_page(
        self, 
        page_ref: str | None,
        config: AuthzeeConfig | None = None
    ) -> ContextDefsPage:
        config = self.config if config is None else self.config | config
        result =  await self._storage.get_context_defs_page(
            page_ref=page_ref,
            config=config
        )
        self._raise_result(result)
        
        return result
    


    async def get_context_def(
        self, 
        context_type: str, 
        config: AuthzeeConfig | None = None
    ) -> ContextDefResult:
        config = self.config if config is None else self.config | config
        result = await self._storage.get_context_def(
            context_type=context_type,
            config=config
        )
        self._raise_result(result)
        
        return result


    async def list_context_defs(
        self,
        config: AuthzeeConfig | None = None
    ) -> AsyncIterable[ContextDef]:
        config = self.config if config is None else self.config | config
        result = await self._storage
        self._raise_result(result)
        
        return result

    async def put_context_def(
        self, 
        context_def: ContextDef, 
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        config = self.config if config is None else self.config | config
        result = await self._storage.put_context_def(
            context_def=context_def,
            config=config
        )
        self._raise_result(result)
        
        return result


    async def delete_context_def(
        self, 
        context_type: str, 
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        config = self.config if config is None else self.config | config
        result = await self._storage.delete_context_def(
            context_type=context_type,
            config=config
        )
        self._raise_result(result)
        
        return result


    async def validate_identity_def(
        self,
        identity_def: IdentityDef, 
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        """Validate an identity definition.
        """
        config = self.config if config is None else self.config | config
        result = core.validate_identity_def(identity_def)
        self._raise_result(result)
        
        return result


    async def get_identity_defs_page(
        self, 
        page_ref: str | None,
        config: AuthzeeConfig | None = None
    ) -> IdentityDefsPage:
        config = self.config if config is None else self.config | config
        result = await self._storage.get_identity_defs_page(
            page_ref=page_ref,
            config=config
        )
        self._raise_result(result)
        
        return result


    async def get_identity_def(
        self, 
        identity_type: str,
        config: AuthzeeConfig | None = None
    ) -> IdentityDefResult:
        config = self.config if config is None else self.config | config
        result = await self._storage.get_identity_def(
            identity_type=identity_type,
            config=config
        )
        self._raise_result(result)
        
        return result


    async def list_identity_defs(
        self, 
        config: AuthzeeConfig | None = None
    ) -> AsyncIterable[IdentityDef]:
        config = self.config if config is None else self.config | config
        result = ""
        self._raise_result(result)
        
        return result


    async def put_identity_def(
        self, 
        identity_def: IdentityDef, 
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        config = self.config if config is None else self.config | config
        result = await self._storage.put_identity_def(
            identity_def=identity_def,
            config=config
        )
        self._raise_result(result)
        
        return result


    async def delete_identity_def(
        self, 
        identity_type: str,
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        config = self.config if config is None else self.config | config
        result = await self._storage.delete_identity_def(
            identity_type=identity_type,
            config=config
        )
        self._raise_result(result)
        
        return result


    async def validate_resource_def(
        self,
        resource_def: ResourceDef, 
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        """Validate a resource definition.
        """
        config = self.config if config is None else self.config | config
        result = core.validate_resource_def(resource_def)
        self._raise_result(result)
        
        return result


    async def get_resource_defs_page(
        self, 
        page_ref: str | None,
        config: AuthzeeConfig | None = None
    ) -> ResourceDefsPage:
        config = self.config if config is None else self.config | config
        result = await self._storage.get_resource_defs_page(
            page_ref=page_ref,
            config=config
        )
        self._raise_result(result)
        
        return result


    async def get_resource_def(
        self, 
        resource_type: str,
        config: AuthzeeConfig | None = None
    ) -> ResourceDefResult:
        config = self.config if config is None else self.config | config
        result = await self._storage.get_resource_def(
            resource_type=resource_type,
            config=config
        )
        self._raise_result(result)
        
        return result


    async def list_resource_defs(
        self, 
        config: AuthzeeConfig | None = None
    ) -> AsyncIterable[ResourceDef]:
        config = self.config if config is None else self.config | config
        result = await self._storage
        self._raise_result(result)
        
        return result
    
    
    async def put_resource_def(
        self, 
        resource_def: ResourceDef,
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        config = self.config if config is None else self.config | config
        result = await self._storage.put_resource_def(
            resource_def=resource_def,
            config=config
        )
        self._raise_result(result)
        
        return result


    async def delete_resource_def(
        self, 
        resource_type: str,
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        config = self.config if config is None else self.config | config
        result = await self._storage.delete_resource_def(
            resource_type=resource_type,
            config=config
        )
        self._raise_result(result)
        
        return result


    async def enact(
        self, 
        grant: Grant,
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        config = self.config if config is None else self.config | config
        result = await self._storage.enact(
            grant=grant,
            config=config
        )
        self._raise_result(result)
        
        return result

        
    async def repeal(
        self, 
        grant_uuid: UUID, 
        purge: bool,
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        config = self.config if config is None else self.config | config
        result = await self._storage.repeal(
            grant_uuid=grant_uuid,
            purge=purge,
            config=config
        )
        self._raise_result(result)
        
        return result


    async def get_grant(
        self, 
        grant_uuid: UUID,
        config: AuthzeeConfig | None = None
    ) -> GrantResult:
        config = self.config if config is None else self.config | config
        result = await self._storage.get_grant(
            grant_uuid=grant_uuid,
            config=config
        )
        self._raise_result(result)
        
        return result


    async def get_grants_page(
        self,
        effect: str | None, 
        action: str | None, 
        page_ref: str | None, 
        config: AuthzeeConfig | None = None
    ) -> GrantsPage:
        config = self.config if config is None else self.config | config
        result = await self._storage.get_grants_page(
            effect=effect,
            action=action,
            page_ref=page_ref,
            config=config
        )
        self._raise_result(result)
        
        return result
    

    async def list_grants(
        self,
        effect: str | None, 
        action: str | None, 
        page_ref: str | None, 
        config: AuthzeeConfig | None = None
    ) -> AsyncIterable[Grant]:
        config = self.config if config is None else self.config | config
        result = await self._storage
        self._raise_result(result)
        
        return result


    async def get_grant_refs_page(
        self,
        effect: str | None, 
        action: str | None, 
        page_ref: str | None, 
        config: AuthzeeConfig | None = None
    ) -> PageRefsPage:
        config = self.config if config is None else self.config | config
        result = await self._storage.get_grant_refs_page(
            effect=effect,
            action=action,
            page_ref=page_ref,
            config=config
        )
        self._raise_result(result)
        
        return result


    async def cleanup_latches(
        self, 
        before: datetime.datetime, 
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        """Delete all latches before the specified datetime.

        - operations should clean up their own latches, but in case of a failure this can be used to clean up zombie latches.
        """
        config = self.config if config is None else self.config | config
        result = await self._storage
        self._raise_result(result)
        
        return result
        

    async def audit_page(
        self,
        request: AuthzeeRequest, 
        page_ref: str | None, 
        config: AuthzeeConfig | None = None
    ) -> AuditResultPage:
        config = self.compute_config if config is None else self.compute_config | config
        result = await self._compute.audit_page(
            request=request,
            page_ref=page_ref,
            config=config
        )
        self._raise_result(result)
        
        return result


    async def authorize(
        self, 
        request: AuthzeeRequest,
        config: AuthzeeConfig | None = None
    ) -> AuthorizeResult:
        config = self.compute_config if config is None else self.compute_config | config
        result = await self._compute.authorize(
            request=request,
            config=config
        )
        self._raise_result(result)
        
        return result


    async def batch_audit_page(
        self,
        batch_request: AuthzeeBatchRequest, 
        page_ref: str | None, 
        config: AuthzeeConfig | None = None
    ) -> BatchAuditResultPage:
        config = self.compute_config if config is None else self.compute_config | config
        result = await self._compute.batch_audit_page(
            batch_request=batch_request,
            page_ref=page_ref,
            config=config
        )
        self._raise_result(result)
        
        return result


    async def batch_authorize(
        self, 
        batch_request: AuthzeeBatchRequest,
        config: AuthzeeConfig | None = None
    ) -> BatchAuthorizeResult:
        config = self.compute_config if config is None else self.compute_config | config
        result = await self._compute.batch_authorize(
            batch_request=batch_request,
            config=config
        )
        self._raise_result(result)
        
        return result
        