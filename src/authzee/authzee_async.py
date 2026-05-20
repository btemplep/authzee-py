
from asyncio import gather
import copy
import datetime
from typing import Any, Callable, Dict, Type
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
        compute_storage_kwargs: Dict[str, Any] = None,
        config: AuthzeeConfig = None,
        compute_config: AuthzeeConfig = None
    ):
        self._execute = execute
        self._compute_type = compute_type
        self._compute_kwargs = compute_kwargs
        self._storage_type = storage_type
        self._storage_kwargs = storage_kwargs
        self._compute_storage_kwargs = storage_kwargs if compute_storage_kwargs is None else storage_kwargs | compute_storage_kwargs
        self._config = _default_config if config is None else _default_config | config
        self._compute_config = self._config if compute_config is None else self._config | compute_config
        self._compute: ComputeModule = None
        self._storage: StorageModule = None
    

    def _raise_result(self, result: GenericResult, config: AuthzeeConfig) -> None:
        if config['raise_crits'] is True and result['has_failed'] is True:
            if "critical_errors" in result:
                errors = result['critical_errors']
            else:
                errors = result['errors']

            for error_type in errors:
                for err in errors[error_type]:
                    if err['is_critical']:
                        raise _exception_map[error_type](
                            message=err['message'],
                            result=result
                        )


    def _combine_errors(self, result: GenericResult, *args: dict) ->  None:
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
        config = self._config if config is None else self._config | config
        self._compute = self._compute_type(**self._compute_kwargs)
        self._storage = self._storage_type(**self._storage_kwargs)
        result = {
            "has_failed": False,
            "errors": {}
        }
        compute_results, storage_result = await gather(
            self._compute.start(
                execute=self._execute,
                storage_type=self._storage_type,
                storage_kwargs=self._compute_storage_kwargs,
                config=config
            ),
            self._storage.start(config)
        )
        core.combine_errors(result, compute_results, storage_result)
        self._raise_result(result, config)

        return result


    async def shutdown(
        self, 
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        config = self._config if config is None else self._config | config
        result = {
            "has_failed": False,
            "errors": {}
        }
        compute_result, storage_result = await gather(
            self._compute.shutdown(config),
            self._storage.shutdown(config)
        )
        core.combine_errors(result, compute_result, storage_result)
        self._raise_result(result, config)

        return result
        

    async def construct(
        self, 
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        config = self._config if config is None else self._config | config
        result = {
            "has_failed": False,
            "errors": {}
        }
        compute_result, storage_result = await gather(
            self._compute.construct(config),
            self._storage.construct(config)
        )
        core.combine_errors(result, compute_result, storage_result)
        self._raise_result(result, config)

        return result


    async def destroy(
        self, 
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        config = self._config if config is None else self._config | config
        result = {
            "has_failed": False,
            "errors": {}
        }
        compute_result, storage_result = await gather(
            self._compute.destroy(config),
            self._storage.destroy(config)
        )
        core.combine_errors(result, compute_result, storage_result)
        self._raise_result(result, config)

        return result


    async def validate_context_def(
        self,
        context_def: ContextDef, 
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        """Validate a context definition.
        """
        config = self._compute_config if config is None else self._compute_config | config
        result = await self._compute.validate_context_def(
            context_def=context_def,
            config=config
        )
        self._raise_result(result, config)
        
        return result


    async def get_context_defs_page(
        self, 
        page_ref: str | None = None,
        config: AuthzeeConfig | None = None
    ) -> ContextDefsPage:
        config = self._config if config is None else self._config | config
        result =  await self._storage.get_context_defs_page(
            page_ref=page_ref,
            config=config
        )
        self._raise_result(result, config)
        
        return result
    


    async def get_context_def(
        self, 
        context_type: str, 
        config: AuthzeeConfig | None = None
    ) -> ContextDefResult:
        config = self._config if config is None else self._config | config
        result = await self._storage.get_context_def(
            context_type=context_type,
            config=config
        )
        self._raise_result(result, config)
        
        return result


    async def put_context_def(
        self, 
        context_def: ContextDef, 
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        valid_result = await self.validate_context_def(
            context_def=context_def,
            config=config
        )
        if valid_result['has_failed'] is True:
            return valid_result

        config = self._config if config is None else self._config | config
        result = await self._storage.put_context_def(
            context_def=context_def,
            config=config
        )
        self._raise_result(result, config)
        
        return result


    async def delete_context_def(
        self, 
        context_type: str, 
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        config = self._config if config is None else self._config | config
        result = await self._storage.delete_context_def(
            context_type=context_type,
            config=config
        )
        self._raise_result(result, config)
        
        return result


    async def validate_identity_def(
        self,
        identity_def: IdentityDef, 
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        """Validate an identity definition.
        """
        config = self._compute_config if config is None else self._compute_config | config
        result = await self._compute.validate_identity_def(
            identity_def=identity_def,
            config=config
        )
        self._raise_result(result, config)
        
        return result


    async def get_identity_defs_page(
        self, 
        page_ref: str | None = None,
        config: AuthzeeConfig | None = None
    ) -> IdentityDefsPage:
        config = self._config if config is None else self._config | config
        result = await self._storage.get_identity_defs_page(
            page_ref=page_ref,
            config=config
        )
        self._raise_result(result, config)
        
        return result


    async def get_identity_def(
        self, 
        identity_type: str,
        config: AuthzeeConfig | None = None
    ) -> IdentityDefResult:
        config = self._config if config is None else self._config | config
        result = await self._storage.get_identity_def(
            identity_type=identity_type,
            config=config
        )
        self._raise_result(result, config)
        
        return result


    async def put_identity_def(
        self, 
        identity_def: IdentityDef, 
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        valid_result = await self.validate_identity_def(
            identity_def=identity_def,
            config=config
        )
        if valid_result['has_failed'] is True:
            return valid_result

        config = self._config if config is None else self._config | config
        result = await self._storage.put_identity_def(
            identity_def=identity_def,
            config=config
        )
        self._raise_result(result, config)
        
        return result


    async def delete_identity_def(
        self, 
        identity_type: str,
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        config = self._config if config is None else self._config | config
        result = await self._storage.delete_identity_def(
            identity_type=identity_type,
            config=config
        )
        self._raise_result(result, config)
        
        return result


    async def validate_resource_def(
        self,
        resource_def: ResourceDef, 
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        """Validate a resource definition.
        """
        config = self._compute_config if config is None else self._compute_config | config
        result = await self._compute.validate_resource_def(
            resource_def=resource_def,
            config=config
        )
        self._raise_result(result, config)
        
        return result


    async def get_resource_defs_page(
        self, 
        page_ref: str | None = None,
        config: AuthzeeConfig | None = None
    ) -> ResourceDefsPage:
        config = self._config if config is None else self._config | config
        result = await self._storage.get_resource_defs_page(
            page_ref=page_ref,
            config=config
        )
        self._raise_result(result, config)
        
        return result


    async def get_resource_def(
        self, 
        resource_type: str,
        config: AuthzeeConfig | None = None
    ) -> ResourceDefResult:
        config = self._config if config is None else self._config | config
        result = await self._storage.get_resource_def(
            resource_type=resource_type,
            config=config
        )
        self._raise_result(result, config)
        
        return result
    
    
    async def put_resource_def(
        self, 
        resource_def: ResourceDef,
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        valid_result = await self.validate_resource_def(
            resource_def=resource_def,
            config=config
        )
        if valid_result['has_failed'] is True:
            return valid_result

        config = self._config if config is None else self._config | config
        result = await self._storage.put_resource_def(
            resource_def=resource_def,
            config=config
        )
        self._raise_result(result, config)
        
        return result


    async def delete_resource_def(
        self, 
        resource_type: str,
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        config = self._config if config is None else self._config | config
        result = await self._storage.delete_resource_def(
            resource_type=resource_type,
            config=config
        )
        self._raise_result(result, config)
        
        return result


    async def validate_grant(
        self, 
        grant: Grant,
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        config = self._compute_config if config is None else self._compute_config | config
        result = await self._compute.validate_grant(
            grant=grant,
            config=config
        )
        self._raise_result(result, config)
        
        return result


    async def enact(
        self, 
        grant: Grant,
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        valid_result = await self.validate_grant(
            grant=grant,
            config=config
        )
        if valid_result['has_failed'] is True:
            return valid_result

        config = self._config if config is None else self._config | config
        result = await self._storage.enact(
            grant=grant,
            config=config
        )
        self._raise_result(result, config)
        
        return result

        
    async def repeal(
        self, 
        grant_uuid: UUID, 
        purge: bool,
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        config = self._config if config is None else self._config | config
        result = await self._storage.repeal(
            grant_uuid=grant_uuid,
            purge=purge,
            config=config
        )
        self._raise_result(result, config)
        
        return result


    async def get_grant(
        self, 
        grant_uuid: UUID,
        config: AuthzeeConfig | None = None
    ) -> GrantResult:
        config = self._config if config is None else self._config | config
        result = await self._storage.get_grant(
            grant_uuid=grant_uuid,
            config=config
        )
        self._raise_result(result, config)
        
        return result


    async def get_grants_page(
        self,
        effect: str | None = None, 
        action: str | None = None, 
        page_ref: str | None = None, 
        config: AuthzeeConfig | None = None
    ) -> GrantsPage:
        config = self._config if config is None else self._config | config
        result = await self._storage.get_grants_page(
            effect=effect,
            action=action,
            page_ref=page_ref,
            config=config
        )
        self._raise_result(result, config)
        
        return result


    async def get_grant_refs_page(
        self,
        effect: str | None = None, 
        action: str | None = None, 
        page_ref: str | None = None, 
        config: AuthzeeConfig | None = None
    ) -> PageRefsPage:
        config = self._config if config is None else self._config | config
        result = await self._storage.get_grant_refs_page(
            effect=effect,
            action=action,
            page_ref=page_ref,
            config=config
        )
        self._raise_result(result, config)
        
        return result


    async def cleanup_latches(
        self, 
        before: datetime.datetime, 
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        """Delete all latches before the specified datetime.

        - operations should clean up their own latches, but in case of a failure this can be used to clean up zombie latches.
        """
        config = self._config if config is None else self._config | config
        result = await self._storage
        self._raise_result(result, config)
        
        return result
        

    async def audit_page(
        self,
        request: AuthzeeRequest, 
        page_ref: str | None = None, 
        config: AuthzeeConfig | None = None
    ) -> AuditResultPage:
        config = self._compute_config if config is None else self._compute_config | config
        valid_result = await self._compute.validate_request(
            request=request,
            config=config | {"raise_crits": False}
        )
        if valid_result['has_failed'] is True:
            result = {
                "grants": [],
                "results": [],
                "next_page_ref": None,
                "has_failed": True,
                "errors": valid_result['errors']
            }
            self._raise_result(result, config)
            
            return result

        result = await self._compute.audit_page(
            request=request,
            page_ref=page_ref,
            config=config
        )
        self._raise_result(result, config)
        
        return result


    def _get_critical_errors(self, errors: ResultErrors) -> ResultErrors:
        critical_errors = {}
        for et in errors:
            for error in errors[et]:
                if error['is_critical']:
                    if et not in critical_errors:
                        critical_errors[et] = []
                    
                    critical_errors[et].append(error)
        
        return critical_errors


    async def authorize(
        self, 
        request: AuthzeeRequest,
        config: AuthzeeConfig | None = None
    ) -> AuthorizeResult:
        config = self._compute_config if config is None else self._compute_config | config
        valid_result = await self._compute.validate_request(
            request=request,
            config=config | {"raise_crits": False}
        )
        
        if valid_result['has_failed'] is True:
            result = {
                "is_authorized": False,
                "grant": None,
                "message": "A critical error has occurred. Therefore, the request is not authorized.",
                "has_failed": valid_result['has_failed'],
                "critical_errors": self._get_critical_errors(valid_result['errors'])
            }
            self._raise_result(valid_result, config)
            
            return result

        result = await self._compute.authorize(
            request=request,
            config=config
        )
        self._raise_result(result, config)
        
        return result


    async def batch_audit_page(
        self,
        batch_request: AuthzeeBatchRequest, 
        page_ref: str | None = None, 
        config: AuthzeeConfig | None = None
    ) -> BatchAuditResultPage:
        config = self._compute_config if config is None else self._compute_config | config
        valid_result = await self._compute.validate_batch_request(
            batch_request=batch_request,
            config=config | {"raise_crits": False}
        )
        if valid_result['has_failed'] is True:
            result = {
                "grants": [],
                "batch_results": [],
                "next_page_ref": None,
                "has_failed": True,
                "errors": valid_result['errors']
            }
            self._raise_result(result, config)
            
            return result
    
        result = await self._compute.batch_audit_page(
            batch_request=batch_request,
            page_ref=page_ref,
            config=config
        )
        self._raise_result(result, config)
        
        return result


    async def batch_authorize(
        self, 
        batch_request: AuthzeeBatchRequest,
        config: AuthzeeConfig | None = None
    ) -> BatchAuthorizeResult:
        config = self._compute_config if config is None else self._compute_config | config
        valid_result = await self._compute.validate_batch_request(
            batch_request=batch_request,
            config=config | {"raise_crits": False}
        )
        if valid_result['has_failed'] is True:
            result = {
                "batch_results": [],
                "has_failed": True,
                "critical": self._get_critical_errors(valid_result['errors'])
            }
            self._raise_result(result, config)
            
            return result

        result = await self._compute.batch_authorize(
            batch_request=batch_request,
            config=config
        )
        self._raise_result(result, config)
        
        return result
        