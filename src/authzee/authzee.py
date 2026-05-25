"""See {py:class}`authzee.authzee.Authzee`"""
__all__ = [
    "Authzee",
]

import asyncio
import datetime
from typing import Any, Callable, Dict, Type

from authzee.types import *
from authzee.compute.compute_module import ComputeModule
from authzee.storage.storage_module import StorageModule
from authzee.authzee_async import AuthzeeAsync


class Authzee:
    """Authzee application.

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
        self._authzee_async = AuthzeeAsync(
            execute=execute,
            compute_type=compute_type,
            compute_kwargs=compute_kwargs,
            storage_type=storage_type,
            storage_kwargs=storage_kwargs,
            compute_storage_kwargs=compute_storage_kwargs,
            config=config,
            compute_config=compute_config
        )

    
    def start(self, config: AuthzeeConfig | None = None) -> GenericResult:
        return asyncio.run(self._authzee_async.start(config))


    def shutdown(
        self, 
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        return asyncio.run(self._authzee_async.shutdown(config))
        

    def construct(
        self, 
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        return asyncio.run(self._authzee_async.construct(config))


    def destroy(
        self, 
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        return asyncio.run(self._authzee_async.destroy(config))


    def validate_context_def(
        self,
        context_def: ContextDef, 
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        return asyncio.run(
            self._authzee_async.validate_context_def(
                context_def=context_def,
                config=config
            )
        )


    def get_context_defs_page(
        self, 
        page_ref: str | None = None,
        config: AuthzeeConfig | None = None
    ) -> ContextDefsPage:
        return asyncio.run(
            self._authzee_async.get_context_defs_page(
                page_ref=page_ref,
                config=config
            )
        )


    def get_context_def(
        self, 
        context_type: str, 
        config: AuthzeeConfig | None = None
    ) -> ContextDefResult:
        return asyncio.run(
            self._authzee_async.get_context_def(
                context_type=context_type,
                config=config
            )
        )


    def put_context_def(
        self, 
        context_def: ContextDef, 
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        return asyncio.run(
            self._authzee_async.put_context_def(
                context_def=context_def,
                config=config
            )
        )


    def delete_context_def(
        self, 
        context_type: str, 
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        return asyncio.run(
            self._authzee_async.delete_context_def(
                context_type=context_type,
                config=config
            )
        )


    def validate_identity_def(
        self,
        identity_def: IdentityDef, 
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        return asyncio.run(
            self._authzee_async.validate_identity_def(
                identity_def=identity_def,
                config=config
            )
        )


    def get_identity_defs_page(
        self, 
        page_ref: str | None = None,
        config: AuthzeeConfig | None = None
    ) -> IdentityDefsPage:
        return asyncio.run(
            self._authzee_async.get_identity_defs_page(
                page_ref=page_ref,
                config=config
            )
        )


    def get_identity_def(
        self, 
        identity_type: str,
        config: AuthzeeConfig | None = None
    ) -> IdentityDefResult:
        return asyncio.run(
            self._authzee_async.get_identity_def(
                identity_type=identity_type,
                config=config
            )
        )


    def put_identity_def(
        self, 
        identity_def: IdentityDef, 
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        return asyncio.run(
            self._authzee_async.put_identity_def(
                identity_def=identity_def,
                config=config
            )
        )


    def delete_identity_def(
        self, 
        identity_type: str,
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        return asyncio.run(
            self._authzee_async.delete_identity_def(
                identity_type=identity_type,
                config=config
            )
        )


    def validate_resource_def(
        self,
        resource_def: ResourceDef, 
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        return asyncio.run(
            self._authzee_async.validate_resource_def(
                resource_def=resource_def,
                config=config
            )
        )


    def get_resource_defs_page(
        self, 
        page_ref: str | None = None,
        config: AuthzeeConfig | None = None
    ) -> ResourceDefsPage:
        return asyncio.run(
            self._authzee_async.get_resource_defs_page(
                page_ref=page_ref,
                config=config
            )
        )


    def get_resource_def(
        self, 
        resource_type: str,
        config: AuthzeeConfig | None = None
    ) -> ResourceDefResult:
        return asyncio.run(
            self._authzee_async.get_resource_def(
                resource_type=resource_type,
                config=config
            )
        )


    def put_resource_def(
        self, 
        resource_def: ResourceDef,
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        return asyncio.run(
            self._authzee_async.put_resource_def(
                resource_def=resource_def,
                config=config
            )
        )


    def delete_resource_def(
        self, 
        resource_type: str,
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        return asyncio.run(
            self._authzee_async.delete_resource_def(
                resource_type=resource_type,
                config=config
            )
        )


    def validate_grant(
        self, 
        grant: Grant,
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        return asyncio.run(
            self._authzee_async.validate_grant(
                grant=grant,
                config=config
            )
        )


    def enact(
        self, 
        grant: Grant,
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        return asyncio.run(
            self._authzee_async.enact(
                grant=grant,
                config=config
            )
        )


    def repeal(
        self, 
        grant_uuid: str, 
        purge: bool,
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        return asyncio.run(
            self._authzee_async.repeal(
                grant_uuid=grant_uuid,
                purge=purge,
                config=config
            )
        )


    def get_grant(
        self, 
        grant_uuid: str,
        config: AuthzeeConfig | None = None
    ) -> GrantResult:
        return asyncio.run(
            self._authzee_async.get_grant(
                grant_uuid=grant_uuid,
                config=config
            )
        )


    def get_grants_page(
        self,
        effect: str | None = None, 
        action: str | None = None, 
        page_ref: str | None = None, 
        config: AuthzeeConfig | None = None
    ) -> GrantsPage:
        return asyncio.run(
            self._authzee_async.get_grants_page(
                effect=effect,
                action=action,
                page_ref=page_ref,
                config=config
            )
        )


    def get_grant_refs_page(
        self,
        effect: str | None = None, 
        action: str | None = None, 
        page_ref: str | None = None, 
        config: AuthzeeConfig | None = None
    ) -> PageRefsPage:
        return asyncio.run(
            self._authzee_async.get_grant_refs_page(
                effect=effect,
                action=action,
                page_ref=page_ref,
                config=config
            )
        )


    def cleanup_latches(
        self, 
        before: datetime.datetime, 
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        return asyncio.run(
            self._authzee_async.cleanup_latches(
                before=before,
                config=config
            )
        )


    def audit_page(
        self,
        request: AuthzeeRequest, 
        page_ref: str | None = None, 
        config: AuthzeeConfig | None = None
    ) -> AuditResultPage:
        return asyncio.run(
            self._authzee_async.audit_page(
                request=request,
                page_ref=page_ref,
                config=config
            )
        )


    def authorize(
        self, 
        request: AuthzeeRequest,
        config: AuthzeeConfig | None = None
    ) -> AuthorizeResult:
        return asyncio.run(
            self._authzee_async.authorize(
                request=request,
                config=config
            )
        )


    def batch_audit_page(
        self,
        batch_request: AuthzeeBatchRequest, 
        page_ref: str | None = None, 
        config: AuthzeeConfig | None = None
    ) -> BatchAuditResultPage:
        return asyncio.run(
            self._authzee_async.batch_audit_page(
                batch_request=batch_request,
                page_ref=page_ref,
                config=config
            )
        )


    def batch_authorize(
        self, 
        batch_request: AuthzeeBatchRequest,
        config: AuthzeeConfig | None = None
    ) -> BatchAuthorizeResult:
        return asyncio.run(
            self._authzee_async.batch_authorize(
                batch_request=batch_request,
                config=config
            )
        )
        