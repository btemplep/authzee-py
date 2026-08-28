"""Base compute module for Authzee.

See [](authzee.compute.compute_module.ComputeModule)
"""

__all__ = [
    "ComputeModule"
]

from typing import Any, Callable, Type

from authzee.exceptions import NotImplementedError
from authzee.module_locality import ModuleLocality
from authzee.storage.storage_module import StorageModule
from authzee.types.authzee import *
from authzee.types.config import (
    AuditConfig,
    AuthorizeConfig,
    BatchAuditConfig,
    BatchAuthorizeConfig,
    ComputeConstructConfig,
    ComputeDestroyConfig,
    ComputeShutdownConfig,
    ComputeStartConfig,
    ValidateBatchRequestConfig,
    ValidateContextDefConfig,
    ValidateGrantConfig,
    ValidateIdentityDefConfig,
    ValidateRequestConfig,
    ValidateResourceDefConfig
)


class ComputeModule:


    async def start(
        self,
        execute: Callable[[str, Any], Any],
        storage_type: Type[StorageModule],
        storage_kwargs: dict[str, Any],
        config: ComputeStartConfig
    ) -> GenericResult:
        """Start up compute module.

        - run before use
        - After this method is complete these public instance vars or getters must be available and stable:
            - locality - Compute [Module Locality](#module-locality)
            - has_parallel_paging - if the compute module supports processing grants with parallel paging
        """
        self._execute = execute
        self._storage_type = storage_type
        self._storage_kwargs = storage_kwargs
        self.locality = ModuleLocality.PROCESS
        self.has_parallel_paging = False


    async def shutdown(self, config: ComputeShutdownConfig) -> GenericResult:
        """Shutdown Compute module.

        - clean up runtime resources
        """
        raise NotImplementedError()


    async def construct(self, config: ComputeConstructConfig) -> GenericResult:
        """Construct backend resources for compute.

        - one time setup
        """
        raise NotImplementedError()


    async def destroy(self, config: ComputeDestroyConfig) -> GenericResult:
        """Tear down backend resources.

        - destructive - may lose all long lasting compute resources
        """
        raise NotImplementedError()


    async def validate_context_def(
        self,
        context_def: ContextDef,
        config: ValidateContextDefConfig
    ) -> GenericResult:
        raise NotImplementedError()


    async def validate_identity_def(
        self,
        identity_def: IdentityDef,
        config: ValidateIdentityDefConfig
    ) -> GenericResult:
        raise NotImplementedError()


    async def validate_resource_def(
        self,
        resource_def: ResourceDef,
        config: ValidateResourceDefConfig
    ) -> GenericResult:
        raise NotImplementedError()


    async def validate_grant(
        self,
        grant: Grant,
        config: ValidateGrantConfig
    ) -> GenericResult:
        raise NotImplementedError()


    async def validate_request(
        self,
        request: AuthzeeRequest,
        config: ValidateRequestConfig
    ) -> GenericResult:
        """Validate a request.
        """
        raise NotImplementedError()


    async def validate_batch_request(
        self,
        batch_request: AuthzeeBatchRequest,
        config: ValidateBatchRequestConfig
    ) -> ValidateBatchRequestResult:
        """Validate a batch request.
        """
        raise NotImplementedError()


    async def audit(
        self,
        request: AuthzeeRequest,
        page_ref: str | None,
        config: AuditConfig
    ) -> AuditResultPage:
        """Run the Audit Operation for a page of results.

        Pass the returned page reference to get the next page until a null page reference is returned.
        """
        raise NotImplementedError()


    async def authorize(
        self,
        request: AuthzeeRequest,
        config: AuthorizeConfig
    ) -> AuthorizeResult:
        """Run the Authorize Operation.
        """
        raise NotImplementedError()


    async def batch_audit(
        self,
        batch_request: AuthzeeBatchRequest,
        page_ref: str | None,
        config: BatchAuditConfig
    ) -> BatchAuditResultPage:
        """Run the Batch Audit Operation for a page of results.

        Pass the returned page reference to get the next page until a null page reference is returned.
        """
        raise NotImplementedError()


    async def batch_authorize(
        self,
        batch_request: AuthzeeBatchRequest,
        config: BatchAuthorizeConfig
    ) -> BatchAuthorizeResult:
        """Run the Batch Authorize Operation.
        """
        raise NotImplementedError()
