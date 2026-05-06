

from typing import Any, Callable, Dict, Type

from authzee.types import *
from authzee.exceptions import NotImplementedError
from authzee.module_locality import ModuleLocality
from authzee.storage.storage_module import StorageModule

class ComputeModule:


    def __init__(
        self,
        storage_kwargs: Dict[str, Any] | None = None
    ):
        self._storage_kwargs = storage_kwargs


    def start(
        self,
        execute: Callable[[str, Any], Any],
        storage_type: Type[StorageModule],
        storage_kwargs: Dict[str, Any],
        config: AuthzeeConfig
    ) -> GenericResult:
        """Start up compute module.

        - run before use
        - After this method is complete these public instance vars or getters must be available and stable:
            - locality - Compute [Module Locality](#module-locality)
            - has_parallel_paging - if the compute module supports processing grants with parallel paging
        """
        self._execute = execute
        self._storage_type = storage_type
        self._storage_kwargs = self._storage_kwargs if self._storage_kwargs is not None else storage_kwargs
        self.locality = ModuleLocality.PROCESS
        self.has_parallel_paging = False


    def shutdown(self, config: AuthzeeConfig) -> GenericResult:
        """Shutdown Compute module.

        - clean up runtime resources
        """
        raise NotImplementedError()


    def construct(self, config: AuthzeeConfig) -> GenericResult:
        """Construct backend resources for compute.

        - one time setup
        """
        raise NotImplementedError()


    def destroy(self, config: AuthzeeConfig) -> GenericResult:
        """Tear down backend resources.

        - destructive - may lose all long lasting compute resources
        """
        raise NotImplementedError()


    def validate_request(
        self,
        request: AuthzeeRequest,
        config: AuthzeeConfig
    ) -> GenericResult:
        """Validate a request.
        """
        raise NotImplementedError()


    def validate_batch_request(
        self,
        batch_request: AuthzeeBatchRequest,
        config: AuthzeeConfig
    ) -> GenericResult:
        """Validate a batch request.
        """
        raise NotImplementedError()


    def audit_page(
        self,
        request: AuthzeeRequest,
        page_ref: str | None,
        config: AuthzeeConfig
    ) -> AuditResultPage:
        """Run the Audit Operation for a page of results.

        Pass the returned page reference to get the next page until a null page reference is returned.
        """
        raise NotImplementedError()


    def authorize(
        self,
        request: AuthzeeRequest,
        config: AuthzeeConfig
    ) -> AuthorizeResult:
        """Run the Authorize Operation.
        """
        raise NotImplementedError()


    def batch_audit_page(
        self,
        batch_request: AuthzeeBatchRequest,
        page_ref: str | None,
        config: AuthzeeConfig
    ) -> BatchAuditResultPage:
        """Run the Batch Audit Operation for a page of results.

        Pass the returned page reference to get the next page until a null page reference is returned.
        """
        raise NotImplementedError()


    def batch_authorize(
        self,
        batch_request: AuthzeeBatchRequest,
        config: AuthzeeConfig
    ) -> BatchAuthorizeResult:
        """Run the Batch Authorize Operation.
        """
        raise NotImplementedError()