
from asyncio import create_task
import json
from typing import Any, Callable, Dict, Type

import jsonschema_rs

from authzee.compute.compute_module import ComputeModule
from authzee.core import paginator, validate_request_schema
from authzee.dcs import *
from authzee.exceptions import NotImplementedError
from authzee.module_locality import ModuleLocality
from authzee.storage.storage_module import StorageModule

class InProcessCompute(ComputeModule):


    async def start(
        self,
        execute: Callable[[str, Any], Any],
        storage_type: Type[StorageModule],
        storage_kwargs: Dict[str, Any]
    ) -> GenericResult:
        """Start up compute module.

        - run before use
        - After this method is complete these public instance vars or getters must be available and stable:
            - locality - Compute [Module Locality](#module-locality)
            - has_parallel_paging - if the compute module supports processing grants with parallel paging
        """
        super().start(
            execute=execute,
            storage_type=storage_type, 
            storage_kwargs=storage_kwargs
        )
        self.locality = ModuleLocality.PROCESS
        self.has_parallel_paging = False
        self._storage = storage_type(**storage_kwargs)


    async def shutdown(self) -> GenericResult:
        """Shutdown Compute module.

        - clean up runtime resources
        """
        await self._storage.shutdown()


    async def construct(self) -> GenericResult:
        """Construct backend resources for compute.

        - one time setup
        """
        pass


    async def destroy(self) -> GenericResult:
        """Tear down backend resources.

        - destructive - may lose all long lasting compute resources
        """
        pass


    async def validate_request(
        self,
        request: AuthzeeRequest,
        page_size: int
    ) -> GenericResult:
        """Validate a request.
        """
        result = validate_request_schema(request)
        if result.has_failed is True:
            return result

        context_def_task = create_task(self._storage.get_context_def(request.context_type))
        resource_def_task = create_task(self._storage.get_resource_def(request.resource_type))
        identity_def_tasks = [create_task(self._storage.get_identity_def(it)) for it in request.identities]

        context_def = (await context_def_task).context_def
        if context_def is None:
            result.has_failed = True
            result.errors.request = [
                GenericError(
                    is_critical=True,
                    message=f"context_type '{request.context_type}' is not a registered context type."
                )
            ]
            
            return result
        
        if jsonschema_rs.validator_for(context_def.schema).is_valid(request.context) is False:
            result.has_failed = True
            result.errors.request = [
                GenericError(
                    is_critical=True,
                    message=f"The given context is not valid against the '{request.context_type}' context type."
                )
            ]

            return result

        resource_def = (await resource_def_task).resource_def
        if resource_def is None:
            result.has_failed = True
            result.errors.request = [
                GenericError(
                    is_critical=True,
                    message=f"resource_type '{request.resource_type}' is not a registered resource type."
                )
            ]

            return result
        
        if jsonschema_rs.validator_for(resource_def.schema).is_valid(request.resource) is False:
            result.has_failed = True
            result.errors.request = [
                GenericError(
                    is_critical=True,
                    message=f"The given resource is not valid against the '{request.resource_type}' resource type."
                )
            ]

            return result
        
        if request.action not in resource_def.actions:
            result.has_failed = True
            result.errors.request = [
                GenericError(
                    is_critical=True,
                    message=f"The given resource action is valid for the '{request.resource_type}' resource type."
                )
            ]

            return result
        
        for id_task, i_type in zip(identity_def_tasks, request.identities):
            identity_def = (await id_task).identity_def
            if identity_def is None:
                result.has_failed = True
                result.errors.request = [
                    GenericError(
                        is_critical=True,
                        message=f"identity_type '{i_type}' is not a registered identity type."
                    )
                ]

                return result
            
            id_validator = jsonschema_rs.validator_for(identity_def.schema)
            for id, i in zip(request.identities[i_type], range(len(request.identities))):
                if id_validator.is_valid(id) is False:
                    result.has_failed = True
                    result.errors.request = [
                        GenericError(
                            is_critical=True,
                            message=f"The given identity in '{i_type}[{i}]' is not valid against the '{i_type}' identity type."
                        )
                    ]

                    return result
        
        return result


    async def validate_batch_request(
        self,
        batch_request: AuthzeeBatchRequest,
        page_size: int
    ) -> GenericResult:
        """Validate a batch request.
        """
        raise NotImplementedError()


    async def audit_page(
        self,
        request: AuthzeeRequest,
        page_ref: str | None,
        grants_page_size: int
    ) -> AuditResultPage:
        """Run the Audit Operation for a page of results.

        Pass the returned page reference to get the next page until a null page reference is returned.
        """
        result = AuditResultPage(
            grants=[],
            results=[],
            next_page_ref=None,
            has_failed=False,
        )
        val_result = await self.validate_request(request=request, page_size=page_size)
        if val_result.has_failed is True:
            result.errors = val_result.errors 

            return result

        grants = (
            await self._storage.get_grants_page(
                effect=None,
                action=request.action,
                page_ref=page_ref,
                page_size=page_size
            ))


    async def authorize(
        self,
        request: AuthzeeRequest,
        page_size: int,
        parallel_pagination: bool,
        refs_page_size: int
    ) -> AuthorizeResult:
        """Run the Authorize Operation.
        """
        raise NotImplementedError()


    async def batch_audit_page(
        self,
        batch_request: AuthzeeBatchRequest,
        page_ref: str | None,
        page_size: int
    ) -> BatchAuditResultPage:
        """Run the Batch Audit Operation for a page of results.

        Pass the returned page reference to get the next page until a null page reference is returned.
        """
        raise NotImplementedError()


    async def batch_authorize(
        self,
        batch_request: AuthzeeBatchRequest,
        page_size: int,
        parallel_pagination: bool,
        refs_page_size: int
    ) -> BatchAuthorizeResult:
        """Run the Batch Authorize Operation.
        """
        raise NotImplementedError()