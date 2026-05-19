
from asyncio import as_completed, create_task, Task
from typing import Any, Callable, Dict, List, Type

import jsonschema_rs

from authzee.compute.compute_module import ComputeModule
from authzee.core import (
    combine_errors,
    evaluate, 
    validate_context_def,
    validate_identity_def,
    validate_resource_def,
    validate_grant,
    validate_request_schema,
    validate_batch_request_schema
)
from authzee.paginator import paginator_async
from authzee.types import *
from authzee.exceptions import NotImplementedError
from authzee.module_locality import ModuleLocality
from authzee.storage.storage_module import StorageModule

class InProcessCompute(ComputeModule):


    async def start(
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
        await super().start(
            execute=execute,
            storage_type=storage_type, 
            storage_kwargs=storage_kwargs,
            config=config
        )
        self.locality = ModuleLocality.PROCESS
        self.has_parallel_paging = False
        self._storage = storage_type(**storage_kwargs)
        await self._storage.start(config)
    
        return {
            "has_failed": False, 
            "errors": {}
        }


    async def shutdown(self, config: AuthzeeConfig) -> GenericResult:
        """Shutdown Compute module.

        - clean up runtime resources
        """
        await self._storage.shutdown()


    async def construct(self, config: AuthzeeConfig) -> GenericResult:
        """Construct backend resources for compute.

        - one time setup
        """
        pass


    async def destroy(self, config: AuthzeeConfig) -> GenericResult:
        """Tear down backend resources.

        - destructive - may lose all long lasting compute resources
        """
        pass


    async def validate_context_def(
        self,
        context_def: ContextDef,
        config: AuthzeeConfig
    ) -> GenericResult:
        return validate_context_def(context_def)


    async def validate_identity_def(
        self,
        identity_def: IdentityDef,
        config: AuthzeeConfig
    ) -> GenericResult:
        return validate_identity_def(identity_def)


    async def validate_resource_def(
        self,
        resource_def: ResourceDef,
        config: AuthzeeConfig
    ) -> GenericResult:
        return validate_resource_def(resource_def)


    async def validate_grant(
        self,
        grant: Grant,
        config: AuthzeeConfig
    ) -> GenericResult:
        return validate_grant(grant)


    async def validate_request(
        self,
        request: AuthzeeRequest,
        config: AuthzeeConfig
    ) -> GenericResult:
        """Validate a request.
        """
        result = validate_request_schema(request)
        if result['has_failed'] is True:
            return result

        context_def_task = create_task(self._storage.get_context_def(request['context_type'], config))
        resource_def_task = create_task(self._storage.get_resource_def(request['resource_type'], config))
        identity_def_tasks = [create_task(self._storage.get_identity_def(it, config)) for it in request['identities']]

        context_def = (await context_def_task)['context_def']
        if context_def is None:
            result['has_failed'] = True
            result['errors']['request'] = [
                {
                    "is_critical": True,
                    "message": f"context_type '{request['context_type']}' is not a registered context type."
                }
            ]
            
            return result
        
        if jsonschema_rs.validator_for(context_def['schema']).is_valid(request['context']) is False:
            result['has_failed'] = True
            result['errors']['request'] = [
                {
                    "is_critical": True,
                    "message": f"The given context is not valid against the '{request['context_type']}' context type."
                }
            ]

            return result

        resource_def = (await resource_def_task)['resource_def']
        if resource_def is None:
            result['has_failed'] = True
            result['errors']['request'] = [
                {
                    "is_critical": True,
                    "message": f"resource_type '{request['resource_type']}' is not a registered resource type."
                }
            ]

            return result
        
        if jsonschema_rs.validator_for(resource_def['schema']).is_valid(request['resource']) is False:
            result['has_failed'] = True
            result['errors']['request'] = [
                {
                    "is_critical": True,
                    "message": f"The given resource is not valid against the '{request['resource_type']}' resource type."
                }
            ]

            return result
        
        if request['action'] not in resource_def['actions']:
            result['has_failed'] = True
            result['errors']['request'] = [
                {
                    "is_critical": True,
                    "message": f"The given resource action is valid for the '{request['resource_type']}' resource type."
                }
            ]

            return result
        
        for id_task, i_type in zip(identity_def_tasks, request['identities']):
            identity_def = (await id_task)['identity_def']
            if identity_def is None:
                result['has_failed'] = True
                result['errors']['request'] = [
                    {
                        "is_critical": True,
                        "message": f"identity_type '{i_type}' is not a registered identity type."
                    }
                ]

                return result
            
            id_validator = jsonschema_rs.validator_for(identity_def['schema'])
            for id, i in zip(request['identities'][i_type], range(len(request['identities']))):
                if id_validator.is_valid(id) is False:
                    result['has_failed'] = True
                    result['errors']['request'] = [
                        {
                            "is_critical": True,
                            "message": f"The given identity in '{i_type}[{i}]' is not valid against the '{i_type}' identity type."
                        }
                    ]

                    return result
        
        return result


    async def validate_batch_request(
        self,
        batch_request: AuthzeeBatchRequest,
        config: AuthzeeConfig
    ) -> GenericResult:
        """Validate a batch request.
        """
        # this is a very inefficient way to do this 
        # TODO try and reuse as needed and only do partial verification of new fields 
        result = validate_batch_request_schema(batch_request)
        if result['has_failed'] is True:
            return result

        base_request: AuthzeeBatchRequest = batch_request.copy(batch_request)
        base_request.pop("batch")
        base_result = await self.validate_request(
            request=base_request,
            config=config
        )
        combine_errors(result['errors'], base_result['errors'])
        if base_result['has_failed'] is True:
            result['has_failed'] = True
            
            return result

        batch_tasks: List[Task] = []
        for item in batch_request['batch']:
            batch_tasks.append(
                create_task(
                    self.validate_request(
                        request=base_request | item,
                        config=config
                    )
                )
            )
        
        async for bt in as_completed(batch_tasks):
            bt: GenericResult
            combine_errors(result['errors'], bt['errors'])
            if bt['has_failed'] is True:
                result['has_failed'] = True
        
        return result


    async def audit_page(
        self,
        request: AuthzeeRequest,
        page_ref: str | None,
        config: AuthzeeConfig
    ) -> AuditResultPage:
        """Run the Audit Operation for a page of results.

        Pass the returned page reference to get the next page until a null page reference is returned.
        """
        result = {
            "grants": [],
            "results": [],
            "next_page_ref": None,
            "has_failed": False,
            "errors": {}
        }
        grants_page = (
            await self._storage.get_grants_page(
                effect=None,
                action=request['action'],
                page_ref=page_ref,
                config=config
            )
        )
        if grants_page['has_failed'] is True:
            result['has_failed'] = True
            result['errors'] = grants_page['errors']

            return result
        
        result['grants'] = grants_page['grants']
        result['next_page_ref'] = grants_page['next_page_ref']
        for grant in result['grants']:
            eval_result = evaluate(
                request=request,
                grant=grant,
                execute=self._execute,
                only_crits=False
            )
            result['results'].append(eval_result)
            if eval_result['has_failed'] is True:
                result['next_page_ref'] = None
                result['has_failed'] = True
                result['errors']['evaluation'] = [
                    {
                        "is_critical": True,
                        "message": f"A critical error occurred when evaluation grants[{len(result['results']) - 1}]."
                    }
                ]

                return result
        
        return result


    async def authorize(
        self,
        request: AuthzeeRequest,
        config: AuthzeeConfig
    ) -> AuthorizeResult:
        """Run the Authorize Operation.
        """
        result = {
            "is_authorized": False,
            "grant": None,
            "message": "A critical error has occurred. Therefore, the request is not authorized.",
            "has_failed": True,
            "critical_errors": {}
        }
        async for page in paginator_async(
            self._storage.get_grants_page,
            effect="deny",
            action=request['action'],
            page_ref=None,
            config=config
        ):
            page: GrantsPage
            if page['has_failed'] is True:
                result['critical_errors'] = page['errors']

                return result
        
            for grant in page['grants']:
                eval_result = evaluate(
                    request=request,
                    grant=grant,
                    execute=self._execute,
                    only_crits=True
                )
                if eval_result['has_failed'] is True:
                    result['grant'] = grant
                    result['critical_errors'] = eval_result['errors']

                    return result

                if eval_result['is_applicable'] is True:
                    return {
                        "is_authorized": False,
                        "grant": grant,
                        "message": "A deny grant is applicable to the request. Therefore, the request is not authorized.",
                        "has_failed": False,
                        "critical_errors": {}
                    }

        # got through all allow grants
        async for page in paginator_async(
            self._storage.get_grants_page,
            effect="allow",
            action=request['action'],
            page_ref=None,
            config=config
        ):
            page: GrantsPage
            if page['has_failed'] is True:
                result['critical_errors'] = page['errors']

                return result
        
            for grant in page['grants']:
                eval_result = evaluate(
                    request=request,
                    grant=grant,
                    execute=self._execute,
                    only_crits=True
                )
                if eval_result['has_failed'] is True:
                    result['grant'] = grant
                    result['critical_errors'] = eval_result['errors']

                    return result

                if eval_result['is_applicable'] is True:
                    return {
                        "is_authorized": True,
                        "grant": grant,
                        "message": "An allow grant is applicable to the request, and there are no deny grants that are applicable to the request. Therefore, the request is authorized.",
                        "has_failed": False,
                        "critical_errors": {}
                    }

        return {
            "is_authorized": False,
            "grant": None,
            "message": "No grants are applicable to the request. Therefore, the request is implicitly denied and is not authorized.",
            "has_failed": False,
            "critical_errors": {}
        }


    async def batch_audit_page(
        self,
        batch_request: AuthzeeBatchRequest,
        page_ref: str | None,
        config: AuthzeeConfig
    ) -> BatchAuditResultPage:
        """Run the Batch Audit Operation for a page of results.

        Pass the returned page reference to get the next page until a null page reference is returned.
        """
        batch_result = {
            "grants": [],
            "batch_results": [],
            "next_page_ref": None,
            "has_failed": False,
            "errors": {}
        }
        grants_page = (
            await self._storage.get_grants_page(
                effect=None,
                action=batch_request['action'],
                page_ref=page_ref,
                config=config
            )
        )
        batch_result['errors'] = grants_page['errors']
        if grants_page['has_failed'] is True:
            batch_result['has_failed'] = True

            return batch_result
        
        batch_result['next_page_ref'] = grants_page['next_page_ref']
        for _ in range(len(batch_request['batch'])):
            batch_result['batch_results'].append(
                {
                    "results": [],
                    "has_failed": False,
                    "errors": {}
                }
            )
        
        base_request = batch_request.copy()
        base_request.pop("batch")
        for grant in grants_page['grants']:
            for request, result in zip(batch_request['batch'], batch_result['batch_results']):
                if result['has_failed'] is True:
                    continue

                eval_result = evaluate(
                    request=base_request | request,
                    grant=grant,
                    execute=self._execute,
                    only_crits=False
                )
                if eval_result['has_failed'] is True:
                    result['has_failed'] = True
                    result['errors']['evaluation'] = [
                        {
                            "is_critical": True,
                            "message": f"A critical error occurred when evaluation grants[{len(result['results']) - 1}]."
                        }
                    ]

        return result


    async def batch_authorize(
        self,
        batch_request: AuthzeeBatchRequest,
        config: AuthzeeConfig
    ) -> BatchAuthorizeResult:
        """Run the Batch Authorize Operation.
        """
        batch_result = {
            "batch_results": [],
            "has_failed": False,
            "critical_errors": []
        }
        for _ in range(len(batch_request['batch'])):
            batch_result.append(
                {
                    "is_authorized": False,
                    "grant": None,
                    "message": "",
                    "has_failed": False,
                    "critical_errors": {},
                    "__complete": False
                }
            )

        base_request = batch_request.copy()
        base_request.pop("batch")
        async for page in paginator_async(
            self._storage.get_grants_page,
            effect="deny",
            action=base_request['action'],
            page_ref=None,
            config=config
        ):
            page: GrantsPage
            if page['has_failed'] is True:
                result['critical_errors'] = page['errors']

                return result

            for grant in page['grants']:
                for request, result in zip(batch_request['batch'], batch_result['batch_results']):
                    if result['__complete'] is True:
                        continue
            
                    eval_result = evaluate(
                        request=base_request | request,
                        grant=grant,
                        execute=self._execute,
                        only_crits=True
                    )
                    if eval_result['has_failed'] is True:
                        result['grant'] = grant
                        result['message'] = "A critical error has occurred. Therefore, the request is not authorized."
                        result['has_failed'] = True
                        result['critical_errors'] = eval_result['errors']
                        result['__complete'] = True
                        continue

                    if eval_result['is_applicable'] is True:
                        result['grant'] = grant
                        result['message'] = "A deny grant is applicable to the request. Therefore, the request is not authorized."
                        result['__complete'] = True
                        continue

        async for page in paginator_async(
            self._storage.get_grants_page,
            effect="allow",
            action=request['action'],
            page_ref=None,
            config=config
        ):
            page: GrantsPage
            if page['has_failed'] is True:
                result['critical_errors'] = page['errors']

                return result
        
            for grant in page['grants']:
                for request, result in zip(batch_request['batch'], batch_result['batch_results']):
                    if result['__complete'] is True:
                        continue

                    eval_result = evaluate(
                        request=base_request | request,
                        grant=grant,
                        execute=self._execute,
                        only_crits=True
                    )
                    if eval_result['has_failed'] is True:
                        result['grant'] = grant
                        result['message'] = "A critical error has occurred. Therefore, the request is not authorized."
                        result['has_failed'] = True
                        result['critical_errors'] = eval_result['errors']
                        result['__complete'] = True
                        continue

                    if eval_result['is_applicable'] is True:
                        result['is_authorized'] = True
                        result['grant'] = grant
                        result['message'] = "An allow grant is applicable to the request, and there are no deny grants that are applicable to the request. Therefore, the request is authorized."
                        result['__complete'] = True
                        continue

        for result in batch_result['batch_results']:
            is_complete = result.pop("__complete")
            if is_complete is False:
                result['message'] = "No grants are applicable to the request. Therefore, the request is implicitly denied and is not authorized."

        return batch_result