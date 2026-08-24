"""In-process compute module for Authzee.

All compute is done within the same process/asyncio event loop.
"""

__all__ = [
    "InProcessCompute"
]

from asyncio import as_completed, create_task, gather, Task
from typing import Any, Callable, Dict, List, Type

import jsonschema_rs

from authzee.compute.compute_module import ComputeModule
from authzee.core import (
    evaluate,
    validate_batch_request_schema,
    validate_context_def,
    validate_grant,
    validate_identity_def,
    validate_request_schema,
    validate_resource_def
)
from authzee.module_locality import ModuleLocality
from authzee.paginators import paginator_async
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


class InProcessCompute(ComputeModule):


    async def start(
        self,
        execute: Callable[[str, Any], Any],
        storage_type: Type[StorageModule],
        storage_kwargs: Dict[str, Any],
        config: ComputeStartConfig
    ) -> GenericResult:
        await super().start(
            execute=execute,
            storage_type=storage_type,
            storage_kwargs=storage_kwargs,
            config=config
        )
        self.locality = ModuleLocality.PROCESS
        self.has_parallel_paging = False
        self._storage = storage_type(**storage_kwargs)
        await self._storage.start(config['storage'])

        return {
            "error": None
        }


    async def shutdown(self, config: ComputeShutdownConfig) -> GenericResult:
        await self._storage.shutdown(config['storage'])

        return {
            "error": None
        }


    async def construct(self, config: ComputeConstructConfig) -> GenericResult:
        return {
            "error": None
        }


    async def destroy(self, config: ComputeDestroyConfig) -> GenericResult:
        return {
            "error": None
        }


    async def validate_context_def(
        self,
        context_def: ContextDef,
        config: ValidateContextDefConfig
    ) -> GenericResult:
        return validate_context_def(context_def)


    async def validate_identity_def(
        self,
        identity_def: IdentityDef,
        config: ValidateIdentityDefConfig
    ) -> GenericResult:
        return validate_identity_def(identity_def)


    async def validate_resource_def(
        self,
        resource_def: ResourceDef,
        config: ValidateResourceDefConfig
    ) -> GenericResult:
        return validate_resource_def(resource_def)


    async def validate_grant(
        self,
        grant: Grant,
        config: ValidateGrantConfig
    ) -> GenericResult:
        return validate_grant(grant)


    async def validate_request(
        self,
        request: AuthzeeRequest,
        config: ValidateRequestConfig
    ) -> GenericResult:
        result = validate_request_schema(request)
        if result['error'] is not None:
            return result

        context_def: ContextDef = None
        cd_page_ref = None
        cd_task: Task = None
        cd_stop = False
        id_lookup: Dict[str, IdentityDef] = {id_type: None for id_type in request['identities']}
        id_page_ref = None
        id_task: Task = None
        id_stop = False
        resource_def: ResourceDef = None
        rd_page_ref = None
        rd_task: Task = None
        rd_stop = False
        while (
            cd_stop is False
            or id_stop is False
            or rd_stop is None
        ):
            if cd_stop is False:
                if config['use_list_context_defs'] is True:
                    if cd_task is None:
                        cd_task = create_task(
                            self._storage.list_context_defs(
                                page_ref=None,
                                config=config['list_context_defs']
                            )
                        )
                    else:
                        cd_page: ContextDefsPage = await cd_task
                        if cd_page['error'] is not None:
                            # TODO cancel all tasks
                            return {
                                "error": cd_page['error']
                            }

                        cd_page_ref = cd_page['next_page_ref']
                        for cd in cd_page['context_defs']:
                            if cd['context_type'] == request['context_type']:
                                context_def = cd
                                cd_stop = True
                                break

                        if cd_page_ref is None:
                            cd_stop = True
                        elif cd_stop is False:
                            cd_task = create_task(
                                self._storage.list_context_defs(
                                    page_ref=cd_page_ref,
                                    config=config['list_context_defs']
                                )
                            )

                else:
                    if cd_task is None:
                        cd_task = create_task(
                            self._storage.get_context_def(
                                request['context_type'],
                                config['get_identity_def']
                            )
                        )
                    else:
                        cd_result: ContextDefResult = await cd_task
                        if (
                            cd_result['error'] is not None
                            and cd_result['error']['error_type'] != "resource_not_found"
                        ):
                            # TODO cancel all tasks
                            return {
                                "error": cd_result['error']
                            }

                        context_def = cd_result['context_def']
                        cd_stop = True

            if id_stop is False:
                if config['use_list_identity_defs'] is True:
                    if id_task is None:
                        id_task = create_task(
                            self._storage.list_identity_defs(
                                page_ref=None,
                                config=config['list_identity_defs']
                            )
                        )
                    else:
                        id_page: IdentityDefsPage = await id_task
                        id_page_ref = id_page['next_page_ref']
                        for id in id_page['identity_defs']:
                            if id['identity_type'] in request['identities']:
                                id_lookup[id['identity_type']] = id
                                id_stop = True
                                for id in id_lookup.values():
                                    if id is None:
                                        id_stop = False
                                        break

                        if id_page_ref is None:
                            id_stop = True
                        elif id_stop is False:
                            id_task = create_task(
                                self._storage.list_identity_defs(
                                    page_ref=id_page_ref,
                                    config=config['list_identity_defs']
                                )
                            )

                else:
                    if id_task is None:
                        id_task = [
                            create_task(self._storage.get_identity_def(it, config['get_identity_def']))
                            for it in request['identities']
                        ]
                    else:
                        cancel_tasks
                        for t in as_completed(id_task):
                            idr: IdentityDefResult = await t
                            if idr['error'] is not None:
                                if idr['error']['error_type'] != "resource_not_found":
                                    # TODO cancel all tasks
                                    return {
                                        "error": idr['error']
                                    }

                                else:
                                    # TOD cancel all IDR tasks
                                    break

                        id_stop = True

            if rd_stop is False:
                if config['use_list_resource_defs'] is True:
                    if rd_task is None:
                        rd_task = create_task(
                            self._storage.list_resource_defs(
                                page_ref=None,
                                config=config['list_resource_defs']
                            )
                        )
                    else:
                        rd_page: ResourceDefsPage = await rd_task
                        if rd_page['error'] is not None:
                            # TODO cancel all tasks
                            return {
                                "error": rd_page['error']
                            }

                        rd_page_ref = rd_page['next_page_ref']
                        for rd in rd_page['resource_defs']:
                            if rd['resource_type'] == request['resource_type']:
                                resource_def = rd
                                rd_stop = True

                        if rd_page_ref is None:
                            rd_stop = True
                        elif rd_stop is False:
                            rd_task = create_task(
                                self._storage.list_resource_defs(
                                    page_ref=rd_page_ref,
                                    config=config['list_resource_defs']
                                )
                            )

                else:
                    if rd_task is None:
                        rd_task = create_task(
                            self._storage.get_resource_def(
                                request['resource_type'],
                                config['get_identity_def']
                            )
                        )
                    else:
                        resource_def = (await rd_task)['resource_def']
                        rd_stop = True

        if context_def is None:
            return {
                "error": {
                    "error_type": "request",
                    "message": f"context_type '{request['context_type']}' is not a registered context type."
                }
            }

        if (
            jsonschema_rs.validator_for(context_def['schema']).is_valid(request['context'])
            is False
        ):
            return {
                "error": {
                    "error_type": "request",
                    "message": f"The given context is not valid against the '{request['context_type']}' context type."
                }
            }

        if resource_def is None:
            return {
                "error": {
                    "error_type": "request",
                    "message": f"resource_type '{request['resource_type']}' is not a registered resource type."
                }
            }

        if (
            jsonschema_rs.validator_for(
                resource_def['schema']
            ).is_valid(
                request['resource']
            )
            is False
        ):
            return {
                "error": {
                    "error_type": "request",
                    "message": f"The given resource is not valid against the '{request['resource_type']}' resource type."
                }
            }

        if request['action'] not in resource_def['actions']:
            return {
                "error": {
                    "error_type": "request",
                    "message": f"The given resource action is not valid for the '{request['resource_type']}' resource type."
                }
            }

        for i_type, id in id_lookup.items():
            if id is None:
                return {
                    "error": {
                        "error_type": "request",
                        "message": f"identity_type '{i_type}' is not a registered identity type."
                    }
                }

            identity_validator = jsonschema_rs.validator_for(id['schema'])
            for identity, i in zip(
                request['identities'][i_type],
                range(len(request['identities'][i_type]))
            ):
                if identity_validator.is_valid(identity) is False:
                    return {
                        "error": {
                            "error_type": "request",
                            "message": f"The given identity in '{i_type}[{i}]' is not valid against the '{i_type}' identity type."
                        }
                    }

        return {
            "error": None
        }


    async def validate_batch_request(
        self,
        batch_request: AuthzeeBatchRequest,
        config: ValidateBatchRequestConfig
    ) -> ValidateBatchRequestResult:
        result = validate_batch_request_schema(batch_request)
        if result['error'] is not None:
            return {
                "error": result['error'],
                "batch": []
            }

        base_request: AuthzeeBatchRequest = batch_request.copy()
        base_request.pop("batch")
        base_result = await self.validate_request(request=base_request, config=config)
        if base_result['error'] is not None:
            return {
                "error": base_result['error'],
                "batch": []
            }

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

        batch_results: List[GenericResult] = await gather(*batch_tasks)
        batch: list = []
        for bt_result in batch_results:
            if bt_result['error'] is not None:
                batch.append(bt_result['error'])
            else:
                batch.append(None)

        return {
            "error": None,
            "batch": batch
        }


    async def audit(
        self,
        request: AuthzeeRequest,
        page_ref: str | None,
        config: AuditConfig
    ) -> AuditResultPage:
        result = {
            "results": [],
            "next_page_ref": None,
            "error": None
        }
        grants_page = (
            await self._storage.list_grants(
                effect=None,
                action=request['action'],
                page_ref=page_ref,
                config=config['list_grants']
            )
        )
        if grants_page['error'] is not None:
            result['error'] = grants_page['error']

            return result

        result['next_page_ref'] = grants_page['next_page_ref']
        for grant in grants_page['grants']:
            eval_result = evaluate(
                request=request,
                grant=grant,
                execute=self._execute
            )
            result['results'].append(
                {
                    "grant": grant,
                    "is_applicable": eval_result['is_applicable'],
                    "query_result": eval_result['query_result'],
                    "failure": eval_result['failure']
                }
            )

        return result


    async def authorize(
        self,
        request: AuthzeeRequest,
        config: AuthorizeConfig
    ) -> AuthorizeResult:
        async for page in paginator_async(
            self._storage.list_grants,
            effect="deny",
            action=request['action'],
            page_ref=None,
            config=config['list_grants']
        ):
            page: GrantsPage
            if page['error'] is not None:
                return {
                    "is_authorized": False,
                    "grant": None,
                    "message": "An error has occurred. Therefore, the request is not authorized.",
                    "error": page['error']
                }

            for grant in page['grants']:
                eval_result = evaluate(
                    request=request,
                    grant=grant,
                    execute=self._execute
                )
                if eval_result['is_applicable'] is True:
                    return {
                        "is_authorized": False,
                        "grant": grant,
                        "message": "A deny grant is applicable to the request. Therefore, the request is not authorized.",
                        "error": None
                    }

        async for page in paginator_async(
            self._storage.list_grants,
            effect="allow",
            action=request['action'],
            page_ref=None,
            config=config['list_grants']
        ):
            page: GrantsPage
            if page['error'] is not None:
                return {
                    "is_authorized": False,
                    "grant": None,
                    "message": "An error has occurred. Therefore, the request is not authorized.",
                    "error": page['error']
                }

            for grant in page['grants']:
                eval_result = evaluate(
                    request=request,
                    grant=grant,
                    execute=self._execute
                )
                if eval_result['is_applicable'] is True:
                    return {
                        "is_authorized": True,
                        "grant": grant,
                        "message": "An allow grant is applicable to the request, and there are no deny grants that are applicable to the request. Therefore, the request is authorized.",
                        "error": None
                    }

        return {
            "is_authorized": False,
            "grant": None,
            "message": "No grants are applicable to the request. Therefore, the request is implicitly denied and is not authorized.",
            "error": None
        }


    async def batch_audit(
        self,
        batch_request: AuthzeeBatchRequest,
        page_ref: str | None,
        config: BatchAuditConfig
    ) -> BatchAuditResultPage:
        batch_result = {
            "grants": [],
            "batch": [],
            "next_page_ref": None,
            "error": None
        }
        grants_page = (
            await self._storage.list_grants(
                effect=None,
                action=batch_request['action'],
                page_ref=page_ref,
                config=config['list_grants']
            )
        )
        if grants_page['error'] is not None:
            batch_result['error'] = grants_page['error']

            return batch_result

        batch_result['grants'] = grants_page['grants']
        batch_result['next_page_ref'] = grants_page['next_page_ref']
        for _ in range(len(batch_request['batch'])):
            batch_result['batch'].append(
                {
                    "results": [],
                    "error": None
                }
            )

        base_request = batch_request.copy()
        base_request.pop("batch")
        for grant in grants_page['grants']:
            for request, item_result in zip(batch_request['batch'], batch_result['batch']):
                eval_result = evaluate(
                    request=base_request | request,
                    grant=grant,
                    execute=self._execute
                )
                item_result['results'].append(
                    {
                        "is_applicable": eval_result['is_applicable'],
                        "query_result": eval_result['query_result'],
                        "failure": eval_result['failure']
                    }
                )

        return batch_result


    async def batch_authorize(
        self,
        batch_request: AuthzeeBatchRequest,
        config: BatchAuthorizeConfig
    ) -> BatchAuthorizeResult:
        batch_result: BatchAuthorizeResult = {
            "batch": [],
            "error": None
        }
        for _ in range(len(batch_request['batch'])):
            batch_result['batch'].append(
                {
                    "is_authorized": False,
                    "grant": None,
                    "message": "",
                    "error": None,
                    "__complete": False
                }
            )

        base_request = batch_request.copy()
        base_request.pop("batch")
        async for page in paginator_async(
            self._storage.list_grants,
            effect="deny",
            action=batch_request['action'],
            page_ref=None,
            config=config['list_grants']
        ):
            page: GrantsPage
            if page['error'] is not None:
                batch_result['error'] = page['error']

                return batch_result

            for grant in page['grants']:
                for request, result in zip(batch_request['batch'], batch_result['batch']):
                    if result['__complete'] is True:
                        continue

                    eval_result = evaluate(
                        request=base_request | request,
                        grant=grant,
                        execute=self._execute
                    )
                    if eval_result['is_applicable'] is True:
                        result['grant'] = grant
                        result['message'] = "A deny grant is applicable to the request. Therefore, the request is not authorized."
                        result['__complete'] = True
                        continue

        async for page in paginator_async(
            self._storage.list_grants,
            effect="allow",
            action=batch_request['action'],
            page_ref=None,
            config=config['list_grants']
        ):
            page: GrantsPage
            if page['error'] is not None:
                batch_result['error'] = page['error']

                return batch_result

            for grant in page['grants']:
                for request, result in zip(batch_request['batch'], batch_result['batch']):
                    if result['__complete'] is True:
                        continue

                    eval_result = evaluate(
                        request=base_request | request,
                        grant=grant,
                        execute=self._execute
                    )
                    if eval_result['is_applicable'] is True:
                        result['is_authorized'] = True
                        result['grant'] = grant
                        result['message'] = "An allow grant is applicable to the request, and there are no deny grants that are applicable to the request. Therefore, the request is authorized."
                        result['__complete'] = True
                        continue

        for result in batch_result['batch']:
            is_complete = result.pop("__complete")
            if is_complete is False:
                result['message'] = "No grants are applicable to the request. Therefore, the request is implicitly denied and is not authorized."

        return batch_result
