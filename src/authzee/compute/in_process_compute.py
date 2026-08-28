"""In-process compute module for Authzee.

All compute is done within the same process/asyncio event loop.
"""

__all__ = [
    "InProcessCompute"
]

from asyncio import create_task, gather, Task
import copy
from typing import Any, Callable, Type

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
    """Compute module that processes authorization requests in the local process.

    All compute is performed within the same process and `asyncio` event loop as the
    caller. It uses the given execute function to evaluate grant queries and a
    [](authzee.storage.storage_module.StorageModule) instance to retrieve definitions
    and grants. Request and batch-request validation caching is self contained per
    request.

    This module takes no constructor arguments. It is not meant to be instantiated or
    started directly. Instead, pass the class to the [](authzee.authzee.Authzee) (or
    [](authzee.authzee_async.AuthzeeAsync)) app as `compute_type`, and the app manages
    its lifecycle.

    Parameters
    ----------
    None

    Examples
    --------

    ```python
    from authzee import Authzee, DictStorage, InProcessCompute, jmespath_execute

    storage_dict = {}
    authz = Authzee(
        execute=jmespath_execute,
        compute_type=InProcessCompute,
        compute_kwargs={},
        storage_type=DictStorage,
        storage_kwargs={
            "storage_dict": storage_dict
        }
    )
    authz.construct()
    authz.start()
    ```
    """


    async def start(
        self,
        execute: Callable[[str, Any], Any],
        storage_type: Type[StorageModule],
        storage_kwargs: dict[str, Any],
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


    def _validate_request_from_cache(
        self,
        request: AuthzeeRequest | BatchItem,
        cd_lookup: dict[str, ContextDef | None],
        id_lookup: dict[str, IdentityDef | None],
        rd_lookup: dict[str, IdentityDef | None]
    ) -> GenericResult:
        """Must validate schema and pull of defs before this.
        """
        if "context_type" in request:
            cd = cd_lookup[request['context_type']]
            if cd is None:
                return {
                    "error": {
                        "error_type": "request",
                        "message": f"context_type '{request['context_type']}' is not a registered context type."
                    }
                }

            if (
                jsonschema_rs.validator_for(cd['schema']).is_valid(request['context'])
                is False
            ):
                return {
                    "error": {
                        "error_type": "request",
                        "message": f"The given context is not valid against the '{request['context_type']}' context type."
                    }
                }

        if "resource_type" in request:
            rd = rd_lookup[request['resource_type']]
            if rd is None:
                return {
                    "error": {
                        "error_type": "request",
                        "message": f"resource_type '{request['resource_type']}' is not a registered resource type."
                    }
                }

            if (
                jsonschema_rs.validator_for(rd['schema']).is_valid(request['resource'])
                is False
            ):
                return {
                    "error": {
                        "error_type": "request",
                        "message": f"The given resource is not valid against the '{request['resource_type']}' resource type."
                    }
                }

            if request['action'] not in rd['actions']:
                return {
                    "error": {
                        "error_type": "request",
                        "message": f"The given resource action is not valid for the '{request['resource_type']}' resource type."
                    }
                }

        if "identities" in request:
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


    async def validate_request(
        self,
        request: AuthzeeRequest,
        config: ValidateRequestConfig
    ) -> GenericResult:
        result = validate_request_schema(request)
        if result['error'] is not None:
            return result

        context_def: ContextDef | None = None
        cd_page_ref = None
        cd_task: Task = None
        cd_stop = False
        id_lookup: dict[str, IdentityDef | None] = {id_type: None for id_type in request['identities']}
        id_page_ref = None
        id_task: Task | list[Task] = None
        id_stop = False
        resource_def: ResourceDef | None = None
        rd_page_ref = None
        rd_task: Task = None
        rd_stop = False
        while (
            cd_stop is False
            or id_stop is False
            or rd_stop is False
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
                                config['get_context_def']
                            )
                        )
                    else:
                        cd_result: ContextDefResult = await cd_task
                        if cd_result['error'] is not None:
                            if cd_result['error']['error_type'] == "resource_not_found":
                                return {
                                    "error": {
                                        "error_type": "request",
                                        "message": f"context_type '{request['context_type']}' is not a registered context type."
                                    }
                                }

                            else:
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
                        if id_page['error'] is not None:
                            return {
                                "error": id_page['error']
                            }

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
                            create_task(self._storage.get_identity_def(i_type, config['get_identity_def']))
                            for i_type in id_lookup
                        ]
                    else:
                        id_results: list[IdentityDefResult] = await gather(*id_task)
                        for id_result, i_type in zip(id_results, id_lookup):
                            if id_result['error'] is not None:
                                if id_result['error']['error_type'] == "resource_not_found":
                                    return {
                                        "error": {
                                            "error_type": "request",
                                            "message": f"identity_type '{i_type}' is not a registered identity type."
                                        }
                                    }

                                else:
                                    return {
                                        "error": id_result['error']
                                    }

                            id_lookup[i_type] = id_result['identity_def']

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
                            return {
                                "error": rd_page['error']
                            }

                        rd_page_ref = rd_page['next_page_ref']
                        for rd in rd_page['resource_defs']:
                            if rd['resource_type'] == request['resource_type']:
                                resource_def = rd
                                rd_stop = True
                                break

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
                                config['get_resource_def']
                            )
                        )
                    else:
                        rd_result: ResourceDefResult = await rd_task
                        if rd_result['error'] is not None:
                            if rd_result['error']['error_type'] == "resource_not_found":
                                return {
                                    "error": {
                                        "error_type": "request",
                                        "message": f"resource_type '{request['resource_type']}' is not a registered resource type."
                                    }
                                }

                            else:
                                return {
                                    "error": rd_result['error']
                                }

                        resource_def = rd_result['resource_def']
                        rd_stop = True

        return self._validate_request_from_cache(
            request=request,
            cd_lookup={
                request['context_type']: context_def
            },
            id_lookup=id_lookup,
            rd_lookup={
                request['resource_type']: resource_def
            }
        )


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

        #first collect lookups for context defs, identity defs and resource defs
        cd_lookup: dict[str, ContextDef] = {
            batch_request['context_type']: None
        }
        id_lookup: dict[str, IdentityDef] = {id_type: None for id_type in batch_request['identities']}
        rd_lookup: dict[str, ResourceDef] = {
            batch_request['resource_type']: None
        }
        for item in batch_request['batch']:
            if (
                "context_type" in item
                and item['context_type'] not in cd_lookup
            ):
                cd_lookup[item['context_type']] = None

            if "identities" in item:
                for i_type in item['identities']:
                    if i_type not in id_lookup:
                        id_lookup[i_type] = None

            if (
                "resource_type" in item
                and item['resource_type'] not in rd_lookup
            ):
                rd_lookup[item['resource_type']] = None

        cd_page_ref = None
        cd_task: Task | list[Task] = None
        cd_stop = False
        id_page_ref = None
        id_task: Task | list[Task] = None
        id_stop = False
        rd_page_ref = None
        rd_task: Task | list[Task] = None
        rd_stop = False
        while (
            cd_stop is False
            or id_stop is False
            or rd_stop is False
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
                            return {
                                "batch": [],
                                "error": cd_page['error']
                            }

                        cd_page_ref = cd_page['next_page_ref']
                        for cd in cd_page['context_defs']:
                            if cd['context_type'] in cd_lookup:
                                cd_lookup[cd['context_type']] = cd
                                cd_stop = True
                                for cd in cd_lookup.values():
                                    if cd is None:
                                        cd_stop = False
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
                        cd_task = [
                            create_task(self._storage.get_context_def(c_type, config['get_context_def']))
                            for c_type in cd_lookup
                        ]
                    else:
                        cd_results: list[ContextDefResult] = await gather(*cd_task)
                        for cd_result, c_type in zip(cd_results, cd_lookup):
                            if cd_result['error'] is None:
                                cd_lookup[c_type] = cd_result['context_def']
                            elif c_type == batch_request['context_type']:
                                # if it's a root request level error, then return base errors
                                if cd_result['error']['error_type'] == "resource_not_found":
                                    return {
                                        "batch": [],
                                        "error": {
                                            "error_type": "request",
                                            "message": f"context_type '{batch_request['context_type']}' is not a registered context type."
                                        }
                                    }

                                else:
                                    return {
                                        "batch": [],
                                        "error": cd_result['error']
                                    }

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
                        if id_page['error'] is not None:
                            return {
                                "batch": [],
                                "error": id_page['error']
                            }

                        id_page_ref = id_page['next_page_ref']
                        for id in id_page['identity_defs']:
                            if id['identity_type'] in id_lookup:
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
                            create_task(self._storage.get_identity_def(i_type, config['get_identity_def']))
                            for i_type in id_lookup
                        ]
                    else:
                        id_results: list[IdentityDefResult] = await gather(*id_task)
                        for id_result, i_type in zip(id_results, id_lookup):
                            if id_result['error'] is None:
                                id_lookup[i_type] = id_result['identity_def']
                            elif i_type in batch_request['identities']:
                                # if it's a root request level error, then return base errors
                                if id_result['error']['error_type'] == "resource_not_found":
                                    return {
                                        "batch": [],
                                        "error": {
                                            "error_type": "request",
                                            "message": f"identity_type '{i_type}' is not a registered identity type."
                                        }
                                    }

                                else:
                                    return {
                                        "batch": [],
                                        "error": id_result['error']
                                    }

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
                        rd_page: ContextDefsPage = await rd_task
                        if rd_page['error'] is not None:
                            return {
                                "batch": [],
                                "error": rd_page['error']
                            }

                        rd_page_ref = rd_page['next_page_ref']
                        for rd in rd_page['resource_defs']:
                            if rd['resource_type'] in rd_lookup:
                                rd_lookup[rd['resource_type']] = rd
                                rd_stop = True
                                for rd in rd_lookup.values():
                                    if rd is None:
                                        rd_stop = False
                                        break

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
                        rd_task = [
                            create_task(self._storage.get_resource_def(r_type, config['get_resource_def']))
                            for r_type in rd_lookup
                        ]
                    else:
                        rd_results: list[ContextDefResult] = await gather(*rd_task)
                        for rd_result, r_type in zip(rd_results, rd_lookup):
                            if rd_result['error'] is None:
                                rd_lookup[r_type] = rd_result['resource_def']
                            elif r_type == batch_request['resource_type']:
                                # if it's a root request level error, then return base errors
                                if rd_result['error']['error_type'] == "resource_not_found":
                                    return {
                                        "batch": [],
                                        "error": {
                                            "error_type": "request",
                                            "message": f"resource_type '{batch_request['resource_type']}' is not a registered resource type."
                                        }
                                    }

                                else:
                                    return {
                                        "batch": [],
                                        "error": rd_result['error']
                                    }

                        rd_stop = True

        val = self._validate_request_from_cache(
            request=batch_request,
            cd_lookup=cd_lookup,
            id_lookup=id_lookup,
            rd_lookup=rd_lookup
        )
        if val['error'] is not None:
            return {
                "batch": [],
                "error": val['error']
            }

        result = {
            "batch": [],
            "error": None
        }
        for b in batch_request['batch']:
            request = copy.deepcopy(b)
            if "context_type" in request or "context" in request:
                if "context" not in request:
                    request['context'] = batch_request['context']

                if "context_type" not in request:
                    request['context_type'] = batch_request['context_type']

            if "resource_type" in request or "resource" in request:
                # must copy because we add to the original
                request['action'] = batch_request['action']
                if "resource_type" not in request:
                    request['resource_type'] = batch_request['resource_type']

                if "resource" not in request:
                    request['resource'] = batch_request['resource']

            result['batch'].append(
                self._validate_request_from_cache(
                    request=request,
                    cd_lookup=cd_lookup,
                    id_lookup=id_lookup,
                    rd_lookup=rd_lookup
                )
            )

        return result


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
