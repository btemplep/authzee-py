"""Core functionality for the Authzee SDK. 

The functionality of this module is optimized for SDK use and does not directly align with the reference implementation. 
"""
from dataclasses import asdict
import datetime
from typing import Any, AsyncGenerator, Callable
from uuid import UUID

import jsonschema_rs 

from authzee.dcs import *
from authzee import reference


context_def_validator = jsonschema_rs.validator_for(reference.context_definition_schema)
identity_def_validator = jsonschema_rs.validator_for(reference.identity_definition_schema)
resource_def_validator = jsonschema_rs.validator_for(reference.resource_definition_schema)
request_validator = jsonschema_rs.validator_for(reference.request_schema)
batch_request_validator = jsonschema_rs.validator_for(reference.batch_request_schema)


def _asjsondict_inner(d: Any) -> dict:
    if type(d) is datetime.datetime:
        return d.isoformat()
    elif type(d) is UUID:
        return str(d)
    elif type(d) is dict:
        new_d = {}
        for k in d:
            new_d[k] = _asjsondict_inner(d[k])
        
        return new_d
    elif type(d) is list:
        return [_asjsondict_inner(i) for i in d]
    else:
        return d



def asjsondict(dc: object) -> dict:
    return _asjsondict_inner(asdict(dc))


def validate_context_def(context_def: ContextDef) -> GenericResult:
    result = GenericResult(has_failed=context_def_validator.is_valid(asjsondict(context_def)))
    if result.has_failed is True:
        result.errors.definition = [
            GenericError(
                is_critical=True,
                message="The given context definition is not valid against the context definition JSON Schema."
            )
        ]
    
    elif not(
        "type" in context_def.schema
        and context_def.schema['type'] == "object"
    ):
        result.errors.definition = [
            GenericError(
                is_critical=True,
                message="Context Definition schemas must have a root type of object."
            )
        ]
    

    return result

        
def validate_identity_def(identity_def: IdentityDef) -> GenericResult:
    result = GenericResult(has_failed=identity_def_validator.is_valid(asjsondict(identity_def)))
    if result.has_failed is True:
        result.errors.definition = [
            GenericError(
                is_critical=True,
                message="The given identity definition is not valid against the identity definition JSON Schema."
            )
        ]
    
    elif not(
        "type" in identity_def.schema
        and identity_def.schema['type'] == "object"
    ):
        result.errors.definition = [
            GenericError(
                is_critical=True,
                message="Identity Definition schemas must have a root type of object."
            )
        ]

    return result


def validate_resource_def(resource_def: ResourceDef) -> GenericResult:
    result = GenericResult(has_failed=resource_def_validator.is_valid(asjsondict(resource_def)))
    if result.has_failed is True:
        result.errors.definition = [
            GenericError(
                is_critical=True,
                message="The given resource definition is not valid against the resource definition JSON Schema."
            )
        ]
    
    elif not(
        "type" in resource_def.schema
        and resource_def.schema['type'] == "object"
    ):
        result.errors.definition = [
            GenericError(
                is_critical=True,
                message="Resource Definition schemas must have a root type of object."
            )
        ]

    return result


def validate_request_schema(request: AuthzeeRequest) -> GenericResult:
    result = GenericResult(has_failed=not request_validator.is_valid(asjsondict(request)))
    if result.has_failed is True:
        result.errors.definition = [
            GenericError(
                is_critical=True,
                message="The given request is not valid against the request JSON Schema."
            )
        ]

    return result


async def paginator(coro, **kwargs) -> AsyncGenerator[Any, None]:
    while True:
        result = await coro(**kwargs)
        
        yield result

        kwargs['page_ref'] = result.next_page_ref
        if result.next_page_ref is None:
            break

import json
    
def evaluate(
    request: AuthzeeRequest, 
    grant: Grant, 
    execute: Callable[[str, AnyJSON], ExecuteResult],
    only_crits: bool
) -> EvaluateResult:
    result = EvaluateResult(
        is_applicable=False,
        query_result=None,
        has_failed=False
    )
    query_result = execute(
        grant.query, 
        {
            "request": asjsondict(request),
            "grant": asjsondict(grant)
        }
    )
    if query_result.has_failed is False:
        result.query_result = query_result.result
        if query_result.result == grant.equality:
            result.is_applicable = True
    else:
        q_val = grant.evaluation_handler if request.evaluation_handler == "grant" else request.evaluation_handler
        is_q_val_crit = q_val == "critical"
        if (
            (
                q_val == "error"
                and only_crits is False
            )
            or is_q_val_crit is True
        ):
            result.errors.evaluation = [
                {
                    "is_critical": is_q_val_crit,
                    "message": f"A JSON Query error has occurred: {query_result.error_message}."
                }
            ]
            if is_q_val_crit is True:
                result['has_failed'] = True

    return result