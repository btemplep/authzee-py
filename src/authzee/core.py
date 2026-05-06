"""Core functionality for the Authzee SDK. 

The functionality of this module is optimized for SDK use and does not directly align with the reference implementation. 
"""
from typing import Any, AsyncGenerator, Callable

import jsonschema_rs 

from authzee.types import *
from authzee import reference


context_def_validator = jsonschema_rs.validator_for(reference.context_definition_schema)
identity_def_validator = jsonschema_rs.validator_for(reference.identity_definition_schema)
resource_def_validator = jsonschema_rs.validator_for(reference.resource_definition_schema)
request_validator = jsonschema_rs.validator_for(reference.request_schema)
batch_request_validator = jsonschema_rs.validator_for(reference.batch_request_schema)


def validate_context_def(context_def: ContextDef) -> GenericResult:
    is_valid = context_def_validator.is_valid(context_def)
    if not is_valid:
        return {
            "has_failed": True,
            "errors": {
                "definition": [
                    {
                        "is_critical": True,
                        "message": "The given context definition is not valid against the context definition JSON Schema."
                    }
                ]
            }
        }
    
    if not(
        "type" in context_def['schema']
        and context_def['schema']['type'] == "object"
    ):
        return {
            "has_failed": True,
            "errors": {
                "definition": [
                    {
                        "is_critical": True,
                        "message": "Context Definition schemas must have a root type of object."
                    }
                ]
            }
        }

    return {"has_failed": False, "errors": {}}

        
def validate_identity_def(identity_def: IdentityDef) -> GenericResult:
    is_valid = identity_def_validator.is_valid(identity_def)
    if not is_valid:
        return {
            "has_failed": True,
            "errors": {
                "definition": [
                    {
                        "is_critical": True,
                        "message": "The given identity definition is not valid against the identity definition JSON Schema."
                    }
                ]
            }
        }
    
    if not(
        "type" in identity_def['schema']
        and identity_def['schema']['type'] == "object"
    ):
        return {
            "has_failed": True,
            "errors": {
                "definition": [
                    {
                        "is_critical": True,
                        "message": "Identity Definition schemas must have a root type of object."
                    }
                ]
            }
        }

    return {"has_failed": False, "errors": {}}


def validate_resource_def(resource_def: ResourceDef) -> GenericResult:
    is_valid = resource_def_validator.is_valid(resource_def)
    if not is_valid:
        return {
            "has_failed": True,
            "errors": {
                "definition": [
                    {
                        "is_critical": True,
                        "message": "The given resource definition is not valid against the resource definition JSON Schema."
                    }
                ]
            }
        }
    
    if not(
        "type" in resource_def['schema']
        and resource_def['schema']['type'] == "object"
    ):
        return {
            "has_failed": True,
            "errors": {
                "definition": [
                    {
                        "is_critical": True,
                        "message": "Resource Definition schemas must have a root type of object."
                    }
                ]
            }
        }

    return {"has_failed": False, "errors": {}}


def validate_request_schema(request: AuthzeeRequest) -> GenericResult:
    is_valid = request_validator.is_valid(request)
    if not is_valid:
        return {
            "has_failed": True,
            "errors": {
                "definition": [
                    {
                        "is_critical": True,
                        "message": "The given request is not valid against the request JSON Schema."
                    }
                ]
            }
        }

    return {"has_failed": False, "errors": {}}


async def paginator(coro, **kwargs) -> AsyncGenerator[Any, None]:
    while True:
        result = await coro(**kwargs)
        
        yield result

        kwargs['page_ref'] = result['next_page_ref']
        if result['next_page_ref'] is None:
            break

def evaluate(
    request: AuthzeeRequest, 
    grant: Grant, 
    execute: Callable[[str, AnyJSON], ExecuteResult],
    only_crits: bool
) -> EvaluateResult:
    result = {
        "is_applicable": False,
        "query_result": None,
        "has_failed": False,
        "errors": {}
    }
    query_result = execute(
        grant['query'], 
        {
            "request": request,
            "grant": grant
        }
    )
    if query_result['has_failed'] is False:
        result['query_result'] = query_result['result']
        if query_result['result'] == grant['equality']:
            result['is_applicable'] = True
    else:
        q_val = grant['evaluation_handler'] if request['evaluation_handler'] == "grant" else request['evaluation_handler']
        is_q_val_crit = q_val == "critical"
        if (
            (
                q_val == "error"
                and only_crits is False
            )
            or is_q_val_crit is True
        ):
            result['errors']['evaluation'] = [
                {
                    "is_critical": is_q_val_crit,
                    "message": f"A JSON Query error has occurred: {query_result['error_message']}."
                }
            ]
            if is_q_val_crit is True:
                result['has_failed'] = True

    return result