"""Core functionality for the Authzee SDK. 

The functionality of this module is optimized for SDK use.  It conforms to the Authzee Specification but is not a one to one copy of the reference implementation.
"""
import copy
from typing import Callable

import jsonschema_rs 

from authzee.types import *
from authzee import reference


context_def_schema = copy.deepcopy(reference.context_definition_schema) | {
    "title": "SDK Context Definition",
    "additionalProperties": False
}
identity_def_schema = copy.deepcopy(reference.identity_definition_schema) | {
    "title": "SDK Identity Definition",
    "additionalProperties": False
}
resource_def_schema = copy.deepcopy(reference.resource_definition_schema) | {
    "title": "SDK Resource Definition",
    "additionalProperties": False
}
grant_schema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "SDK Grant",
    "description": "A grant is an object representing enacted authorization rules. SDK specific schema.",
    "type": "object",
    "additionalProperties": False,
    "required": [
        "grant_uuid",
        "name",
        "description",
        "tags",
        "effect",
        "actions",
        "data",
        "query",
        "evaluation_handler",
        "equality"
    ],
    "properties": {
        "grant_uuid": {
            "type": "string",
            "format": "uuid"
        },
        "name": {
            "type": "string",
            "description": "Short, people friendly name. Not unique."
        },
        "description": {
            "type": "string",
            "description": "Long, people friendly description."
        },
        "tags": {
            "type": "object",
            "description": "String key/value pairs to help organize grants.",
            "properties": {
                "patternProperties": {
                    ".+": {
                        "type": "string"
                    }
                }
            }
        },
        "effect": {
            "type": "string",
            "enum": [
                "allow",
                "deny"
            ],
            "description": (
                "Any applicable deny grant will always cause the request to be unauthorized. "
                "If there are no applicable deny grants, and there is an applicable allow grant, the request is authorized. "
                "If there no applicable allow or deny grants, requests are implicitly denied and is not authorized."
            )
        },
        "actions": {
            "type": "array",
            "uniqueItems": True,
            "items": reference._action_schema,
            "description": "List of actions this grant applies to or null to match any resource action."
        },
        "data": {
            "type": "object",
            "description": "Data that is made available at query time for the grant evaluation. Easy place to store data so it doesn't have to be embedded in the query."
        },
        "query": {
            "type": "string",
            "description": "JSON query to run on the authorization data. {\"grant\": <grant>, \"request\": <request>}"
        },
        "evaluation_handler": reference._evaluation_handler_schema,
        "equality": {
            "description": "Expected value for the query to return.  If the query result matches this value the grant is a considered applicable to the request."
        }
    }
}

context_def_validator = jsonschema_rs.validator_for(context_def_schema)
identity_def_validator = jsonschema_rs.validator_for(identity_def_schema)
resource_def_validator = jsonschema_rs.validator_for(resource_def_schema)
grant_validator = jsonschema_rs.validator_for(grant_schema)
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


def validate_grant(grant: Grant) -> GenericResult:
    is_valid = grant_validator.is_valid(grant)
    if not is_valid:
        return {
            "has_failed": True,
            "errors": {
                "grant": [
                    {
                        "is_critical": True,
                        "message": "The grant is not valid against the Grant Schema." 
                    }
                ]
            }
        }

    return {
        "has_failed": False, 
        "errors": {}
    }


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

    return {
        "has_failed": False, 
        "errors": {}
    }


def validate_batch_request_schema(batch_request: AuthzeeBatchRequest) -> GenericResult:
    is_valid = batch_request_validator.is_valid(batch_request)
    if not is_valid:
        return {
            "has_failed": True,
            "errors": {
                "definition": [
                    {
                        "is_critical": True,
                        "message": "The given batch request is not valid against the batch request JSON Schema."
                    }
                ]
            }
        }

    return {
        "has_failed": False, 
        "errors": {}
    }


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


def combine_errors(result: GenericResult, *args: dict) ->  None:
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
