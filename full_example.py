"""Comprehensive example demonstrating ALL methods in the Authzee class.

This file is runnable as-is. Destructive or unnecessary calls are commented out.
"""

import datetime
import json
from uuid import uuid4

from authzee import (
    AuditResultPage,
    Authzee,
    authzee_specification_version,
    BatchAuditResultPage,
    DictStorage,
    InProcessCompute,
    jmespath_execute,
    paginator
)


# The version of the authzee specification that this library implements.
print(f"Authzee Specification Version: {authzee_specification_version}")

# Create an Authzee instance with the execute function, compute module, and storage module.
# DictStorage uses an in-memory dict. InProcessCompute runs evaluation in the current process.
storage_dict = {}
authz = Authzee(
    execute=jmespath_execute, # JSON query function (JMESPath)
    compute_type=InProcessCompute, # Compute module type
    compute_kwargs={}, # KWArgs for compute module instances
    storage_type=DictStorage, # Storage module type
    storage_kwargs={ # KWArgs for storage module instances
        "storage_dict": storage_dict
    },
    config={ # Optional AuthzeeConfigOverride
        "authzee": {
            "raise_errors": True # raise exceptions on errors
        }
        # "method_name": {<method config>}  # per-method config overrides
    }
)

# One time setup for the life of storage and compute. Creates DB tables, storage setup, etc.
# Should only be run once per storage/compute lifecycle.
result = authz.construct()
print(json.dumps(result, indent=4))
# {
#     "error": null
# }

# Initialize the authzee app. Must be run once for every Authzee instance.
result = authz.start()
print(json.dumps(result, indent=4))
# {
#     "error": null
# }

# Validate a context definition without storing it.
# Context is used to pass structured data to authorization requests.
result = authz.validate_context_def(
    context_def={
        "context_type": "NONE", # unique identifier for this context type
        "schema": { # JSON Schema
            "type": "object",
            "additionalProperties": False
        }
    }
)
print(json.dumps(result, indent=4))
# {
#     "error": null
# }

# Create or update a context definition.
result = authz.put_context_def(
    context_def={
        "context_type": "NONE",
        "schema": {
            "type": "object",
            "additionalProperties": False
        }
    }
)
print(json.dumps(result, indent=4))
# {
#     "error": null
# }

# Retrieve a context definition by its context_type.
result = authz.get_context_def(context_type="NONE")
print(json.dumps(result, indent=4))
# {
#     "context_def": {
#         "context_type": "NONE",
#         "schema": {
#             "type": "object",
#             "additionalProperties": false
#         }
#     },
#     "error": null
# }

# Retrieve all context definitions. Use paginator for full iteration.
# For AuthzeeAsync use: async for page in paginator_async(authz.list_context_defs):
for page in paginator(authz.list_context_defs):
    for context_def in page['context_defs']:
        print(json.dumps(context_def, indent=4))

# {
#     "context_type": "NONE",
#     "schema": {
#         "type": "object",
#         "additionalProperties": false
#     }
# }

# Delete a context definition by its context_type.
# Commented out so NONE context remains available for the rest of the example.
# result = authz.delete_context_def(context_type="NONE")
# print(json.dumps(result, indent=4))
# {
#     "error": null
# }

# Validate an identity definition without storing it.
# Identities describe who is being authorized.
result = authz.validate_identity_def(
    identity_def={
        "identity_type": "user", # unique identifier for this identity type
        "schema": {
            "type": "object",
            "required": [
                "username",
                "department"
            ],
            "additionalProperties": False,
            "properties": {
                "username": {
                    "type": "string"
                },
                "department": {
                    "type": "string"
                }
            }
        }
    }
)
print(json.dumps(result, indent=4))
# {
#     "error": null
# }

# Create or update an identity definition.
result = authz.put_identity_def(
    identity_def={
        "identity_type": "user",
        "schema": {
            "type": "object",
            "required": [
                "username",
                "department"
            ],
            "additionalProperties": False,
            "properties": {
                "username": {
                    "type": "string"
                },
                "department": {
                    "type": "string"
                }
            }
        }
    }
)
print(json.dumps(result, indent=4))
# {
#     "error": null
# }

# Retrieve an identity definition by its identity_type.
result = authz.get_identity_def(identity_type="user")
print(json.dumps(result, indent=4))
# {
#     "identity_def": {
#         "identity_type": "user",
#         "schema": {
#             "type": "object",
#             "required": [
#                 "username",
#                 "department"
#             ],
#             "additionalProperties": false,
#             "properties": {
#                 "username": {
#                     "type": "string"
#                 },
#                 "department": {
#                     "type": "string"
#                 }
#             }
#         }
#     },
#     "error": null
# }

# Retrieve all identity definitions. Use paginator for full iteration.
# For AuthzeeAsync use: async for page in paginator_async(authz.list_identity_defs):
for page in paginator(authz.list_identity_defs):
    for identity_def in page['identity_defs']:
        print(json.dumps(identity_def, indent=4))

# Delete an identity definition by its identity_type.
# Commented out so user identity remains available for the rest of the example.
# result = authz.delete_identity_def(identity_type="user")
# print(json.dumps(result, indent=4))
# {
#     "error": null
# }

# Validate a resource definition without storing it.
# Resources define resource types and the actions that can be taken on them.
result = authz.validate_resource_def(
    resource_def={
        "resource_type": "balloon", # unique identifier for this resource type
        "actions": [ # actions that can be taken on this resource
            "balloon:read",
            "balloon:inflate",
            "balloon:pop"
        ],
        "schema": {
            "type": "object",
            "required": [
                "color",
                "is_inflated"
            ],
            "additionalProperties": False,
            "properties": {
                "color": {
                    "type": "string"
                },
                "is_inflated": {
                    "type": "boolean"
                }
            }
        }
    }
)
print(json.dumps(result, indent=4))
# {
#     "error": null
# }

# Create or update a resource definition.
result = authz.put_resource_def(
    resource_def={
        "resource_type": "balloon",
        "actions": [
            "balloon:read",
            "balloon:inflate",
            "balloon:pop"
        ],
        "schema": {
            "type": "object",
            "required": [
                "color",
                "is_inflated"
            ],
            "additionalProperties": False,
            "properties": {
                "color": {
                    "type": "string"
                },
                "is_inflated": {
                    "type": "boolean"
                }
            }
        }
    }
)
print(json.dumps(result, indent=4))
# {
#     "error": null
# }

# Retrieve a resource definition by its resource_type.
result = authz.get_resource_def(resource_type="balloon")
print(json.dumps(result, indent=4))
# {
#     "resource_def": {
#         "resource_type": "balloon",
#         "actions": [
#             "balloon:read",
#             "balloon:inflate",
#             "balloon:pop"
#         ],
#         "schema": {
#             "type": "object",
#             "required": [
#                 "color",
#                 "is_inflated"
#             ],
#             "additionalProperties": false,
#             "properties": {
#                 "color": {
#                     "type": "string"
#                 },
#                 "is_inflated": {
#                     "type": "boolean"
#                 }
#             }
#         }
#     },
#     "error": null
# }

# Retrieve all resource definitions. Use paginator for full iteration.
# For AuthzeeAsync use: async for page in paginator_async(authz.list_resource_defs):
for page in paginator(authz.list_resource_defs):
    for resource_def in page['resource_defs']:
        print(json.dumps(resource_def, indent=4))

# Delete a resource definition by its resource_type.
# Commented out so balloon resource remains available for the rest of the example.
# result = authz.delete_resource_def(resource_type="balloon")
# print(json.dumps(result, indent=4))
# {
#     "error": null
# }

# Validate a grant without storing it.
# Grants are authorization rules that allow or deny actions.
grant_uuid = str(uuid4())
grant = {
    "grant_uuid": grant_uuid,
    "name": "Allow inflate for balloon department",
    "description": "Balloon department people are allowed to read and inflate all balloons.",
    "tags": {
        "team": "balloon"
    },
    "effect": "allow",
    "actions": [
        "balloon:read",
        "balloon:inflate"
    ],
    "query": "length(request.identities.user[?department == 'Balloon Dept']) > `0`",
    "equality": True,
    "applicable_on_failure": False,
    "data": {}
}
result = authz.validate_grant(grant=grant)
print(json.dumps(result, indent=4))
# {
#     "error": null
# }

# Enact (store) a grant to create an authorization rule.
# Grants should only ever be created or destroyed.  It is not guaranteed to have an error you try to update an existing grant.
result = authz.enact(grant=grant)
print(json.dumps(result, indent=4))
# {
#     "error": null
# }

# Retrieve a grant by its UUID.
result = authz.get_grant(grant_uuid=grant_uuid)
print(json.dumps(result, indent=4))
# {
#     "grant": {
#         "grant_uuid": "<uuid>",
#         "name": "Allow inflate for balloon department",
#         "description": "Balloon department people are allowed to read and inflate all balloons.",
#         "tags": {
#             "team": "balloon"
#         },
#         "effect": "allow",
#         "actions": [
#             "balloon:read",
#             "balloon:inflate"
#         ],
#         "query": "length(request.identities.user[?department == 'Balloon Dept']) > `0`",
#         "equality": true,
#         "applicable_on_failure": false,
#         "data": {}
#     },
#     "error": null
# }

# Retrieve grants with optional effect and action filtering. Use paginator for full iteration.
# For AuthzeeAsync use: async for page in paginator_async(authz.list_grants, effect="allow"):
for page in paginator(authz.list_grants, effect="allow"):
    for g in page['grants']:
        print(json.dumps(g, indent=4))

# {
#     "grant_uuid": "<uuid>",
#     "name": "Allow inflate for balloon department",
#     "description": "Balloon department people are allowed to read and inflate all balloons.",
#     "tags": {
#         "team": "balloon"
#     },
#     "effect": "allow",
#     "actions": [
#         "balloon:read",
#         "balloon:inflate"
#     ],
#     "query": "length(request.identities.user[?department == 'Balloon Dept']) > `0`",
#     "equality": true,
#     "applicable_on_failure": false,
#     "data": {}
# }

# Retrieve grant page references for parallel pagination.
# Useful for distributing grant evaluation across workers.
# For AuthzeeAsync use: async for page in paginator_async(authz.list_grant_refs, effect="allow"):
for page in paginator(authz.list_grant_refs, effect="allow"):
    for ref in page['page_refs']:
        print(f"  Page ref: {ref}")

# Repeal (remove) a grant by its UUID.
# purge=True scans all partitions to completely remove (useful if corruption suspected).
# Commented out so the grant remains available for authorize/audit below.
# result = authz.repeal(grant_uuid=grant_uuid, purge=False)
# print(json.dumps(result, indent=4))
# {
#     "error": null
# }

# Define the authorization request used for authorize, audit, and batch methods.
request = {
    "identities": {
        "user": [
            {
                "username": "balloon_person",
                "department": "Balloon Dept"
            }
        ]
    },
    "action": "balloon:inflate",
    "resource_type": "balloon",
    "resource": {
        "color": "blue",
        "is_inflated": False
    },
    "context_type": "NONE",
    "context": {}
}

# Determine if a request is authorized. Returns is_authorized, the matching grant, and a message.
result = authz.authorize(request=request)
print(json.dumps(result, indent=4))
# {
#     "is_authorized": true,
#     "grant": {
#         "grant_uuid": "<uuid>",
#         "name": "Allow inflate for balloon department",
#         "description": "Balloon department people are allowed to read and inflate all balloons.",
#         "tags": {
#             "team": "balloon"
#         },
#         "effect": "allow",
#         "actions": [
#             "balloon:read",
#             "balloon:inflate"
#         ],
#         "query": "length(request.identities.user[?department == 'Balloon Dept']) > `0`",
#         "equality": true,
#         "applicable_on_failure": false,
#         "data": {}
#     },
#     "message": "An allow grant is applicable to the request, and there are no deny grants that are applicable to the request. Therefore, the request is authorized.",
#     "error": null
# }

# Audit how each grant evaluates against the request. Returns per-grant results.
# Use paginator for full iteration over all grants.
# For AuthzeeAsync use: async for page in paginator_async(authz.audit, request=request):
for page in paginator(authz.audit, request=request):
    page: AuditResultPage
    for r in page['results']:
        print(f"Grant: {r['grant']['name']}")
        print(f"    is_applicable: {r['is_applicable']}")
        print(f"    query_result: {r['query_result']}")

#   Grant: Allow inflate for balloon department
#     is_applicable: True
#     query_result: True

# Batch requests evaluate multiple resource variations against the same base request.
batch_request = {
    "identities": {
        "user": [
            {
                "username": "balloon_person",
                "department": "Balloon Dept"
            }
        ]
    },
    "action": "balloon:inflate",
    "resource_type": "balloon",
    "resource": {
        "color": "blue",
        "is_inflated": False
    },
    "context_type": "NONE",
    "context": {},
    "batch": [ # each batch item overrides the specified root fields
        {
            "resource": {
                "color": "red",
                "is_inflated": True
            }
        },
        {
            "resource": {
                "color": "green",
                "is_inflated": False
            }
        }
    ]
}

# Determine if each item in the batch request is authorized.
result = authz.batch_authorize(batch_request=batch_request)
print(json.dumps(result, indent=4))
# {
#     "batch": [
#         {
#             "is_authorized": true,
#             "grant": {
#                 "grant_uuid": "<uuid>",
#                 "name": "Allow inflate for balloon department",
#                 ...
#             },
#             "message": "An allow grant is applicable to the request...",
#             "error": null
#         },
#         ...
#     ],
#     "error": null
# }

# Audit how each grant evaluates against each item in the batch request.
# Use paginator for full iteration over all grants.
# For AuthzeeAsync use: async for page in paginator_async(authz.batch_audit, batch_request=batch_request):
for page in paginator(authz.batch_audit, batch_request=batch_request):
    page: BatchAuditResultPage
    for g in page['grants']:
        print(f"  Grant: {g['name']}")

    for batch_result in page['batch']:
        for r in batch_result['results']:
            print(f"    is_applicable: {r['is_applicable']}, query_result: {r['query_result']}")

#   Grant: Allow inflate for balloon department
#     is_applicable: True, query_result: True
#     is_applicable: True, query_result: True

# Clean up storage latches created before a given datetime.
# Operations should clean up their own latches, but this handles zombie latches from failures.
result = authz.cleanup_latches(before=datetime.datetime(2026, 1, 1))
print(json.dumps(result, indent=4))
# {
#     "error": null
# }

# Shutdown the authzee app. Should be run before exit for every Authzee instance.
result = authz.shutdown()
print(json.dumps(result, indent=4))
# {
#     "error": null
# }

# Tear down everything that construct set up. Deletes DB tables, storage, etc.
# DESTRUCTIVE - only run if you want to completely remove storage.
# result = authz.destroy()
# print(json.dumps(result, indent=4))
# {
#     "error": null
# }
