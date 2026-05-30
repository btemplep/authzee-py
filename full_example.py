import json
from uuid import uuid4

from authzee import Authzee, DictStorage, InProcessCompute, jmespath_execute, paginator, types


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
authz.construct() # one time setup for life of storage and compute
authz.start() # initialize the authzee app - must be run once for every instance
result = authz.put_context_def( # Context is used to pass structured data to authorization requests. Register/update them first as context definitions.
    {
        "context_type": "NONE", # unique
        "schema": { # JSON Schema
            "type": "object",
            "additionalProperties": False
        }
    }
)
print(json.dumps(result, indent=4))
# Note that most results will be a "GenericResult" if there is no expected output, unless specified.  Each methods docs also have input and output examples.
# { 
#     "has_failed": False,
#     "errors": {
#         "definition": [
#             {
#                 "is_critical": False,
#                 "message": "Error message."
#             }
#         ]
#     }
# }
# List all context defs
for page in paginator(authz.list_context_defs):
    for context_def in page['context_defs']:
        print(json.dumps(context_def, indent=4))
# {
#     "context_type": "NONE", # unique
#     "schema": { # JSON Schema
#         "type": "object",
#         "additionalProperties": False
#     }
# }
# Delete context defs by type
# authz.delete_context_def("NONE")

authz.put_identity_def( # identities describe who is being authorized. Register them first as identity definitions.
    identity_def={
        "identity_type": "user", # unique
        "schema": { # JSON Schema
            "type":"object",
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
# List all identity defs
for page in paginator(authz.list_identity_defs):
    for context_def in page['identity_defs']:
        print(json.dumps(context_def, indent=4))
# {
#     "identity_type": "user", # unique
#     "schema": { # JSON Schema
#         "type":"object",
#         "required": [
#             "username",
#             "department"
#         ],
#         "additionalProperties": False,
#         "properties": {
#             "username": {
#                 "type": "string"
#             },
#             "department": {
#                 "type": "string"
#             }
#         }
#     }
# }
# Delete identity defs by type
authz.delete_identity_def("NONE")
authz.put_resource_def( # resources define resource types and actions that can be taken on those resources.  Register them first as resource definitions.
    resource_def={
        "resource_type": "balloon", # unique
        "actions": [ # can be shared between resource types
            "balloon:read",
            "balloon:inflate",
            "balloon:pop"
        ],
        "schema": { # JSON Schema
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
authz.enact( # Enact grants to create authorization rules
    grant={
        "grant_uuid": str(uuid4()),
        "name": "Allow inflate for balloon department", # not unique
        "description": "Balloon department people are allowed to read and inflate all balloons.",
        "tags": {}, # tags for categorizing grants
        "effect": "allow", # allow or deny
        "actions": [ # list of actions to match
            "balloon:read",
            "balloon:inflate"
        ],
        "query": "length(request.identities.user[?department == 'Balloon Dept']) > `0`", # JSON Query for the request. JMESPath is preferred
        # query runs on {"request": <request>, "grant": <grant>}
        "evaluation_handler": "evaluate", 
        "equality": True, # expected result of the query
        "data": {} # data available to this grant
    }
)
result = authz.authorize(
    { # request for authorization runs on:
        "identities": { # identities
            "user": [ # identity_type with array of instances
                {
                    "username": "balloon_person",
                    "department": "Balloon Dept"
                }
            ]
        },
        "action": "balloon:inflate",
        "resource_type": "balloon",
        "resource": {
            "color": "inflated",
            "is_inflated": False
        },
        "evaluation_handler": "grant",
        "context_type": "NONE",
        "context": {}
    }
)
print(json.dumps(result, indent=4))
print(json.dumps(authz.list_grants(), indent=4))