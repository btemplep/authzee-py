# `authzee`


<!-- ![authzee-logo](./docs/logo.svg) Documentation(Link TBD) -->
<img src="https://raw.githubusercontent.com/btemplep/authzee/main/docs/logo.svg" alt="Authzee Logo" width="100">

This is the official python SDK for Authzee! It is a general usage SDK that is async, extensible, and scalable. 

Authzee is a highly expressive grant-based authorization engine. Check out the [Authzee Repo](https://github.com/btemplep/authzee) for the core engine and specification.

<!-- [Authzee Docs](https://docs.authzee.org) -->


<!-- See the [full documentation](https://authzee-py.docs.authzee.org) for more info! -->


- [Installation](#installation)
- [Tutorial](#tutorial)
    - [Simple Example](#simple-example)
    - [Authzee App](#authzee-app)
    - [Context](#context)
    - [Identity](#identity)
    - [Resource](#resource)
    - [Grant](#grant)
    - [Authorize](#authorize)


## Installation


```text
$ pip install authzee
```

<!-- For different compute or storage backends you may need to install extra deps. 

```text
$ pip install authzee[sql]
```

Extra dependencies:

- `sql` - For `SQLStorage`.  -->


## Tutorial

This is the simple tutorial covering all of the main pieces to start using Authzee locally. 

### Simple Example
```python
import json
from uuid import uuid4

from authzee import Authzee, DictStorage, InProcessCompute, jmespath_execute
# for asyncio use
# from authzee import AuthzeeAsync


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
authz.put_context_def( # Context is used to pass structured data to authorization requests. Register them first as context definitions.
    {
        "context_type": "NONE", # unique
        "schema": { # JSON Schema
            "type": "object",
            "additionalProperties": False
        }
    }
)
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
```
Authorization response: 
```json
{
    "is_authorized": true,
    "grant": {
        "grant_uuid": "49d5398a-cd5e-4944-bdb9-6543d061a53e",
        "name": "Allow inflate for balloon department",
        "description": "Balloon department people are allowed to read and inflate all balloons.",
        "tags": {},
        "effect": "allow",
        "actions": [
            "balloon:read",
            "balloon:inflate"
        ],
        "query": "length(request.identities.user[?department == 'Balloon Dept']) > `0`",
        "evaluation_handler": "evaluate",
        "equality": true,
        "data": {}
    },
    "message": "An allow grant is applicable to the request, and there are no deny grants that are applicable to the request. Therefore, the request is authorized.",
    "has_failed": false,
    "critical_errors": {}
}
```

### Authzee App

The `Authzee` class is the entrypoint to all authzee functionality.  `AuthzeeAsync` is available for asyncio.

```python
from authzee import Authzee, DictStorage, InProcessCompute, jmespath_execute
# for asyncio use
# from authzee import AuthzeeAsync


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
```

The Authzee class splits requires a JSON query function, compute module, and storage module. 
The compute is uses do process authorization requests.  Storage is used to store grants and definitions. The Authzee SDK includes several of these out of the box. 

<!-- See the docs for examples -->


### Context

Context is structured data that is passed in with authorization requests. Context types are registered in authzee with context definitions. 

```python
authz.put_context_def( # Context is used to pass structured data to authorization requests. Register them first as context definitions.
    {
        "context_type": "NONE", # unique
        "schema": { # JSON Schema
            "type": "object",
            "additionalProperties": False
        }
    }
)
```


### Identity

Identities define who is being authorized. They are registered with authzee via identity definitions. 

```python
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
```

### Resource

Resources are structured data to represents your resources and actions that can be taken on them.  They are registered with Authzee as resource definitions.

```python
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
```


### Grant 

Grants are how authorization rules are represented in Authzee. 

Grants are enacted in Authzee ie a new authorization rule is added.  Grants apply to any request that has a matching resource action.

```python
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
```


### Authorize

Authorization is one of the core function of an authorization engine and brings all of the pieces together. 

```python
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
```
Authorization response: 
```json
{
    "is_authorized": true,
    "grant": {
        "grant_uuid": "49d5398a-cd5e-4944-bdb9-6543d061a53e",
        "name": "Allow inflate for balloon department",
        "description": "Balloon department people are allowed to read and inflate all balloons.",
        "tags": {},
        "effect": "allow",
        "actions": [
            "balloon:read",
            "balloon:inflate"
        ],
        "query": "length(request.identities.user[?department == 'Balloon Dept']) > `0`",
        "evaluation_handler": "evaluate",
        "equality": true,
        "data": {}
    },
    "message": "An allow grant is applicable to the request, and there are no deny grants that are applicable to the request. Therefore, the request is authorized.",
    "has_failed": false,
    "critical_errors": {}
}
```

For authorization, Authzee evaluates the given request against each grant.  

How this works is the grant query, in this case JMESpath, is run on the combined data structure. 

In the case of the above grant and request is would run on this data:

```json
{
    "request": { 
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
            "color": "inflated",
            "is_inflated": false
        },
        "evaluation_handler": "grant",
        "context_type": "NONE",
        "context": {}
    },
    "grant": {
        "grant_uuid": "49d5398a-cd5e-4944-bdb9-6543d061a53e",
        "name": "Allow inflate for balloon department",
        "description": "Balloon department people are allowed to read and inflate all balloons.",
        "tags": {},
        "effect": "allow",
        "actions": [
            "balloon:read",
            "balloon:inflate"
        ],
        "query": "length(request.identities.user[?department == 'Balloon Dept']) > `0`",
        "evaluation_handler": "evaluate",
        "equality": true,
        "data": {}
    }
}
```

The query:

```
length(request.identities.user[?department == 'Balloon Dept']) > `0`
```

Will filter all user identities by those that are in the 'Balloon Dept'.  If there are more that 0 users like that, then, the result of the query will be `true`.  The expected value for the grant to be considered application, the `equality` is `true`.  Since the result and equality are equal the grant is considered applicable to the request and the grant effect will be applied. 

For authorization, by default, no requests are allowed.  If a grant with the deny effect is applicable, the request is denied, no matter any other outcomes.  If a grant with the allow effect is applicable, and there are no deny grants applicable then the request is allowed. 


