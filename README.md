
<!-- ![authzee-logo](./docs/logo.svg) Documentation(Link TBD) -->
<img src="https://raw.githubusercontent.com/btemplep/authzee/main/docs/authzee_logo.svg" alt="Authzee Logo" width="500">

# Authzee Python SDK

This is the official python SDK for Authzee! It is a general usage SDK that is async, extensible, and scalable. 

Authzee is a highly expressive grant-based authorization engine. Check out the [Authzee Repo](https://github.com/btemplep/authzee) for the core engine and specification.

<!-- [Authzee Docs](https://docs.authzee.org) -->


<!-- See the [full documentation](https://authzee-py.docs.authzee.org) for more info! -->


- [Installation](#installation)
- [Tutorial](#tutorial)
    - [Simple Example](#simple-example)
    - [Authzee App](#authzee-app)
        - [Execute Function](#execute-function)
        - [Compute and Storage Modules](#compute-and-storage-modules)
    - [Context](#context)
    - [Identity](#identity)
    - [Resource](#resource)
    - [Grant](#grant)
    - [Authorize](#authorize)
- [Full Example](#full-example)
- [Development](#development)
    - [Compute and Storage Module Development](#compute-and-storage-module-development)
        - [Return Values and Error Handling](#return-values-and-error-handling)
        - [Configuration](#configuration)
    - [Module Caching](#module-caching)


## Installation

```text
$ pip install authzee
```

Extra dependencies can be installed like

```console
pip install authzee[jmespath,sql-storage]
```

Extra dependencies available/needed:

- `jmespath` - needed if using the built in jmespath execute functions
- `sql` - needed for `SQLStorage` class
- `all` - for all extra dependencies except for `dev`
- `dev` - development dependencies 


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
authz = Authzee( # for asyncio use AuthzeeAsync
    execute=jmespath_execute,
    compute_type=InProcessCompute,
    compute_kwargs={},
    storage_type=DictStorage,
    storage_kwargs={
        "storage_dict": storage_dict
    },
    config={  # optional - AuthzeeConfigOverride | None - All root and nested keys are optional
        "authzee": {
            "raise_errors": True
        }
        # "method_name": {<method config>}
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
        "equality": True, # expected result of the query
        "applicable_on_failure": False, # if True, grant is applicable even when query fails
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
        "equality": true,
        "applicable_on_failure": false,
        "data": {}
    },
    "message": "An allow grant is applicable to the request, and there are no deny grants that are applicable to the request. Therefore, the request is authorized.",
    "error": null
}
```

### Authzee App

The `Authzee` class is the entrypoint to all authzee functionality.  `AuthzeeAsync` is available for asyncio — it has the same interface as `Authzee` except all methods are async.

You can check which version of the authzee specification the SDK implements via `authzee.authzee_specification_version`.

```python
from authzee import (
    Authzee, 
    DictStorage,
    InProcessCompute,
    jmespath_execute, 
    paginator,
    authzee_specification_version
)
# for asyncio use AuthzeeAsync - same interface, all methods are async (use await)
# from authzee import AuthzeeAsync, paginator_async
# to include custom JMESPath functions use
# from authzee import jmespath_custom_execute

print(f"Authzee Specification Version: {authzee_specification_version}")

storage_dict = {}
authz = Authzee( # for asyncio use AuthzeeAsync
    execute=jmespath_execute, # for custom JMESPath functions use jmespath_custom_execute 
    compute_type=InProcessCompute, # compute backend type
    compute_kwargs={},
    storage_type=DictStorage, # storage backend type
    storage_kwargs={
        "storage_dict": storage_dict
    },
    config={  # optional - AuthzeeConfigOverride | None - All keys are optional
        "authzee": {
            "raise_errors": True
        }
        # "method_name": {<method config>}
    }
)
authz.construct() # one time setup for life of storage and compute
authz.start() # initialize the authzee app - must be run once for every instance
```

The Authzee class requires a JSON query function, compute module, and storage module. 


#### Execute Function

The execute function is a wrapper around your choice of JSON query language.
Out of the box, the SDK has:

- `jmespath_execute` 
    - Standard JMESPath python implementation
    - Must install `authzee[jmespath]`
- `jmespath_custom_execute`
    - Standard JMESPath python implementation plus extra functions as outlined in the [SDK recommendations](https://github.com/btemplep/authzee/blob/main/docs/sdks.md#standard-jmespath-extensions)
    - Must install `authzee[jmespath]`


If you want to create your own, see `jmespath_execute` for a simple example.

#### Compute and Storage Modules
The compute is used to process authorization requests.  Storage is used to store grants and definitions. The Authzee SDK includes several of these out of the box. 

Built in Compute Modules include:

- `InProcessCompute` - Compute is all done within the same process/asyncio event loop.

Built in Storage Modules include:

- `DictStorage` - Storage is in main memory within a python dict.



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
        "equality": True, # expected result of the query
        "applicable_on_failure": False, # if True, grant is applicable even when query fails
        "data": {} # data available to this grant
    }
)
```


### Authorize

Authorization is one of the core *operations* (or ops) of an authorization engine and brings all of the pieces together. 

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
        "equality": true,
        "applicable_on_failure": false,
        "data": {}
    },
    "message": "An allow grant is applicable to the request, and there are no deny grants that are applicable to the request. Therefore, the request is authorized.",
    "error": null
}
```

For authorization, Authzee evaluates the given request against each grant.  

How this works is the grant query, in this case JMESpath, is run on the combined data structure. 

In the case of the above grant and request it would run on this data:

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
        "equality": true,
        "applicable_on_failure": false,
        "data": {}
    }
}
```

The query:

```
length(request.identities.user[?department == 'Balloon Dept']) > `0`
```

Will filter all user identities by those that are in the 'Balloon Dept'.  If there are more than 0 users like that, then the result of the query will be `true`.  The expected value for the grant to be considered applicable, the `equality` is `true`.  Since the result and equality are equal the grant is considered applicable to the request and the grant effect will be applied. 

For authorization, by default, no requests are allowed.  If a grant with the deny effect is applicable, the request is denied, no matter any other outcomes.  If a grant with the allow effect is applicable, and there are no deny grants applicable then the request is allowed. 


## Full Example

For a more comprehensive example that demonstrates all Authzee methods, see [`full_example.py`](./full_example.py).


## Development

Install all dependencies
```console
pip install -e .[dev,all]
```

Use [nox](https://nox.thea.codes/en/stable/) for the most common setups. 

### Compute and Storage Module Development

The compute and storage modules are meant to be that - modular!

You should be able to build custom ones based off of the base classes `ComputeModule` and `StorageModule`. Note that all underlying methods must be async.  

#### Return Values and Error Handling

Every method on a compute or storage module returns a result body (a `dict`) rather than raising on failure.

- On success, populate the result fields and set `error` to `None`.
- On a handled failure, return the result body with its non-`error` fields set to safe defaults and `error` set to an error object (`{"error_type": ..., "message": ...}`). For example:
    - A `GenericResult` method returns `{"error": <error>}`.
    - A single-item method (like `get_context_def`) returns the item as `None` alongside the error, e.g. `{"context_def": None, "error": <error>}`.
    - A page method (like `list_grants`) returns an empty list and a `None` page reference alongside the error, e.g. `{"grants": [], "next_page_ref": None, "error": <error>}`.

You do not have to wrap every method body in a try/except. `ComputeModule` and `StorageModule` use metaclasses (`_ComputeMeta` / `_StorageMeta`) that wrap every method so any raised exception is automatically caught and translated into that method's expected result body, with `error` populated and `error_type` set to `"compute"` or `"storage"` depending on where the exception originated. You can simply raise on unexpected failures and rely on this translation.

A compute module retrieves definitions and grants from a storage module. Since storage methods return errors in their result body rather than raising, a compute module **must** check the `error` field of every storage result it receives and handle it - typically by short-circuiting and returning its own result body with that error propagated (its `error_type` will already be `"storage"`, identifying where the failure originated). Do not ignore storage errors or assume storage calls always succeed.

See the `ComputeModule` and `StorageModule` class and method docstrings for per-method return shapes and success/error examples.

#### Configuration

Every method receives a per-call `config` (a `dict`). A module does **not** have to provide or honor a value for every key its config type allows - a config type describes every option that *could* apply to that call across all module implementations, so any key a given module does not understand can simply be ignored. But a module **should** utilize the config keys that map to behavior it actually implements (for example `page_size` and `use_cache` on list methods, or `use_cache` on get methods), so callers can tune those behaviors.

Each base class documents the config it does **not** use:

- `StorageModule` does not use the nested `get_*` / `use_list_*` / `list_*` sub-configs inside the put and delete definition configs and the repeal config (`PutContextDefConfig`, `DeleteContextDefConfig`, `PutIdentityDefConfig`, `DeleteIdentityDefConfig`, `PutResourceDefConfig`, `DeleteResourceDefConfig`, `RepealConfig`). A storage module acts on the target directly by type or UUID; those nested sub-configs describe an optional "look the target up first" step that belongs to the orchestration layer, not storage.
- `ComputeModule` does not use the storage-only definition/grant persistence and retrieval configs (get/put/delete definition configs, `GetGrantConfig`, `EnactConfig`, `RepealConfig`, and the standalone `List*Config` types) or the storage latch configs, since a compute module has no corresponding operation. When a compute operation does trigger a storage call (such as listing grants during an audit or authorize), it uses the sub-config embedded in the compute config it received rather than a standalone top-level config.

See the `ComputeModule` and `StorageModule` class docstrings for the full, exact list of unused config.

### Module Caching

Caching for validating a request or batch request should be self contained within the compute model per request. Besides that, it is up to the storage module to control caching for storage calls.
