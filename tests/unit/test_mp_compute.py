"""Unit tests for authzee.compute MPCompute.

`MPCompute` offloads every compute operation to a worker process pool where a
per-worker `InProcessCompute` does the real work. To exercise `MPCompute`'s own
code deterministically (without the cost and `__main__` requirements of spawning
real processes), these tests:

- Patch the running loop's `run_in_executor` so the executor task runs inline in
the test process, driving each delegation method through its real body.
- Seed a real in-process worker `InProcessCompute` via the module-level
`_executor_start` so `_executor_run` resolves against it.
- Patch `ProcessPoolExecutor` in `start` so no real pool is spawned.

The module-level `_executor_start` / `_executor_run` helpers are also tested
directly.
"""

import asyncio
import os
import sys
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest


sys.path.insert(0, os.path.dirname(__file__))

from authzee.compute.in_process_compute import InProcessCompute
import authzee.compute.mp_compute as mp_compute_module
from authzee.compute.mp_compute import _executor_run, _executor_start, MPCompute
from authzee.jmespath import jmespath_execute
from authzee.module_locality import ModuleLocality
from authzee.storage.dict_storage import DictStorage


CONTEXT_DEF = {
    "context_type": "NONE",
    "schema": {
        "type": "object",
        "additionalProperties": False
    }
}
IDENTITY_DEF = {
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
RESOURCE_DEF = {
    "resource_type": "balloon",
    "actions": [
        "balloon:read",
        "balloon:inflate"
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
GRANT = {
    "grant_uuid": "0da5dfc6-c919-4bd6-b80f-a351a9ac8d27",
    "name": "Allow inflate",
    "description": "",
    "tags": {},
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
REQUEST = {
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
        "color": "red",
        "is_inflated": False
    },
    "context_type": "NONE",
    "context": {}
}
BATCH_REQUEST = {
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
        "color": "red",
        "is_inflated": False
    },
    "context_type": "NONE",
    "context": {},
    "batch": [
        {}
    ]
}
VALIDATE_REQUEST_CONFIG = {
    "get_context_def": {
        "use_cache": False
    },
    "use_list_context_defs": False,
    "list_context_defs": {
        "page_size": 1000,
        "use_cache": False
    },
    "get_identity_def": {
        "use_cache": False
    },
    "use_list_identity_defs": False,
    "list_identity_defs": {
        "page_size": 1000,
        "use_cache": False
    },
    "get_resource_def": {
        "use_cache": False
    },
    "use_list_resource_defs": False,
    "list_resource_defs": {
        "page_size": 1000,
        "use_cache": False
    }
}
LIST_GRANTS_CONFIG = {
    "page_size": 1000,
    "use_cache": False
}


class _InlineFuture:
    """Awaitable wrapper that resolves to a precomputed value."""


    def __init__(self, value):
        self._value = value


    def __await__(self):
        if False:
            yield

        return self._value


@pytest.fixture
def storage_dict():
    return {}


@pytest.fixture
def worker(storage_dict):
    """Seed the module-level worker `InProcessCompute` used by `_executor_run`.

    Also seeds a `DictStorage` (sharing `storage_dict`) with defs and a grant so
    the request/audit/authorize delegations have data to work against.
    """

    async def setup():
        storage = DictStorage(storage_dict=storage_dict)
        await storage.construct(config={})
        await storage.start(config={})
        await storage.put_context_def(CONTEXT_DEF, config={})
        await storage.put_identity_def(IDENTITY_DEF, config={})
        await storage.put_resource_def(RESOURCE_DEF, config={})
        await storage.enact(grant=GRANT, config={})

    asyncio.run(setup())
    _executor_start(
        InProcessCompute,
        {},
        jmespath_execute,
        DictStorage,
        {
            "storage_dict": storage_dict
        },
        {
            "storage": {}
        }
    )

    yield mp_compute_module._authzee_compute

    asyncio.run(
        mp_compute_module._authzee_compute.shutdown(config={})
    )


@pytest.fixture
def compute(worker):
    """An `MPCompute` whose `run_in_executor` runs inline against the worker."""
    c = MPCompute(
        max_workers=2,
        worker_compute=InProcessCompute,
        worker_kwargs={}
    )
    c._executor = MagicMock()

    return c


class _InlineLoop:
    """Fake event loop whose ``run_in_executor`` runs the target inline."""


    def run_in_executor(self, executor, func, *args):
        return _InlineFuture(func(*args))


def _run_inline(coro_func):
    """Drive a single delegation coroutine to completion with no running loop.

    The delegation methods are ``return await asyncio.get_running_loop().run_in_executor(...)``.
    We patch `get_running_loop` to a fake loop that runs the executor target
    inline and stepping the coroutine manually via ``send`` means there is no
    real running event loop when the target `_executor_run` calls `asyncio.run`,
    faithfully mirroring a real worker process.
    """
    with patch.object(
        asyncio,
        "get_running_loop",
        return_value=_InlineLoop()
    ):
        coro = coro_func()
        try:
            coro.send(None)
        except StopIteration as stop:
            return stop.value

        raise AssertionError("coroutine did not complete synchronously")


def test_mp_start_sets_locality_and_creates_executor(storage_dict):
    c = MPCompute(
        max_workers=3,
        worker_compute=InProcessCompute,
        worker_kwargs={}
    )
    fake_executor = MagicMock()
    with patch.object(
        mp_compute_module,
        "ProcessPoolExecutor",
        return_value=fake_executor
    ) as ppe:
        async def run():
            return await c.start(
                execute=jmespath_execute,
                storage_type=DictStorage,
                storage_kwargs={
                    "storage_dict": storage_dict
                },
                config={
                    "storage": {}
                }
            )

        result = asyncio.run(run())

    assert result['error'] is None
    assert c.locality == ModuleLocality.SYSTEM
    assert c.has_parallel_paging is False
    assert c._executor is fake_executor
    assert ppe.call_args.kwargs['max_workers'] == 3
    assert ppe.call_args.kwargs['initargs'][0] is InProcessCompute
    assert ppe.call_args.kwargs['initargs'][1] == {}


def test_mp_shutdown_shuts_down_executor():
    c = MPCompute(
        max_workers=None,
        worker_compute=InProcessCompute,
        worker_kwargs={}
    )
    fake_executor = MagicMock()
    c._executor = fake_executor
    result = asyncio.run(c.shutdown(config={}))
    assert result['error'] is None
    fake_executor.shutdown.assert_called_once_with(wait=True)
    assert c._executor is None


def test_mp_shutdown_with_no_executor():
    c = MPCompute(
        max_workers=None,
        worker_compute=InProcessCompute,
        worker_kwargs={}
    )
    result = asyncio.run(c.shutdown(config={}))
    assert result['error'] is None
    assert c._executor is None


def test_mp_construct():
    c = MPCompute(
        max_workers=None,
        worker_compute=InProcessCompute,
        worker_kwargs={}
    )
    result = asyncio.run(c.construct(config={}))
    assert result['error'] is None


def test_mp_destroy():
    c = MPCompute(
        max_workers=None,
        worker_compute=InProcessCompute,
        worker_kwargs={}
    )
    result = asyncio.run(c.destroy(config={}))
    assert result['error'] is None


def test_mp_validate_context_def(compute):
    result = _run_inline(
        lambda: compute.validate_context_def(
            context_def=CONTEXT_DEF,
            config={}
        )
    )
    assert result['error'] is None


def test_mp_validate_identity_def(compute):
    result = _run_inline(
        lambda: compute.validate_identity_def(
            identity_def=IDENTITY_DEF,
            config={}
        )
    )
    assert result['error'] is None


def test_mp_validate_resource_def(compute):
    result = _run_inline(
        lambda: compute.validate_resource_def(
            resource_def=RESOURCE_DEF,
            config={}
        )
    )
    assert result['error'] is None


def test_mp_validate_grant(compute):
    result = _run_inline(
        lambda: compute.validate_grant(grant=GRANT, config={})
    )
    assert result['error'] is None


def test_mp_validate_request(compute):
    result = _run_inline(
        lambda: compute.validate_request(
            request=REQUEST,
            config=VALIDATE_REQUEST_CONFIG
        )
    )
    assert result['error'] is None


def test_mp_validate_batch_request(compute):
    result = _run_inline(
        lambda: compute.validate_batch_request(
            batch_request=BATCH_REQUEST,
            config=VALIDATE_REQUEST_CONFIG
        )
    )
    assert result['error'] is None
    assert result['batch'] == [
        {
            "error": None
        }
    ]


def test_mp_audit(compute):
    result = _run_inline(
        lambda: compute.audit(
            request=REQUEST,
            page_ref=None,
            config={
                "validate_request": VALIDATE_REQUEST_CONFIG,
                "list_grants": LIST_GRANTS_CONFIG
            }
        )
    )
    assert result['error'] is None
    assert len(result['results']) == 1


def test_mp_authorize(compute):
    result = _run_inline(
        lambda: compute.authorize(
            request=REQUEST,
            config={
                "validate_request": VALIDATE_REQUEST_CONFIG,
                "list_grants": LIST_GRANTS_CONFIG,
                "parallel_paging": False,
                "list_grant_refs": {
                    "page_size": 10,
                    "use_cache": False
                }
            }
        )
    )
    assert result['error'] is None
    assert result['is_authorized'] is True


def test_mp_batch_audit(compute):
    result = _run_inline(
        lambda: compute.batch_audit(
            batch_request=BATCH_REQUEST,
            page_ref=None,
            config={
                "validate_batch_request": VALIDATE_REQUEST_CONFIG,
                "list_grants": LIST_GRANTS_CONFIG
            }
        )
    )
    assert result['error'] is None
    assert len(result['batch']) == 1


def test_mp_batch_authorize(compute):
    result = _run_inline(
        lambda: compute.batch_authorize(
            batch_request=BATCH_REQUEST,
            config={
                "validate_batch_request": VALIDATE_REQUEST_CONFIG,
                "list_grants": LIST_GRANTS_CONFIG,
                "parallel_paging": False,
                "list_grant_refs": {
                    "page_size": 10,
                    "use_cache": False
                }
            }
        )
    )
    assert result['error'] is None
    assert len(result['batch']) == 1
    assert result['batch'][0]['is_authorized'] is True


def test_executor_start_and_run(storage_dict):
    async def seed():
        storage = DictStorage(storage_dict=storage_dict)
        await storage.construct(config={})

    asyncio.run(seed())
    start_result = _executor_start(
        InProcessCompute,
        {},
        jmespath_execute,
        DictStorage,
        {
            "storage_dict": storage_dict
        },
        {
            "storage": {}
        }
    )
    assert start_result['error'] is None
    assert isinstance(
        mp_compute_module._authzee_compute,
        InProcessCompute
    )

    run_result = _executor_run(
        "validate_context_def",
        {
            "context_def": CONTEXT_DEF,
            "config": {}
        }
    )
    assert run_result['error'] is None

    asyncio.run(
        mp_compute_module._authzee_compute.shutdown(config={})
    )
