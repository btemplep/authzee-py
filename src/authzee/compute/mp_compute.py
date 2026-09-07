"""Multiprocess compute module for Authzee.

All requests are offloaded to a worker process pool.
"""

__all__ = [
    "MPCompute"
]

import asyncio
from concurrent.futures import ProcessPoolExecutor
import multiprocessing
from typing import Any, Callable, Type

from authzee.compute.compute_module import ComputeModule
from authzee.compute.in_process_compute import InProcessCompute
from authzee.module_locality import ModuleLocality
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


class MPCompute(ComputeModule):
    """Multiprocess Compute Module.

    Parameters
    ----------
    max_workers : int | None
        Maximum number of worker processes. If None, defaults to number of machine processors.
    worker_compute : Type[ComputeModule]
        The type of the compute module for each worker process to use.
    worker_kwargs : dict[str, Any]
        KWArgs to pass when creating compute modules for the worker processes.

    Examples
    --------
    ```python
    from authzee import (
        Authzee,
        DictStorage,
        InProcessCompute,
        jmespath_execute,
        MPCompute
    )


    storage_dict = {}
    authz = Authzee(
        execute=jmespath_execute,
        compute_type=MPCompute,
        compute_kwargs={
            "max_workers": None,
            "worker_compute": InProcessCompute,
            "worker_kwargs": {}
        },
        storage_type=DictStorage,
        storage_kwargs={
            "storage_dict": storage_dict
        },
        config={ # optional - AuthzeeConfigOverride | None - All keys are optional
            "authzee": {
                "raise_errors": True
            }
            # "method_name": {<method config>}
        }
    )
    """


    def __init__(
        self,
        max_workers: int | None,
        worker_compute: Type[ComputeModule],
        worker_kwargs: dict[str, Any]
    ):
        self._max_workers = max_workers
        self._worker_compute = worker_compute
        self._worker_kwargs = worker_kwargs
        self._executor = None


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
        self.locality = ModuleLocality.SYSTEM
        self.has_parallel_paging = False
        self._executor = ProcessPoolExecutor(
            max_workers=self._max_workers,
            mp_context=multiprocessing.get_context("spawn"),
            initializer=_executor_start,
            initargs=(
                self._worker_compute,
                self._worker_kwargs,
                execute,
                storage_type,
                storage_kwargs,
                config
            )
        )

        return {
            "error": None
        }


    async def shutdown(self, config: ComputeShutdownConfig) -> GenericResult:
        if self._executor is not None:
            self._executor.shutdown(wait=True)
            self._executor = None

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
        return await asyncio.get_running_loop().run_in_executor(
            self._executor,
            _executor_run,
            "validate_context_def",
            {
                "context_def": context_def,
                "config": config
            }
        )


    async def validate_identity_def(
        self,
        identity_def: IdentityDef,
        config: ValidateIdentityDefConfig
    ) -> GenericResult:
        return await asyncio.get_running_loop().run_in_executor(
            self._executor,
            _executor_run,
            "validate_identity_def",
            {
                "identity_def": identity_def,
                "config": config
            }
        )


    async def validate_resource_def(
        self,
        resource_def: ResourceDef,
        config: ValidateResourceDefConfig
    ) -> GenericResult:
        return await asyncio.get_running_loop().run_in_executor(
            self._executor,
            _executor_run,
            "validate_resource_def",
            {
                "resource_def": resource_def,
                "config": config
            }
        )


    async def validate_grant(
        self,
        grant: Grant,
        config: ValidateGrantConfig
    ) -> GenericResult:
        return await asyncio.get_running_loop().run_in_executor(
            self._executor,
            _executor_run,
            "validate_grant",
            {
                "grant": grant,
                "config": config
            }
        )


    async def validate_request(
        self,
        request: AuthzeeRequest,
        config: ValidateRequestConfig
    ) -> GenericResult:
        return await asyncio.get_running_loop().run_in_executor(
            self._executor,
            _executor_run,
            "validate_request",
            {
                "request": request,
                "config": config
            }
        )


    async def validate_batch_request(
        self,
        batch_request: AuthzeeBatchRequest,
        config: ValidateBatchRequestConfig
    ) -> ValidateBatchRequestResult:
        return await asyncio.get_running_loop().run_in_executor(
            self._executor,
            _executor_run,
            "validate_batch_request",
            {
                "batch_request": batch_request,
                "config": config
            }
        )


    async def audit(
        self,
        request: AuthzeeRequest,
        page_ref: str | None,
        config: AuditConfig
    ) -> AuditResultPage:
        return await asyncio.get_running_loop().run_in_executor(
            self._executor,
            _executor_run,
            "audit",
            {
                "request": request,
                "page_ref": page_ref,
                "config": config
            }
        )


    async def authorize(
        self,
        request: AuthzeeRequest,
        config: AuthorizeConfig
    ) -> AuthorizeResult:
        return await asyncio.get_running_loop().run_in_executor(
            self._executor,
            _executor_run,
            "authorize",
            {
                "request": request,
                "config": config
            }
        )


    async def batch_audit(
        self,
        batch_request: AuthzeeBatchRequest,
        page_ref: str | None,
        config: BatchAuditConfig
    ) -> BatchAuditResultPage:
        return await asyncio.get_running_loop().run_in_executor(
            self._executor,
            _executor_run,
            "batch_audit",
            {
                "batch_request": batch_request,
                "page_ref": page_ref,
                "config": config
            }
        )


    async def batch_authorize(
        self,
        batch_request: AuthzeeBatchRequest,
        config: BatchAuthorizeConfig
    ) -> BatchAuthorizeResult:
        return await asyncio.get_running_loop().run_in_executor(
            self._executor,
            _executor_run,
            "batch_authorize",
            {
                "batch_request": batch_request,
                "config": config
            }
        )


def _executor_start(
    worker_compute: Type[ComputeModule],
    worker_kwargs: dict[str, Any],
    execute: Callable[[str, Any], Any],
    storage_type: Type[StorageModule],
    storage_kwargs: dict[str, Any],
    config: ComputeStartConfig
) -> None:
    global _authzee_compute
    _authzee_compute = worker_compute(**worker_kwargs)

    return asyncio.run(
        _authzee_compute.start(
            execute=execute,
            storage_type=storage_type,
            storage_kwargs=storage_kwargs,
            config=config
        )
    )


def _executor_run(method: str, method_kwargs: dict[str, Any]) -> Any:
    global _authzee_compute

    return asyncio.run(
        getattr(_authzee_compute, method)(**method_kwargs)
    )
