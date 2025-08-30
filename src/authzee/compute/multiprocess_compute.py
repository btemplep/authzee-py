__all__ = [
    "MultiprocessCompute"
]

import asyncio
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from typing import Any, Callable, Dict, List, Type

from authzee.compute.compute_module import ComputeModule
from authzee.compute.in_process_compute import InProcessCompute
from authzee.module_locality import ModuleLocality
from authzee.storage.storage_module import StorageModule


class MultiprocessCompute(ComputeModule):
    """Compute using multiple processes with system locality.
    
    Each request for audit page or authorize are simple sent to a single process from a pool. 

    Parameters
    ----------
    max_workers : int, optional
        Maximum number of worker processes. If None, uses default from ProcessPoolExecutor.
    """

    def __init__(self, max_workers: int | None = None):
        self.max_workers = max_workers
        self._executor = None


    async def start(
        self,
        identity_defs: List[Dict[str, Any]],
        resource_defs: List[Dict[str, Any]],
        search: Callable[[str, Any], Any], 
        storage_type: Type[StorageModule], 
        storage_kwargs: Dict[str, Any]
    ) -> None:
        """Create runtime resources for the compute module."""
        await super().start(
            identity_defs=identity_defs,
            resource_defs=resource_defs,
            search=search,
            storage_type=storage_type,
            storage_kwargs=storage_kwargs
        )
        self.locality = ModuleLocality.SYSTEM
        self._executor = ProcessPoolExecutor(
            max_workers=self.max_workers,
            mp_context=multiprocessing.get_context("spawn"),
            initializer=_executor_start,
            initargs=(
                {
                    "identity_defs": identity_defs,
                    "resource_defs": resource_defs,
                    "search": search,
                    "storage_type": storage_type,
                    "storage_kwargs": storage_kwargs
                },
            )
        )


    async def shutdown(self) -> None:
        if self._executor:
            self._executor.shutdown(wait=True)
            self._executor = None


    async def audit_page(
        self, 
        request: dict, 
        page_ref: str | None, 
        grants_page_size: int, 
        parallel_paging: bool, 
        refs_page_size: int 
    ) -> dict:
        return await asyncio.get_running_loop().run_in_executor(
            self._executor,
            _executor_audit_page,
            (
                {
                    "request": request,
                    "page_ref": page_ref,
                    "grants_page_size": grants_page_size,
                    "parallel_paging": parallel_paging,
                    "refs_page_size": refs_page_size
                },
            )
        )


    async def authorize(
        self, 
        request: dict, 
        grants_page_size: int, 
        parallel_paging: bool, 
        refs_page_size: int 
    ) -> dict:
        return await asyncio.get_running_loop().run_in_executor(
            self._executor,
            _executor_authorize,
            (
                {
                    "request": request,
                    "grants_page_size": grants_page_size,
                    "parallel_paging": parallel_paging,
                    "refs_page_size": refs_page_size
                },
            )
        )


def _executor_start(
    start_kwargs: Dict[str, Any]
) -> None:
    global authzee_compute
    authzee_compute = InProcessCompute()
    return asyncio.run(authzee_compute.start(**start_kwargs))


def _executor_audit_page(
    audit_page_kwargs: Dict[str, Any]
) -> dict:
    global authzee_compute
    return asyncio.run(
        authzee_compute.audit_page(
            **audit_page_kwargs
        )
    )


def _executor_authorize(
    authorize_kwargs: Dict[str, Any]
) -> dict:
    global authzee_compute
    return asyncio.run(
        authzee_compute.authorize(
            **authorize_kwargs
        )
    )
