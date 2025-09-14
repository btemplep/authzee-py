

import asyncio
from concurrent.futures import ProcessPoolExecutor
from functools import partial
import multiprocessing as mp
from multiprocessing.connection import Connection
from multiprocessing.managers import SharedMemoryManager
from typing import Any, Callable, Dict, List, Optional, Type, Union

from authzee.compute.compute_module import ComputeModule
from authzee.compute.shared_mem_latch import SharedMemLatch
from authzee.module_locality import ModuleLocality
from authzee.storage.storage_module import StorageModule


class FanOutMPCompute(ComputeModule):
    """Pool of processes is used to spread work among as many processes as feasible.

    Generally aims to serve one request as fast as possible.

    Parameters
    ----------
    max_workers : int | None
        Maximum number of worker processes. If None, defaults to number of machine processors.

    Examples
    --------
    .. code-block:: python

        from authzee import Authzee

    """


    def __init__(
        self,
        max_workers: int | None
    ):
        self.max_workers = max_workers
        if self.max_workers is None:
            self.max_workers = mp.cpu_count()
            
        self._process_pool = None
        self._shared_mem_manager = None


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
        self._shared_mem_manager = SharedMemoryManager()
        self._shared_mem_manager.start()
        self._process_pool = ProcessPoolExecutor(
            max_workers=self.max_workers,
            mp_context=mp.get_context("spawn"),
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
        """Early clean up of compute backend resources.

        Will shutdown the process pool without waiting for current tasks to finish and free shared memory.
        """
        self._process_pool.shutdown(wait=False)
        self._process_pool = None
        self._shared_mem_manager.shutdown()
        self._shared_mem_manager = None


    async def audit_page(
        self, 
        request: dict, 
        page_ref: str | None, 
        grants_page_size: int, 
        parallel_paging: bool, 
        refs_page_size: int 
    ) -> dict:
        raise exceptions.NotImplementedError()


    async def authorize(
        self, 
        request: dict, 
        grants_page_size: int, 
        parallel_paging: bool, 
        refs_page_size: int 
    ) -> dict:
        """Authorize a request.

        Parameters
        ----------
        request : dict
            Authzee request data.
        grants_page_size : int
            Number of grants per page to process. Not exact.
        parallel_paging : bool
            Enable parallel pagination. Used to control how compute and storage process pages.
        refs_page_size : int
            Number of page reference to process. Not exact.

        Returns
        -------
        dict
            Authorization decision with effect and supporting information.

        Raises
        ------
        authzee.exceptions.ContextError
            Critical error when validating context.
        authzee.exceptions.JMESPathError
            Critical error when executing JMESPath query.
        authzee.exceptions.NotImplementedError
            This method is not implemented.
        """
        raise exceptions.NotImplementedError()


    async def authorize(
        self, 
        resource_type: Type[BaseModel],
        action: ResourceAction,
        jmespath_data: Dict[str, Any],
        page_size: Optional[int] = None
    ) -> bool:
        """Authorize a given resource and action, with the JMESPath data against stored grants.

        First ``GrantEffect.DENY`` grants should be checked.
        If any match, then it is denied.

        Then ``GrantEffect.ALLOW`` grants are checked.
        If any match, it is allowed. If there are no matches, it is denied.

        Parameters
        ----------
        resource_type : BaseModel
            The resource type to compare grants to.
        action : ResourceAction
            The resource action to compare grants to.
        jmespath_data : Dict[str, Any]
            JMESPath data that the grants will be computed with.
        page_size : Optional[int], optional
            The page size to use for the storage backend.
            The default is set on the storage backend.

        Returns
        -------
        bool
            ``True`` if allowed, ``False`` if denied.
        """ 
        loop = asyncio.get_running_loop()
        deny_futures: List[asyncio.Future] = []
        next_page_ref = None
        did_once = False
        cancel_latch = SharedMemLatch(smm=self._shared_mem_manager)
        while (
            (
                did_once is not True
                or next_page_ref is not None
            )
            and cancel_latch.is_set() is False
        ):
            did_once = True
            recv_conn, send_conn = mp.Pipe(duplex=False)
            deny_futures.append(
                loop.run_in_executor(
                    self._process_pool,
                    partial(
                        _executor_grant_page_matches_deny,
                        effect=GrantEffect.DENY,
                        resource_type=resource_type,
                        action=action,
                        page_size=page_size,
                        page_ref=next_page_ref,
                        jmespath_data=jmespath_data,
                        pipe_conn=send_conn,
                        cancel_latch=cancel_latch
                    )
                )
            )
            # wait for next page ref from child
            next_page_ref = await loop.run_in_executor(
                self._thread_pool,
                recv_conn.recv
            )

        allow_futures: List[asyncio.Future] = []
        next_page_ref = None
        did_once = False
        allow_match_event = SharedMemLatch(smm=self._shared_mem_manager)
        while (
            (
                did_once is not True
                or next_page_ref is not None
            )
            and cancel_latch.is_set() is False
            and allow_match_event.is_set() is False
        ):
            did_once = True
            recv_conn, send_conn = mp.Pipe(duplex=False)
            allow_futures.append(
                loop.run_in_executor(
                    self._process_pool,
                    partial(
                        _executor_grant_page_matches_allow,
                        effect=GrantEffect.ALLOW,
                        resource_type=resource_type,
                        action=action,
                        page_size=page_size,
                        page_ref=next_page_ref,
                        jmespath_data=jmespath_data,
                        pipe_conn=send_conn,
                        cancel_latch=cancel_latch,
                        allow_match_event=allow_match_event
                    )
                )
            )
            # wait for next page ref from child
            next_page_ref = await loop.run_in_executor(
                self._thread_pool,
                recv_conn.recv
            )
        
        # If we found a deny then cleanup tasks and return False
        if cancel_latch.is_set() is True:
            await self._cleanup_futures(futures=deny_futures + allow_futures)
            cancel_latch.unlink()
            allow_match_event.unlink()

            return False
        # Then check if we ran any deny tasks and recheck cancel status
        elif len(deny_futures) > 0:
            await asyncio.gather(*deny_futures)
            if cancel_latch.is_set() is True:
                await self._cleanup_futures(futures=allow_futures)
                cancel_latch.unlink()
                allow_match_event.unlink()

                return False
        
        # Check for allow match
        if allow_match_event.is_set() is True:
            await self._cleanup_futures(allow_futures)
            cancel_latch.unlink()
            allow_match_event.unlink()

            return True
        # Then check if we ran any allow tasks and recheck allow match status
        elif len(allow_futures) > 0:
            await asyncio.gather(*allow_futures)
            if allow_match_event.is_set() is True:
                cancel_latch.unlink()
                allow_match_event.unlink()
                
                return True
        
        cancel_latch.unlink()
        allow_match_event.unlink()

        return False


    async def authorize_many(
        self, 
        resource_type: Type[BaseModel],
        action: ResourceAction,
        jmespath_data_entries: List[Dict[str, Any]],
        page_size: Optional[int] = None
    ) -> List[bool]:
        """Authorize a given resource and action, with the JMESPath data against stored grants.

        First ``GrantEffect.DENY`` grants should be checked.
        If any match, then it is denied.

        Then ``GrantEffect.ALLOW`` grants are checked.
        If any match, it is allowed. If there are no matches, it is denied.

        Parameters
        ----------
        resource_type : BaseModel
            The resource type to compare grants to.
        action : ResourceAction
            The resource action to compare grants to.
        jmespath_data_entries : List[Dict[str, Any]]
            List of JMESPath data that the grants will be computed with.
        page_size : Optional[int], optional
            The page size to use for the storage backend.
            The default is set on the storage backend.

        Returns
        -------
        List[bool]
            List of bools directory corresponding to ``jmespath_data_entries``.  
            ``True`` if authorized, ``False`` if denied.
        """
        results = {i: None for i in range(len(jmespath_data_entries))}
        loop = get_running_loop()
        deny_futures: List[asyncio.Future] = []
        next_page_ref = None
        did_once = False
        while (
            did_once is not True
            or next_page_ref is not None
        ):
            did_once = True
            recv_conn, send_conn = mp.Pipe(duplex=False)
            deny_futures.append(
                loop.run_in_executor(
                    self._process_pool,
                    partial(
                        _executor_authorize_many,
                        effect=GrantEffect.DENY,
                        resource_type=resource_type,
                        action=action,
                        page_size=page_size,
                        page_ref=next_page_ref,
                        jmespath_data_entries=jmespath_data_entries,
                        pipe_conn=send_conn
                    )
                )
            )
            # wait for next page ref from child
            next_page_ref = await loop.run_in_executor(
                self._thread_pool,
                recv_conn.recv
            )

        allow_futures: List[asyncio.Future] = []
        next_page_ref = None
        did_once = False
        while (
            did_once is not True
            or next_page_ref is not None
        ):
            did_once = True
            recv_conn, send_conn = mp.Pipe(duplex=False)
            allow_futures.append(
                loop.run_in_executor(
                    self._process_pool,
                    partial(
                        _executor_authorize_many,
                        effect=GrantEffect.ALLOW,
                        resource_type=resource_type,
                        action=action,
                        page_size=page_size,
                        page_ref=next_page_ref,
                        jmespath_data_entries=jmespath_data_entries,
                        pipe_conn=send_conn
                    )
                )
            )
            # wait for next page ref from child
            next_page_ref = await loop.run_in_executor(
                self._thread_pool,
                recv_conn.recv
            )

        if len(deny_futures) > 0:
            deny_results: List[List[bool]] = await asyncio.gather(*deny_futures)
            for result_set in deny_results:
                for i, result in zip(results, result_set):
                    if result is True:
                        results[i] = False

        if len(allow_futures) > 0:
            allow_results: List[List[bool]] = await asyncio.gather(*allow_futures)
            for result_set in allow_results:
                for i, result in zip(results, result_set):
                    if result is True:
                        results[i] = True
        
        return [val is True for val in list(results.values())]


    async def get_matching_grants_page(
        self, 
        effect: GrantEffect,
        resource_type: Type[BaseModel],
        action: ResourceAction,
        jmespath_data: Dict[str, Any],
        page_size: Optional[int] = None,
        page_ref: Optional[str] = None
    ) -> GrantsPage:
        """Retrieve a page of matching grants. 

        If ``GrantsPage.next_page_ref`` is not ``None`` , there are more grants to retrieve.
        To get the next page, pass ``page_ref=GrantsPage.next_page_ref`` .

        **NOTE** - There is no guarantee of how many grants will be returned if any.

        ``max_worker`` pages of grants (using ``page_size`` ) will be pulled and checked for matches. 

        Parameters
        ----------
        effect : GrantEffect
            The effect of the grant.
        resource_type : BaseModel
            The resource type to compare grants to.
        action : ResourceAction
            The resource action to compare grants to.
        jmespath_data : Dict[str, Any]
            JMESPath data that the grants will be computed with.
        page_size : Optional[int], optional
            The page size to use for the storage backend.
            This is not directly related to the returned number of grants, and can vary by compute backend.
            The default is set on the storage backend.
        page_ref : Optional[str], optional
            The reference to the next page that is returned in ``GrantsPage``.
            By default this will return the first page.

        Returns
        -------
        GrantsPage
            The page of matching grants.
        """
        loop = get_running_loop()
        futures: List[asyncio.Future] = []
        next_page_ref = None
        did_once = False
        worker_num = 0
        while (
            worker_num < self._max_workers
            and did_once is not True
            or next_page_ref is not None
        ):
            worker_num += 1
            did_once = True
            recv_conn, send_conn = mp.Pipe(duplex=False)
            futures.append(
                loop.run_in_executor(
                    self._process_pool,
                    partial(
                        _executor_matching_grants,
                        effect=effect,
                        resource_type=resource_type,
                        action=action,
                        page_size=page_size,
                        page_ref=next_page_ref,
                        jmespath_data=jmespath_data,
                        pipe_conn=send_conn
                    )
                )
            )
            # wait for next page ref from child
            next_page_ref = await loop.run_in_executor(
                self._thread_pool,
                recv_conn.recv
            )
        
        results = await asyncio.gather(*futures)
        
        return GrantsPage(
            grants=[grant for grants_list in results for grant in grants_list],
            next_page_ref=page_ref
        )
        

    async def _cleanup_futures(self, futures: List[asyncio.Future]) -> None:
        gather_futures: List[asyncio.Future] = []
        for future in futures:
            if future.cancel() is False:
                gather_futures.append(future)
        
        await asyncio.gather(*gather_futures)

 
def _executor_start(
    start_kwargs: Dict[str, Any]
) -> None:
    global authzee_search
    authzee_search: Callable[[str, Any], Any] = start_kwargs['search']
    global authzee_storage
    authzee_storage: StorageModule = start_kwargs['storage_type'](**start_kwargs['storage_kwargs'])
    asyncio.run(
        authzee_storage.start(
            identity_defs=start_kwargs['identity_defs'],
            resource_defs=start_kwargs['resource_defs']
        )
    )


def _executor_grant_page_matches_deny(
    effect: GrantEffect,
    resource_type: Type[BaseModel],
    action: ResourceAction,
    page_size: int,
    page_ref: Union[str, None],
    jmespath_data: Dict[str, Any],
    pipe_conn: Connection,
    cancel_latch: SharedMemEvent
) -> bool:
    global authzee_jmespath_options
    global authzee_storage
    loop = get_event_loop()
    raw_grants = loop.run_until_complete(
        authzee_storage.get_raw_grants_page(
            effect=effect,
            resource_type=resource_type,
            action=action,
            page_size=page_size,
            page_ref=page_ref
        )
    )
    
    # Send back next page ref to parent
    pipe_conn.send(raw_grants.next_page_ref)
    if cancel_latch.is_set() is True:
        return False

    grants_page = loop.run_until_complete(
        authzee_storage.normalize_raw_grants_page(
            raw_grants_page=raw_grants
        )
    )
    if cancel_latch.is_set() is True:
        return False
    
    for grant in grants_page.grants:
        if gc.grant_matches(
            grant=grant,
            jmespath_data=jmespath_data,
            jmespath_options=authzee_jmespath_options
        ) is True:
            cancel_latch.set()
            return True

        if cancel_latch.is_set() is True:
            return False

    return False
 

def _executor_grant_page_matches_allow(
    effect: GrantEffect,
    resource_type: Type[BaseModel],
    action: ResourceAction,
    page_size: int,
    page_ref: Union[str, None],
    jmespath_data: Dict[str, Any],
    pipe_conn: Connection,
    cancel_latch: SharedMemEvent,
    allow_match_event: SharedMemEvent
) -> bool:
    global authzee_jmespath_options
    global authzee_storage
    loop = get_event_loop()
    raw_grants = loop.run_until_complete(
        authzee_storage.get_raw_grants_page(
            effect=effect,
            resource_type=resource_type,
            action=action,
            page_size=page_size,
            page_ref=page_ref
        )
    )
    pipe_conn.send(raw_grants.next_page_ref)
    if (
        cancel_latch.is_set() is True
        or allow_match_event.is_set() is True
    ):
        return False

    grants_page = loop.run_until_complete(
        authzee_storage.normalize_raw_grants_page(
            raw_grants_page=raw_grants
        )
    )
    for grant in grants_page.grants:
        if gc.grant_matches(
            grant=grant,
            jmespath_data=jmespath_data,
            jmespath_options=authzee_jmespath_options
        ) is True:
            allow_match_event.set()
            return True

        if (
            cancel_latch.is_set() is True
            or allow_match_event.is_set() is True
        ):
            return False

    return False


def _executor_authorize_many(
    effect: GrantEffect,
    resource_type: Type[BaseModel],
    action: ResourceAction,
    page_size: int,
    page_ref: Union[str, None],
    jmespath_data_entries: List[Dict[str, Any]],
    pipe_conn: Connection
) -> List[bool]:
    global authzee_storage
    global authzee_jmespath_options
    loop = get_event_loop()
    raw_page = loop.run_until_complete(
        authzee_storage.get_raw_grants_page(
            effect=effect,
            resource_type=resource_type,
            action=action,
            page_size=page_size,
            page_ref=page_ref
        )
    )
    pipe_conn.send(raw_page.next_page_ref)
    grants_page = loop.run_until_complete(
        authzee_storage.normalize_raw_grants_page(raw_grants_page=raw_page)
    )

    return gc.authorize_many_grants(
        grants_page=grants_page,
        jmespath_data_entries=jmespath_data_entries,
        jmespath_options=authzee_jmespath_options
    )


def _executor_matching_grants(
    effect: GrantEffect,
    resource_type: Type[BaseModel],
    action: ResourceAction,
    page_size: int,
    page_ref: Union[str, None],
    jmespath_data: Dict[str, Any],
    pipe_conn: Connection
) -> List[Grant]:
    global authzee_storage
    global authzee_jmespath_options
    loop = get_event_loop()
    raw_page = loop.run_until_complete(
        authzee_storage.get_raw_grants_page(
            effect=effect,
            resource_type=resource_type,
            action=action,
            page_size=page_size,
            page_ref=page_ref
        )
    )
    pipe_conn.send(raw_page.next_page_ref)
    grants_page = loop.run_until_complete(
        authzee_storage.normalize_raw_grants_page(raw_grants_page=raw_page)
    )

    return gc.compute_matching_grants(
        grants_page=grants_page,
        jmespath_data=jmespath_data,
        jmespath_options=authzee_jmespath_options
    )
