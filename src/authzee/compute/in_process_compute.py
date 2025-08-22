__all__ = [
    "InProcessCompute"
]

from typing import Any, AsyncGenerator, Callable, Dict, List, Type

from authzee import core
from authzee.compute.compute_module import ComputeModule
from authzee.module_locality import ModuleLocality
from authzee.storage.storage_module import StorageModule


class InProcessCompute(ComputeModule):
    """Compute using the same process as the Authzee app.

    While parallel pagination compute is supported it offers no performance benefits since this is single threaded.
    It will just process all of the specified pages at once. 
    """
    

    async def start(
        self,
        identity_defs: List[Dict[str, Any]],
        resource_defs: List[Dict[str, Any]],
        search: Callable[[str, Any], Any], 
        storage_type: Type[StorageModule], 
        storage_kwargs: Dict[str, Any]
    ) -> None:
        """Create runtime resources for the compute module.

        Parameters
        ----------
        identity_defs : List[dict[str]]
            Identity definitions registered and validated with Authzee.
        resource_defs : List[dict[str]]
            Resource Definitions to registered and validated with Authzee.
        search : Callable[[str, Any], Any]
            JMESPath search function.
        storage_type : Type[StorageModule]
            Storage Module Type. 
        storage_kwargs : Dict[str, Any]
            Storage module KWArgs used to create instances.
        """
        await super().start(
            identity_defs=identity_defs,
            resource_defs=resource_defs,
            search=search,
            storage_type=storage_type,
            storage_kwargs=storage_kwargs

        )
        self.locality = ModuleLocality.PROCESS
        self._storage = storage_type(**storage_kwargs)
        await self._storage.start(identity_defs=identity_defs, resource_defs=resource_defs)


    async def audit_page(
        self, 
        request: dict, 
        page_ref: str | None, 
        grants_page_size: int, 
        parallel_paging: bool, 
        refs_page_size: int 
    ) -> dict:
        """Process a page of grants that are applicable to an authorization request.

        Parameters
        ----------
        request : dict
            Authzee request data.
        page_ref : str | None
            Page reference of the page to retrieve. None to get the first page.
        grants_page_size : int
            Number of grants per page to process. Not exact.
        parallel_paging : bool
            Enable parallel pagination. May return many more results at once.
        refs_page_size : int
            Number of page reference to process. Not exact.

        Returns
        -------
        dict
            Audit response.

        Raises
        ------
        authzee.exceptions.PageReferenceError
            Invalid page reference provided.
        """
        page = await self._get_page_or_pages(
            effect=None,
            action=request['action'],
            page_ref=page_ref,
            grants_page_size=grants_page_size,
            parallel_paging=parallel_paging,
            refs_page_size=refs_page_size
        )
        resp = core.audit(
            request=request,
            grants=page['grants'],
            search=self.search
        )
        resp['next_page_ref'] = page['next_page_ref']
        
        return resp
        

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
        """
        async for grants in self._page_gen(
            effect="deny",
            action=request['action'],
            grants_page_size=grants_page_size,
            parallel_paging=parallel_paging,
            refs_page_size=refs_page_size
        ):
            resp = core.authorize(
                request=request,
                grants=grants,
                search=self.search
            )
            if resp['completed'] is False or resp['grant'] is not None:
                return resp
        
        async for grants in self._page_gen(
            effect="allow",
            action=request['action'],
            grants_page_size=grants_page_size,
            parallel_paging=parallel_paging,
            refs_page_size=refs_page_size
        ):
            resp = core.authorize(
                request=request,
                grants=grants,
                search=self.search
            )
            if resp['completed'] is False and resp['grant'] is not None:
                return resp
        
        return resp
    

    async def _get_page_or_pages(
        self, 
        effect: str | None,
        action: str | None,
        page_ref: str | None, 
        grants_page_size: int, 
        parallel_paging: bool, 
        refs_page_size: int
    ) -> dict:
        if parallel_paging is True:
            refs = await self._storage.get_grant_page_refs_page(
                effect=effect,
                action=action,
                page_ref=page_ref,
                grants_page_size=grants_page_size,
                refs_page_size=refs_page_size
            )
            page = {
                "grants": [],
                "next_page_ref": refs['next_page_ref']
            }
            for ref in refs['page_refs']:
                page['grants'] += (await self._storage.get_grants_page(
                    effect=effect,
                    action=action,
                    page_ref=ref,
                    grants_page_size=grants_page_size
                ))['grants']
        else:
            page = await self._storage.get_grants_page(
                effect=effect,
                action=action,
                page_ref=page_ref,
                grants_page_size=grants_page_size
            )

        return page

    
    async def _page_gen(
        self, 
        effect: str | None,
        action: str | None,
        grants_page_size: int, 
        parallel_paging: bool, 
        refs_page_size: int
    ) -> AsyncGenerator[List[dict], None]:
        page_ref = None
        while True:
            page = await self._get_page_or_pages(
                effect=effect,
                action=action,
                page_ref=page_ref,
                grants_page_size=grants_page_size,
                parallel_paging=parallel_paging,
                refs_page_size=refs_page_size
            )

            yield page['grants']

            page_ref = page['next_page_ref']
            if page_ref is None:
                break
