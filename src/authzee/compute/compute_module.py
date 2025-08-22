

from typing import Any, Callable, Dict, List, Type

from authzee import exceptions
from authzee.module_locality import ModuleLocality
from authzee.storage.storage_module import StorageModule


class ComputeModule:
    """Base class for Compute Modules.
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

        Set locality here as needed.  Can also start storage modules as needed.

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
        self.locality = ModuleLocality.PROCESS
        self.identity_defs = identity_defs
        self.resource_defs = resource_defs
        self.search = search
        self.storage_type = storage_type
        self.storage_kwargs = storage_kwargs


    async def shutdown(self) -> None:
        """Early clean up of compute backend resources.
        """
        pass 


    async def setup(self) -> None:
        """One time setup for compute backend resources.
        """
        pass

    
    async def teardown(self) -> None:
        """Teardown and delete the results of ``setup()`` .
        """
        pass


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
            Audit response for a page.

        Raises
        ------
        authzee.exceptions.ContextError
            Critical error when validating context.
        authzee.exceptions.JMESPathError
            Critical error when executing JMESPath query.
        authzee.exceptions.PageReferenceError
            Invalid page reference provided.
        authzee.exceptions.NotImplementedError
            This method is not implemented.
        """
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
    

  