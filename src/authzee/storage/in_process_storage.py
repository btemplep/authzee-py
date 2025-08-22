__all__ = [
    "InProcessStorage"
]

import copy
import datetime
from typing import Any, Dict, List
from uuid import UUID, uuid4

from authzee import exceptions
from authzee.module_locality import ModuleLocality
from authzee.storage.storage_module import StorageModule


class InProcessStorage(StorageModule):
    """Storage module that uses the Authzee app process's memory as the storage medium.

    Upon ``shutdown()`` or exit all storage is lost.

    Parameters
    ----------
    storage_ptr : dict
        A dictionary that will be used as the storage medium for all instances of this storage module.
    """

    def __init__(self, storage_ptr: dict):
        self._storage_ptr = storage_ptr
        if "created" not in self._storage_ptr:
            self._storage_ptr['grant_lut'] = {}
            self._storage_ptr['grant_effect_filter'] = {}
            self._storage_ptr['grant_action_filter'] = {}
            self._storage_ptr['grant_both_filter'] = {}
            self._storage_ptr['latch_lut'] = {}
            self._grant_lut: Dict[UUID, dict] = self._storage_ptr['grant_lut']
            self._grant_effect_filter: Dict[str, List[dict]] = self._storage_ptr['grant_effect_filter']
            self._grant_effect_filter.update(
                {
                    "allow": [],
                    "deny": []
                }
            )
            self._grant_action_filter: Dict[str, List[dict]] = self._storage_ptr['grant_action_filter']
            self._grant_both_filter: Dict[str, Dict[str, List[dict]]] = self._storage_ptr['grant_both_filter']
            self._grant_both_filter.update(
                {
                    "allow": {},
                    "deny": {}
                }
            )
            self._latch_lut: Dict[UUID, dict] = self._storage_ptr['latch_lut']
            self._storage_ptr['created'] = True
        else:
            self._grant_lut: Dict[UUID, dict] = self._storage_ptr['grant_lut']
            self._grant_effect_filter: Dict[str, List[dict]] = self._storage_ptr['grant_effect_filter']
            self._grant_action_filter: Dict[str, List[dict]] = self._storage_ptr['grant_action_filter']
            self._grant_both_filter: Dict[str, Dict[str, List[dict]]] = self._storage_ptr['grant_both_filter']
            self._latch_lut: Dict[UUID, dict] = self._storage_ptr['latch_lut']


    async def start(
        self, 
        identity_defs: List[Dict[str, Any]],
        resource_defs: List[Dict[str, Any]]
    ) -> None:
        """Initialize the storage module. 

        Parameters
        ----------
        identity_defs : List[dict[str]]
            Identity definitions registered and validated with Authzee.
        resource_defs : List[dict[str]]
            ``ResourceAuthz`` instances that have been registered with Authzee.
        """
        await super().start(
            identity_defs=identity_defs,
            resource_defs=resource_defs
        )
        self.locality = ModuleLocality.PROCESS
        self.parallel_paging_supported = True
        if "started" not in self._storage_ptr:
            for rd in resource_defs:
                for action in rd['actions']:
                    self._grant_action_filter[action] = []
                    self._grant_both_filter['allow'][action] = []
                    self._grant_both_filter['deny'][action] = []
            
            # For grants that match all actions, ie empty actions list
            self._grant_action_filter[None] = []
            self._grant_both_filter['allow'][None] = []
            self._grant_both_filter['deny'][None] = []
            self._storage_ptr['started'] = True
        

    async def shutdown(self):
        self._grant_lut = {}
        self._grant_action_filter = {}
        self._grant_effect_filter = {}
        self._grant_both_filter = {}
        self._latch_lut = {}
        

    async def enact(self, new_grant: dict) -> dict:
        """Add a grant. 

        Parameters
        ----------
        new_grant : dict
            The new grant data.

        Returns
        -------
        dict
            The grant that has been added.
        """
        grant = copy.deepcopy(new_grant)
        grant_uuid = uuid4()
        grant['grant_uuid'] = str(grant_uuid)
        self._grant_lut[grant_uuid] = grant
        self._grant_effect_filter[grant['effect']].append(grant)
        actions = grant['actions']
        if len(grant['actions']) == 0:
            actions = [None]

        for action in actions:
            self._grant_action_filter[action].append(grant)
            self._grant_both_filter[grant['effect']][action].append(grant)

        return copy.deepcopy(grant)


    async def repeal(self, grant_uuid: UUID) -> None:
        """Delete a grant.

        Parameters
        ----------
        grant_uuid : UUID
            The UUID of the grant to delete.

        Raises
        ------
        authzee.exceptions.GrantNotFoundError
            The grant with the given UUID could not be found.
        """
        if grant_uuid not in self._grant_lut:
            raise exceptions.GrantNotFoundError(grant_uuid=grant_uuid)
    
        grant = self._grant_lut.pop(grant_uuid)
        uuid_str = grant['grant_uuid']
        self._grant_effect_filter[grant['effect']] = [g for g in self._grant_effect_filter[grant['effect']] if g['grant_uuid'] != uuid_str]
        actions = grant['actions']
        if len(grant['actions']) == 0:
            actions = [None]

        for action in actions:
            self._grant_action_filter[action] = [g for g in self._grant_action_filter[action] if g['grant_uuid'] != uuid_str]
            self._grant_both_filter[grant['effect']][action] = [g for g in self._grant_both_filter[grant['effect']][action] if g['grant_uuid'] != uuid_str]
    

    async def get_grant(self, grant_uuid: UUID) -> dict:
        """Retrieve a grant by UUID.

        Parameters
        ----------
        grant_uuid : UUID
            Grant UUID.

        Returns
        -------
        dict
            The grant with the matching UUID.

        Raises
        ------
        authzee.exceptions.NotImplementedError
            ``StorageModule`` sub-classes must implement this method.
        authzee.exceptions.GrantNotFoundError
            The grant with the given UUID could not be found.
        """
        if grant_uuid not in self._grant_lut:
            raise exceptions.GrantNotFoundError(grant_uuid=grant_uuid)

        return copy.deepcopy(self._grant_lut[grant_uuid])
        

    async def get_grants_page(
        self,
        effect: str | None,
        action: str | None, 
        page_ref: str | None, 
        grants_page_size: int
    ) -> dict:
        """Get a page of grants.

        Parameters
        ----------
        effect : str | None
            Filter by grant effect. None for no filter.
        action : str | None
            Filter by grant action. None for no filter.
        page_ref : str | None
            Page reference of the page to retrieve. None to get the first page.
        grants_page_size : int
            Number of grants to return. Not exact.

        Returns
        -------
        dict
            Page of grants with the next page reference.
        """
        if effect is not None and action is not None:
            grants = self._grant_both_filter[effect][action] + self._grant_both_filter[effect][None]
        elif effect is not None:
            grants = self._grant_effect_filter[effect]
        elif action is not None:
            grants = self._grant_action_filter[action] + self._grant_action_filter[None]
        else:
            grants = list(self._grant_lut.values())

        # Handle pagination
        start_index = 0
        if page_ref is not None:
            try:
                start_index = int(page_ref)
            except ValueError:
                start_index = 0

        end_index = start_index + grants_page_size
        page_grants = grants[start_index:end_index]
        
        # Determine next page reference
        next_page_ref = None
        if end_index < len(grants):
            next_page_ref = str(end_index)

        return {
            "grants": copy.deepcopy(page_grants),
            "next_page_ref": next_page_ref
        }
    

    async def get_grant_page_refs_page(
        self,
        effect: str | None, 
        action: str | None, 
        page_ref: str | None, 
        grants_page_size: int,
        refs_page_size: int
    ) -> dict:
        """Get a page of page references for parallel pagination. 

        Parameters
        ----------
        effect : str | None
            Filter by grant effect. None for no filter.
        action : str | None
            Filter by grant action. None for no filter.
        page_ref : str | None
            Page reference of the page to retrieve. None to get the first page.
        grants_page_size : int
            Number of grants per page. Not exact.
        refs_page_size : int
            Number of page reference to return. Not exact.

        Returns
        -------
        dict
            Page of page references with the next page reference.
        """
        # Get the appropriate grant list based on filters
        if effect is not None and action is not None:
            # Include grants that match the specific action AND grants that match any action (empty actions)
            grants = self._grant_both_filter[effect][action] + self._grant_both_filter[effect][None]
        elif effect is not None:
            grants = self._grant_effect_filter[effect]
        elif action is not None:
            # Include grants that match the specific action AND grants that match any action (empty actions)
            grants = self._grant_action_filter[action] + self._grant_action_filter[None]
        else:
            grants = list(self._grant_lut.values())

        # lesser of page * grants page size or total length
        total_grants = len(grants)
            
        # Generate page references based on grants_page_size
        page_refs = []
        for i in range(0, total_grants, grants_page_size):
            page_refs.append(str(i))

        # Handle pagination of page references using page_size
        start_index = 0
        if page_ref is not None:
            try:
                start_index = int(page_ref)
            except ValueError:
                start_index = 0

        end_index = start_index + refs_page_size
        page_ref_page = page_refs[start_index:end_index]
        
        # Determine next page reference
        next_page_ref = None
        if end_index < len(page_refs):
            next_page_ref = str(end_index)

        return {
            "page_refs": page_ref_page,
            "next_page_ref": next_page_ref
        }
    
    
    async def create_latch(self) -> dict:
        """Create a new shared latch in the storage module.

        Returns
        -------
        dict
            New storage latch.
        """
        new_latch = {
            "storage_latch_uuid": uuid4(),
            "set": False,
            "created_at": datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
        }
        self._latch_lut[new_latch['storage_latch_uuid']] = new_latch

        return copy.deepcopy(new_latch)


    async def get_latch(self, storage_latch_uuid: UUID) -> dict:
        """Retrieve latch by UUID.

        Parameters
        ----------
        storage_latch_uuid : UUID
            Storage latch UUID.

        Returns
        -------
        dict
            The storage latch with the given UUID.
        
        Raises
        ------
        authzee.exceptions.LatchNotFoundError
            The latch with the given UUID could not be found.
        """
        if storage_latch_uuid not in self._latch_lut:
            raise exceptions.LatchNotFoundError(storage_latch_uuid=storage_latch_uuid)
        
        return copy.deepcopy(self._latch_lut[storage_latch_uuid])


    async def set_latch(self, storage_latch_uuid: UUID) -> dict:
        """Set a latch for a given UUID. 

        Parameters
        ----------
        storage_latch_uuid : UUID
            Storage latch UUID.

        Returns
        -------
        dict
            The storage latch with the given UUID and the latch set.
        
        Raises
        ------
        authzee.exceptions.LatchNotFoundError
            The latch with the given UUID could not be found.
        """
        if storage_latch_uuid not in self._latch_lut:
            raise exceptions.LatchNotFoundError(storage_latch_uuid=storage_latch_uuid)
        
        self._latch_lut[storage_latch_uuid]["set"] = True
        return copy.deepcopy(self._latch_lut[storage_latch_uuid])


    async def delete_latch(self, storage_latch_uuid: UUID) -> None:
        """Delete a storage latch by UUID.

        Parameters
        ----------
        storage_latch_uuid : UUID
            Storage latch UUID.
        
        Raises
        ------
        authzee.exceptions.LatchNotFoundError
            The latch with the given UUID could not be found.
        """
        if storage_latch_uuid not in self._latch_lut:
            raise exceptions.LatchNotFoundError(storage_latch_uuid=storage_latch_uuid)
        
        del self._latch_lut[storage_latch_uuid]


    async def cleanup_latches(self, before: datetime.datetime) -> None:
        """Delete zombie storage latches created before a certain point in time.

        Parameters
        ----------
        before : datetime.datetime
            Delete latches created before this datetime.
        """
        latches_to_delete = []
        for latch_uuid, latch in self._latch_lut.items():
            created_at = datetime.datetime.fromisoformat(latch["created_at"])
            if created_at < before:
                latches_to_delete.append(latch_uuid)
        
        for latch_uuid in latches_to_delete:
            del self._latch_lut[latch_uuid]
