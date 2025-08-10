
from enum import Enum


class ModuleLocality(Enum):
    """Describes the the scope or "locality" of where compute or storage modules exist and communicate among Authzee apps.

    - ``PROCESS`` 
        - Compute runs in same process as the Authzee app.
        - Storage is limited to the same process as the Authzee app.
    - ``SYSTEM`` 
        - Compute resources are on the same system as the Authzee app.
        - Storage is limited to the system running the Authzee app.
    - ``NETWORK`` 
        - Compute resources are communicated to over the network.  They are external to the system running the Authzee app.
        - Storage is reachable over the network. It is (or can be) external to the system running the Authzee app.
    
    The purpose of this enum is to help identify incompatibilities in compute and storage modules for authzee. 
    See the ``authzee.locality_compatibility`` dictionary for the compatibility matrix. 
    """
    PROCESS: str = "PROCESS"
    SYSTEM: str = "SYSTEM"
    NETWORK: str = "NETWORK"
    

locality_compatibility = {
    ModuleLocality.PROCESS: {
        ModuleLocality.PROCESS,
        ModuleLocality.SYSTEM,
        ModuleLocality.NETWORK
    },
    ModuleLocality.SYSTEM: {
        ModuleLocality.SYSTEM,
        ModuleLocality.NETWORK
    },
    ModuleLocality.NETWORK: {
        ModuleLocality.NETWORK
    }
}
"""Map of Compatibility from compute or authzee app to storage localities."""
