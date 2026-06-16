__all__ = [
    "SharedMemLatch"
]

from multiprocessing.managers import SharedMemoryManager


class SharedMemLatch:
    """ Shared Memory Latch linked to a ``SharedMemoryManager``.

    Must call ``unlink()`` to free the memory. 

    Parameters
    ----------
    smm : SharedMemoryManager
        Shared memory manager to create.
    """


    def __init__(self, smm: SharedMemoryManager):
        self._sm = smm.SharedMemory(size=1)
    

    def is_set(self) -> bool:
        return self._sm.buf[0] == 1
    

    def set(self) -> None:
        self._sm.buf[0] = 1
    

    def unlink(self) -> None:
        self._sm.unlink()