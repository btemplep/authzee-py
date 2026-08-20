"""Unit tests for authzee.compute.shared_mem_latch module."""

from multiprocessing.managers import SharedMemoryManager

from authzee.compute.shared_mem_latch import SharedMemLatch


def test_shared_mem_latch_initial_state():
    smm = SharedMemoryManager()
    smm.start()
    try:
        latch = SharedMemLatch(smm)
        assert latch.is_set() is False
    finally:
        latch.unlink()
        smm.shutdown()


def test_shared_mem_latch_set():
    smm = SharedMemoryManager()
    smm.start()
    try:
        latch = SharedMemLatch(smm)
        latch.set()
        assert latch.is_set() is True
    finally:
        latch.unlink()
        smm.shutdown()


def test_shared_mem_latch_unlink():
    smm = SharedMemoryManager()
    smm.start()
    try:
        latch = SharedMemLatch(smm)
        latch.set()
        latch.unlink()
    finally:
        smm.shutdown()
