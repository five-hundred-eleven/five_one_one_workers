import asyncio
import bisect

from .message import Message

class PriorityBucketQueue(object):

    def __init__(self):
        """
        This class is designed to have the same interface as an
        asyncio.PriorityQueue EXCEPT that priority is determined only from the
        from the first item (assuming the value is a tuple). Values that have
        the same priority based on the first item are handled on a FIFO basis.

        This approach is optimized for the PostalWorker's inbox and it is not
        recommended to use it for other purposes.
        """
        self._buckets = []
        self._priorities = []
        self._sem = asyncio.Semaphore(value=1)

    def qsize(self):
        """
        Gets the approximate size of the queue.
        """
        return sum(q.qsize() for q in self._buckets)

    def empty(self):
        """
        Returns True if there are no items in the queue and False otherwise.
        """
        return all(q.empty() for q in self._buckets)

    async def get(self) -> Message:
        """
        Gets the front of the queue.

        The "front" is the value whose first item (priority) is numerically
        lowest, or the item that was `put` first if there is a priority tie.
        """
        while True:
            await self._sem.acquire()
            try:
                for q in self._buckets:
                    if not q.empty():
                        return await q.get()
            except Exception:
                raise
            finally:
                self._sem.release()
            # bad situation if we get here... just sleep and reiterate
            await asyncio.sleep(1)
    
    async def put(self, value: Message) -> None:
        """
        Puts the value in the queue.

        The value's place in the queue is determined by the first item
        (priority), where numerically lower values will be ahead, or
        if there is a priority tie it will be behind tied values.
        """
        await self._sem.acquire()
        try:
            priority = value[0]
            ix = bisect.bisect_left(self._priorities, priority)
            if ix >= len(self._priorities):
                self._priorities.append(priority)
                self._buckets.append(asyncio.Queue())
            elif self._priorities[ix] != priority:
                self._priorities.insert(ix, priority)
                self._buckets.insert(ix, asyncio.Queue())
            await self._buckets[ix].put(value)
        except Exception:
            raise
        finally:
            self._sem.release()
