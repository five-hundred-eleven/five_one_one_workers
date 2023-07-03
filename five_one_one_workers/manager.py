import asyncio
import traceback
from typing import Union

from .base import AbstractWorker
from .constants import (
    NOTIFY_RESIGN,
    NOTIFY_ADD_INTEREST,
    NOTIFY_REMOVE_INTEREST,
)
from .hr_worker import HRWorker
from .message import Message
from .postal_worker import PostalWorker


class Manager(AbstractWorker):

    def __init__(self, **kwargs):
        """
        Manager workers are a special case of worker that contains references
        to other workers (or managers) and can delegate messages to them.
        """
        super().__init__(**kwargs)

        # init hr, this does nothing if hr is already initialized
        self._hr_worker = HRWorker.get_hr_worker(logger=self._logger)
        # init postal, this does nothing if post is already initialized
        self._postal_worker = PostalWorker.get_postal_worker(
            logger=self._logger,
        )

        self._hr_worker.s_register_worker(self)
        self._hr_worker.s_register_worker(self._postal_worker)

        self._postal_worker.s_register_worker(self)
        self._postal_worker.s_register_worker(self._hr_worker)

        self._worker_id_to_worker = {}
        self._worker_name_to_worker = {}
        self._interest_to_workers = {}
        self._manager_semaphore = asyncio.Semaphore(value=1)

        # subjects to be handled by the manager
        self.s_add_handled_callback(
            NOTIFY_RESIGN,
            self._notify_add_interest_callback,
        )
        self.s_add_handled_callback(
            NOTIFY_ADD_INTEREST,
            self._notify_add_interest_callback,
        )
        self.s_add_handled_callback(
            NOTIFY_REMOVE_INTEREST,
            self._notify_remove_interest_callback,
        )

    async def add_worker(self, worker) -> bool:
        """
        Adds the worker to the manager's pool.

        If `start` is called after this method, the manager will automatically
        ensure `worker` is running.

        If `start` is called before this method, `worker.start()` will have to
        be called manually.

        Args:
            worker: worker to be added.
        """
        await self._manager_semaphore.acquire()
        res = self.s_add_worker(worker)
        self._manager_semaphore.release()
        return res

    def s_add_worker(self, worker) -> bool:
        """
        Adds the worker to the manager's pool.

        This is the synchronous version of the method and it should not be
        directly called after the worker has started.

        Args:
            worker: worker to be added.
        """
        # set the worker's manager to self
        worker.set_manager(self._name)

        # set the worker's logger
        if worker.logger is None and self._logger is not None:
            worker.set_logger(self._logger)

        # register the worker
        self._hr_worker.s_register_worker(worker)
        self._postal_worker.s_register_worker(worker)

        try:
            self._worker_id_to_worker[worker.id] = worker
            self._worker_name_to_worker[worker.name] = worker
            for interest in worker.interests:
                if interest not in self._interest_to_workers:
                    self._interest_to_workers[interest] = []
                self._interest_to_workers[interest].append(worker)
                self.add_interest(interest)

        except Exception:
            if self._logger is not None:
                self._logger.debug(
                    "%s encountered error adding worker: %s",
                    self,
                    traceback.format_exc(),
                )
            return False

        return True

    def start(self) -> None:
        """
        Convenience method for starting the manager and all subordinates.
        """
        if self._main_task is None:
            # make sure post and hr are running
            self._postal_worker.start()
            self._hr_worker.start()
            # start self
            self._main_task = self._loop.create_task(self._main_loop())
            # start subordinates
            for worker in self._worker_id_to_worker.values():
                worker.start()
        return self._main_task

    async def delegate(self, message: Message) -> bool:
        """
        Basic delegate method, to determine which subordinate `message` should
        be sent to.
        Workers are considered if they have an interest matching the subject.
        If no workers or more than one worker is interested, the manager will
        check which worker has the smallest backlog.
        """
        if isinstance(message.subject, str):
            interested_workers = self._interest_to_workers.get(
                message.subject,
                [],
            )
        else:
            interested_workers = []

        if len(interested_workers) == 0:
            interested_workers = list(self._worker_id_to_worker.values())

        # careful not to give it back to sender
        interested_workers = [
            worker
            for worker in interested_workers
            if worker.id != message.sender and worker.name != message.sender
        ]

        if len(interested_workers) == 0:
            if self._logger is not None:
                self._logger.debug(
                    "%s was not able to find a worker to delegate message "
                    "to: %s",
                    self,
                    message,
                )
            return False

        if len(interested_workers) > 1:
            interested_workers.sort(key=lambda w: w.get_backlog_size())

        await self.send_message(
            priority=message.priority,
            to=interested_workers[0].name,
            subject=message.subject,
            body=message.body,
        )
        return True

    async def handle_message(self, message: Message):
        """
        Handle message callback.
        """
        await self.delegate(message)

    async def remove_interest(self, interest: str):
        """
        [experimental after `start` has been called] remove `interest` from the
        worker's interests and notify manager.
        """
        await super().remove_interest(interest)
        if not self._interest_to_workers[interest]:
            await self._manager_semaphore.acquire()
            del self._interest_to_workers[interest]
            self._manager_semaphore.release()

    async def _notify_retire_callback(self, message: Message) -> None:
        await self._manager_semaphore.acquire()
        try:
            worker = self._lookup(message.sender)
            del self._worker_id_to_worker[worker.id]
            del self._worker_name_to_worker[worker.name]
            for interest in worker.interests:
                self._interest_to_workers[interest].remove(worker)
        except Exception:
            if self._logger is not None:
                self._logger.debug(
                    "%s encountered exception in retire callback: %s",
                    self,
                    traceback.format_exc(),
                )
            return
        finally:
            self._manager_semaphore.release()

        for interest in list(self._interest_to_workers.keys()):
            if not self._interest_to_keys[interest]:
                await self.remove_interest(interest)

    async def _notify_add_interest_callback(self, message: Message) -> None:
        await self._manager_semaphore.acquire()
        try:
            worker = self._lookup(message.sender)
            interest = message.body["interest"]
            await self.add_interest(interest)
            if interest not in self._interest_to_workers:
                self._interest_to_workers[interest] = []
            self._interest_to_workers[interest].append(worker)
        except Exception:
            if self._logger is not None:
                self._logger.debug(
                    "%s encountered exception in add interest callback: %s",
                    self,
                    traceback.format_exc(),
                )
            return
        finally:
            self._manager_semaphore.release()

    async def _notify_remove_interest_callback(self, message: Message) -> None:
        await self._manager_semaphore.acquire()
        try:
            worker = self._lookup(message.sender)
            interest = message.body["interest"]
            self._interest_to_workers[interest].remove(worker)
            self.remove_interest(interest)
        except Exception:
            if self._logger is not None:
                self._logger.debug(
                    "%s encountered exception in remove interest callback: %s",
                    self,
                    traceback.format_exc(),
                )
            return
        finally:
            self._manager_semaphore.release()

    def _lookup(self, name: Union[str, int]) -> AbstractWorker:
        """
        Convenience method for looking up a worker.
        """
        if name is None:
            return None
        if isinstance(name, int):
            return self._worker_id_to_worker.get(name, None)
        if isinstance(name, str):
            return self._worker_name_to_worker.get(name, None)
        return None
