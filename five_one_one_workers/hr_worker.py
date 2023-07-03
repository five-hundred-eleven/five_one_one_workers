import asyncio
import logging
import traceback

from .base import AbstractWorker
from .constants import (
    HR,
    REGISTER_WORKER,
    DELETE_WORKER,
)
from .message import Message

_HR_WORKER = None


class HRWorker(AbstractWorker):

    def __init__(self, **kwargs):
        """
        HR Worker is a special case which manages names and ids.
        """
        super().__init__(name=HR, **kwargs)

        self._hr_semaphore = asyncio.Semaphore(value=1)
        self._worker_id_counter = 0
        self._class_to_count = {}
        self._worker_id_to_manager_id = {}
        self._manager_id_to_worker_ids = {}
        self._worker_name_to_id = {}

        self.s_register_worker(self)

        self.s_add_handled_callback(
            REGISTER_WORKER,
            self._handle_register_worker_callback,
        )
        self.s_add_handled_callback(
            DELETE_WORKER,
            self._handle_delete_worker_callback,
        )

    @staticmethod
    def get_hr_worker(logger: logging.Logger = None):
        """
        This method should always be used to get a reference to the HR worker.
        """
        global _HR_WORKER
        if _HR_WORKER is None:
            _HR_WORKER = HRWorker(logger=logger)
        return _HR_WORKER

    async def register_worker(self, worker) -> bool:
        """
        Intakes worker `worker` and sets id and name, if needed.

        Args:
            worker (AbstractWorker): worker to intake.

        Returns:
            bool: whether the operation was successful.
        """
        await self._hr_semaphore.acquire()
        res = self.s_register_worker(worker)
        self._hr_semaphore.release()
        return res

    def s_register_worker(self, worker) -> bool:
        """
        Intakes worker `worker` and sets id and name, if needed.

        This is the synchronous version of this function and should not be
        directly called after the worker has started.

        Args:
            worker (AbstractWorker): worker to intake.

        Returns:
            bool: whether the operation was successful.
        """
        if worker.name and worker.name in self._worker_name_to_id:
            return True

        worker_id = self.s_new_worker_id()
        worker.set_id(worker_id)

        try:
            # worker dict
            worker_dict = worker.asdict()
            # get stuff for setting worker name
            classname = worker_dict["class"]
            classcount = self.s_increment_classcount(classname)
            # set worker name
            if not worker.name:
                worker.set_name(f"{classname}-{classcount}")

            # set remainder of attributes
            manager_id = self._worker_name_to_id.get(worker.manager, None)
            if manager_id is not None:
                self._worker_id_to_manager_id[worker.id] = manager_id
                if manager_id not in self._manager_id_to_worker_ids:
                    self._manager_id_to_worker_ids[manager_id] = []
                self._manager_id_to_worker_ids[manager_id].append(worker.id)

            self._worker_name_to_id[worker.name] = worker.id

        except Exception:
            if self._logger is not None:
                self._logger.debug(
                    "%s got exception registering worker: %s",
                    self,
                    traceback.format_exc(),
                )
            return False

        return True

    async def delete_worker(self, name: str) -> bool:
        """
        Deletes the worker given by "name" from the records.
        """
        await self._hr_semaphore.acquire()
        res = self.s_delete_worker(name)
        self._hr_semaphore.release()
        return res

    def s_delete_worker(self, name: str) -> bool:
        """
        Deletes the worker given by "name" from the records.

        This is a synchronous function and it should not be directly called
        after the worker has started.
        """
        try:
            worker_id = self._worker_name_to_id[name]
            manager_id = self._worker_id_to_manager_id[worker_id]
        except KeyError:
            if self._logger is not None:
                self._logger.debug(
                    "%s tried to delete worker that was not found.",
                    self,
                )
            return False
        try:
            del self._worker_id_to_manager_id[worker_id]
            del self._manager_id_to_worker_ids[manager_id]
            del self._worker_name_to_id[name]
        except KeyError:
            if self._logger is not None:
                self._logger.debug(
                    "%s tried to delete worker that was not found.",
                    self,
                )
            return False
        return True

    def s_new_worker_id(self) -> int:
        """
        Acquires the semaphore, captures the worker id counter, increments the
        counter, and returns the id.

        This is a synchronous function and it should not be directly called
        after the worker has started.

        Returns:
            int: the worker id.
        """
        worker_id = self._worker_id_counter
        self._worker_id_counter += 1
        return worker_id

    def s_increment_classcount(self, classname: str) -> int:
        """
        Acquires the semaphore, and increments the count of the class given
        by `classname`.

        This is a synchronous function and it should not be directly called
        after the worker has started.

        Args:
            classname: name of class.

        Returns:
            int: the new number of the class.
        """
        classcount = self._class_to_count.get(classname, 0) + 1
        self._class_to_count[classname] = classcount
        return classcount

    async def _handle_register_worker_callback(self, message: Message) -> None:
        """
        Convenience method for handling messages addressed to hr.

        Args:
            message: message to be processed
        """
        try:
            worker = message.body["worker"]
        except KeyError:
            if self._logger:
                self._logger.debug(
                    "%s got register worker request with no worker",
                    self,
                )
            return
        await self.register_worker(worker)

    async def _handle_delete_worker_callback(self, message: Message) -> None:
        """
        Convenience method for handling messages addressed to hr.

        Args:
            message: message to be processed
        """
        try:
            name = message.body["name"]
        except KeyError:
            if self._logger:
                self._logger.debug(
                    "%s got delete worker request with no worker name",
                    self,
                )
            return
        await self.delete_worker(name)
